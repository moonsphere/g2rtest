use mem_ring::{Guard, Queue, QueueMeta, WriteQueue};
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::sync::OnceLock;
use std::time::Duration;
use tokio::runtime::Runtime;
use tokio::sync::oneshot;
use tokio::time::sleep;

const DEFAULT_QUEUE_CAPACITY: usize = 1024;
const SLOT_PAYLOAD_SIZE: usize = 2048;

static RUNTIME: Lazy<Runtime> = Lazy::new(|| {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("failed to build Tokio runtime")
});

static CHANNELS: OnceLock<Result<Channels, std::io::Error>> = OnceLock::new();

struct Channels {
    _request_guard: Guard,
    _response_writer: WriteQueue<MessageSlot>,
    request_meta: QueueMeta,
    response_meta: QueueMeta,
}

#[repr(C)]
#[derive(Clone, Copy)]
struct MessageSlot {
    token: u64,
    len: u32,
    _reserved: u32,
    buf: [u8; SLOT_PAYLOAD_SIZE],
}

impl MessageSlot {
    fn from_bytes(token: u64, bytes: &[u8]) -> Result<Self, SlotError> {
        if bytes.len() > SLOT_PAYLOAD_SIZE {
            return Err(SlotError::TooLarge(bytes.len()));
        }

        let mut buf = [0u8; SLOT_PAYLOAD_SIZE];
        buf[..bytes.len()].copy_from_slice(bytes);

        Ok(Self {
            token,
            len: bytes.len() as u32,
            _reserved: 0,
            buf,
        })
    }

    fn token(&self) -> u64 {
        self.token
    }

    fn into_bytes(self) -> Vec<u8> {
        let len = usize::min(self.len as usize, SLOT_PAYLOAD_SIZE);
        self.buf[..len].to_vec()
    }
}

#[derive(Debug)]
enum SlotError {
    TooLarge(usize),
}

impl fmt::Display for SlotError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SlotError::TooLarge(len) => write!(
                f,
                "payload of {len} bytes exceeds slot capacity {SLOT_PAYLOAD_SIZE}"
            ),
        }
    }
}

#[derive(Deserialize)]
struct Request {
    number: i32,
    message: String,
    #[serde(default)]
    tags: Vec<String>,
}

#[derive(Serialize)]
struct Response {
    code: i32,
    message: String,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct G2RQueueMeta {
    pub buffer_ptr: usize,
    pub buffer_len: usize,
    pub head_ptr: usize,
    pub tail_ptr: usize,
    pub working_ptr: usize,
    pub stuck_ptr: usize,
    pub working_fd: i32,
    pub unstuck_fd: i32,
}

impl From<QueueMeta> for G2RQueueMeta {
    fn from(meta: QueueMeta) -> Self {
        Self {
            buffer_ptr: meta.buffer_ptr,
            buffer_len: meta.buffer_len,
            head_ptr: meta.head_ptr,
            tail_ptr: meta.tail_ptr,
            working_ptr: meta.working_ptr,
            stuck_ptr: meta.stuck_ptr,
            working_fd: meta.working_fd,
            unstuck_fd: meta.unstuck_fd,
        }
    }
}

#[repr(C)]
pub struct G2RInitResult {
    pub request_queue: G2RQueueMeta,
    pub response_queue: G2RQueueMeta,
    pub slot_capacity: u32,
}

#[no_mangle]
pub extern "C" fn g2r_init(queue_capacity: u32, out: *mut G2RInitResult) -> i32 {
    if out.is_null() {
        return -1;
    }

    let capacity = if queue_capacity == 0 {
        DEFAULT_QUEUE_CAPACITY
    } else {
        queue_capacity as usize
    };

    let channels_result = CHANNELS.get_or_init(|| init_channels(capacity));
    if let Err(err) = channels_result {
        eprintln!("g2r_init: failed to initialize channels: {err}");
        return -1;
    }
    let channels = channels_result.as_ref().unwrap();

    unsafe {
        *out = G2RInitResult {
            request_queue: channels.request_meta.into(),
            response_queue: channels.response_meta.into(),
            slot_capacity: SLOT_PAYLOAD_SIZE as u32,
        };
    }

    0
}

fn init_channels(capacity: usize) -> Result<Channels, std::io::Error> {
    let (tx, rx) = oneshot::channel();
    RUNTIME.spawn(async move {
        let res = init_channels_async(capacity).await;
        let _ = tx.send(res);
    });

    match RUNTIME.block_on(async { rx.await }) {
        Ok(Ok(channels)) => Ok(channels),
        Ok(Err(err)) => Err(err),
        Err(_canceled) => Err(std::io::Error::new(
            std::io::ErrorKind::Other,
            "init task canceled",
        )),
    }
}

async fn init_channels_async(capacity: usize) -> Result<Channels, std::io::Error> {
    let (request_queue, request_meta) = Queue::<MessageSlot>::new(capacity)?;
    let (response_queue, response_meta) = Queue::<MessageSlot>::new(capacity)?;

    let handle = tokio::runtime::Handle::current();
    let read_queue = request_queue.read_with_tokio_handle(handle.clone());
    let write_queue = response_queue.write_with_tokio_handle(&handle)?;

    let handler_writer = write_queue.clone();
    let guard = read_queue.run_handler(move |slot| {
        let writer_for_task = handler_writer.clone();
        RUNTIME.spawn(async move {
            handle_request_slot(slot, writer_for_task).await;
        });
    })?;

    Ok(Channels {
        _request_guard: guard,
        _response_writer: write_queue,
        request_meta,
        response_meta,
    })
}

async fn handle_request_slot(slot: MessageSlot, writer: WriteQueue<MessageSlot>) {
    let token = slot.token();
    let payload = slot.into_bytes();

    match serde_json::from_slice::<Request>(&payload) {
        Ok(request) => {
            let response = process_request(request).await;
            send_response(writer, token, response).await;
        }
        Err(err) => {
            send_error(
                writer,
                token,
                format!("failed to decode request JSON: {err}"),
            )
            .await;
        }
    }
}

async fn send_error(writer: WriteQueue<MessageSlot>, token: u64, message: String) {
    let response = Response { code: -1, message };
    send_response(writer, token, response).await;
}

async fn send_response(writer: WriteQueue<MessageSlot>, token: u64, response: Response) {
    match serde_json::to_vec(&response) {
        Ok(bytes) => match MessageSlot::from_bytes(token, &bytes) {
            Ok(slot) => push_slot(writer, slot),
            Err(err) => {
                let fallback = Response {
                    code: -1,
                    message: format!("internal error: failed to encode response into slot ({err})"),
                };
                let fallback_bytes = fallback_message_bytes(&fallback);
                match MessageSlot::from_bytes(token, &fallback_bytes) {
                    Ok(slot) => push_slot(writer, slot),
                    Err(inner_err) => {
                        eprintln!("g2r: failed to enqueue fallback response: {inner_err}");
                    }
                }
            }
        },
        Err(err) => {
            let fallback = Response {
                code: -1,
                message: format!("internal error: failed to serialize response: {err}"),
            };
            let fallback_bytes = fallback_message_bytes(&fallback);
            match MessageSlot::from_bytes(token, &fallback_bytes) {
                Ok(slot) => push_slot(writer, slot),
                Err(inner_err) => {
                    eprintln!("g2r: failed to enqueue serialization error response: {inner_err}");
                }
            }
        }
    }
}

fn fallback_message_bytes(response: &Response) -> Vec<u8> {
    serde_json::to_vec(response).unwrap_or_else(|_| {
        let minimal = Response {
            code: -1,
            message: "unrecoverable serialization error".to_string(),
        };
        serde_json::to_vec(&minimal).expect("minimal serialization should succeed")
    })
}

fn push_slot(writer: WriteQueue<MessageSlot>, slot: MessageSlot) {
    let _ = writer.push(slot);
}

async fn process_request(req: Request) -> Response {
    let delay = Duration::from_millis((req.number.unsigned_abs() as u64 % 500) + 100);
    sleep(delay).await;

    let tag_summary = if req.tags.is_empty() {
        String::from("<no tags>")
    } else {
        req.tags.join(",")
    };

    let detail = format!(
        "processed: {} | msg: {} | tags: {}",
        req.number, req.message, tag_summary
    );

    Response {
        code: req.number * 2,
        message: detail,
    }
}
