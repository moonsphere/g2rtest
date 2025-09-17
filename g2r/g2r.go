package g2r

/*
#cgo CFLAGS: -I${SRCDIR}/../rustlib/include
#cgo LDFLAGS: -L${SRCDIR}/../rustlib/target/release -lg2r -lpthread
#include "g2r.h"
*/
import "C"

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	memring "github.com/ihciah/rust2go/mem-ring"
)

const (
	defaultQueueCapacity = 1024
	slotPayloadSize      = 2048
)

type Request struct {
	Number  int32    `json:"number"`
	Message string   `json:"message"`
	Tags    []string `json:"tags"`
}

type Response struct {
	Code    int32  `json:"code"`
	Message string `json:"message"`
}

type requestPayload struct {
	Number  int32    `json:"number"`
	Message string   `json:"message"`
	Tags    []string `json:"tags"`
}

type ringMessageSlot struct {
	Token uint64
	Len   uint32
	Pad   uint32
	Buf   [slotPayloadSize]byte
}

var (
	initOnce        sync.Once
	initErr         error
	requestQueue    memring.WriteQueue[ringMessageSlot]
	pendingRequests sync.Map
	tokenCounter    uint64
)

func ensureInitialized() error {
	initOnce.Do(func() {
		var result C.G2RInitResult
		rv := C.g2r_init(C.uint32_t(defaultQueueCapacity), &result)
		if rv != 0 {
			initErr = fmt.Errorf("g2r: initialization failed with status %d", int32(rv))
			return
		}

		if uint32(result.slot_capacity) != slotPayloadSize {
			initErr = fmt.Errorf(
				"g2r: slot size mismatch (rust reports %d, go expects %d)",
				uint32(result.slot_capacity),
				slotPayloadSize,
			)
			return
		}

		reqMeta := convertQueueMeta(result.request_queue)
		respMeta := convertQueueMeta(result.response_queue)

		reqQueue := memring.NewQueue[ringMessageSlot](reqMeta)
		requestQueue = reqQueue.Write()

		respQueue := memring.NewQueue[ringMessageSlot](respMeta).Read()
		respQueue.RunHandler(handleResponseSlot)
	})

	return initErr
}

func convertQueueMeta(meta C.G2RQueueMeta) memring.QueueMeta {
	return memring.QueueMeta{
		BufferPtr:  uintptr(meta.buffer_ptr),
		BufferLen:  uintptr(meta.buffer_len),
		HeadPtr:    uintptr(meta.head_ptr),
		TailPtr:    uintptr(meta.tail_ptr),
		WorkingPtr: uintptr(meta.working_ptr),
		StuckPtr:   uintptr(meta.stuck_ptr),
		WorkingFd:  int32(meta.working_fd),
		UnstuckFd:  int32(meta.unstuck_fd),
	}
}

func ProcessAsync(req Request) (<-chan Response, error) {
	if err := ensureInitialized(); err != nil {
		return nil, err
	}

	payloadReq := requestPayload{
		Number:  req.Number,
		Message: req.Message,
		Tags:    append([]string(nil), req.Tags...),
	}
	if payloadReq.Tags == nil {
		payloadReq.Tags = []string{}
	}

	payload, err := json.Marshal(payloadReq)
	if err != nil {
		return nil, fmt.Errorf("g2r: failed to encode request JSON: %w", err)
	}

	tokenID := atomic.AddUint64(&tokenCounter, 1)
	resultCh := make(chan Response, 1)
	pendingRequests.Store(tokenID, resultCh)

	slot, err := newRingMessageSlot(tokenID, payload)
	if err != nil {
		pendingRequests.Delete(tokenID)
		close(resultCh)
		return nil, err
	}

	requestQueue.Push(slot)
	return resultCh, nil
}

func Process(req Request) (Response, error) {
	ch, err := ProcessAsync(req)
	if err != nil {
		return Response{}, err
	}

	resp, ok := <-ch
	if !ok {
		return Response{}, errors.New("g2r: result channel closed unexpectedly")
	}
	return resp, nil
}

func newRingMessageSlot(token uint64, payload []byte) (ringMessageSlot, error) {
	if len(payload) > slotPayloadSize {
		return ringMessageSlot{}, fmt.Errorf(
			"g2r: payload (%d bytes) exceeds slot capacity (%d bytes)",
			len(payload),
			slotPayloadSize,
		)
	}

	var slot ringMessageSlot
	slot.Token = token
	slot.Len = uint32(len(payload))
	copy(slot.Buf[:], payload)
	return slot, nil
}

func (s ringMessageSlot) bytes() []byte {
	limit := int(s.Len)
	if limit > len(s.Buf) {
		limit = len(s.Buf)
	}
	buf := make([]byte, limit)
	copy(buf, s.Buf[:limit])
	return buf
}

func handleResponseSlot(slot ringMessageSlot) {
	token := slot.Token
	payload := slot.bytes()

	var decoded Response
	if err := json.Unmarshal(payload, &decoded); err != nil {
		decoded = Response{
			Code:    -1,
			Message: fmt.Sprintf("g2r: failed to decode response JSON: %v", err),
		}
	}

	if ch, ok := pendingRequests.LoadAndDelete(token); ok {
		resultCh := ch.(chan Response)
		resultCh <- decoded
		close(resultCh)
	} else {
		// No waiting consumer; drop the response.
	}
}
