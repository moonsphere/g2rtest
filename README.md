# Go ↔ Rust Async Demo

This repository contains a minimal example showing how Go can call into Rust where Rust exposes an async function. The Go side now serialises requests using the standard `protowire` helpers from `google.golang.org/protobuf`, and Rust decodes them with `prost` before running the async work on a Tokio runtime. Responses travel back as protobuf bytes as well, keeping the FFI surface to raw byte buffers.

## Layout

- `rustlib/`: Rust static library exposing the FFI surface.
- `g2r/`: Go wrapper that handles cgo interop and exposes a friendly API.
- `cmd/demo/`: Small Go program demonstrating both blocking and non-blocking calls into Rust.

## Build & Run

```bash
# Build the Rust static library
(cd rustlib && cargo build --release)

# Build or run the Go demo (make sure the Rust build step ran first)
GOCACHE=$(pwd)/.gocache go run ./cmd/demo
```

The demo prints responses from the Rust async function for both a blocking call and a non-blocking call that allows other work to proceed while Rust completes.

To keep the workspace clean you can remove the temporary Go build cache afterwards:

```bash
rm -rf .gocache
```
