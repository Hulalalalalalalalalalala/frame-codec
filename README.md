# frame-codec

Binary frame codec for a length-prefixed wire format, with a checksum, a version byte and precise errors for truncated or damaged input.

## Requirements

Go 1.22 or newer. Standard library only.

## Build

    go build ./...
    go run ./cmd/framedecode --file <path>

## Public interface

`codec.Encode(frame Frame) ([]byte, error)` returns the framed bytes.
- `codec.Decode(data []byte) (Frame, error)` returns the decoded frame.
- `type Frame struct { Version uint8; Flags uint16; Kind uint8; Payload []byte }`.
- `codec.HeaderSize`, `codec.MaxPayload` constants.
- `codec.ErrShortFrame`, `codec.ErrBadVersion`, `codec.ErrChecksum`, `codec.ErrTooLarge` error values.

## Multi-frame sessions

`codec.NewSession(w io.Writer, r io.Reader) *Session` layers a multi-frame
conversation over a byte sink and source. The stream is the plain
concatenation of `Encode` outputs, so it stays byte-for-byte compatible
with the single-frame format.

- `WriteFrame(frame Frame) error` queues an encoded frame; a payload over
  `MaxPayload` fails with `ErrTooLarge` and queues nothing.
- `Flush() error` writes all queued frames to `w` in write order (and
  flushes `w` itself if it has a `Flush() error` method).
- `ReadFrame() (Frame, error)` incrementally parses the next frame from
  arbitrarily chunked input. A corrupt frame returns its `Decode` error
  and ends the stream; a truncated tail at end of input returns
  `ErrShortFrame`; a clean frame-boundary end returns `io.EOF`.

A `Session` is safe for concurrent writers and readers.

## Tests

    go test ./...

## Limits

Single frames are held in memory; the session read side buffers one
frame's worth of bytes at a time.
No compression and no encryption.
Standard library only.
