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

`codec.NewSession(w io.Writer, r io.Reader) *Session` wraps one byte
writer and one byte reader in a concurrency-safe session. Frames are
just concatenated single-frame encodings, so the wire format and the
single-frame entries are unchanged.

- `(*Session).WriteFrame(Frame) error` appends one whole encoded frame
  in call-arrival order; an oversized payload returns `ErrTooLarge`
  before any byte is buffered.
- `(*Session).Flush() error` writes everything buffered so far to the
  underlying writer (and flushes it when it implements `Flush` itself).
- `(*Session).ReadFrame() (Frame, error)` parses incrementally: complete
  frames are returned one per call while a trailing half frame stays
  buffered for later bytes. A clean end of input returns `io.EOF`; a
  truncated trailing frame returns `ErrShortFrame`; a corrupt frame
  returns the same value `Decode` would return for it and stops further
  delivery, with previously returned frames already in the caller's
  hands.

Reads and writes are independently serialized, so one session may be
shared by concurrent writers and readers; the bytes of a single frame
are never interleaved.


## Tests

    go test ./...

## Limits

Single frame in memory; no streaming reader in this scope.
No compression and no encryption.
Standard library only.
