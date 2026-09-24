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

## Reliable transfer layer

`codec.NewReliableSession(sess *Session, window int, checkpointPath, logPath string) (*ReliableSession, error)`
wraps an existing session in sequence numbers, cumulative
acknowledgements and a sliding window, with optional crash-restart
continuation. The underlying single-frame encoding is unchanged: the
new headers live in this layer's own payload convention, carried by
ordinary underlying frames (`Kind` `0xF0` = DATA with
`seq|flags|kind|length|payload`, `Kind` `0xF1` = ACK with one
cumulative `ackSeq`).

- `(*ReliableSession).WriteFrame(Frame) error` numbers the frame,
  appends it durably to the replay log (when persistent) before it can
  reach the wire, and keeps it pending until a cumulative ACK covers
  it. A write while `[base, base+window)` is full, or whose inner
  payload would exceed the unchanged outer `MaxPayload`, returns
  `ErrTooLarge` and drops that frame without taking a sequence slot.
- `(*ReliableSession).Flush() error` pushes buffered DATA frames;
  `(*ReliableSession).Retransmit() error` resends every unacked frame
  with its original sequence number.
- `(*ReliableSession).ReadFrame() (Frame, error)` delivers frames in
  write order exactly once. Duplicates are re-ACKed but not delivered,
  in-window out-of-order frames are parked until the gap closes, and a
  pre-wrap residue or a declared sequence at/beyond
  `deliverNext+window` (or any malformed inner frame) fails with
  `ErrChecksum`; the error is sticky and delivery stops at the last
  frame already handed to the caller. `io.EOF` after queued frames are
  drained behaves as on `Session`.

### Crash recovery

When checkpoint and log paths are given (both or neither), every
accepted write is appended to the replay log with its own checksum
before going on the wire, and every ACK advance first atomically
publishes a checkpoint record (temp file + `rename`, so a half-written
checkpoint is recognized and the previous complete one is used) and
only then compacts the log. On restart the sender resumes from the
earliest position still held by checkpoint/log — already-acked frames
are never resent, a redelivered unacked range is replayed once and
deduplicated, and a checkpoint/log disagreement never skips or repeats
a frame. A truncated or bit-flipped log or committed checkpoint fails
recovery with `ErrChecksum` instead of continuing.

## Tests

    go test ./...

## Limits

Single frame in memory; no streaming reader in this scope.
No compression and no encryption.
Standard library only.
