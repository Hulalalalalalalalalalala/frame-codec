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

## Reliable sessions

`codec.NewReliable(sess *Session, cfg ReliableConfig) (*ReliableSession, error)`
wraps an existing session in a reliability layer. The lower wire format
is not changed: every reliable message is the payload of one ordinary
codec frame, prefixed by this layer's own envelope, so `Encode`/`Decode`,
the header constants and the four established error values keep their
exact byte-level semantics.

- `ReliableConfig{Window int; StateDir string}`: send/receive window
  width in frames and a directory for write-side state (checkpoint and
  replay log); it is created if missing.
- `ReliableFrame{Flags uint16; Kind uint8; Payload []byte}` is the
  caller-visible frame. `EnvelopeSize` (16) bytes are reserved inside
  the underlying frame, so the inner payload limit is
  `ReliableMaxPayload = MaxPayload - EnvelopeSize`.
- `(*ReliableSession).WriteFrame(ReliableFrame) error` numbers frames in
  write order, journals each one, and parks it in the send window;
  nothing goes out before `Flush`. When the window is full it returns
  the established over-limit value `ErrWindowFull` (the same sentinel as
  `ErrTooLarge`) and drops the frame without consuming a sequence. An
  oversized inner payload returns `ErrTooLarge`.
- `(*ReliableSession).Flush() error` sends all windowed frames in seq
  order, each piggybacking a cumulative "next expected" acknowledgement,
  plus one pure ack; on a restart the window is reloaded from the replay
  log, so `Flush` is also the retransmit operation.
- `(*ReliableSession).ReadFrame() (ReliableFrame, error)` delivers in seq
  order exactly once. Acks advance the send window; duplicates of
  delivered or parked frames are re-acknowledged but never redelivered;
  in-window out-of-order frames are parked and delivered as a contiguous
  run once the gap closes; a declared sequence outside the window's
  reach (an ancient residual older than one window width, or one width
  ahead of the current position) is refused with `ErrChecksum`, stopping
  delivery stickily at the current position. Outer-layer results
  (`io.EOF`, `ErrShortFrame`, outer checksum/version/size errors) pass
  through unchanged.

Standalone acks are written by a small background pump, so a writer
that needs the peer's acks to drain its window must let `ReadFrame` run
(or run it in a dedicated goroutine for one-directional traffic).

### Crash recovery

Written data frames are recorded in a CRC'd replay log (`reliable.log`);
the acknowledged position is recorded in a dual-slot checkpoint
(`reliable.ckp`), writing the alternate slot with a higher generation
each time:

- a checkpoint torn by a crash mid-write fails its own CRC and is
  ignored, falling back to the previous complete record;
- a truncated replay log, or a flipped bit in a complete record, fails
  recovery with `ErrChecksum` rather than delivering unverified frames;
- the checkpoint is made durable before the log prefix is discarded, so
  reconciliation always resumes from the earlier confirmed position: no
  acknowledged frame is resent past the peer's state, no unacknowledged
  frame is skipped, and the receiver deduplicates the overlap.

After recovery the first `Flush` re-sends every frame past the resume
point; backlogs wider than the window drain as acks refill the freed
tail slots from the log.

## Tests

    go test ./...

## Limits

Single frame in memory; no streaming reader in this scope.
No compression and no encryption.
Standard library only.
