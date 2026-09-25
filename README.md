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

## Channel multiplexing

`codec.NewMux(rel *ReliableSession, cfg MuxConfig) (*Mux, error)`
carries several independent logical channels over one reliable
session. The lower wire format is not changed: every multiplexed
message is the payload of one reliable data frame, prefixed by this
layer's own 15-byte header
(`chan uint16`, `seq uint32`, `flags uint8`, `ack uint32`, `crc32`),
so the reliable envelope, the codec frame, the header constants and
all established error values keep their exact semantics.

- `MuxConfig{Channels int; Window int; StateDir string}`: the number of
  channels (numbered `0..Channels-1`), the per-channel in-flight quota
  in frames (enforced independently), and a write-side state directory
  (per-channel dual-slot checkpoints, a shared replay journal and
  per-channel close markers), created if missing.
- `ChannelFrame{Channel uint16; Flags uint16; Kind uint8; Payload []byte}`
  is the caller-visible frame. `MuxHeaderSize` (15) bytes are reserved
  inside the reliable payload, so the inner payload limit is
  `MuxMaxPayload = ReliableMaxPayload - MuxHeaderSize`.
- `(*Mux).WriteFrame(ChannelFrame) error` numbers frames per channel in
  write order, journals each one and parks it in the channel's quota;
  nothing goes out before `Flush`. A full per-channel quota returns the
  established over-limit sentinel `ErrWindowFull` (the same value as
  `ErrTooLarge`); an oversized payload returns `ErrTooLarge`. A rejected
  frame is neither queued nor journaled and consumes no sequence, so it
  takes no quota; total buffering is bounded by `Channels*Window`.
  Writing to a closed channel returns the new sentinel
  `ErrChannelClosed`; an out-of-range channel returns `ErrChecksum`.
- `(*Mux).CloseChannel(ch) error` closes one channel's write side: the
  FIN is itself a sequence entry (it takes one quota slot, so a full
  quota gives `ErrWindowFull` and the close is retried after acks); a
  channel that was never written is opened first, so an unused channel
  still delivers a clean EOF. Closing or writing an already closed
  channel returns `ErrChannelClosed`.
- `(*Mux).Flush() error` sends queued frames round-robin across
  channels (each pass serves at most one queued frame per channel,
  starting where the previous Flush stopped), so one busy channel can
  never starve another and a channel's frames never pass its own
  earlier frames. Every frame piggybacks the sender's current
  cumulative receive position for its channel; cumulative
  acknowledgements are also sent by a small background pump. As on the
  reliable layer, a writer that needs the peer's acks to drain quotas
  must let `ReadFrame` run.
- `(*Mux).ReadFrame() (ChannelFrame, error)` delivers across all
  channels: a gap-blocked channel never stalls another channel, while
  within a channel frames arrive in write order exactly once. After a
  channel's remaining frames, the read that reaches its FIN returns the
  `ChannelFrame` naming the channel together with `io.EOF`; other
  channels keep flowing. A channel number outside the session's reach,
  a residual older than one channel-window, a declaration more than one
  window ahead, an acknowledgement past the send frontier, a
  same-sequence retransmission with different content, an unknown flag,
  an ack carrying payload, or a damaged header is refused with
  `ErrChecksum`, stopping stickily at the current delivery positions.
  Reliable-layer results (`io.EOF`, `ErrShortFrame`, outer
  checksum/version/size and reader errors) pass through unchanged; when
  a terminal lower-layer read also brings complete frames, the buffered
  frames are delivered first.

Restart uses the same durable rules as the reliable layer: written data
frames are CRC-recorded in a shared journal (`mux.log`), acknowledged
per-channel positions in dual-slot checkpoints (`muxNNN.ckp`), and a
close marker (`muxNNN.closed`) survives a FIN whose journal prefix was
discarded. Recovery resumes each channel at the earlier of the
checkpoint and journal positions, so no unacknowledged frame is skipped
and the receiver deduplicates the overlap; a truncated journal or a
flipped bit in a complete record fails recovery with `ErrChecksum`. If
the close marker cannot be written, the close still takes effect: the
FIN is already journaled, so the in-memory state and the journal stay
consistent and a later restart recovers the same closed channel.

## Fragmentation and reassembly

`codec.NewFrag(m *Mux, cfg FragConfig) (*FragSession, error)` carries
whole payloads larger than the single-frame limit over one multiplexed
session: the caller writes each complete payload once and this layer
cuts it into ordered fragments that ride ordinary mux frames, so the
mux header, the reliable envelope, the codec frame, the header
constants and all established error values keep their exact semantics.
The fragment header lives only inside the mux payload
(`msg uint32`, `idx uint32`, `total uint32`, `flags uint8`,
`ack uint32`, `crc32`); no new error values are introduced.

- `FragConfig{MaxMessage int; StateDir string}`: the per-channel
  reassembly cap in bytes (no message may declare more) and a directory
  for the per-channel reassembly state files, created if missing.
- `FragFrame{Channel uint16; Flags uint16; Kind uint8; Payload []byte}`
  is the caller-visible message. `FragHeaderSize` (21) bytes are
  reserved inside the mux payload, so one fragment carries at most
  `FragMaxPayload = MuxMaxPayload - FragHeaderSize` chunk bytes.
- `(*FragSession).WriteFrame(FragFrame) error` slices the payload into
  ordered fragments and queues every fragment of the message
  atomically, so two messages of one channel never interleave; nothing
  goes out before `Flush`. A payload larger than `MaxMessage` returns
  `ErrTooLarge`; a channel whose in-flight quota is already exhausted
  returns `ErrWindowFull` (the same value as `ErrTooLarge`); both drop
  the write without consuming a message or frame sequence, so a
  rejected payload takes no buffer. Writing to a closed channel returns
  `ErrChannelClosed`; an out-of-range channel returns `ErrChecksum`.
- `(*FragSession).Flush() error` is the mux flush: fragments go out
  round-robin across channels, so one channel's fragments may be
  interleaved with other channels' frames, and a message whose
  fragments span several windows drains as acknowledgements arrive.
- `(*FragSession).ReadFrame() (FragFrame, error)` delivers one
  reassembled message per call, exactly once, with byte content and
  call order identical to the writes; a message stalled mid-assembly on
  one channel never blocks another channel's delivery. A fragment whose
  declared total exceeds the reassembly cap drops its whole message
  with `ErrTooLarge` and the stream continues. A fragment from an old
  message generation, one beyond the current receive position, a
  same-position duplicate with different content, an acknowledgement
  past the send frontier, an unknown flag, a malformed standalone
  acknowledgement, or a damaged fragment header is refused with
  `ErrChecksum` - the bare established value, comparable directly with
  `==` - and stops delivery stickily at the current position. Mux
  results (the per-channel `io.EOF` at a channel's FIN, the transport's
  `io.EOF`, `ErrShortFrame`, outer checksum/version/size and reader
  errors) pass through unchanged.
- `(*FragSession).CloseChannel(ch) error` closes the channel's write
  side through the mux and waits for the acknowledgement pump to wind
  down before returning; a second close returns `ErrChannelClosed`, and
  concurrent closes of one channel are safe.
- `(*FragSession).Close() error` stops the acknowledgement pump and
  closes the wrapped mux.

Restart uses the same durable rules as the lower layers: the send
position, the receive position and any partially reassembled message
are recorded per channel in a checksummed state file (`fragNNN.state`),
rewritten atomically on every accepted fragment, so an interrupted
reassembly resumes from the last confirmed position with no payload
delivered twice or skipped, and retransmitted fragments are
deduplicated. A state file that is truncated, torn or bit-flipped fails
recovery with `ErrChecksum`.

## Tests

    go test ./...

## Limits

Single frame in memory; no streaming reader in this scope.
No compression and no encryption.
Standard library only.
