package codec

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
)

// Reliable transport on top of the multi-frame Session.
//
// This layer adds sequence numbers, cumulative acknowledgements and a
// sliding-window pipeline. It does not touch the wire encoding of the
// layer below: every reliable message is carried inside the Payload of
// one ordinary codec frame, prefixed by this layer's own envelope. The
// Encode/Decode entries, the header constants and the four existing
// error values keep their exact byte-level semantics.
//
// Envelope layout inside a codec frame payload (all multi-byte fields
// big-endian):
//
//		+---------+---------+--------+----------+---------+----------+=========+
//		| seq (4) | ack (4) | type(1)| flags(2) | kind(1) | crc32(4) | payload |
//		+---------+---------+--------+----------+---------+----------+=========+
//
//	  - seq: sender-assigned data sequence; ACK frames carry no data seq.
//	  - ack: cumulative acknowledgement as a "next expected" count: every
//	    data frame with seq < ack has been delivered to the reader.
//	  - type: 1 = data, 2 = pure acknowledgement.
//	  - flags/kind: the caller frame's Flags and Kind, carried inside the
//	    envelope; the outer codec Kind is a fixed marker of this layer.
//	  - crc32: IEEE CRC of the other 12 envelope bytes plus the payload.
//
// The underlying frame's payload limit stays MaxPayload; EnvelopeSize
// bytes are reserved, so the inner payload limit is ReliableMaxPayload.
//
// Delivery guarantees, across disconnects and sender-side restarts:
//   - Write side: frames are numbered in write order and streamed
//     through a fixed send window. Written frames are journaled in a
//     CRC'd replay log; the acknowledged position is recorded in a
//     dual-slot checkpoint. After a restart, sending resumes after the
//     earliest confirmed position recorded by either file, so an
//     acknowledged frame is never counted twice and no frame past it is
//     skipped (a resupplied frame is deduped on arrival).
//   - Read side: frames are delivered in sequence order exactly once.
//     A duplicate of an already delivered or already held frame is
//     acknowledged again but delivered once; an in-order gap closed by
//     an out-of-order arrival flushes the held run in order; a sequence
//     older than the window's duplicate span or more than one window
//     width ahead is refused and delivery stops at the current
//     position.
const (
	// EnvelopeSize is this layer's per-frame overhead:
	// 4 (seq) + 4 (ack) + 1 (type) + 2 (flags) + 1 (kind) + 4 (crc32).
	EnvelopeSize = 16

	// ReliableMaxPayload is the largest inner payload a reliable data
	// frame may carry; EnvelopeSize bytes are reserved inside the
	// underlying frame, whose own limit stays MaxPayload.
	ReliableMaxPayload = MaxPayload - EnvelopeSize

	// envelope field offsets.
	envSeqOff   = 0
	envAckOff   = 4
	envTypeOff  = 8
	envFlagsOff = 9
	envKindOff  = 11
	envCRCOff   = 12
	envBodyOff  = 16
)

const (
	envTypeData = 1
	envTypeAck  = 2

	// outerKind is the codec-frame Kind of every reliable message; the
	// caller's Kind travels inside the envelope and is never confused
	// with this marker.
	outerKind uint8 = 0xA5
)

// ErrWindowFull is the established over-limit value a write returns when
// the send window already holds the maximum number of unacknowledged
// frames. It is the same sentinel the single-frame encoder uses for an
// over-limit payload: an over-window write is an over-limit write, and
// the offered frame is dropped without entering the window or the log
// and without consuming a sequence number.
var ErrWindowFull = ErrTooLarge

// ReliableFrame is one caller-visible frame on the reliable layer.
type ReliableFrame struct {
	Flags   uint16
	Kind    uint8
	Payload []byte
}

// envelope is one parsed reliable message.
type envelope struct {
	seq     uint32
	ack     uint32
	typ     uint8
	flags   uint16
	kind    uint8
	payload []byte
}

// encodeEnvelope serializes one message into the payload bytes of an
// ordinary codec frame.
func encodeEnvelope(e envelope) ([]byte, error) {
	if e.typ == envTypeData && len(e.payload) > ReliableMaxPayload {
		return nil, ErrTooLarge
	}
	out := make([]byte, EnvelopeSize+len(e.payload))
	binary.BigEndian.PutUint32(out[envSeqOff:envSeqOff+4], e.seq)
	binary.BigEndian.PutUint32(out[envAckOff:envAckOff+4], e.ack)
	out[envTypeOff] = e.typ
	binary.BigEndian.PutUint16(out[envFlagsOff:envFlagsOff+2], e.flags)
	out[envKindOff] = e.kind
	copy(out[envBodyOff:], e.payload)
	h := crc32.NewIEEE()
	h.Write(out[:envCRCOff])
	h.Write(out[envBodyOff:])
	binary.BigEndian.PutUint32(out[envCRCOff:envCRCOff+4], h.Sum32())
	return out, nil
}

// decodeEnvelope parses one message from a codec frame payload. A
// damaged envelope is reported with the established checksum value;
// nothing is delivered from it.
func decodeEnvelope(p []byte) (envelope, error) {
	var e envelope
	if len(p) < EnvelopeSize {
		return e, ErrShortFrame
	}
	h := crc32.NewIEEE()
	h.Write(p[:envCRCOff])
	h.Write(p[envBodyOff:])
	if h.Sum32() != binary.BigEndian.Uint32(p[envCRCOff:envCRCOff+4]) {
		return e, ErrChecksum
	}
	e.seq = binary.BigEndian.Uint32(p[envSeqOff : envSeqOff+4])
	e.ack = binary.BigEndian.Uint32(p[envAckOff : envAckOff+4])
	e.typ = p[envTypeOff]
	e.flags = binary.BigEndian.Uint16(p[envFlagsOff : envFlagsOff+2])
	e.kind = p[envKindOff]
	body := make([]byte, len(p)-EnvelopeSize)
	copy(body, p[EnvelopeSize:])
	e.payload = body
	return e, nil
}

// ReliableConfig configures a ReliableSession.
type ReliableConfig struct {
	// Window is the send and receive window width in frames. It must be
	// positive. Both peers of one connection should use the same width.
	Window int

	// StateDir holds the checkpoint file and replay log used to resume
	// the write side after a restart. It is created if missing.
	StateDir string
}

// ReliableSession is a reliability layer wrapping one Session. Create it
// with NewReliable; the wrapped session must not be used directly while
// the ReliableSession owns it.
//
// The two directions are independent and may be used concurrently, like
// the underlying session. A writer that wants the peer's
// acknowledgements to advance its window must let ReadFrame run
// (acknowledgements are consumed there, in a dedicated goroutine when
// traffic is one-directional).
type ReliableSession struct {
	sess *Session
	cfg  ReliableConfig

	wmu sync.Mutex
	// sendBase is the oldest sequence not yet acknowledged (as a
	// next-expected ack value); sendNext is the next sequence a newly
	// written frame gets. Slot i holds sequence sendBase+i while it is
	// unacknowledged.
	sendBase uint32
	sendNext uint32
	window   []windowSlot
	log      *replayLog
	closed   bool

	rmu sync.Mutex
	// recvBase is the next sequence due for delivery. It is atomic so the
	// send paths (Flush, the ack pump) can read the position without
	// taking rmu: ReadFrame must keep rmu free while blocked in the
	// underlying reader, otherwise two peers reading at once deadlock.
	recvBase atomic.Uint32
	// held parks frames that arrived in-window but ahead of order; it is
	// guarded by rmu.
	held map[uint32]ReliableFrame
	rerr error

	// ackCh is a coalescing signal: every delivery that moves recvBase
	// (or repeats a duplicate the sender is still resending) pokes it
	// non-blockingly, and pumpAck flushes one cumulative acknowledgement
	// for the latest position. A capacity of 1 is enough because acks
	// are cumulative. Keeping the write off the read path means two
	// peers reading at the same time cannot block each other.
	ackCh  chan struct{}
	stopCh chan struct{}
	doneCh chan struct{}
}

type windowSlot struct {
	used bool
	seq  uint32
	f    ReliableFrame
}

// NewReliable wraps sess in a reliable session. Write state in
// cfg.StateDir is consulted when present: a fresh directory starts at
// sequence 0, while a directory left by a crashed or restarted process
// resumes from the earliest confirmed position held by its checkpoint
// and replay log.
//
// A checkpoint record torn by a crash mid-write fails its own checksum
// and is ignored in favor of the previous complete record. A replayed
// log that is truncated or contains a flipped bit fails with
// ErrChecksum.
func NewReliable(sess *Session, cfg ReliableConfig) (*ReliableSession, error) {
	if cfg.Window <= 0 {
		return nil, errors.New("codec: reliable window must be positive")
	}
	if cfg.StateDir == "" {
		return nil, errors.New("codec: reliable session requires a state directory")
	}
	if err := os.MkdirAll(cfg.StateDir, 0o755); err != nil {
		return nil, err
	}

	rs := &ReliableSession{
		sess:   sess,
		cfg:    cfg,
		window: make([]windowSlot, cfg.Window),
		held:   make(map[uint32]ReliableFrame),
		ackCh:  make(chan struct{}, 1),
		stopCh: make(chan struct{}),
		doneCh: make(chan struct{}),
	}

	// Newest valid checkpoint record; a torn record is invisible here.
	cpAck, _, err := loadCheckpoint(cfg.StateDir)
	if err != nil {
		return nil, err
	}

	rs.log, err = openReplayLog(cfg.StateDir)
	if err != nil {
		return nil, err
	}

	logBase, logNext, logEmpty := rs.log.bounds()
	switch {
	case logEmpty || cpAck >= logNext:
		// Either there is nothing left to replay, or the checkpoint
		// confirms a position at/beyond the log's tail (the journaled
		// records are all older, already-delivered frames). Resume at the
		// checkpoint: numbering new frames any earlier would collide with
		// sequences the peer already received.
		rs.sendBase = cpAck
		rs.sendNext = cpAck
	default:
		// Log tail extends past the checkpoint. Resume at the log's
		// first replayable frame. A checkpoint is durably written BEFORE
		// its log prefix is discarded, so a crash can leave a log that
		// still reaches further back than the checkpoint; resending that
		// overlap is safe because the receiver deduplicates it. Starting
		// any later would skip a frame that was never confirmed; starting
		// earlier than logBase is impossible because those frames were
		// already discarded by a confirmed checkpoint.
		rs.sendBase = logBase
		// sendNext stays one past the last journaled frame. A replay
		// backlog wider than the window drains as acks advance and the
		// freed tail slots are refilled from the log.
		rs.sendNext = logNext
	}

	rs.reloadWindow()
	// Start the ack pump only after every error-return path is past: on
	// a failed NewReliable no session (and no pump goroutine) exists.
	go rs.pumpAck()
	return rs, nil
}

// reloadWindow refills the in-memory window slots from the replay log
// after a restart, so the first Flush resends every frame past the
// resume point.
func (rs *ReliableSession) reloadWindow() {
	for i := range rs.window {
		rs.window[i] = windowSlot{}
	}
	for _, rec := range rs.log.framesFrom(rs.sendBase) {
		idx := int(rec.seq - rs.sendBase)
		if idx >= len(rs.window) {
			break
		}
		rs.window[idx] = windowSlot{used: true, seq: rec.seq, f: rec.f}
	}
}

// WriteFrame numbers f with the next send sequence, journals it and
// parks it in the send window. Nothing is transmitted before Flush.
// When the window already holds Window unacknowledged frames, WriteFrame
// returns ErrWindowFull (the established over-limit value) and drops the
// frame: it is neither windowed nor journaled and consumes no sequence.
// An oversized inner payload returns ErrTooLarge before anything else.
func (rs *ReliableSession) WriteFrame(f ReliableFrame) error {
	if len(f.Payload) > ReliableMaxPayload {
		return ErrTooLarge
	}
	rs.wmu.Lock()
	defer rs.wmu.Unlock()

	if rs.closed {
		return errors.New("codec: write on closed reliable session")
	}
	if rs.sendNext-rs.sendBase >= uint32(rs.cfg.Window) {
		return ErrWindowFull
	}

	seq := rs.sendNext
	if err := rs.log.append(seq, f); err != nil {
		return err
	}
	rs.window[int(seq-rs.sendBase)] = windowSlot{used: true, seq: seq, f: f}
	rs.sendNext++
	return nil
}

// Flush sends every windowed frame in sequence order, each carrying the
// read side's current delivered position as a cumulative ack, followed
// by one pure acknowledgement, and flushes the wrapped session.
//
// Retransmission is a plain Flush on a restarted writer: the window was
// reloaded from the replay log, so Flush resends every frame past the
// resume point; the reader delivers each at most once.
func (rs *ReliableSession) Flush() error {
	// recvBase is atomic, so no read lock is taken here: Flush never
	// blocks a concurrent ReadFrame and vice versa.
	ack := rs.recvBase.Load()

	rs.wmu.Lock()
	out := make([]Frame, 0, len(rs.window)+1)
	for i := range rs.window {
		sl := &rs.window[i]
		if !sl.used {
			continue
		}
		b, err := encodeEnvelope(envelope{
			seq:     sl.seq,
			ack:     ack,
			typ:     envTypeData,
			flags:   sl.f.Flags,
			kind:    sl.f.Kind,
			payload: sl.f.Payload,
		})
		if err != nil {
			rs.wmu.Unlock()
			return err
		}
		out = append(out, Frame{Kind: outerKind, Payload: b})
	}
	out = append(out, rs.ackFrameLocked(ack))
	rs.wmu.Unlock()

	for i := range out {
		if err := rs.sess.WriteFrame(out[i]); err != nil {
			return err
		}
	}
	return rs.sess.Flush()
}

// ackFrameLocked builds a pure acknowledgement. Caller holds wmu; the
// envelope's own seq is zero because ACK frames carry no data sequence.
func (rs *ReliableSession) ackFrameLocked(ack uint32) Frame {
	b, _ := encodeEnvelope(envelope{ack: ack, typ: envTypeAck})
	return Frame{Kind: outerKind, Payload: b}
}

// ReadFrame returns the next in-order caller frame, exactly once. It
// internally processes reliable messages: acknowledgements (pure ones
// and piggybacked ones) advance the send window, duplicates are
// re-acknowledged but not redelivered, and in-window out-of-order frames
// are held until the gap closes and then delivered as a contiguous run.
// A sequence outside the window's reach - a residual older than the
// duplicate span or a declared sequence more than one window width
// ahead - is refused with ErrChecksum and delivery stops at the last
// in-order frame; the refusal is sticky.
func (rs *ReliableSession) ReadFrame() (ReliableFrame, error) {
	for {
		rs.rmu.Lock()
		if rs.rerr != nil {
			err := rs.rerr
			rs.rmu.Unlock()
			return ReliableFrame{}, err
		}
		// A contiguous held frame can be delivered without another read.
		if f, ok := rs.held[rs.recvBase.Load()]; ok {
			base := rs.recvBase.Load()
			delete(rs.held, base)
			rs.recvBase.Store(base + 1)
			rs.signalAck()
			rs.rmu.Unlock()
			return f, nil
		}
		rs.rmu.Unlock()

		// Block in the underlying reader WITHOUT holding rmu: the read
		// may wait indefinitely for bytes, and the send paths must stay
		// able to observe recvBase meanwhile. The wrapped session
		// serializes concurrent readers, so each wakeup owns one frame.
		outer, rerr := rs.sess.ReadFrame()

		rs.rmu.Lock()
		// Another reader may have recorded a terminal result while this
		// one was blocked in the underlying read.
		if rs.rerr != nil {
			err := rs.rerr
			rs.rmu.Unlock()
			return ReliableFrame{}, err
		}
		if rerr != nil {
			// Outer-layer results (io.EOF, a corrupt outer frame, a
			// truncated trailing frame, an underlying reader error) pass
			// through with their established values.
			return rs.failReadLocked(rerr)
		}

		env, err := decodeEnvelope(outer.Payload)
		if err != nil {
			return rs.failReadLocked(err)
		}

		// Every inbound message may carry a cumulative acknowledgement
		// for data we sent.
		rs.applyAck(env.ack)

		if env.typ == envTypeAck {
			rs.rmu.Unlock()
			continue
		}
		if env.typ != envTypeData {
			// An unknown message type cannot be placed in the ordered
			// stream; refuse it like a damaged envelope.
			return rs.failReadLocked(fmt.Errorf("codec: unknown reliable message type %d: %w", env.typ, ErrChecksum))
		}

		rf := ReliableFrame{Flags: env.flags, Kind: env.kind, Payload: env.payload}
		base := rs.recvBase.Load()

		switch classify(env.seq, base, rs.cfg.Window) {
		case seqInWindow:
			if _, dup := rs.held[env.seq]; !dup {
				// First arrival: park it. A retransmission of the same
				// parked frame keeps the first copy.
				rs.held[env.seq] = rf
			}
			// Restate the cumulative position either way (a fresh frame
			// extends it only after in-order delivery; the duplicate case
			// needs it so the peer stops resending).
			rs.signalAck()
			rs.rmu.Unlock()
			continue // deliver held[recvBase] from the top of the loop
		case seqDuplicate:
			// Already delivered in an earlier call - a resend after its
			// acknowledgement was lost. Do not deliver again; restate the
			// cumulative position.
			rs.signalAck()
			rs.rmu.Unlock()
			continue
		case seqStale:
			// Older than the duplicate span, or more than a window width
			// ahead: refuse and stop where delivery currently is.
			return rs.failReadLocked(fmt.Errorf("codec: sequence %d outside window at %d (width %d): %w",
				env.seq, base, rs.cfg.Window, ErrChecksum))
		}
	}
}

type seqClass int

const (
	seqDuplicate seqClass = iota // behind base, within one window width
	seqInWindow                  // [base, base+Window)
	seqStale                     // anywhere else: ancient residual or too far ahead
)

// classify positions an inbound sequence relative to a receive window
// starting at base. uint32 modular subtraction keeps the check correct
// across sequence wraparound.
func classify(seq, base uint32, window int) seqClass {
	if fwd := seq - base; fwd < uint32(window) {
		return seqInWindow
	}
	if back := base - seq; back >= 1 && back <= uint32(window) {
		return seqDuplicate
	}
	return seqStale
}

// signalAck coalescingly asks the ack pump to restate recvBase. It never
// blocks and never writes from under the read lock.
func (rs *ReliableSession) signalAck() {
	select {
	case rs.ackCh <- struct{}{}:
	default:
	}
}

// pumpAck serializes all standalone acknowledgement writes. One poke
// yields at most one ack stating the latest delivered position, so a
// burst of deliveries sends a few cumulative acks rather than one per
// frame. Writes happen on this goroutine, not on the caller's read
// goroutine, which is what lets two peers read simultaneously without
// deadlocking.
func (rs *ReliableSession) pumpAck() {
	defer close(rs.doneCh)
	for {
		select {
		case <-rs.stopCh:
			return
		case <-rs.ackCh:
		}

		// Coalesce pokes that accumulated while the previous ack was
		// being written.
		select {
		case <-rs.ackCh:
		default:
		}

		ack := rs.recvBase.Load()
		rs.wmu.Lock()
		frame := rs.ackFrameLocked(ack)
		rs.wmu.Unlock()
		// Best effort: a failed or blocked ack is harmless because every
		// later Flush piggybacks the same cumulative position.
		if err := rs.sess.WriteFrame(frame); err == nil {
			_ = rs.sess.Flush()
		}
	}
}

// applyAck advances the send window past a cumulative "next expected"
// acknowledgement and makes the new position durable. Called with rmu
// held; takes wmu (rmu-then-wmu order).
func (rs *ReliableSession) applyAck(ack uint32) {
	rs.wmu.Lock()
	defer rs.wmu.Unlock()

	// ack states that every seq < ack is delivered. Ignore values that
	// do not move the window forward or pass the highest numbered seq.
	if ack <= rs.sendBase || ack > rs.sendNext {
		return
	}
	shift := ack - rs.sendBase
	if shift > uint32(len(rs.window)) {
		shift = uint32(len(rs.window))
	}

	// Slide occupied slots down; sources are always read at higher
	// indices than the destination, so overlap cannot erase an unread
	// slot.
	for i := 0; i+int(shift) < len(rs.window); i++ {
		rs.window[i] = rs.window[i+int(shift)]
	}
	for i := len(rs.window) - int(shift); i < len(rs.window); i++ {
		rs.window[i] = windowSlot{}
	}
	rs.sendBase += shift

	// After a restart the replay backlog may be wider than the window:
	// the freed tail slots are refilled from the log as the window
	// advances, so every journaled frame eventually goes out.
	rs.reloadWindow()

	// Make the new position durable BEFORE discarding the journaled
	// prefix: a crash between the two leaves extra replayable frames
	// (deduped on resend), never a gap. Prefix only after the checkpoint
	// actually landed: a failed checkpoint plus a successful prefix would
	// otherwise be the one ordering that can lose frames.
	if err := saveCheckpoint(rs.cfg.StateDir, rs.sendBase); err == nil && rs.log != nil {
		_ = rs.log.prefix(rs.sendBase)
	}
}

// failReadLocked records the terminal read-side result, releases rmu and
// returns it. Every caller returns immediately after calling it.
func (rs *ReliableSession) failReadLocked(err error) (ReliableFrame, error) {
	rs.rerr = err
	rs.rmu.Unlock()
	return ReliableFrame{}, err
}

// Close marks the session closed and asks the acknowledgement pump to
// stop; a pump blocked on an unread transport unwinds once that
// transport is closed. Fully journaled but unacknowledged frames remain
// in the replay log for a later restart to resend.
func (rs *ReliableSession) Close() error {
	rs.wmu.Lock()
	if rs.closed {
		rs.wmu.Unlock()
		return nil
	}
	rs.closed = true
	rs.wmu.Unlock()
	close(rs.stopCh)
	return nil
}

// ----------------------------------------------------------------------
// Durable state: a dual-slot, checksummed checkpoint and a strict,
// CRC'd replay log.
// ----------------------------------------------------------------------

// Checkpoint: two fixed, independently checksummed records in one file.
// Every save writes the ALTERNATE slot with a strictly higher
// generation, so a crash mid-write leaves the previous complete record
// intact in the other slot; on load the highest-generation record that
// validates wins.
const (
	checkpointName = "reliable.ckp"
	cpRecordSize   = 4 + 4 + 4 // generation + ack + crc32
	cpSlotCount    = 2
	checkpointSize = cpRecordSize * cpSlotCount
)

type cpRecord struct {
	generation uint32
	ack        uint32
}

func checkpointPath(dir string) string {
	return filepath.Join(dir, checkpointName)
}

func marshalCPRecord(r cpRecord) []byte {
	b := make([]byte, cpRecordSize)
	binary.BigEndian.PutUint32(b[0:4], r.generation)
	binary.BigEndian.PutUint32(b[4:8], r.ack)
	binary.BigEndian.PutUint32(b[8:12], crc32.ChecksumIEEE(b[0:8]))
	return b
}

func unmarshalCPRecord(b []byte) (cpRecord, bool) {
	var r cpRecord
	if len(b) < cpRecordSize {
		return r, false
	}
	if binary.BigEndian.Uint32(b[8:12]) != crc32.ChecksumIEEE(b[0:8]) {
		return r, false
	}
	r.generation = binary.BigEndian.Uint32(b[0:4])
	r.ack = binary.BigEndian.Uint32(b[4:8])
	return r, true
}

// loadCheckpoint returns the acknowledged "next expected" position of
// the newest valid record. A missing/empty file, or one whose records
// all fail validation, means "no checkpoint yet" (position 0): with no
// confirmed position the safe action is to replay everything the log
// holds, and the reader deduplicates the overlap.
func loadCheckpoint(dir string) (uint32, bool, error) {
	data, err := os.ReadFile(checkpointPath(dir))
	if err != nil {
		if os.IsNotExist(err) {
			return 0, false, nil
		}
		return 0, false, err
	}
	if len(data) == 0 {
		return 0, false, nil
	}

	var best cpRecord
	found := false
	for off := 0; off+cpRecordSize <= len(data); off += cpRecordSize {
		if r, ok := unmarshalCPRecord(data[off : off+cpRecordSize]); ok {
			if !found || r.generation > best.generation ||
				(r.generation == best.generation && r.ack > best.ack) {
				best = r
				found = true
			}
		}
	}
	if !found {
		return 0, false, nil
	}
	return best.ack, true, nil
}

// nextCPSlot chooses the slot to overwrite and the next generation:
// alternate slots, strictly increasing generations.
func nextCPSlot(dir string) (slot int, generation uint32, err error) {
	data, rerr := os.ReadFile(checkpointPath(dir))
	if rerr != nil && !os.IsNotExist(rerr) {
		return 0, 0, rerr
	}
	var gens [cpSlotCount]uint32
	var ok [cpSlotCount]bool
	for i := 0; i < cpSlotCount; i++ {
		lo, hi := i*cpRecordSize, (i+1)*cpRecordSize
		if len(data) >= hi {
			if r, valid := unmarshalCPRecord(data[lo:hi]); valid {
				gens[i], ok[i] = r.generation, true
			}
		}
	}
	switch {
	case !ok[0] && !ok[1]:
		return 0, 1, nil
	case ok[0] && (!ok[1] || gens[0] >= gens[1]):
		return 1, gens[0] + 1, nil
	default:
		return 0, gens[1] + 1, nil
	}
}

// saveCheckpoint writes one new record into the alternate slot. The
// other slot is untouched, so at every durable instant at least the
// previous complete record survives. The very first write sizes the
// file to both slots; later writes never touch the slot they are not
// rewriting.
func saveCheckpoint(dir string, ack uint32) error {
	slot, generation, err := nextCPSlot(dir)
	if err != nil {
		return err
	}
	path := checkpointPath(dir)

	// Size the file before the record write so the padding never lands
	// on a slot an earlier checkpoint already filled.
	needPad := true
	if info, statErr := os.Stat(path); statErr == nil && info.Size() >= checkpointSize {
		needPad = false
	}

	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()

	if needPad {
		if _, err := f.WriteAt(make([]byte, checkpointSize), 0); err != nil {
			return err
		}
	}
	if _, err := f.WriteAt(marshalCPRecord(cpRecord{generation: generation, ack: ack}),
		int64(slot*cpRecordSize)); err != nil {
		return err
	}
	return f.Sync()
}

// ----------------------------------------------------------------------
// Replay log: appended, length-prefixed, CRC'd records in sequence
// order. Scanning is strict: every byte must belong to one complete,
// valid record. A truncated file or a flipped bit therefore fails with
// ErrChecksum instead of silently delivering or skipping frames.
//
// Record layout:
//
//	+---------+---------+----------+--------+----------+=========+
//	| seq (4) | kind(1) | flags(2) | len(4) | crc32(4) | payload |
//	+---------+---------+----------+--------+----------+=========+
//
// The CRC covers the 11 header bytes and the payload.
// ----------------------------------------------------------------------

const (
	logName    = "reliable.log"
	logHdrSize = 4 + 1 + 2 + 4 + 4
)

type logRecord struct {
	seq uint32
	f   ReliableFrame
}

type replayLog struct {
	path string
	mu   sync.Mutex
	recs []logRecord
}

func openReplayLog(dir string) (*replayLog, error) {
	l := &replayLog{path: filepath.Join(dir, logName)}
	if err := l.load(); err != nil {
		return nil, err
	}
	return l, nil
}

func (l *replayLog) load() error {
	data, err := os.ReadFile(l.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	for off := 0; off < len(data); {
		// Any bytes that do not form a complete, valid record are a
		// truncated tail: strict failure, no further delivery.
		if len(data)-off < logHdrSize {
			return fmt.Errorf("codec: replay log %s truncated: %w", logName, ErrChecksum)
		}
		hdr := data[off : off+logHdrSize]
		seq := binary.BigEndian.Uint32(hdr[0:4])
		kind := hdr[4]
		flags := binary.BigEndian.Uint16(hdr[5:7])
		plen := int(binary.BigEndian.Uint32(hdr[7:11]))
		wantCRC := binary.BigEndian.Uint32(hdr[11:15])

		end := off + logHdrSize + plen
		if plen < 0 || end > len(data) {
			return fmt.Errorf("codec: replay log %s truncated: %w", logName, ErrChecksum)
		}
		h := crc32.NewIEEE()
		h.Write(hdr[:11])
		h.Write(data[off+logHdrSize : end])
		if h.Sum32() != wantCRC {
			return fmt.Errorf("codec: replay log %s corrupted: %w", logName, ErrChecksum)
		}

		payload := make([]byte, plen)
		copy(payload, data[off+logHdrSize:end])
		l.recs = append(l.recs, logRecord{
			seq: seq,
			f:   ReliableFrame{Flags: flags, Kind: kind, Payload: payload},
		})
		off = end
	}
	return nil
}

// append journals one frame as a single fully checksummed record.
func (l *replayLog) append(seq uint32, f ReliableFrame) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	hdr := make([]byte, logHdrSize)
	binary.BigEndian.PutUint32(hdr[0:4], seq)
	hdr[4] = f.Kind
	binary.BigEndian.PutUint16(hdr[5:7], f.Flags)
	binary.BigEndian.PutUint32(hdr[7:11], uint32(len(f.Payload)))
	h := crc32.NewIEEE()
	h.Write(hdr[:11])
	h.Write(f.Payload)
	binary.BigEndian.PutUint32(hdr[11:15], h.Sum32())

	file, err := os.OpenFile(l.path, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	// One write of the whole record keeps the on-disk tail either absent
	// or complete from the point of view of a killing crash.
	if _, err := file.Write(append(hdr, f.Payload...)); err != nil {
		file.Close()
		return err
	}
	if err := file.Sync(); err != nil {
		file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}

	l.recs = append(l.recs, logRecord{seq: seq, f: f})
	return nil
}

// bounds reports the lowest and one-past-the-highest held sequence and
// whether the log is empty.
func (l *replayLog) bounds() (base, next uint32, empty bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.recs) == 0 {
		return 0, 0, true
	}
	return l.recs[0].seq, l.recs[len(l.recs)-1].seq + 1, false
}

// framesFrom returns the logged frames with sequence >= from, in order.
func (l *replayLog) framesFrom(from uint32) []logRecord {
	l.mu.Lock()
	defer l.mu.Unlock()
	var out []logRecord
	for _, r := range l.recs {
		if r.seq >= from {
			out = append(out, r)
		}
	}
	return out
}

// prefix discards logged frames with sequence < base (already
// acknowledged), rewriting the log atomically via temp file + rename.
func (l *replayLog) prefix(base uint32) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	keep := l.recs[:0]
	for _, r := range l.recs {
		if r.seq >= base {
			keep = append(keep, r)
		}
	}
	if len(keep) == len(l.recs) {
		return nil
	}
	l.recs = keep

	tmp := l.path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	for _, r := range keep {
		hdr := make([]byte, logHdrSize)
		binary.BigEndian.PutUint32(hdr[0:4], r.seq)
		hdr[4] = r.f.Kind
		binary.BigEndian.PutUint16(hdr[5:7], r.f.Flags)
		binary.BigEndian.PutUint32(hdr[7:11], uint32(len(r.f.Payload)))
		h := crc32.NewIEEE()
		h.Write(hdr[:11])
		h.Write(r.f.Payload)
		binary.BigEndian.PutUint32(hdr[11:15], h.Sum32())
		if _, err := f.Write(hdr); err != nil {
			f.Close()
			return err
		}
		if _, err := f.Write(r.f.Payload); err != nil {
			f.Close()
			return err
		}
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, l.path)
}
