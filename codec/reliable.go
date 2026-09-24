package codec

// Reliable transfer layer.
//
// ReliableSession wraps an existing *Session without touching the
// single-frame wire encoding: sequence numbers, acknowledgements and
// the sliding window live entirely inside this layer's own payload
// convention, carried by ordinary underlying frames:
//
//	DATA (Kind = reliableKindData), underlying frame payload:
//	  seq (8) | flags (2) | kind (1) | length (4) | inner payload
//	ACK  (Kind = reliableKindAck), underlying frame payload:
//	  ackSeq (8)
//
// An ACK is cumulative: every DATA frame with seq <= ackSeq has been
// delivered to the receiving caller exactly once and in write order.
//
// Send side:
//   - WriteFrame assigns the next sequence number, durably appends the
//     frame to the replay log BEFORE its bytes ever go on the wire, and
//     keeps the frame pending until a cumulative ACK covers it.
//   - Frames with seq in [base, base+window) may be in flight; a write
//     at seq == base+window returns ErrTooLarge and is dropped.
//   - Already-acked frames are never retransmitted; after a restart the
//     sender resumes at the earliest position still held by the
//     checkpoint/replay log, so a redelivered but unacked range is simply
//     sent again.
//
// Receive side:
//   - DATA with seq == deliverNext is delivered, together with any
//     already-buffered consecutive run; frames ahead of deliverNext are
//     held inside the receive window [deliverNext, deliverNext+window).
//   - A retransmitted frame from the recent duplicate window
//     [deliverNext-window, deliverNext) is answered with the current
//     cumulative ACK but delivered at most once.
//   - A frame older than the duplicate window (a pre-wrap residue) or at
//    /above deliverNext+window (a declared seq past the window width) is
//     rejected: ReadFrame returns ErrChecksum and delivery stops at the
//     current position.
//
// Persistence (send side):
//   - The replay log is a sequence of fixed-header records, each with
//     its own checksum: seq(8) | bodyLen(4) | checksum(2) | body with
//     body = flags(2) | kind(1) | inner payload.
//   - The checkpoint file holds one 30-byte record
//     magic | base(8) | nextSeq(8) | ackedTo(8) | checksum(2), published
//     atomically (temp file + rename): a half-written checkpoint is
//     recognized as an unfinished temp file and the previous complete
//     checkpoint is used instead.
//   - On every ACK advance the new checkpoint is published first and the
//     log is compacted afterwards. A crash in between therefore only
//     ever falls back to an EARLIER acknowledged position (some
//     still-acked frames are resent once and deduplicated), never to a
//     later one: no frame is skipped and none is delivered twice.
//   - A truncated or bit-flipped log/checkpoint fails recovery with
//     ErrChecksum and the session is not started.

import (
	"bytes"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"sync"
)

const (
	// reliableKindData / reliableKindAck distinguish this layer's frames
	// through the underlying frame Kind field. The underlying encoding
	// itself is unchanged.
	reliableKindData = 0xF0
	reliableKindAck  = 0xF1

	// DATA header placed inside the underlying frame payload:
	// seq(8) + flags(2) + kind(1) + length(4).
	reliableDataHeaderSize = 8 + 2 + 1 + 4
	// reliableAckSize is the ACK payload size: one cumulative seq.
	reliableAckSize = 8

	// reliableMaxDataPayload is derived from the existing per-frame
	// limit: a DATA frame's outer encoding stays at most MaxPayload.
	reliableMaxDataPayload = MaxPayload - reliableDataHeaderSize

	reliableCPMagic  = "FRCP"
	reliableCPRecord = 4 + 8 + 8 + 8 + 2 // magic, base, nextSeq, ackedTo, checksum
	reliableLogHdr   = 8 + 4 + 2         // seq, bodyLen, checksum
)

// reliableEntry is one persisted/in-flight user frame: the fields the
// inner DATA header restores on the receiving side plus a private copy
// of its payload.
type reliableEntry struct {
	flags   uint16
	kind    uint8
	payload []byte
}

// ReliableSession adds sequence numbers, cumulative acknowledgements, a
// sliding window and crash-restart continuation on top of an existing
// *Session. The read and write directions are independently serialized
// (as on Session itself); ACK handling briefly takes the write lock, so
// the lock order is always read lock -> write lock.
type ReliableSession struct {
	sess   *Session
	window uint64

	// Send-side state (wmu).
	wmu     sync.Mutex
	base    uint64                   // oldest seq not cumulatively acked
	nextSeq uint64                   // next seq to assign
	pending map[uint64]reliableEntry // in-flight, replayable, seq -> frame

	// Receive-side state (rmu).
	rmu         sync.Mutex
	deliverNext uint64
	recv        map[uint64]reliableEntry // in-window, not yet deliverable
	ready       []Frame                  // decoded frames awaiting ReadFrame
	rerr        error                    // sticky terminal read-side error

	cpPath, logPath string
	logFile         *os.File
}

// NewReliableSession wraps sess in a reliable transfer layer with a
// sliding window of window frames on both directions. checkpointPath
// and logPath name the send-side checkpoint and replay-log files; when
// both are empty the session is ephemeral: reliability holds for the
// process lifetime only, as no state survives a restart.
//
// When the files exist, the sender state is recovered from them; a
// half-written checkpoint temp file is discarded and the previous
// complete checkpoint is used, while a torn or bit-flipped log (or
// committed checkpoint) fails with ErrChecksum.
func NewReliableSession(sess *Session, window int, checkpointPath, logPath string) (*ReliableSession, error) {
	if sess == nil {
		return nil, errors.New("codec: reliable session requires a session")
	}
	if window < 1 {
		return nil, errors.New("codec: window must be at least 1")
	}
	if (checkpointPath == "") != (logPath == "") {
		return nil, errors.New("codec: checkpoint and log paths must both be set or both empty")
	}

	rs := &ReliableSession{
		sess:    sess,
		window:  uint64(window),
		pending: make(map[uint64]reliableEntry),
		recv:    make(map[uint64]reliableEntry),
		cpPath:  checkpointPath,
		logPath: logPath,
	}

	if checkpointPath != "" {
		// Staging files left by a crash mid-publish are never valid
		// inputs; drop them before recovery.
		_ = os.Remove(checkpointPath + ".tmp")
		_ = os.Remove(logPath + ".tmp")
		if err := rs.recover(); err != nil {
			return nil, err
		}
		f, err := os.OpenFile(logPath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
		if err != nil {
			return nil, err
		}
		rs.logFile = f
	}

	// A recovered sender resumes by putting every still-pending DATA
	// frame back into the underlying session's write buffer; the next
	// Flush therefore continues from exactly the recovered position.
	for seq := rs.base; seq < rs.nextSeq; seq++ {
		e := rs.pending[seq]
		if err := rs.sess.WriteFrame(buildDataFrame(seq, e)); err != nil {
			return nil, err
		}
	}
	return rs, nil
}

// WriteFrame assigns the next sequence number and buffers one DATA
// frame. The frame is durably appended to the replay log before it can
// reach the wire, so a crash never loses an accepted write.
//
// It returns ErrTooLarge when the inner payload would make the outer
// underlying frame exceed MaxPayload, and also when the send window is
// full: such a frame is dropped before it is numbered or persisted, so
// it occupies no sequence slot and leaves no residue.
func (rs *ReliableSession) WriteFrame(f Frame) error {
	if len(f.Payload) > reliableMaxDataPayload {
		return ErrTooLarge
	}

	rs.wmu.Lock()
	defer rs.wmu.Unlock()

	if rs.nextSeq >= rs.base+rs.window {
		return ErrTooLarge
	}
	seq := rs.nextSeq

	if rs.logPath != "" {
		if err := appendLogRecord(rs.logFile, seq, f); err != nil {
			return err
		}
	}

	// Number only after the frame is durably recorded.
	rs.pending[seq] = reliableEntry{
		flags:   f.Flags,
		kind:    f.Kind,
		payload: append([]byte(nil), f.Payload...),
	}
	rs.nextSeq = seq + 1
	return rs.sess.WriteFrame(buildDataFrame(seq, reliableEntry{f.Flags, f.Kind, f.Payload}))
}

// Flush pushes every buffered DATA frame (including the pending frames
// re-queued by a restart) to the underlying writer.
func (rs *ReliableSession) Flush() error {
	rs.wmu.Lock()
	defer rs.wmu.Unlock()
	return rs.sess.Flush()
}

// Retransmit sends every frame in [base, nextSeq) again. Retransmitted
// frames carry their original sequence numbers; the receive side
// deduplicates them, so content is never delivered twice even when an
// ACK was lost and the frame had to be resent.
func (rs *ReliableSession) Retransmit() error {
	rs.wmu.Lock()
	defer rs.wmu.Unlock()
	for seq := rs.base; seq < rs.nextSeq; seq++ {
		if err := rs.sess.WriteFrame(buildDataFrame(seq, rs.pending[seq])); err != nil {
			return err
		}
	}
	return rs.sess.Flush()
}

// Close releases the persistence handles.
func (rs *ReliableSession) Close() error {
	rs.wmu.Lock()
	defer rs.wmu.Unlock()
	if rs.logFile != nil {
		err := rs.logFile.Close()
		rs.logFile = nil
		return err
	}
	return nil
}

// ReadFrame delivers the next in-order user frame, reading underlying
// bytes as they arrive. ACK frames and retransmitted duplicates are
// consumed internally and never delivered. Out-of-order DATA inside the
// window is held until the gap closes; a frame outside the window rules
// or a malformed inner frame fails with ErrChecksum and the error
// becomes sticky: delivery stops at the last frame already handed out.
// A clean end of the underlying stream returns io.EOF, after every
// frame already queued has been delivered.
func (rs *ReliableSession) ReadFrame() (Frame, error) {
	rs.rmu.Lock()
	defer rs.rmu.Unlock()

	if rs.rerr != nil {
		return Frame{}, rs.rerr
	}

	for len(rs.ready) == 0 && rs.rerr == nil {
		f, err := rs.sess.ReadFrame()
		if err != nil {
			rs.rerr = err
			break
		}
		// One DATA release may queue several consecutive frames; even
		// when the trailing ACK write then fails, those frames already
		// belong to the caller and are drained before the sticky error.
		if perr := rs.handleUnderlying(f); perr != nil {
			rs.rerr = perr
		}
	}
	if len(rs.ready) > 0 {
		out := rs.ready[0]
		rs.ready = rs.ready[1:]
		return out, nil
	}
	return Frame{}, rs.rerr
}

// handleUnderlying consumes one underlying frame: an ACK advances the
// sender, a DATA is buffered, delivered or rejected.
func (rs *ReliableSession) handleUnderlying(f Frame) error {
	switch f.Kind {
	case reliableKindAck:
		if len(f.Payload) != reliableAckSize {
			return ErrChecksum
		}
		return rs.handleAck(binary.BigEndian.Uint64(f.Payload[0:8]))
	case reliableKindData:
		return rs.handleData(f)
	default:
		// A frame that is not part of this layer's convention cannot be
		// placed in the ordered sequence.
		return ErrChecksum
	}
}

// handleAck advances the cumulative send window and persists the new
// frontier. A stale ACK at or below the current frontier is harmless.
// An ACK at or above nextSeq acknowledges a frame this sender never
// sent (a declared seq past the send frontier) and is rejected rather
// than allowed to clear the pending set.
func (rs *ReliableSession) handleAck(ackSeq uint64) error {
	rs.wmu.Lock()
	defer rs.wmu.Unlock()

	// base is the oldest unacked seq, so an ACK below it is stale.
	if ackSeq < rs.base {
		return nil
	}
	// An ACK at or above nextSeq names a frame this sender never sent.
	if ackSeq >= rs.nextSeq {
		return ErrChecksum
	}
	newBase := ackSeq + 1
	for seq := rs.base; seq < newBase; seq++ {
		delete(rs.pending, seq)
	}
	rs.base = newBase
	return rs.persistLocked()
}

// handleData applies the receive window rules to one DATA frame.
func (rs *ReliableSession) handleData(f Frame) error {
	p := f.Payload
	if len(p) < reliableDataHeaderSize {
		return ErrChecksum
	}
	seq := binary.BigEndian.Uint64(p[0:8])
	entry := reliableEntry{
		flags:   binary.BigEndian.Uint16(p[8:10]),
		kind:    p[10],
		payload: p[reliableDataHeaderSize:],
	}
	if declared := binary.BigEndian.Uint32(p[11:15]); declared != uint32(len(entry.payload)) {
		return ErrChecksum
	}

	if seq < rs.deliverNext {
		// Already delivered range. A legitimate retransmit can only be
		// the peer's current pending window, whose oldest frame is at
		// most deliverNext-1 (the window never contained more than
		// window outstanding frames): such a frame is answered with the
		// fresh cumulative ACK and delivered at most once. A frame older
		// than that cannot come from the live window at all -- it is a
		// stale residue (for example left over before a sequence-space
		// wrap) and is rejected.
		if rs.deliverNext-seq > rs.window {
			return ErrChecksum
		}
		return rs.sendAckLocked()
	}
	if seq-rs.deliverNext >= rs.window {
		// Declared seq at or beyond deliverNext+window: accepting it
		// would skip the gap. Reject and stop at the delivered position.
		return ErrChecksum
	}

	if seq == rs.deliverNext {
		rs.release(seq, entry)
		// Release every consecutive frame that was already buffered.
		for {
			e, ok := rs.recv[rs.deliverNext]
			if !ok {
				break
			}
			rs.release(rs.deliverNext, e)
		}
	} else if old, ok := rs.recv[seq]; ok {
		// A retransmit of a parked frame must carry identical content;
		// same seq, different bytes is corruption, not a duplicate.
		if !sameEntry(old, entry) {
			return ErrChecksum
		}
	} else {
		// Out of order but inside the window: park it.
		rs.recv[seq] = cloneEntry(entry)
	}
	return rs.sendAckLocked()
}

// release moves one frame (and advances deliverNext) into the ready
// queue. Payload is copied out because the underlying session reuses
// its read buffer after the frame is consumed.
func (rs *ReliableSession) release(seq uint64, e reliableEntry) {
	rs.ready = append(rs.ready, Frame{
		Version: Version,
		Flags:   e.flags,
		Kind:    e.kind,
		Payload: append([]byte(nil), e.payload...),
	})
	delete(rs.recv, seq)
	rs.deliverNext = seq + 1
}

// sendAckLocked emits the cumulative ACK (all seq < deliverNext). It is
// called with the read lock held and briefly takes the write lock.
func (rs *ReliableSession) sendAckLocked() error {
	if rs.deliverNext == 0 {
		return nil
	}
	var b [reliableAckSize]byte
	binary.BigEndian.PutUint64(b[:], rs.deliverNext-1)

	rs.wmu.Lock()
	defer rs.wmu.Unlock()
	if err := rs.sess.WriteFrame(Frame{Kind: reliableKindAck, Payload: b[:]}); err != nil {
		return err
	}
	return rs.sess.Flush()
}

// buildDataFrame serializes one DATA frame's inner convention as the
// payload of an ordinary underlying frame.
func buildDataFrame(seq uint64, e reliableEntry) Frame {
	p := make([]byte, reliableDataHeaderSize+len(e.payload))
	binary.BigEndian.PutUint64(p[0:8], seq)
	binary.BigEndian.PutUint16(p[8:10], e.flags)
	p[10] = e.kind
	binary.BigEndian.PutUint32(p[11:15], uint32(len(e.payload)))
	copy(p[reliableDataHeaderSize:], e.payload)
	return Frame{Kind: reliableKindData, Payload: p}
}

func cloneEntry(e reliableEntry) reliableEntry {
	return reliableEntry{e.flags, e.kind, append([]byte(nil), e.payload...)}
}

// sameEntry reports whether two frames carrying the same seq agree on
// every byte.
func sameEntry(a, b reliableEntry) bool {
	return a.flags == b.flags && a.kind == b.kind && bytes.Equal(a.payload, b.payload)
}

// --- persistence ---

// recover reconstructs base/nextSeq/pending from the checkpoint and the
// replay log, resolving any disagreement in favor of the EARLIEST
// acknowledged position: the log may retain frames older than the
// checkpoint (compaction happens only after the checkpoint is
// published), in which case those frames are replayed once and
// deduplicated by the peer.
func (rs *ReliableSession) recover() error {
	var cpBase, cpNext uint64
	haveCP := false
	if data, err := os.ReadFile(rs.cpPath); err == nil {
		b, n, perr := parseCheckpoint(data)
		if perr != nil {
			return perr
		}
		cpBase, cpNext, haveCP = b, n, true
	} else if !os.IsNotExist(err) {
		return err
	}

	recs, err := readLog(rs.logPath)
	if err != nil {
		return err
	}

	base, next := uint64(0), uint64(0)
	if haveCP {
		base, next = cpBase, cpNext
	}
	if len(recs) > 0 {
		logBase := recs[0].seq
		logNext := recs[len(recs)-1].seq + 1
		// The log must not be ahead of the checkpoint: payloads missing
		// from the replay range could neither be resent nor skipped.
		if logBase > base {
			return ErrChecksum
		}
		if logNext > next {
			next = logNext
		}
		base = logBase
	}
	for _, r := range recs {
		if r.seq < base {
			continue
		}
		rs.pending[r.seq] = r.entry
	}
	for seq := base; seq < next; seq++ {
		if _, ok := rs.pending[seq]; !ok {
			// A hole in the replay range makes exact continuation
			// impossible: resuming would skip a frame.
			return ErrChecksum
		}
	}
	rs.base, rs.nextSeq = base, next
	return nil
}

// persistLocked publishes the current frontier: checkpoint first (so a
// crash can only fall back to an earlier, never a later, position),
// then the log is compacted to exactly the pending range.
func (rs *ReliableSession) persistLocked() error {
	if rs.cpPath == "" {
		return nil
	}

	var rec [reliableCPRecord]byte
	copy(rec[0:4], reliableCPMagic)
	binary.BigEndian.PutUint64(rec[4:12], rs.base)
	binary.BigEndian.PutUint64(rec[12:20], rs.nextSeq)
	ackedTo := uint64(0)
	if rs.base > 0 {
		ackedTo = rs.base - 1
	}
	binary.BigEndian.PutUint64(rec[20:28], ackedTo)
	binary.BigEndian.PutUint16(rec[28:30], checksum(rec[0:28]))
	if err := atomicWriteFile(rs.cpPath, rec[:]); err != nil {
		return err
	}

	// Compact the log to [base, nextSeq). The checkpoint is already
	// durable, so losing the old log here only causes a one-time replay
	// of acked frames, never a gap.
	var buf []byte
	for seq := rs.base; seq < rs.nextSeq; seq++ {
		buf = appendLogRecordBytes(buf, seq, rs.pending[seq])
	}
	if rs.logFile != nil {
		_ = rs.logFile.Close()
		rs.logFile = nil
	}
	if err := atomicWriteFile(rs.logPath, buf); err != nil {
		return err
	}
	f, err := os.OpenFile(rs.logPath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	rs.logFile = f
	return nil
}

// appendLogRecord appends one checked record to the open replay log and
// forces it to disk before the frame is allowed onto the wire.
func appendLogRecord(f *os.File, seq uint64, fr Frame) error {
	e := reliableEntry{flags: fr.Flags, kind: fr.Kind, payload: fr.Payload}
	rec := appendLogRecordBytes(nil, seq, e)
	if _, err := f.Write(rec); err != nil {
		return err
	}
	return f.Sync()
}

// appendLogRecordBytes encodes seq/e as: seq(8) | bodyLen(4) |
// checksum(2) | flags(2) | kind(1) | payload. The checksum covers the
// fixed header fields and the whole body.
func appendLogRecordBytes(dst []byte, seq uint64, e reliableEntry) []byte {
	body := make([]byte, 3+len(e.payload))
	binary.BigEndian.PutUint16(body[0:2], e.flags)
	body[2] = e.kind
	copy(body[3:], e.payload)

	var hdr [reliableLogHdr]byte
	binary.BigEndian.PutUint64(hdr[0:8], seq)
	binary.BigEndian.PutUint32(hdr[8:12], uint32(len(body)))
	sum := checksum(hdr[0:12]) + checksum(body)
	binary.BigEndian.PutUint16(hdr[12:14], sum)

	dst = append(dst, hdr[:]...)
	dst = append(dst, body...)
	return dst
}

type logRecord struct {
	seq   uint64
	entry reliableEntry
}

// readLog parses the whole replay log. A trailing partial record, a
// failed checksum, an implausible length, or a gap in the sequence run
// are all corruption and surface as ErrChecksum: callers must not
// continue delivering past a damaged log.
func readLog(path string) ([]logRecord, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var recs []logRecord
	for len(raw) > 0 {
		if len(raw) < reliableLogHdr {
			return nil, ErrChecksum
		}
		seq := binary.BigEndian.Uint64(raw[0:8])
		bodyLen := binary.BigEndian.Uint32(raw[8:12])
		if bodyLen < 3 || bodyLen-3 > reliableMaxDataPayload {
			return nil, ErrChecksum
		}
		total := reliableLogHdr + int(bodyLen)
		if len(raw) < total {
			return nil, ErrChecksum
		}
		body := raw[reliableLogHdr:total]
		want := checksum(raw[0:12]) + checksum(body)
		if want != binary.BigEndian.Uint16(raw[12:14]) {
			return nil, ErrChecksum
		}
		if len(recs) > 0 && seq != recs[len(recs)-1].seq+1 {
			return nil, ErrChecksum
		}
		recs = append(recs, logRecord{
			seq: seq,
			entry: reliableEntry{
				flags:   binary.BigEndian.Uint16(body[0:2]),
				kind:    body[2],
				payload: append([]byte(nil), body[3:]...),
			},
		})
		raw = raw[total:]
	}
	return recs, nil
}

// parseCheckpoint validates one checkpoint record.
func parseCheckpoint(data []byte) (base, nextSeq uint64, err error) {
	if len(data) != reliableCPRecord || string(data[0:4]) != reliableCPMagic {
		return 0, 0, ErrChecksum
	}
	if checksum(data[0:28]) != binary.BigEndian.Uint16(data[28:30]) {
		return 0, 0, ErrChecksum
	}
	base = binary.BigEndian.Uint64(data[4:12])
	nextSeq = binary.BigEndian.Uint64(data[12:20])
	ackedTo := binary.BigEndian.Uint64(data[20:28])
	if nextSeq < base {
		return 0, 0, ErrChecksum
	}
	if base == 0 {
		if ackedTo != 0 {
			return 0, 0, ErrChecksum
		}
	} else if ackedTo != base-1 {
		return 0, 0, ErrChecksum
	}
	return base, nextSeq, nil
}

// atomicWriteFile writes data to path via a temp file, an fsync and an
// atomic rename, so a crash mid-write leaves the previous file complete
// and the temp file recognizable as unfinished.
func atomicWriteFile(path string, data []byte) error {
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	if dir, err := os.Open(filepath.Dir(path)); err == nil {
		_ = dir.Sync()
		_ = dir.Close()
	}
	return nil
}
