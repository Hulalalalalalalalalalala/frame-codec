package codec

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
)

// Fragmentation and reassembly on top of one Mux.
//
// A FragSession lets the caller write one complete payload per call even
// when it exceeds the single-frame limit: the send side cuts the payload
// into ordered fragments and every fragment rides an ordinary mux frame,
// so the mux layer, the reliable layer, the codec frame format and every
// established entry, constant and error value below this layer keep
// their exact semantics. The fragment header lives only inside the mux
// payload.
//
// Fragment header layout inside a mux payload (all multi-byte fields
// big-endian):
//
//		+---------+---------+-----------+--------+---------+----------+=========+
//		| msg (4) | idx (4) | total (4) | fl (1) | ack (4) | crc32(4) | chunk   |
//		+---------+---------+-----------+--------+---------+----------+=========+
//
//	  - msg: per-channel message sequence, numbered in write order; every
//	    fragment of one message carries the same msg.
//	  - idx: fragment index within the message, 0-based; every chunk
//	    except the last carries exactly FragMaxPayload bytes.
//	  - total: total payload bytes of the whole message.
//	  - fl: 1 = LAST, the message's final fragment; 2 = standalone
//	    cumulative acknowledgement (msg/idx/total are zero, no chunk).
//	    The two are never combined.
//	  - ack: cumulative "next expected message" for the channel,
//	    piggybacked on every fragment: every message of the channel with
//	    msg < ack has been delivered to the reader.
//	  - crc32: IEEE CRC of the other 17 header bytes plus the chunk.
//
// The mux payload limit stays MuxMaxPayload; FragHeaderSize bytes are
// reserved, so one fragment carries at most FragMaxPayload chunk bytes.
//
// Delivery guarantees:
//   - Messages within one channel are delivered in write order exactly
//     once, with byte content identical to what was written; fragments of
//     one message never interleave with another message of the same
//     channel. Channels are independent: a message stalled mid-assembly
//     on one channel never blocks another channel's delivery.
//   - The send side numbers messages in write order and queues every
//     fragment of an accepted message atomically; nothing goes out
//     before Flush. A payload larger than the configured per-channel
//     reassembly cap returns ErrTooLarge, and a channel whose in-flight
//     quota is already exhausted returns ErrWindowFull (the same value
//     as ErrTooLarge); both drop the write without consuming a message
//     or frame sequence, so a rejected payload takes no buffer.
//   - The receive side keeps one bounded reassembly buffer per channel
//     (at most MaxMessage bytes). A fragment whose declared total
//     exceeds the cap is dropped with ErrTooLarge and the stream
//     continues. A fragment from an old message generation, one beyond
//     the current receive position, a same-position duplicate with
//     different content, an acknowledgement past the send frontier, or
//     a damaged fragment header is refused with ErrChecksum - the bare
//     established value, comparable with == - and stops delivery
//     stickily at the current position.
//
// As on the lower layers, nothing is transmitted before Flush; after a
// restart the mux journal resends every unconfirmed fragment, so Flush
// is also the retransmit operation and the receiver deduplicates the
// overlap.
const (
	// FragHeaderSize is this layer's per-fragment overhead:
	// 4 (msg) + 4 (idx) + 4 (total) + 1 (flags) + 4 (ack) + 4 (crc32).
	FragHeaderSize = 4 + 4 + 4 + 1 + 4 + 4

	// FragMaxPayload is the largest chunk one fragment may carry;
	// FragHeaderSize bytes are reserved inside the mux payload, whose
	// own limit stays MuxMaxPayload.
	FragMaxPayload = MuxMaxPayload - FragHeaderSize

	// fragment header field offsets.
	fhMsgOff   = 0
	fhIdxOff   = 4
	fhTotalOff = 8
	fhFlagOff  = 12
	fhAckOff   = 13
	fhCRCOff   = 17
	fhBodyOff  = 21

	fhFlagLast = 1 << 0
	fhFlagAck  = 1 << 1
)

// FragConfig configures a FragSession.
type FragConfig struct {
	// MaxMessage is the per-channel reassembly cap in bytes: the largest
	// total payload one message may declare. It must be positive and fit
	// a uint32. Both peers of one connection should use the same cap.
	MaxMessage int

	// StateDir holds the per-channel reassembly state files used to
	// resume message numbering and interrupted reassemblies after a
	// restart. It is created if missing.
	StateDir string
}

// FragFrame is one caller-visible message on the fragmentation layer.
// Channel names the channel it was written to.
type FragFrame struct {
	Channel uint16
	Flags   uint16
	Kind    uint8
	Payload []byte
}

// fragRecv is one channel's receive-side reassembly state. While a
// message is being assembled, buf holds the chunks received so far;
// every chunk in an incomplete message is exactly FragMaxPayload bytes,
// so a retransmitted chunk is compared against buf at idx*FragMaxPayload.
type fragRecv struct {
	next     uint32 // next message sequence due for delivery
	started  bool   // a message is mid-assembly
	dropping bool   // an over-cap message is being skipped until its LAST
	ptotal   uint32
	pflags   uint16
	pkind    uint8
	buf      []byte
}

// FragSession is a fragmentation layer wrapping one Mux. Create it with
// NewFrag; the wrapped mux must not be used directly while the
// FragSession owns it.
type FragSession struct {
	mux      *Mux
	cfg      FragConfig
	channels int

	// mu guards every piece of mutable state below; WriteFrame holds it
	// across the mux batch so two concurrent writes can never interleave
	// the fragments of two messages on one channel. ReadFrame never
	// holds it while blocked in the mux reader.
	mu       sync.Mutex
	sendNext []uint32
	recv     []fragRecv
	rerr     error

	// ackPoke coalescingly wakes the acknowledgement pump (capacity 1 is
	// enough because acks are cumulative and the pump reads the latest
	// positions under mu); syncCh carries rendezvous requests from
	// CloseChannel, which returns only after the pump has wound down.
	ackPoke   chan struct{}
	syncCh    chan chan struct{}
	stopCh    chan struct{}
	doneCh    chan struct{}
	closeOnce sync.Once
}

// NewFrag wraps m in a fragmentation session. Reassembly state in
// cfg.StateDir is consulted when present: a fresh directory starts every
// channel at message 0, while a directory left by a crashed or restarted
// process resumes message numbering and any interrupted reassembly from
// the last confirmed positions. A state file that is truncated, torn or
// bit-flipped fails recovery with ErrChecksum rather than reassembling
// unverified bytes.
func NewFrag(m *Mux, cfg FragConfig) (*FragSession, error) {
	if cfg.MaxMessage <= 0 || cfg.MaxMessage > 1<<32-1 {
		return nil, errors.New("codec: frag max message size must be in (0, 2^32)")
	}
	if cfg.StateDir == "" {
		return nil, errors.New("codec: frag session requires a state directory")
	}
	if err := os.MkdirAll(cfg.StateDir, 0o755); err != nil {
		return nil, err
	}

	nch := m.cfg.Channels
	fs := &FragSession{
		mux:      m,
		cfg:      cfg,
		channels: nch,
		sendNext: make([]uint32, nch),
		recv:     make([]fragRecv, nch),
		ackPoke:  make(chan struct{}, 1),
		syncCh:   make(chan chan struct{}),
		stopCh:   make(chan struct{}),
		doneCh:   make(chan struct{}),
	}
	for ch := 0; ch < nch; ch++ {
		sendNext, rc, err := loadFragState(cfg.StateDir, uint16(ch), cfg.MaxMessage)
		if err != nil {
			return nil, err
		}
		fs.sendNext[ch] = sendNext
		fs.recv[ch] = rc
	}

	// The mux journal may hold fragments newer than the last persisted
	// send position (a crash between journaling and the state write):
	// resume numbering past anything already journaled so a rewritten
	// message can never collide with one the peer may have received.
	for _, rec := range m.log.recs {
		if int(rec.ch) >= nch {
			continue
		}
		msg, _, _, flags, _, _, err := decodeFragHeader(rec.f.Payload)
		if err != nil || flags&fhFlagAck != 0 {
			continue
		}
		if msg+1 > fs.sendNext[rec.ch] {
			fs.sendNext[rec.ch] = msg + 1
		}
	}

	go fs.pumpAck()
	return fs, nil
}

// WriteFrame slices f.Payload into ordered fragments and queues every
// fragment on channel f.Channel as ordinary mux frames; nothing reaches
// the wire before Flush. The whole message is accepted atomically, so
// its fragments can never interleave with another message of the same
// channel.
//
// It returns ErrTooLarge for a payload larger than the configured
// reassembly cap, ErrChecksum for a channel number outside the session,
// ErrWindowFull (the same value as ErrTooLarge) when the channel's
// in-flight quota is already exhausted, and ErrChannelClosed when the
// channel's write side has been closed. A rejected payload is neither
// queued nor journaled and consumes no message or frame sequence.
func (fs *FragSession) WriteFrame(f FragFrame) error {
	if len(f.Payload) > fs.cfg.MaxMessage {
		return ErrTooLarge
	}
	if int(f.Channel) >= fs.channels {
		return ErrChecksum
	}

	fs.mu.Lock()
	defer fs.mu.Unlock()

	msg := fs.sendNext[f.Channel]
	total := uint32(len(f.Payload))
	n := 1
	if len(f.Payload) > 0 {
		n = (len(f.Payload) + FragMaxPayload - 1) / FragMaxPayload
	}
	frames := make([]ChannelFrame, 0, n)
	for i := 0; i < n; i++ {
		lo := i * FragMaxPayload
		hi := lo + FragMaxPayload
		if hi > len(f.Payload) {
			hi = len(f.Payload)
		}
		flags := uint8(0)
		if i == n-1 {
			flags = fhFlagLast
		}
		frames = append(frames, ChannelFrame{
			Channel: f.Channel,
			Flags:   f.Flags,
			Kind:    f.Kind,
			Payload: encodeFragHeader(msg, uint32(i), total, flags,
				fs.recv[f.Channel].next, f.Payload[lo:hi]),
		})
	}
	if err := fs.mux.writeFragments(frames); err != nil {
		return err
	}
	fs.sendNext[f.Channel]++
	// The send position is durable before the write returns: after a
	// restart, numbering must never reuse a message sequence the peer
	// may already have received.
	return fs.persistLocked(f.Channel)
}

// Flush sends every queued fragment through the wrapped mux: fragments
// go out round-robin across channels, so one channel's fragments may be
// interleaved with other channels' frames but never overtake an earlier
// fragment of their own channel. After a restart the queue was reloaded
// from the journal, so Flush is also the retransmit operation.
func (fs *FragSession) Flush() error {
	return fs.mux.Flush()
}

// ReadFrame returns the next completely reassembled message across all
// channels, exactly once. Fragments of one channel arrive in write
// order; a channel whose current message is still incomplete never
// blocks another channel's delivery.
//
// A fragment declaring a total larger than the reassembly cap drops its
// whole message with ErrTooLarge and the stream continues. The
// following are refused with the bare established ErrChecksum value and
// stop delivery stickily at the current position: a fragment from an
// old message generation, one beyond the current receive position, a
// same-position duplicate with different content, an acknowledgement
// past the send frontier, an unknown flag, a malformed standalone
// acknowledgement, and a damaged fragment header. Mux results (the
// per-channel io.EOF at a channel's FIN, the transport's io.EOF,
// ErrShortFrame, outer checksum/version/size and reader errors) pass
// through unchanged.
func (fs *FragSession) ReadFrame() (FragFrame, error) {
	for {
		fs.mu.Lock()
		if fs.rerr != nil {
			err := fs.rerr
			fs.mu.Unlock()
			return FragFrame{}, err
		}
		// A message whose final fragment already arrived is finalized
		// (position persisted) and delivered here, so a persist failure
		// is retried on the next call instead of losing the message.
		if ff, ok, err := fs.tryFinalizeLocked(); err != nil {
			fs.mu.Unlock()
			return FragFrame{}, err
		} else if ok {
			fs.mu.Unlock()
			return ff, nil
		}
		fs.mu.Unlock()

		// Block in the mux reader WITHOUT holding mu, exactly like the
		// lower layers: sends and ack processing must keep moving while
		// this waits for bytes.
		cf, rerr := fs.mux.ReadFrame()
		if rerr != nil {
			if errors.Is(rerr, io.EOF) {
				// A channel's FIN (or the transport end) passes through
				// with the channel named, as the mux reported it.
				return FragFrame{Channel: cf.Channel}, io.EOF
			}
			return FragFrame{}, rerr
		}

		msg, idx, total, flags, ack, chunk, err := decodeFragHeader(cf.Payload)
		if err != nil {
			return fs.failRead(ErrChecksum)
		}
		ch := cf.Channel

		fs.mu.Lock()
		if fs.rerr != nil {
			err := fs.rerr
			fs.mu.Unlock()
			return FragFrame{}, err
		}

		// Every inbound message states a cumulative position for its
		// channel; one past anything this side ever sent is impossible
		// in a well-behaved stream and is refused before anything else.
		if ack > fs.sendNext[ch] {
			return fs.failReadLocked(ErrChecksum)
		}

		if flags&fhFlagAck != 0 {
			// A standalone acknowledgement carries no fragment.
			if flags != fhFlagAck || msg != 0 || idx != 0 || total != 0 || len(chunk) != 0 {
				return fs.failReadLocked(ErrChecksum)
			}
			fs.mu.Unlock()
			continue
		}
		if flags&^uint8(fhFlagLast) != 0 {
			return fs.failReadLocked(ErrChecksum)
		}
		last := flags&fhFlagLast != 0

		rc := &fs.recv[ch]
		// Only the message currently due may arrive: anything older is
		// an old-generation residual, anything newer is beyond the
		// receive position's reach.
		if msg != rc.next {
			return fs.failReadLocked(ErrChecksum)
		}

		if rc.dropping {
			// Skip the remaining fragments of a dropped over-cap
			// message; its LAST fragment retires it.
			if last {
				rc.dropping = false
				rc.next++
				fs.signalAckLocked()
			}
			fs.mu.Unlock()
			continue
		}

		if !rc.started {
			if idx != 0 {
				// A message starting past its first fragment is
				// unreachable from the current position.
				return fs.failReadLocked(ErrChecksum)
			}
			if uint64(total) > uint64(fs.cfg.MaxMessage) {
				// The declared total exceeds the reassembly cap: drop
				// the whole message with the established over-limit
				// value; the stream itself continues.
				rc.dropping = true
				rc.ptotal = total
				if last {
					rc.dropping = false
					rc.next++
				}
				fs.signalAckLocked()
				fs.mu.Unlock()
				return FragFrame{}, ErrTooLarge
			}
			if err := checkFragChunk(total, idx, last, chunk); err != nil {
				return fs.failReadLocked(ErrChecksum)
			}
			rc.started = true
			rc.ptotal = total
			rc.pflags = cf.Flags
			rc.pkind = cf.Kind
			rc.buf = append(rc.buf[:0], chunk...)
		} else {
			if total != rc.ptotal {
				return fs.failReadLocked(ErrChecksum)
			}
			// Every chunk of an incomplete message is full-size, so the
			// next index due is exact.
			nextIdx := uint32(len(rc.buf) / FragMaxPayload)
			switch {
			case idx < nextIdx:
				// A retransmission of a stored fragment must describe
				// the same fragment; a conflicting duplicate is refused
				// instead of guessed.
				off := int(idx) * FragMaxPayload
				if last || !bytes.Equal(rc.buf[off:off+FragMaxPayload], chunk) {
					return fs.failReadLocked(ErrChecksum)
				}
				fs.mu.Unlock()
				continue
			case idx > nextIdx:
				return fs.failReadLocked(ErrChecksum)
			}
			if err := checkFragChunk(total, idx, last, chunk); err != nil {
				return fs.failReadLocked(ErrChecksum)
			}
			rc.buf = append(rc.buf, chunk...)
		}
		// Write the reassembly state through before the mux can
		// acknowledge this fragment past the sender: after a restart the
		// assembly resumes from exactly this position.
		_ = fs.persistLocked(ch)
		fs.signalAckLocked()
		fs.mu.Unlock()
	}
}

// tryFinalizeLocked delivers one channel's just-completed message. The
// advanced receive position is persisted BEFORE the message is handed
// out, so a restart never delivers it twice; on a persist failure the
// state is rolled back and retried on a later call. Caller holds mu.
func (fs *FragSession) tryFinalizeLocked() (FragFrame, bool, error) {
	for ch := 0; ch < fs.channels; ch++ {
		rc := &fs.recv[ch]
		if !rc.started || rc.dropping || uint64(len(rc.buf)) != uint64(rc.ptotal) {
			continue
		}
		ff := FragFrame{Channel: uint16(ch), Flags: rc.pflags, Kind: rc.pkind, Payload: rc.buf}
		rc.next++
		rc.started = false
		rc.buf = nil
		if err := fs.persistLocked(uint16(ch)); err != nil {
			rc.next--
			rc.started = true
			rc.buf = ff.Payload
			return FragFrame{}, false, err
		}
		fs.signalAckLocked()
		return ff, true, nil
	}
	return FragFrame{}, false, nil
}

// failRead records the terminal read-side result and returns it.
func (fs *FragSession) failRead(err error) (FragFrame, error) {
	fs.mu.Lock()
	fs.rerr = err
	fs.mu.Unlock()
	return FragFrame{}, err
}

// failReadLocked is failRead with mu already held; it releases mu.
func (fs *FragSession) failReadLocked(err error) (FragFrame, error) {
	fs.rerr = err
	fs.mu.Unlock()
	return FragFrame{}, err
}

// signalAckLocked coalescingly asks the acknowledgement pump to restate
// the receive positions. It never blocks. Caller holds mu.
func (fs *FragSession) signalAckLocked() {
	select {
	case fs.ackPoke <- struct{}{}:
	default:
	}
}

// pumpAck serializes standalone acknowledgement writes so they happen on
// this goroutine, never on a reader's. Every wakeup sends at most one
// cumulative ack per channel whose receive position moved since the last
// send, so a burst of fragments costs a handful of frames rather than
// one per arrival.
func (fs *FragSession) pumpAck() {
	defer close(fs.doneCh)
	sent := make([]uint32, fs.channels)
	for {
		select {
		case <-fs.stopCh:
			return
		case <-fs.ackPoke:
			fs.sendAcks(sent)
		case done := <-fs.syncCh:
			fs.sendAcks(sent)
			close(done)
		}
	}
}

// sendAcks writes one standalone cumulative acknowledgement for every
// channel whose receive position moved since sent. Best effort: a
// failed or blocked ack is harmless because every later fragment
// piggybacks the same cumulative position.
func (fs *FragSession) sendAcks(sent []uint32) {
	fs.mu.Lock()
	pos := make([]uint32, fs.channels)
	for ch := range pos {
		pos[ch] = fs.recv[ch].next
	}
	fs.mu.Unlock()

	wrote := false
	for ch := 0; ch < fs.channels; ch++ {
		if pos[ch] == sent[ch] {
			continue
		}
		hdr := encodeFragHeader(0, 0, 0, fhFlagAck, pos[ch], nil)
		if err := fs.mux.WriteFrame(ChannelFrame{Channel: uint16(ch), Payload: hdr}); err != nil {
			continue
		}
		sent[ch] = pos[ch]
		wrote = true
	}
	if wrote {
		_ = fs.mux.Flush()
	}
}

// CloseChannel closes the write side of ch through the wrapped mux and
// waits for the acknowledgement pump to wind down before returning, so
// every position delivered before the close is on its way to the peer.
// Closing (or writing to) an already closed channel returns
// ErrChannelClosed; an out-of-range channel returns ErrChecksum. Both
// values compare equal to the established sentinels directly.
func (fs *FragSession) CloseChannel(ch uint16) error {
	if int(ch) >= fs.channels {
		return ErrChecksum
	}
	if err := fs.mux.CloseChannel(ch); err != nil {
		return err
	}
	// Rendezvous with the pump: it answers after flushing every
	// acknowledgement queued so far. If it already stopped (Close), or
	// stops before answering, the rendezvous completes immediately.
	done := make(chan struct{})
	select {
	case fs.syncCh <- done:
		select {
		case <-done:
		case <-fs.doneCh:
		}
	case <-fs.doneCh:
	}
	return nil
}

// Close stops the acknowledgement pump, waits for it to wind down and
// closes the wrapped mux. Fully journaled but unacknowledged fragments
// remain in the mux journal for a later restart to resend.
func (fs *FragSession) Close() error {
	fs.closeOnce.Do(func() { close(fs.stopCh) })
	<-fs.doneCh
	return fs.mux.Close()
}

// ----------------------------------------------------------------------
// Fragment header encoding.
// ----------------------------------------------------------------------

// encodeFragHeader serializes one fragment header plus its chunk into
// mux payload bytes.
func encodeFragHeader(msg, idx, total uint32, flags uint8, ack uint32, chunk []byte) []byte {
	out := make([]byte, FragHeaderSize+len(chunk))
	binary.BigEndian.PutUint32(out[fhMsgOff:fhIdxOff], msg)
	binary.BigEndian.PutUint32(out[fhIdxOff:fhTotalOff], idx)
	binary.BigEndian.PutUint32(out[fhTotalOff:fhFlagOff], total)
	out[fhFlagOff] = flags
	binary.BigEndian.PutUint32(out[fhAckOff:fhCRCOff], ack)
	copy(out[fhBodyOff:], chunk)
	h := crc32.NewIEEE()
	h.Write(out[:fhCRCOff])
	h.Write(out[fhBodyOff:])
	binary.BigEndian.PutUint32(out[fhCRCOff:fhBodyOff], h.Sum32())
	return out
}

// decodeFragHeader parses one fragment header. A message too short for
// the fixed header or one whose CRC does not match is reported with the
// established checksum value.
func decodeFragHeader(p []byte) (msg, idx, total uint32, flags uint8, ack uint32, chunk []byte, err error) {
	if len(p) < FragHeaderSize {
		return 0, 0, 0, 0, 0, nil, ErrChecksum
	}
	h := crc32.NewIEEE()
	h.Write(p[:fhCRCOff])
	h.Write(p[fhBodyOff:])
	if h.Sum32() != binary.BigEndian.Uint32(p[fhCRCOff:fhBodyOff]) {
		return 0, 0, 0, 0, 0, nil, ErrChecksum
	}
	msg = binary.BigEndian.Uint32(p[fhMsgOff:fhIdxOff])
	idx = binary.BigEndian.Uint32(p[fhIdxOff:fhTotalOff])
	total = binary.BigEndian.Uint32(p[fhTotalOff:fhFlagOff])
	flags = p[fhFlagOff]
	ack = binary.BigEndian.Uint32(p[fhAckOff:fhCRCOff])
	body := make([]byte, len(p)-FragHeaderSize)
	copy(body, p[fhBodyOff:])
	return msg, idx, total, flags, ack, body, nil
}

// fragCount is the number of fragments a message of total bytes splits
// into; an empty message is exactly one (empty) fragment.
func fragCount(total uint32) uint32 {
	if total == 0 {
		return 1
	}
	return uint32((uint64(total) + FragMaxPayload - 1) / FragMaxPayload)
}

// checkFragChunk validates one fragment's shape against the message's
// declared total: the index must belong to the message, the LAST
// marking must fall exactly on the final index, and the chunk must
// carry exactly its share of the declared bytes.
func checkFragChunk(total, idx uint32, last bool, chunk []byte) error {
	fc := fragCount(total)
	if idx >= fc {
		return ErrChecksum
	}
	if last != (idx == fc-1) {
		return ErrChecksum
	}
	want := FragMaxPayload
	if idx == fc-1 {
		want = int(total) - int(idx)*FragMaxPayload
	}
	if len(chunk) != want {
		return ErrChecksum
	}
	return nil
}

// ----------------------------------------------------------------------
// Durable state: one checksummed reassembly state file per channel,
// rewritten atomically via temp file + rename on every accepted
// fragment, so an interrupted reassembly resumes from the last confirmed
// position and a delivered message is never delivered twice.
//
// File layout (all multi-byte fields big-endian):
//
//	+------------+------------+----------+-----------------------+----------+
//	| send (4)   | recv (4)   | mode (1) | [partial record]      | crc32(4) |
//	+------------+------------+----------+-----------------------+----------+
//
// mode 0 carries no partial record. mode 1 (assembly in progress)
// carries: total(4) flags(2) kind(1) datalen(4) data(datalen).
// ----------------------------------------------------------------------

const (
	fragStatePrefix = "frag"
	fragStateSuffix = ".state"

	fragStateHdrSize = 4 + 4 + 1 // sendNext + recvNext + mode

	fragModeNone    = 0
	fragModePartial = 1
)

// fragDiskState is one channel's persisted state.
type fragDiskState struct {
	sendNext uint32
	recvNext uint32
	mode     uint8
	ptotal   uint32
	pflags   uint16
	pkind    uint8
	pdata    []byte
}

func fragStatePath(dir string, ch uint16) string {
	return filepath.Join(dir, fmt.Sprintf("%s%03d%s", fragStatePrefix, ch, fragStateSuffix))
}

// persistLocked writes the channel's current state through to its state
// file. Caller holds mu.
func (fs *FragSession) persistLocked(ch uint16) error {
	rc := &fs.recv[ch]
	st := fragDiskState{
		sendNext: fs.sendNext[ch],
		recvNext: rc.next,
	}
	if rc.started && !rc.dropping {
		st.mode = fragModePartial
		st.ptotal = rc.ptotal
		st.pflags = rc.pflags
		st.pkind = rc.pkind
		st.pdata = rc.buf
	}
	return saveFragState(fs.cfg.StateDir, ch, st)
}

func marshalFragState(st fragDiskState) []byte {
	body := make([]byte, 0, fragStateHdrSize+11+len(st.pdata)+4)
	var tmp [4]byte
	put32 := func(v uint32) {
		binary.BigEndian.PutUint32(tmp[:], v)
		body = append(body, tmp[:]...)
	}
	put32(st.sendNext)
	put32(st.recvNext)
	body = append(body, st.mode)
	if st.mode == fragModePartial {
		put32(st.ptotal)
		binary.BigEndian.PutUint16(tmp[:2], st.pflags)
		body = append(body, tmp[:2]...)
		body = append(body, st.pkind)
		put32(uint32(len(st.pdata)))
		body = append(body, st.pdata...)
	}
	binary.BigEndian.PutUint32(tmp[:], crc32.ChecksumIEEE(body))
	return append(body, tmp[:]...)
}

// saveFragState rewrites the channel's state file atomically.
func saveFragState(dir string, ch uint16, st fragDiskState) error {
	path := fragStatePath(dir, ch)
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(marshalFragState(st)); err != nil {
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
	return os.Rename(tmp, path)
}

// loadFragState reads one channel's state file strictly: every byte must
// belong to one complete, valid record. A missing file means a fresh
// channel; a truncated file, a bad CRC, an unknown mode or an
// inconsistent partial record fails recovery with ErrChecksum rather
// than reassembling unverified bytes.
func loadFragState(dir string, ch uint16, maxMessage int) (sendNext uint32, rc fragRecv, err error) {
	data, err := os.ReadFile(fragStatePath(dir, ch))
	if err != nil {
		if os.IsNotExist(err) {
			return 0, fragRecv{}, nil
		}
		return 0, fragRecv{}, err
	}
	fail := func() (uint32, fragRecv, error) {
		return 0, fragRecv{}, fmt.Errorf("codec: frag state %s corrupted: %w",
			filepath.Base(fragStatePath(dir, ch)), ErrChecksum)
	}
	if len(data) < fragStateHdrSize+4 {
		return fail()
	}
	body := data[:len(data)-4]
	if crc32.ChecksumIEEE(body) != binary.BigEndian.Uint32(data[len(data)-4:]) {
		return fail()
	}
	sendNext = binary.BigEndian.Uint32(body[0:4])
	rc.next = binary.BigEndian.Uint32(body[4:8])
	mode := body[8]
	switch mode {
	case fragModeNone:
		if len(body) != fragStateHdrSize {
			return fail()
		}
	case fragModePartial:
		if len(body) < fragStateHdrSize+11 {
			return fail()
		}
		ptotal := binary.BigEndian.Uint32(body[9:13])
		pflags := binary.BigEndian.Uint16(body[13:15])
		pkind := body[15]
		dlen := binary.BigEndian.Uint32(body[16:20])
		if len(body) != fragStateHdrSize+11+int(dlen) {
			return fail()
		}
		// A partial assembly holds only whole full-size chunks, plus
		// possibly the final short chunk of a message that completed
		// exactly as the process stopped (an empty message stops with
		// zero bytes); anything else cannot have been written by this
		// layer.
		if uint64(ptotal) > uint64(maxMessage) || dlen > ptotal ||
			(dlen < ptotal && (dlen == 0 || dlen%FragMaxPayload != 0)) {
			return fail()
		}
		rc.started = true
		rc.ptotal = ptotal
		rc.pflags = pflags
		rc.pkind = pkind
		rc.buf = append([]byte(nil), body[fragStateHdrSize+11:]...)
	default:
		return fail()
	}
	return sendNext, rc, nil
}
