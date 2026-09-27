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

// Channel multiplexing on top of one ReliableSession.
//
// A Mux carries several independent logical channels over a single
// reliable session. Every multiplexed message is the payload of one
// reliable data frame, prefixed by this layer's own mux header; the
// reliable wire format, the codec frame format and every established
// entry, constant and error value below this layer keep their exact
// byte-level semantics.
//
// Mux header layout inside a reliable payload (all multi-byte fields
// big-endian):
//
//		+----------+---------+--------+---------+----------+=========+
//		| chan (2) | seq (4) | fl (1) | ack (4) | crc32(4) | payload |
//		+----------+---------+--------+---------+----------+=========+
//
//	  - chan: the channel number the frame belongs to.
//	  - seq: per-channel write sequence, numbered in write order; an ack
//	    frame carries no data seq and sets it to zero.
//	  - fl: 1 = FIN, the channel's closing frame (no payload, no caller
//	    flags/kind); 2 = standalone cumulative acknowledgement. The two
//	    are never combined.
//	  - ack: cumulative "next expected" acknowledgement for chan,
//	    piggybacked on every message: every data frame of chan with seq
//	    < ack has been delivered to the reader.
//	  - crc32: IEEE CRC of the other 11 header bytes plus the payload.
//
// The reliable frame's own payload limit stays ReliableMaxPayload;
// MuxHeaderSize bytes are reserved, so the inner payload limit is
// MuxMaxPayload.
//
// Delivery guarantees:
//   - Frames within one channel are delivered in write order exactly
//     once; channels are independent, so a gap on one channel never
//     blocks another channel's delivery.
//   - The send side hands transmission opportunities out round-robin
//     across channels, so one channel sending continuously can never
//     starve the others.
//   - Every channel has its own adaptive in-flight quota in the range
//     [1,Window]; the channels never influence one another's quota. The
//     quota grows when acknowledgements advance the channel's position
//     and tightens back towards 1 when a confirmed frame had to be
//     retransmitted. A write that cannot be admitted under the current
//     quota is not rejected: it parks in arrival order and completes
//     once acknowledgements free quota, so a legal payload is never
//     dropped for being in flight.
//   - Admission is strictly FIFO across all writers (a FIN queues like
//     any other frame), which keeps cross-channel fairness and the
//     per-channel ordering intact. The in-flight count of a channel can
//     never cross its current quota: a quota only tightens down to the
//     frames already in flight and reaches 1 once they drain.
//   - A channel opens on its first write and its write side can be
//     closed independently; its FIN rides the sequence stream like a
//     data frame. After the peer has received the channel's remaining
//     frames, the read that reaches the FIN returns io.EOF together with
//     a ChannelFrame naming the channel; other channels keep flowing.
//     Writing to, or closing, an already closed channel returns
//     ErrChannelClosed.
//
// As on the reliable layer, nothing is transmitted before Flush; after
// a restart the queue is reloaded from the journal, so Flush is also
// the retransmit operation. The adaptive quota is recovered with the
// per-channel state and keeps adjusting from the recovered value.
const (
	// MuxHeaderSize is this layer's per-frame overhead:
	// 2 (chan) + 4 (seq) + 1 (flags) + 4 (ack) + 4 (crc32).
	MuxHeaderSize = 2 + 4 + 1 + 4 + 4

	// MuxMaxPayload is the largest payload a multiplexed data frame may
	// carry; MuxHeaderSize bytes are reserved inside the reliable
	// payload, whose own limit stays ReliableMaxPayload.
	MuxMaxPayload = ReliableMaxPayload - MuxHeaderSize

	// mux header field offsets.
	mhChanOff = 0
	mhSeqOff  = 2
	mhFlagOff = 6
	mhAckOff  = 7
	mhCRCOff  = 11
	mhBodyOff = 15

	mhFlagFin = 1 << 0
	mhFlagAck = 1 << 1
)

// ErrChannelClosed is returned by a write or a close on a channel whose
// write side has already been closed.
var ErrChannelClosed = errors.New("codec: channel closed")

// MuxConfig configures a Mux.
type MuxConfig struct {
	// Channels is the number of logical channels the session carries,
	// numbered 0..Channels-1. It must be positive.
	Channels int

	// Window is the per-channel window width in frames: the fixed receive
	// reach used to place inbound sequences and the UPPER bound of each
	// channel's adaptive in-flight quota (the lower bound is 1). It must
	// be positive.
	Window int

	// StateDir holds the per-channel checkpoint files, the per-channel
	// adaptive quota files and the shared replay journal used to resume
	// the write side after a restart. It is created if missing.
	StateDir string
}

// ChannelFrame is one caller-visible multiplexed frame. Channel names
// the channel it was written to.
type ChannelFrame struct {
	Channel uint16
	Flags   uint16
	Kind    uint8
	Payload []byte
}

// queuedFrame is one admitted send-side frame waiting for a
// transmission opportunity or an acknowledgement.
type queuedFrame struct {
	channel uint16
	seq     uint32
	flags   uint16
	kind    uint8
	payload []byte
	fin     bool
	// sent is set once the frame has actually been handed to the wire;
	// handing it again afterwards sets retried. Observing retried when
	// the frame is acknowledged tightens the channel's quota.
	sent    bool
	retried bool
}

// sendChannel is one direction of one channel on the send side.
type sendChannel struct {
	// next is the next sequence a write gets; acked is the cumulative
	// "next expected" position confirmed by the peer. In-flight quota
	// accounting is exactly next-acked because rejected frames consume
	// no sequence.
	next   uint32
	acked  uint32
	open   bool // opened by a first write or close
	closed bool // write side closed (the FIN may still be queued)

	// quota is the channel's current adaptive in-flight quota in
	// [1,Window]; inflight (next-acked) is never allowed past it.
	quota uint32

	// FIFO admission: waiters take tickets in arrival order and only the
	// ticket at headTicket may claim a slot, so a parked writer can never
	// overtake an earlier parked writer or FIN. waiters counts tickets
	// actually parked on the condition (Pressure reports it).
	headTicket uint64
	nextTicket uint64
	waiters    int
}

// inboundFrame is a parked receive-side frame; fin markers carry no
// caller frame.
type inboundFrame struct {
	cf  ChannelFrame
	fin bool
}

// muxSeq identifies a frame within one channel.
type muxSeq struct {
	ch  uint16
	seq uint32
}

// ChannelPressure is one channel's send-side flow-control snapshot.
type ChannelPressure struct {
	// Channel is the channel number.
	Channel uint16
	// InFlight is the number of admitted, unacknowledged frames.
	InFlight int
	// Quota is the channel's current adaptive in-flight quota, in
	// [1,Window].
	Quota int
	// Waiting reports whether at least one write or close is parked
	// waiting for quota to be released.
	Waiting bool
}

// PressureReport is a read-only snapshot of every channel's send-side
// pressure, channel by channel.
type PressureReport struct {
	Channels []ChannelPressure
}

// Mux is a multiplexing layer wrapping one ReliableSession. Create it
// with NewMux; the wrapped session must not be used directly while the
// Mux owns it.
type Mux struct {
	rel *ReliableSession
	cfg MuxConfig

	mu   sync.Mutex
	cond *sync.Cond
	// closing is set by Close so writers parked on quota unwind instead
	// of waiting forever.
	closing bool
	queue   []*queuedFrame // admitted, unacknowledged frames, arrival order
	sc      []sendChannel
	// rr is the round-robin cursor: the channel at which the next Flush
	// starts handing out opportunities.
	rr  int
	log *muxLog

	rmu sync.Mutex
	// bases is the per-channel next-due receive sequence; finEOF marks
	// channels whose FIN already reached the reader. seen remembers the
	// first copy of every sequence inside the duplicate span (the
	// window width behind the base) so a retransmitted frame that
	// conflicts with its first arrival is refused even after that
	// sequence was delivered and left held.
	bases  []uint32
	seen   map[muxSeq]inboundFrame
	held   map[muxSeq]inboundFrame
	finEOF []bool
	rerr   error

	// standCh coalesces standalone ack send requests; capacity Channels
	// keeps one pending position per channel because acks are cumulative.
	standCh   chan standAck
	stopCh    chan struct{}
	doneCh    chan struct{}
	closeOnce sync.Once
}

// standAck is one standalone channel acknowledgement handed to the
// background sender.
type standAck struct {
	ch   uint16
	next uint32
}

// NewMux wraps rel in a multiplexing session. Write state in
// cfg.StateDir is consulted when present: a fresh directory starts
// every channel at sequence 0, while a directory left by a crashed or
// restarted process resumes from the earliest confirmed per-channel
// positions held by its checkpoints and journal.
//
// A truncated journal or a flipped bit in a complete record fails
// recovery with ErrChecksum rather than replaying unverified frames.
func NewMux(rel *ReliableSession, cfg MuxConfig) (*Mux, error) {
	if cfg.Channels <= 0 {
		return nil, errors.New("codec: mux channel count must be positive")
	}
	if cfg.Window <= 0 {
		return nil, errors.New("codec: mux window must be positive")
	}
	if cfg.StateDir == "" {
		return nil, errors.New("codec: mux session requires a state directory")
	}
	if err := os.MkdirAll(cfg.StateDir, 0o755); err != nil {
		return nil, err
	}

	m := &Mux{
		rel: rel,
		cfg: cfg,
		sc:  make([]sendChannel, cfg.Channels),
		// At most Channels*Window deliveries (one per in-flight frame)
		// can be awaiting an ack send at once, so the queue is bounded by
		// the same total buffer bound and a progress-bearing signal is
		// never dropped; duplicate arrivals beyond it are coalesced
		// away because acks are cumulative.
		bases:   make([]uint32, cfg.Channels),
		seen:    make(map[muxSeq]inboundFrame),
		held:    make(map[muxSeq]inboundFrame),
		finEOF:  make([]bool, cfg.Channels),
		standCh: make(chan standAck, cfg.Channels*cfg.Window),
		stopCh:  make(chan struct{}),
		doneCh:  make(chan struct{}),
	}
	m.cond = sync.NewCond(&m.mu)

	// Load the strict shared journal first, then reconcile per-channel
	// positions against the checkpoints.
	var err error
	m.log, err = openMuxLog(cfg.StateDir)
	if err != nil {
		return nil, err
	}

	// Per-channel bookkeeping derived strictly from the journal: lowest
	// and one-past-highest sequence, whether a FIN was seen, and strict
	// contiguity (a gap or a duplicate would mean a skipped or doubled
	// frame, which recovery must never silently replay).
	type span struct {
		min, max uint32
		have     bool
		fin      bool
	}
	spans := make([]span, cfg.Channels)
	for _, rec := range m.log.recs {
		if int(rec.ch) >= cfg.Channels {
			return nil, fmt.Errorf("codec: mux journal carries channel %d beyond %d: %w",
				rec.ch, cfg.Channels-1, ErrChecksum)
		}
		sp := &spans[rec.ch]
		if sp.have {
			if rec.seq != sp.max+1 {
				return nil, fmt.Errorf(
					"codec: mux journal ch %d not contiguous (seq %d after %d): %w",
					rec.ch, rec.seq, sp.max, ErrChecksum)
			}
			if sp.fin {
				return nil, fmt.Errorf(
					"codec: mux journal ch %d has frame seq %d after FIN: %w",
					rec.ch, rec.seq, ErrChecksum)
			}
		}
		if !sp.have {
			sp.min, sp.have = rec.seq, true
		}
		sp.max = rec.seq
		if rec.fin {
			sp.fin = true
		}
	}

	for ch := 0; ch < cfg.Channels; ch++ {
		cpAck, err := loadMuxCheckpoint(cfg.StateDir, uint16(ch))
		if err != nil {
			return nil, err
		}
		c := &m.sc[ch]
		sp := spans[ch]
		switch {
		case !sp.have:
			// Nothing left to replay: resume numbering at the confirmed
			// position; earlier sequences already reached the peer.
			c.acked, c.next = cpAck, cpAck
		case cpAck >= sp.max+1:
			// Checkpoint at/beyond the journal tail.
			c.acked, c.next = cpAck, cpAck
		default:
			// The journal reaches further back than the checkpoint (a
			// crash between checkpoint and prefix). Resume at the
			// EARLIER position: the overlap is resent and the receiver
			// deduplicates it; starting later would skip an
			// unacknowledged frame.
			c.acked, c.next = sp.min, sp.max+1
		}
		// A FIN record closes the channel; even after its prefix was
		// discarded the durable close marker keeps it closed.
		closedByMarker, err := muxChannelClosed(cfg.StateDir, uint16(ch))
		if err != nil {
			return nil, err
		}
		if sp.have {
			c.open = true
			c.closed = sp.fin || closedByMarker
		} else if closedByMarker {
			c.open = true
			c.closed = true
		}

		// Recover the adaptive quota and continue adjusting from it. A
		// channel without a flow file starts fully open; a recovered
		// value is clamped to [1,Window] and raised to whatever frames
		// are already in flight, so the in-flight count can never cross
		// the quota.
		inflight := c.next - c.acked
		q, qok, err := loadMuxFlow(cfg.StateDir, uint16(ch))
		if err != nil {
			return nil, err
		}
		switch {
		case !qok:
			c.quota = uint32(cfg.Window)
		default:
			if q < 1 {
				q = 1
			}
			if q > uint32(cfg.Window) {
				q = uint32(cfg.Window)
			}
			if q < inflight {
				q = inflight
			}
			c.quota = q
		}
	}

	// The send queue is the journal in arrival order, minus frames
	// already at/behind their channel's resumed position; it feeds
	// round-robin Flush and bounds in-flight accounting. The first
	// Flush of an incarnation is each frame's initial transmission; a
	// later Flush handing the same frame again is the retransmit that
	// tightens its quota.
	m.queue = make([]*queuedFrame, 0, len(m.log.recs))
	for i := range m.log.recs {
		rec := m.log.recs[i]
		if rec.seq < m.sc[rec.ch].acked {
			continue
		}
		qf := rec.frame()
		m.queue = append(m.queue, &qf)
	}

	go m.pumpAck()
	return m, nil
}

// takeTicketsLocked reserves n consecutive positions in ch's FIFO
// admission queue and returns the first ticket. A closed channel refuses
// before any ticket is taken. The caller later awaits and claims each
// ticket in order; n is 1 for an ordinary frame or FIN and equals the
// fragment count for one fragmented message, so that message's fragments
// occupy consecutive positions and can never be interleaved by a later
// write to the same channel. Caller holds mu.
func (m *Mux) takeTicketsLocked(ch uint16, n int) (uint64, error) {
	c := &m.sc[ch]
	if c.closed {
		return 0, ErrChannelClosed
	}
	ticket := c.nextTicket
	c.nextTicket += uint64(n)
	return ticket, nil
}

// awaitSlotLocked parks until ticket reaches the head of ch's FIFO queue
// and one in-flight slot is free (in-flight < quota). cond.Wait releases
// mu, so acknowledgements and Flush keep progressing while this waits.
// A session Close, or a channel whose FIN was admitted while this ticket
// waited, unwinds the wait with ErrChannelClosed; the ticket is released
// and the wait behind it are cascaded awake. Caller holds mu.
func (m *Mux) awaitSlotLocked(ch uint16, ticket uint64) error {
	for ticket != m.sc[ch].headTicket || m.sc[ch].next-m.sc[ch].acked >= m.sc[ch].quota {
		// Count only tickets actually parked, so Pressure's Waiting flag
		// never reports an immediately-admitted write as blocked.
		m.sc[ch].waiters++
		m.cond.Wait()
		m.sc[ch].waiters--
		if m.closing {
			return ErrChannelClosed
		}
		// This ticket's turn arrived only to find the FIN of a concurrent
		// close ahead of its claims: refuse it rather than admit into a
		// closed channel.
		if m.sc[ch].closed && ticket == m.sc[ch].headTicket {
			return ErrChannelClosed
		}
	}
	return nil
}

// releaseTicketsLocked unwinds n consecutive tickets starting at first
// whose wait (or whose whole run) aborted, advancing the FIFO head past
// them and cascading the wakeup to the tickets behind. Caller holds mu.
func (m *Mux) releaseTicketsLocked(ch uint16, first uint64, n int) {
	c := &m.sc[ch]
	end := first + uint64(n)
	if c.headTicket >= first && c.headTicket < end {
		c.headTicket = end
	}
	m.cond.Broadcast()
}

// claimSlotLocked releases the FIFO turn of one claimed ticket and wakes
// the next waiter. It is called once per ticket after a successful
// awaitSlotLocked, whether the journal append then committed or failed:
// a failed append never consumes a sequence, so its turn must move on.
// Caller holds mu.
func (m *Mux) claimSlotLocked(ch uint16, ticket uint64) {
	m.releaseTicketsLocked(ch, ticket, 1)
}

// WriteFrame enqueues f on channel f.Channel and assigns it that
// channel's next sequence; nothing reaches the wire before Flush.
//
// It returns ErrTooLarge for a payload larger than MuxMaxPayload and the
// established checksum value for a channel number outside the session.
// When the channel's in-flight count already reaches its adaptive quota,
// WriteFrame does not reject the frame: it parks in arrival order behind
// the channel's earlier writes (and its FIN) and completes once
// acknowledgements release quota, so a legal payload is never dropped and
// every admitted frame keeps its arrival-determined sequence. A journal
// write failure returns the underlying I/O error without consuming a
// sequence or a quota slot.
func (m *Mux) WriteFrame(f ChannelFrame) error {
	if len(f.Payload) > MuxMaxPayload {
		return ErrTooLarge
	}
	if int(f.Channel) >= m.cfg.Channels {
		return fmt.Errorf("codec: mux channel %d out of range [0,%d): %w",
			f.Channel, m.cfg.Channels, ErrChecksum)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	ticket, err := m.takeTicketsLocked(f.Channel, 1)
	if err != nil {
		return err
	}
	if err := m.awaitSlotLocked(f.Channel, ticket); err != nil {
		m.releaseTicketsLocked(f.Channel, ticket, 1)
		return err
	}

	c := &m.sc[f.Channel]
	qf := queuedFrame{
		channel: f.Channel,
		seq:     c.next,
		flags:   f.Flags,
		kind:    f.Kind,
		payload: append([]byte(nil), f.Payload...),
	}
	if err := m.log.append(qf); err != nil {
		m.claimSlotLocked(f.Channel, ticket)
		return err
	}
	c.open = true
	c.next++
	m.queue = append(m.queue, &qf)
	m.claimSlotLocked(f.Channel, ticket)
	return nil
}

// tryWriteFrame attempts to enqueue f without ever parking: it admits
// only when the channel has no outstanding FIFO tickets and a quota slot
// is free right now. It returns admitted=false when the frame would have
// to wait (the standalone-ack pump uses this to stay best-effort, like
// skipping ErrWindowFull before admission blocked). Validation errors
// (oversize/out-of-range/closed) are returned directly; a skipped frame
// consumes nothing.
func (m *Mux) tryWriteFrame(f ChannelFrame) (admitted bool, err error) {
	if len(f.Payload) > MuxMaxPayload {
		return false, ErrTooLarge
	}
	if int(f.Channel) >= m.cfg.Channels {
		return false, fmt.Errorf("codec: mux channel %d out of range [0,%d): %w",
			f.Channel, m.cfg.Channels, ErrChecksum)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	c := &m.sc[f.Channel]
	if c.closed {
		return false, ErrChannelClosed
	}
	if c.nextTicket != c.headTicket || c.next-c.acked >= c.quota {
		return false, nil
	}
	ticket, err := m.takeTicketsLocked(f.Channel, 1)
	if err != nil {
		return false, err
	}
	qf := queuedFrame{
		channel: f.Channel,
		seq:     c.next,
		flags:   f.Flags,
		kind:    f.Kind,
		payload: append([]byte(nil), f.Payload...),
	}
	if err := m.log.append(qf); err != nil {
		m.claimSlotLocked(f.Channel, ticket)
		return false, err
	}
	c.open = true
	c.next++
	m.queue = append(m.queue, &qf)
	m.claimSlotLocked(f.Channel, ticket)
	return true, nil
}

// prepareRun validates one channel's frames and reserves their
// consecutive FIFO admission tickets WITHOUT blocking on quota. It lets
// a caller reserve run ordering under its own (upper-layer) lock and
// release that lock before the blocking commit, so inbound
// acknowledgements can still be processed while the run waits.
func (m *Mux) prepareRun(frames []ChannelFrame) (ch uint16, first uint64, err error) {
	if len(frames) == 0 {
		return 0, 0, nil
	}
	ch = frames[0].Channel
	if int(ch) >= m.cfg.Channels {
		return 0, 0, fmt.Errorf("codec: mux channel %d out of range [0,%d): %w",
			ch, m.cfg.Channels, ErrChecksum)
	}
	for _, f := range frames {
		if f.Channel != ch {
			return 0, 0, fmt.Errorf("codec: mux run mixes channels %d and %d: %w",
				ch, f.Channel, ErrChecksum)
		}
		if len(f.Payload) > MuxMaxPayload {
			return 0, 0, ErrTooLarge
		}
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	first, err = m.takeTicketsLocked(ch, len(frames))
	if err != nil {
		return 0, 0, err
	}
	return ch, first, nil
}

// commitRun drives a run previously reserved with prepareRun to its
// blocking admission, fragment by fragment. It takes only the mux lock
// (released while parked), so an upper layer must NOT hold its own lock
// across this call: inbound acks must keep flowing while the run waits
// for quota.
func (m *Mux) commitRun(frames []ChannelFrame, ch uint16, ticket uint64) error {
	if len(frames) == 0 {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	for k, f := range frames {
		t := ticket + uint64(k)
		if err := m.awaitSlotLocked(ch, t); err != nil {
			// Unwind this fragment's ticket and every later ticket of the
			// run in one cascade; already-claimed fragments keep theirs.
			m.releaseTicketsLocked(ch, t, len(frames)-k)
			return err
		}
		c := &m.sc[ch]
		qf := queuedFrame{
			channel: ch,
			seq:     c.next,
			flags:   f.Flags,
			kind:    f.Kind,
			payload: append([]byte(nil), f.Payload...),
		}
		if err := m.log.append(qf); err != nil {
			m.releaseTicketsLocked(ch, t, len(frames)-k)
			return err
		}
		c.open = true
		c.next++
		m.queue = append(m.queue, &qf)
		m.claimSlotLocked(ch, t)
	}
	return nil
}

// CloseChannel closes the write side of ch. The FIN is itself a framed
// sequence entry: it consumes one in-flight slot, so CloseChannel parks
// behind the channel's earlier writes and completes once a quota slot is
// free. Closing a channel that was never written opens it first, so an
// unused channel still delivers a clean io.EOF to the peer. Closing (or
// writing to) an already closed channel returns ErrChannelClosed.
//
// If the durable close marker cannot be written, the marker's error is
// returned but the close still takes effect: the FIN is already
// journaled, so the in-memory state and the journal stay consistent and
// a later restart recovers the same closed channel instead of rejecting
// the journal for a duplicated sequence.
func (m *Mux) CloseChannel(ch uint16) error {
	if int(ch) >= m.cfg.Channels {
		return fmt.Errorf("codec: mux channel %d out of range [0,%d): %w",
			ch, m.cfg.Channels, ErrChecksum)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	ticket, err := m.takeTicketsLocked(ch, 1)
	if err != nil {
		return err
	}
	if err := m.awaitSlotLocked(ch, ticket); err != nil {
		m.releaseTicketsLocked(ch, ticket, 1)
		return err
	}

	c := &m.sc[ch]
	qf := queuedFrame{channel: ch, seq: c.next, fin: true}
	if err := m.log.append(qf); err != nil {
		m.claimSlotLocked(ch, ticket)
		return err
	}
	// Durable close marker: once a FIN is acked and the journal prefix
	// that contained it is discarded, this file is the only surviving
	// proof that the channel must never reopen. Written after the FIN
	// record, so a crash between the two leaves the FIN in the journal
	// to make the channel closed on recovery instead. Whether or not the
	// marker lands, the FIN is journaled: the in-memory state must match
	// the journal, otherwise a retried close would journal a second FIN
	// at the same sequence and recovery would reject the journal.
	markerErr := markMuxChannelClosed(m.cfg.StateDir, ch)
	c.open = true
	c.closed = true
	c.next++
	m.queue = append(m.queue, &qf)
	m.claimSlotLocked(ch, ticket)
	return markerErr
}

// Flush sends every queued frame with transmission opportunities handed
// out round-robin across channels: starting where the previous Flush
// stopped, each pass takes at most one queued frame per channel, so a
// continuously busy channel can never starve the others and frames
// never overtake an earlier frame of their own channel. Every data
// frame piggybacks the sender's current receive position for its
// channel; standalone acks for every channel follow the run.
//
// Per-channel reach is that channel's CURRENT adaptive quota (never more
// than the configured window width), so frames parked under a tightened
// quota simply wait for the next acknowledgement instead of overflowing
// the peer. A frame handed to the wire on a later Flush than the first
// one that sent it is recorded as a retransmit; seeing that record when
// the frame is acknowledged is what tightens the channel's quota.
//
// After a restart the queue was reloaded from the journal, so Flush is
// also the retransmit operation; the receiver deduplicates overlaps.
func (m *Mux) Flush() error {
	// Snapshot receive positions BEFORE taking the send lock: the read
	// path takes mu while applying acks (rmu-then-mu order), so mu must
	// never be held while taking rmu.
	m.rmu.Lock()
	acks := make([]uint32, m.cfg.Channels)
	copy(acks, m.bases)
	m.rmu.Unlock()

	m.mu.Lock()
	out := m.buildFlushLocked(acks)
	m.mu.Unlock()

	// Hand the planned run to the reliable session in round-robin
	// order. Its own window can fill (a restart backlog wider than one
	// window, or a slow peer): that is backpressure, not an error. Stop
	// there, flush the accepted prefix and leave every unhanded frame
	// queued for the next Flush; the receiver deduplicates the overlap.
	lastServed := -1
	handed := make([]*queuedFrame, 0, len(out))
	for i := range out {
		err := m.rel.WriteFrame(out[i].rf)
		if errors.Is(err, ErrWindowFull) {
			break
		}
		if err != nil {
			return err
		}
		handed = append(handed, out[i].qf)
		lastServed = int(out[i].qf.channel)
	}
	// Record transmission history while holding the lock: a frame handed
	// to the wire a second time (a Flush retransmit after an earlier
	// Flush already sent it, or a restart backlog re-sent once more) is a
	// retransmit and tightens its channel's quota when it is later
	// acknowledged. Frames the reliable window refused stay unrecorded
	// for this Flush. Marking happens before rel.Flush so a peer ack
	// arriving on another goroutine always observes the history first.
	m.mu.Lock()
	for _, qf := range handed {
		if qf.sent {
			qf.retried = true
		}
		qf.sent = true
	}
	m.mu.Unlock()
	if err := m.rel.Flush(); err != nil {
		return err
	}

	// Advance the round-robin cursor only past channels that actually
	// got a frame onto the wire this Flush, and only after the lower
	// flush succeeded: a failed Flush retries from the same channel.
	if lastServed >= 0 {
		m.mu.Lock()
		m.rr = (lastServed + 1) % m.cfg.Channels
		m.mu.Unlock()
	}
	return nil
}

// plannedFrame pairs one queued frame with the reliable frame built for
// the current Flush, so the sender can mark exactly the frames handed to
// wire.
type plannedFrame struct {
	qf *queuedFrame
	rf ReliableFrame
}

// buildFlushLocked returns the data frames of one Flush in wire order.
// It never mutates the queue, the quotas or the round-robin cursor. The
// caller holds mu.
func (m *Mux) buildFlushLocked(acks []uint32) []plannedFrame {
	out := make([]plannedFrame, 0, len(m.queue))
	// lastSeq remembers, per channel, the sequence already selected in an
	// earlier pass of this Flush: the queue is never mutated here, so the
	// next per-channel pick must be strictly past it.
	lastSeq := make([]uint32, m.cfg.Channels)
	haveSeq := make([]bool, m.cfg.Channels)
	cursor := m.rr
	for {
		startCh := cursor
		taken := false
		for step := 0; step < m.cfg.Channels; step++ {
			ch := uint16((startCh + step) % m.cfg.Channels)
			// The peer accepts exactly this channel's next quota many
			// sequences; frames past the reach wait for an ack, so a
			// restart backlog, or a queue parked under a tightened quota,
			// drains gradually instead of being refused as "too far
			// ahead". Per-channel queue entries are ascending, so the
			// first in-reach entry past the last picked one is the one to
			// send.
			c := &m.sc[ch]
			reach := c.acked + c.quota
			var qf *queuedFrame
			for _, q := range m.queue {
				if q.channel != ch {
					continue
				}
				if haveSeq[ch] && q.seq <= lastSeq[ch] {
					continue // already handed out in an earlier pass
				}
				if q.seq >= reach {
					break
				}
				qf = q
				break
			}
			if qf == nil {
				continue
			}
			lastSeq[ch] = qf.seq
			haveSeq[ch] = true
			flag := uint8(0)
			if qf.fin {
				flag = mhFlagFin
			}
			out = append(out, plannedFrame{
				qf: qf,
				rf: ReliableFrame{
					Flags:   qf.flags,
					Kind:    qf.kind,
					Payload: encodeMuxHeader(qf.channel, qf.seq, flag, acks[qf.channel], qf.payload),
				},
			})
			taken = true
			cursor = int(ch) + 1 // next pass starts after the served channel
		}
		if !taken {
			break
		}
	}
	return out
}

// ReadFrame returns the next deliverable frame across all channels.
// Channels are scanned independently: a channel parked waiting for a
// gap is skipped, so it never stalls another channel. Within a channel
// frames arrive in write order exactly once; out-of-order frames are
// parked until their gap closes.
//
// When a channel's FIN is reached - after every earlier frame of that
// channel was delivered - the call returns io.EOF together with a
// ChannelFrame whose Channel names the closed channel; reads on other
// channels continue unaffected.
//
// The following are refused with the established ErrChecksum and stop
// delivery stickily at the current positions: a channel number outside
// the session's reach, a residual frame older than one channel-window,
// a frame more than one channel-window ahead, an acknowledgement past
// the send frontier, a same-sequence retransmission with different
// content, an unknown flag, an ack frame carrying payload, and a
// damaged mux header. Reliable-layer results (the transport's io.EOF,
// ErrShortFrame, outer checksum/version/size and reader errors) pass
// through unchanged, and complete frames already buffered are delivered
// before a terminal lower-layer result is surfaced.
func (m *Mux) ReadFrame() (ChannelFrame, error) {
	for {
		m.rmu.Lock()
		if m.rerr != nil {
			err := m.rerr
			m.rmu.Unlock()
			return ChannelFrame{}, err
		}
		if cf, eof, ok := m.nextDeliverableLocked(); ok {
			m.rmu.Unlock()
			if eof {
				return cf, io.EOF
			}
			return cf, nil
		}
		m.rmu.Unlock()

		// Block in the reliable reader WITHOUT holding rmu, exactly like
		// the layer below: sends and ack processing must keep moving
		// while this waits for bytes.
		outer, rerr := m.rel.ReadFrame()

		m.rmu.Lock()
		if m.rerr != nil {
			err := m.rerr
			m.rmu.Unlock()
			return ChannelFrame{}, err
		}
		if rerr != nil {
			// The lower layer hands over its terminal result together
			// with any bytes that arrived in the same read; complete
			// frames already parsed below are parked in held. Deliver
			// every buffered frame across all channels first, and only
			// surface the terminal result once the buffer is drained;
			// the result is sticky and returns again on later calls.
			if cf, eof, ok := m.nextDeliverableLocked(); ok {
				m.rmu.Unlock()
				if eof {
					return cf, io.EOF
				}
				return cf, nil
			}
			return m.failReadLocked(rerr)
		}

		ch, seq, flag, ack, payload, err := decodeMuxHeader(outer.Payload)
		if err != nil {
			return m.failReadLocked(err)
		}
		if int(ch) >= m.cfg.Channels {
			return m.failReadLocked(fmt.Errorf(
				"codec: mux channel %d out of range [0,%d): %w",
				ch, m.cfg.Channels, ErrChecksum))
		}
		if flag&^uint8(mhFlagFin|mhFlagAck) != 0 {
			return m.failReadLocked(fmt.Errorf(
				"codec: mux ch %d seq %d unknown flags %#x: %w",
				ch, seq, flag, ErrChecksum))
		}
		isAck := flag&mhFlagAck != 0
		isFin := flag&mhFlagFin != 0
		if isAck && (isFin || seq != 0 || len(payload) != 0) {
			return m.failReadLocked(fmt.Errorf(
				"codec: mux ch %d malformed ack frame (fin=%v seq=%d payload=%d): %w",
				ch, isFin, seq, len(payload), ErrChecksum))
		}
		if isFin && len(payload) != 0 {
			return m.failReadLocked(fmt.Errorf(
				"codec: mux ch %d FIN seq %d carries %d payload bytes: %w",
				ch, seq, len(payload), ErrChecksum))
		}

		// Every inbound message states a cumulative position for its
		// channel; one past anything this side ever sent is impossible
		// in a well-behaved stream and is refused before anything is
		// parked or delivered.
		m.mu.Lock()
		overFrontier := ack > m.sc[ch].next
		frontier := m.sc[ch].next
		m.mu.Unlock()
		if overFrontier {
			return m.failReadLocked(fmt.Errorf(
				"codec: mux ch %d ack %d past send frontier %d: %w",
				ch, ack, frontier, ErrChecksum))
		}
		m.applyAck(ch, ack)

		if isAck {
			m.rmu.Unlock()
			continue
		}

		cf := ChannelFrame{Channel: ch, Flags: outer.Flags, Kind: outer.Kind, Payload: payload}
		fr := inboundFrame{cf: cf, fin: isFin}
		key := muxSeq{ch: ch, seq: seq}
		base := m.bases[ch]

		switch classify(seq, base, m.cfg.Window) {
		case seqInWindow:
			// A closed channel never carries another NEW sequence: its
			// FIN was the stream's last entry. Duplicate retransmits
			// (below the base) fall through to seqDuplicate and are
			// matched against the first delivered copy.
			if m.finEOF[ch] {
				return m.failReadLocked(fmt.Errorf(
					"codec: mux ch %d received new frame seq %d after FIN at %d: %w",
					ch, seq, base, ErrChecksum))
			}
			if prev, dup := m.held[key]; dup {
				// A retransmission of a parked frame must describe the
				// same frame. A same-sequence conflict is refused
				// instead of guessed, and delivery stops here.
				if !sameInbound(prev, fr) {
					return m.failReadLocked(fmt.Errorf(
						"codec: mux ch %d seq %d retransmission conflicts: %w",
						ch, seq, ErrChecksum))
				}
			} else if prev, seen := m.seen[key]; seen {
				// Already delivered once: any conflicting second
				// declaration of the same sequence is a protocol error.
				if !sameInbound(prev, fr) {
					return m.failReadLocked(fmt.Errorf(
						"codec: mux ch %d seq %d duplicate conflicts with delivered frame: %w",
						ch, seq, ErrChecksum))
				}
			} else {
				m.held[key] = fr
			}
			// Restate the channel's cumulative position either way so a
			// lost acknowledgement does not make the peer resend
			// forever.
			m.signalAckLocked(ch)
			m.rmu.Unlock()
			continue // deliver held[base] from the top of the loop
		case seqDuplicate:
			// Behind the base but within one window width. Compare with
			// the first copy delivered for that sequence; an
			// irreconcilable retransmission is refused.
			prev, known := m.seen[key]
			if known && !sameInbound(prev, fr) {
				return m.failReadLocked(fmt.Errorf(
					"codec: mux ch %d seq %d duplicate conflicts with delivered frame: %w",
					ch, seq, ErrChecksum))
			}
			m.signalAckLocked(ch)
			m.rmu.Unlock()
			continue
		default: // seqStale
			return m.failReadLocked(fmt.Errorf(
				"codec: mux ch %d seq %d outside window at %d (width %d): %w",
				ch, seq, base, m.cfg.Window, ErrChecksum))
		}
	}
}

// sameInbound reports whether two arrivals claiming one sequence carry
// the same frame (FIN marking, flags, kind and payload).
func sameInbound(a, b inboundFrame) bool {
	if a.fin != b.fin {
		return false
	}
	if a.fin {
		return true // FIN entries carry no caller frame
	}
	return a.cf.Flags == b.cf.Flags && a.cf.Kind == b.cf.Kind &&
		bytes.Equal(a.cf.Payload, b.cf.Payload)
}

// nextDeliverableLocked finds the lowest-numbered channel whose due
// sequence is parked (a FIN-closed channel is skipped), records the
// delivery and asks for an acknowledgement. It returns eof=true when the
// delivered entry is the channel's FIN: the caller then returns
// (ChannelFrame{Channel: ch}, io.EOF). Caller holds rmu.
func (m *Mux) nextDeliverableLocked() (cf ChannelFrame, eof bool, ok bool) {
	for ch := 0; ch < m.cfg.Channels; ch++ {
		if m.finEOF[ch] {
			continue
		}
		key := muxSeq{ch: uint16(ch), seq: m.bases[ch]}
		fr, present := m.held[key]
		if !present {
			continue
		}
		delete(m.held, key)
		m.bases[ch] = key.seq + 1

		// Remember the delivered frame for conflict checks on its
		// duplicates. Exactly one sequence falls out of the duplicate
		// span per one-step advance (the span ends one window width
		// behind the base), so the seen table stays bounded by
		// Channels*Window with an O(1) prune.
		m.seen[key] = fr
		delete(m.seen, muxSeq{ch: key.ch, seq: key.seq - uint32(m.cfg.Window)})

		m.signalAckLocked(key.ch)
		if fr.fin {
			m.finEOF[ch] = true
			return ChannelFrame{Channel: key.ch}, true, true
		}
		return fr.cf, false, true
	}
	return ChannelFrame{}, false, false
}

// applyAck advances one channel's cumulative send position, releases
// the freed quota slots, adjusts the channel's adaptive quota and
// discards the journal prefix once the new position is durable.
// Positions that do not move the channel forward are ignored. The
// caller holds rmu (rmu-then-mu order).
func (m *Mux) applyAck(ch uint16, ack uint32) {
	m.mu.Lock()
	defer m.mu.Unlock()

	c := &m.sc[ch]
	if ack <= c.acked || ack > c.next {
		return
	}
	advanced := ack - c.acked

	sawRetransmit := false
	kept := m.queue[:0]
	for _, qf := range m.queue {
		if qf.channel == ch && qf.seq < ack {
			if qf.retried {
				sawRetransmit = true
			}
			continue
		}
		kept = append(kept, qf)
	}
	m.queue = kept
	c.acked = ack

	// Adaptive rule, per channel and independent of every other
	// channel:
	//   - timely acks widen the quota by how far the position advanced,
	//     up to the configured window width (and never below the frames
	//     already in flight);
	//   - a retransmitted frame among those just confirmed means the path
	//     needed repair: tighten to exactly the frames still in flight,
	//     reaching the floor of 1 once they drain.
	// The in-flight count can therefore never cross the resulting quota.
	inflight := c.next - c.acked
	width := uint32(m.cfg.Window)
	if sawRetransmit {
		c.quota = inflight
		if c.quota < 1 {
			c.quota = 1
		}
	} else {
		q := c.quota + advanced
		if q < inflight {
			q = inflight
		}
		if q > width {
			q = width
		}
		c.quota = q
	}
	// The recovered/adjusted quota joins the per-channel durable state;
	// a failed write only costs the adjustment since the last checkpoint.
	_ = saveMuxFlow(m.cfg.StateDir, ch, c.quota)

	// Durable per-channel position BEFORE the journal prefix is
	// discarded: a crash between the two only leaves extra frames to
	// dedupe, never a gap.
	if err := saveMuxCheckpoint(m.cfg.StateDir, ch, ack); err == nil && m.log != nil {
		_ = m.log.prefix(m.sc)
	}

	// Released slots and a possibly grown quota unblock exactly the FIFO
	// waiters that can now admit.
	m.cond.Broadcast()
}

// Pressure returns a read-only snapshot of every channel's send-side
// pressure: the admitted in-flight frame count, the current adaptive
// quota and whether a write or close is parked waiting for quota. It
// never blocks admission or transmission.
func (m *Mux) Pressure() PressureReport {
	m.mu.Lock()
	defer m.mu.Unlock()

	rep := PressureReport{Channels: make([]ChannelPressure, m.cfg.Channels)}
	for ch := 0; ch < m.cfg.Channels; ch++ {
		c := &m.sc[ch]
		rep.Channels[ch] = ChannelPressure{
			Channel:  uint16(ch),
			InFlight: int(c.next - c.acked),
			Quota:    int(c.quota),
			Waiting:  c.waiters > 0,
		}
	}
	return rep
}

// signalAckLocked coalescingly asks the background sender to restate
// the channel's cumulative position. It never blocks. Caller holds rmu.
func (m *Mux) signalAckLocked(ch uint16) {
	select {
	case m.standCh <- standAck{ch: ch, next: m.bases[ch]}:
	default:
	}
}

// pumpAck serializes standalone acknowledgement writes so they happen on
// this goroutine, never on a reader's: two peers reading at once cannot
// block each other. Every wakeup drains the whole queue and sends at
// most one cumulative ack per channel stating its latest position, so a
// burst of deliveries or duplicate retransmits costs a handful of
// frames rather than one per arrival.
func (m *Mux) pumpAck() {
	defer close(m.doneCh)
	for {
		select {
		case <-m.stopCh:
			return
		case first := <-m.standCh:
			latest := map[uint16]uint32{first.ch: first.next}
			draining := true
			for draining {
				select {
				case a := <-m.standCh:
					if a.next >= latest[a.ch] {
						latest[a.ch] = a.next
					}
				default:
					draining = false
				}
			}
			// Send in channel order for deterministic wire output.
			for ch := 0; ch < m.cfg.Channels; ch++ {
				next, ok := latest[uint16(ch)]
				if !ok {
					continue
				}
				b := encodeMuxHeader(uint16(ch), 0, mhFlagAck, next, nil)
				if err := m.rel.WriteFrame(ReliableFrame{Payload: b}); err == nil {
					_ = m.rel.Flush()
				}
			}
		}
	}
}

// failReadLocked records the terminal read-side result, releases rmu and
// returns it. Every caller returns immediately after calling it.
func (m *Mux) failReadLocked(err error) (ChannelFrame, error) {
	m.rerr = err
	m.rmu.Unlock()
	return ChannelFrame{}, err
}

// Close stops the background acknowledgement sender and wakes every
// writer parked on quota. Fully journaled but unacknowledged frames
// remain in the mux journal for a later restart to resend.
func (m *Mux) Close() error {
	m.closeOnce.Do(func() {
		close(m.stopCh)
		m.mu.Lock()
		m.closing = true
		m.cond.Broadcast()
		m.mu.Unlock()
	})
	return nil
}

// ----------------------------------------------------------------------
// Mux header encoding.
// ----------------------------------------------------------------------

// encodeMuxHeader serializes one mux message into reliable payload
// bytes.
func encodeMuxHeader(ch uint16, seq uint32, flags uint8, ack uint32, payload []byte) []byte {
	out := make([]byte, MuxHeaderSize+len(payload))
	binary.BigEndian.PutUint16(out[mhChanOff:mhSeqOff], ch)
	binary.BigEndian.PutUint32(out[mhSeqOff:mhFlagOff], seq)
	out[mhFlagOff] = flags
	binary.BigEndian.PutUint32(out[mhAckOff:mhCRCOff], ack)
	copy(out[mhBodyOff:], payload)
	h := crc32.NewIEEE()
	h.Write(out[:mhCRCOff])
	h.Write(out[mhBodyOff:])
	binary.BigEndian.PutUint32(out[mhCRCOff:mhBodyOff], h.Sum32())
	return out
}

// decodeMuxHeader parses one mux message. A message too short for the
// fixed header is reported with ErrShortFrame; a damaged header or
// payload with ErrChecksum.
func decodeMuxHeader(p []byte) (ch uint16, seq uint32, flags uint8, ack uint32, payload []byte, err error) {
	if len(p) < MuxHeaderSize {
		return 0, 0, 0, 0, nil, ErrShortFrame
	}
	h := crc32.NewIEEE()
	h.Write(p[:mhCRCOff])
	h.Write(p[mhBodyOff:])
	if h.Sum32() != binary.BigEndian.Uint32(p[mhCRCOff:mhBodyOff]) {
		return 0, 0, 0, 0, nil, ErrChecksum
	}
	ch = binary.BigEndian.Uint16(p[mhChanOff:mhSeqOff])
	seq = binary.BigEndian.Uint32(p[mhSeqOff:mhFlagOff])
	flags = p[mhFlagOff]
	ack = binary.BigEndian.Uint32(p[mhAckOff:mhCRCOff])
	body := make([]byte, len(p)-MuxHeaderSize)
	copy(body, p[mhBodyOff:])
	return ch, seq, flags, ack, body, nil
}

// ----------------------------------------------------------------------
// Durable state: one dual-slot, checksummed checkpoint file per channel,
// one strict CRC'd adaptive-quota file per channel and one strict CRC'd
// replay journal shared by all channels.
// ----------------------------------------------------------------------

const (
	muxCheckpointPrefix = "mux"
	muxCheckpointSuffix = ".ckp"
	muxFlowSuffix       = ".flow"
	muxJournalName      = "mux.log"

	muxCPRecordSize = 4 + 4 + 4 // generation + ack + crc32
	muxCPSlotSize   = 2 * muxCPRecordSize

	// quota(4) + crc32(4).
	muxFlowSize = 8

	// ch(2) + seq(4) + fin(1) + flags(2) + kind(1) + len(4) + crc(4).
	muxLogHdrSize = 18
)

// muxLogRecord is one journaled mux frame on disk.
type muxLogRecord struct {
	ch    uint16
	seq   uint32
	fin   bool
	flags uint16
	kind  uint8
	f     ChannelFrame
}

// frame rebuilds the queued frame carried by a journal record.
func (r muxLogRecord) frame() queuedFrame {
	return queuedFrame{
		channel: r.ch,
		seq:     r.seq,
		flags:   r.flags,
		kind:    r.kind,
		payload: append([]byte(nil), r.f.Payload...),
		fin:     r.fin,
	}
}

// muxLog is the append-only shared journal.
type muxLog struct {
	path string
	mu   sync.Mutex
	recs []muxLogRecord
}

func muxCheckpointPath(dir string, ch uint16) string {
	return filepath.Join(dir, fmt.Sprintf("%s%03d%s", muxCheckpointPrefix, ch, muxCheckpointSuffix))
}

// muxFlowPath names a channel's durable adaptive-quota file.
func muxFlowPath(dir string, ch uint16) string {
	return filepath.Join(dir, fmt.Sprintf("%s%03d%s", muxCheckpointPrefix, ch, muxFlowSuffix))
}

// loadMuxFlow reads one channel's durable adaptive quota. A missing
// file means "never adjusted" (the caller picks the default). A
// truncated or bit-flipped file fails recovery with ErrChecksum rather
// than resuming the quota from an unverified value.
func loadMuxFlow(dir string, ch uint16) (uint32, bool, error) {
	data, err := os.ReadFile(muxFlowPath(dir, ch))
	if err != nil {
		if os.IsNotExist(err) {
			return 0, false, nil
		}
		return 0, false, err
	}
	if len(data) != muxFlowSize ||
		crc32.ChecksumIEEE(data[:4]) != binary.BigEndian.Uint32(data[4:8]) {
		return 0, false, fmt.Errorf("codec: mux flow state ch %d corrupted: %w", ch, ErrChecksum)
	}
	q := binary.BigEndian.Uint32(data[0:4])
	if q == 0 {
		return 0, false, fmt.Errorf("codec: mux flow state ch %d carries zero quota: %w", ch, ErrChecksum)
	}
	return q, true, nil
}

// saveMuxFlow writes one channel's adjusted quota atomically.
func saveMuxFlow(dir string, ch uint16, quota uint32) error {
	path := muxFlowPath(dir, ch)
	rec := make([]byte, muxFlowSize)
	binary.BigEndian.PutUint32(rec[0:4], quota)
	binary.BigEndian.PutUint32(rec[4:8], crc32.ChecksumIEEE(rec[0:4]))
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(rec); err != nil {
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

// muxCloseSuffix names a channel's durable write-side close marker; the
// file's mere presence records that the FIN was durably issued.
const muxCloseSuffix = ".closed"

func muxClosePath(dir string, ch uint16) string {
	return filepath.Join(dir, fmt.Sprintf("%s%03d%s", muxCheckpointPrefix, ch, muxCloseSuffix))
}

// markMuxChannelClosed durably records that ch's write side is closed.
func markMuxChannelClosed(dir string, ch uint16) error {
	f, err := os.OpenFile(muxClosePath(dir, ch), os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write([]byte("closed\n")); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

// muxChannelClosed reports whether the durable close marker exists.
func muxChannelClosed(dir string, ch uint16) (bool, error) {
	switch _, err := os.Stat(muxClosePath(dir, ch)); {
	case err == nil:
		return true, nil
	case os.IsNotExist(err):
		return false, nil
	default:
		return false, err
	}
}

// loadMuxCheckpoint returns a channel's newest valid checkpointed
// "next expected" position. Missing state, an empty file or records
// that all fail validation mean position 0: with nothing confirmed the
// safe action is to replay what the journal holds and let the receiver
// deduplicate the overlap.
func loadMuxCheckpoint(dir string, ch uint16) (uint32, error) {
	data, err := os.ReadFile(muxCheckpointPath(dir, ch))
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}
	var bestGen, bestAck uint32
	found := false
	for off := 0; off+muxCPRecordSize <= len(data); off += muxCPRecordSize {
		if binary.BigEndian.Uint32(data[off+8:off+12]) != crc32.ChecksumIEEE(data[off:off+8]) {
			continue
		}
		gen := binary.BigEndian.Uint32(data[off : off+4])
		ack := binary.BigEndian.Uint32(data[off+4 : off+8])
		if !found || gen > bestGen || (gen == bestGen && ack > bestAck) {
			bestGen, bestAck, found = gen, ack, true
		}
	}
	if !found {
		return 0, nil
	}
	return bestAck, nil
}

// nextMuxCPSlot chooses the slot to overwrite and the next generation:
// alternate slots, strictly increasing generations.
func nextMuxCPSlot(dir string, ch uint16) (slot int, generation uint32, err error) {
	data, rerr := os.ReadFile(muxCheckpointPath(dir, ch))
	if rerr != nil && !os.IsNotExist(rerr) {
		return 0, 0, rerr
	}
	var gens [2]uint32
	var ok [2]bool
	for i := 0; i < 2; i++ {
		lo, hi := i*muxCPRecordSize, (i+1)*muxCPRecordSize
		if len(data) >= hi &&
			binary.BigEndian.Uint32(data[lo+8:lo+12]) == crc32.ChecksumIEEE(data[lo:lo+8]) {
			gens[i], ok[i] = binary.BigEndian.Uint32(data[lo:lo+4]), true
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

// saveMuxCheckpoint writes one new record into the alternate slot of
// the channel's checkpoint file, leaving the other slot untouched so at
// least the previous complete record always survives.
func saveMuxCheckpoint(dir string, ch uint16, ack uint32) error {
	slot, generation, err := nextMuxCPSlot(dir, ch)
	if err != nil {
		return err
	}
	path := muxCheckpointPath(dir, ch)

	needPad := true
	if info, statErr := os.Stat(path); statErr == nil && info.Size() >= int64(muxCPSlotSize) {
		needPad = false
	}
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	if needPad {
		if _, err := f.WriteAt(make([]byte, muxCPSlotSize), 0); err != nil {
			return err
		}
	}
	rec := make([]byte, muxCPRecordSize)
	binary.BigEndian.PutUint32(rec[0:4], generation)
	binary.BigEndian.PutUint32(rec[4:8], ack)
	binary.BigEndian.PutUint32(rec[8:12], crc32.ChecksumIEEE(rec[0:8]))
	if _, err := f.WriteAt(rec, int64(slot*muxCPRecordSize)); err != nil {
		return err
	}
	return f.Sync()
}

// openMuxLog loads the shared journal strictly.
func openMuxLog(dir string) (*muxLog, error) {
	l := &muxLog{path: filepath.Join(dir, muxJournalName)}
	if err := l.load(); err != nil {
		return nil, err
	}
	return l, nil
}

func (l *muxLog) load() error {
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
		if len(data)-off < muxLogHdrSize {
			return fmt.Errorf("codec: mux journal %s truncated: %w", muxJournalName, ErrChecksum)
		}
		hdr := data[off : off+muxLogHdrSize]
		ch := binary.BigEndian.Uint16(hdr[0:2])
		seq := binary.BigEndian.Uint32(hdr[2:6])
		fin := hdr[6] != 0
		if hdr[6]&^1 != 0 {
			return fmt.Errorf("codec: mux journal %s bad fin flag: %w", muxJournalName, ErrChecksum)
		}
		flags := binary.BigEndian.Uint16(hdr[7:9])
		kind := hdr[9]
		plen := int(binary.BigEndian.Uint32(hdr[10:14]))
		wantCRC := binary.BigEndian.Uint32(hdr[14:18])
		end := off + muxLogHdrSize + plen
		if plen < 0 || end > len(data) {
			return fmt.Errorf("codec: mux journal %s truncated: %w", muxJournalName, ErrChecksum)
		}
		h := crc32.NewIEEE()
		h.Write(hdr[:14])
		h.Write(data[off+muxLogHdrSize : end])
		if h.Sum32() != wantCRC {
			return fmt.Errorf("codec: mux journal %s corrupted: %w", muxJournalName, ErrChecksum)
		}
		payload := make([]byte, plen)
		copy(payload, data[off+muxLogHdrSize:end])
		l.recs = append(l.recs, muxLogRecord{
			ch: ch, seq: seq, fin: fin, flags: flags, kind: kind,
			f: ChannelFrame{Channel: ch, Flags: flags, Kind: kind, Payload: payload},
		})
		off = end
	}
	return nil
}

// append journals one accepted frame as one fully checksummed record.
func (l *muxLog) append(qf queuedFrame) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	hdr := make([]byte, muxLogHdrSize)
	binary.BigEndian.PutUint16(hdr[0:2], qf.channel)
	binary.BigEndian.PutUint32(hdr[2:6], qf.seq)
	if qf.fin {
		hdr[6] = 1
	}
	binary.BigEndian.PutUint16(hdr[7:9], qf.flags)
	hdr[9] = qf.kind
	binary.BigEndian.PutUint32(hdr[10:14], uint32(len(qf.payload)))
	h := crc32.NewIEEE()
	h.Write(hdr[:14])
	h.Write(qf.payload)
	binary.BigEndian.PutUint32(hdr[14:18], h.Sum32())

	file, err := os.OpenFile(l.path, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	// One write of the whole record keeps the on-disk tail either
	// absent or complete from the point of view of a killing crash.
	if _, err := file.Write(append(hdr, qf.payload...)); err != nil {
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

	l.recs = append(l.recs, muxLogRecord{
		ch:    qf.channel,
		seq:   qf.seq,
		fin:   qf.fin,
		flags: qf.flags,
		kind:  qf.kind,
		f: ChannelFrame{
			Channel: qf.channel, Flags: qf.flags, Kind: qf.kind, Payload: qf.payload,
		},
	})
	return nil
}

// prefix discards every journaled frame already at or behind its
// channel's confirmed position, rewriting the journal atomically via
// temp file + rename.
func (l *muxLog) prefix(sc []sendChannel) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	keep := l.recs[:0]
	for _, r := range l.recs {
		if int(r.ch) < len(sc) && r.seq < sc[r.ch].acked {
			continue
		}
		keep = append(keep, r)
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
	writeRec := func(r muxLogRecord) error {
		hdr := make([]byte, muxLogHdrSize)
		binary.BigEndian.PutUint16(hdr[0:2], r.ch)
		binary.BigEndian.PutUint32(hdr[2:6], r.seq)
		if r.fin {
			hdr[6] = 1
		}
		binary.BigEndian.PutUint16(hdr[7:9], r.flags)
		hdr[9] = r.kind
		binary.BigEndian.PutUint32(hdr[10:14], uint32(len(r.f.Payload)))
		h := crc32.NewIEEE()
		h.Write(hdr[:14])
		h.Write(r.f.Payload)
		binary.BigEndian.PutUint32(hdr[14:18], h.Sum32())
		if _, err := f.Write(hdr); err != nil {
			return err
		}
		_, err := f.Write(r.f.Payload)
		return err
	}
	for _, r := range keep {
		if err := writeRec(r); err != nil {
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
