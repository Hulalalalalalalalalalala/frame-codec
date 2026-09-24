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
// A Mux carries several independent logical channels over one reliable
// session. This layer does not touch the wire encoding below it: every
// multiplexed message is carried in the Payload of one reliable data
// frame, prefixed by this layer's own header. The single-frame format,
// the reliable envelope and every established entry, constant and error
// value keep their exact byte-level semantics.
//
// Mux header layout inside a reliable payload (all multi-byte fields
// big-endian):
//
//		+----------+---------+--------+---------+----------+=========+
//		| chan (2) | seq (4) | fl (1) | ack (4) | crc32(4) | payload |
//		+----------+---------+--------+---------+----------+=========+
//
//	  - chan: the channel number the frame belongs to, 0..Channels-1.
//	  - seq: per-channel write sequence, assigned in write order; a pure
//	    acknowledgement carries no data sequence and sets seq to zero.
//	  - fl: bit 0 = FIN, the channel's closing entry (no payload, no
//	    caller flags/kind); bit 1 = pure cumulative acknowledgement. The
//	    two are never combined.
//	  - ack: cumulative "next expected" acknowledgement for chan, carried
//	    on every outbound message: every data frame of chan with seq < ack
//	    has been delivered to the reader.
//	  - crc32: IEEE CRC of the other 11 header bytes plus the payload.
//
// The reliable payload limit stays ReliableMaxPayload; MuxHeaderSize
// bytes are reserved, so the inner payload limit is MuxMaxPayload.
//
// Delivery guarantees:
//   - Frames within one channel are delivered in write order exactly
//     once; channels are independent, so a gap on one channel never
//     blocks another channel's delivery.
//   - The send side hands transmission opportunities out round-robin
//     across channels, so one channel sending continuously can never
//     starve the others.
//   - Every channel has its own in-flight quota; a write that finds the
//     quota full returns the established over-limit value ErrWindowFull
//     and an oversized payload returns ErrTooLarge. A rejected frame is
//     neither queued nor journaled and consumes no sequence, so it takes
//     no quota; the buffered backlog stays bounded by Channels*Window
//     frames.
//   - A channel opens on its first write and its write side can be
//     closed independently; its FIN rides the per-channel sequence
//     stream like a data frame. Once the peer has taken the channel's
//     remaining frames, the read that reaches the FIN returns io.EOF
//     together with a ChannelFrame naming the channel; reads on other
//     channels continue unaffected. Writing to, or closing, an already
//     closed channel returns ErrChannelClosed.
//
// As on the reliable layer, nothing is transmitted before Flush; after a
// restart the queue is reloaded from the journal, so Flush is also the
// retransmit operation.
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

	// Window is the per-channel in-flight quota in frames, enforced
	// independently for every channel; the same width is the receive
	// side's reach when placing inbound sequences. It must be positive.
	Window int

	// StateDir holds the per-channel checkpoint files and the shared
	// replay journal used to resume the write side after a restart. It
	// is created if missing.
	StateDir string
}

// ChannelFrame is one caller-visible multiplexed frame. Channel names
// the channel it belongs to.
type ChannelFrame struct {
	Channel uint16
	Flags   uint16
	Kind    uint8
	Payload []byte
}

// queuedFrame is one accepted send-side entry (a data frame or a FIN)
// waiting for a transmission opportunity or an acknowledgement.
type queuedFrame struct {
	channel uint16
	seq     uint32
	flags   uint16
	kind    uint8
	payload []byte
	fin     bool
}

// sendChannel is the send-side state of one channel.
type sendChannel struct {
	// next is the next sequence an accepted entry gets; acked is the
	// cumulative "next expected" position the peer confirmed. In-flight
	// quota is exactly next-acked because a rejected entry consumes no
	// sequence (the FIN is numbered like a data frame and counts too).
	next   uint32
	acked  uint32
	closed bool // write side closed; the FIN may still be queued
}

// inboundFrame is a parked receive-side entry. A FIN entry carries no
// caller frame.
type inboundFrame struct {
	cf  ChannelFrame
	fin bool
}

// muxSeq identifies one sequence within one channel.
type muxSeq struct {
	ch  uint16
	seq uint32
}

// Mux is a multiplexing layer wrapping one ReliableSession. Create it
// with NewMux; the wrapped session must not be used directly while the
// Mux owns it.
//
// The two directions are independent and may be used concurrently, like
// the layers below. A writer that wants the peer's acknowledgements to
// release channel quota must let ReadFrame run (standalone
// acknowledgements are sent from a small background pump).
type Mux struct {
	rel *ReliableSession
	cfg MuxConfig

	mu sync.Mutex
	// queue holds every accepted, not-yet-fully-acknowledged entry in
	// call-arrival order. Entries already handed to the lower layer stay
	// queued until acknowledged, exactly like the reliable window slots,
	// so a later Flush retransmits them.
	queue []*queuedFrame
	sc    []sendChannel
	// rr is the round-robin cursor: the channel at which the next Flush
	// starts handing out opportunities.
	rr  int
	log *muxLog

	rmu sync.Mutex
	// bases is the per-channel next-due receive sequence. finEOF marks
	// channels whose FIN already reached the reader. seen remembers the
	// first arrival of every sequence inside the duplicate span so a
	// same-sequence retransmission that conflicts with its first copy is
	// refused even after the sequence was delivered.
	bases  []uint32
	held   map[muxSeq]inboundFrame
	seen   map[muxSeq]inboundFrame
	finEOF []bool
	rerr   error

	// standCh coalesces standalone ack requests; its capacity keeps at
	// most one pending position per channel because acks are cumulative.
	standCh chan standAck
	stopCh  chan struct{}
	doneCh  chan struct{}
}

// standAck is one standalone channel acknowledgement handed to the
// background pump.
type standAck struct {
	ch   uint16
	next uint32
}

// NewMux wraps rel in a multiplexing session. Write state in
// cfg.StateDir is consulted when present: a fresh directory starts
// every channel at sequence 0, while a directory left by a crashed or
// restarted process resumes each channel from the earliest confirmed
// position held by its checkpoint and the shared journal.
//
// A truncated journal, a flipped bit in a complete record, or a journal
// that is not strictly contiguous per channel fails recovery with
// ErrChecksum rather than replaying unverified frames.
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
		rel:     rel,
		cfg:     cfg,
		sc:      make([]sendChannel, cfg.Channels),
		bases:   make([]uint32, cfg.Channels),
		held:    make(map[muxSeq]inboundFrame),
		seen:    make(map[muxSeq]inboundFrame),
		finEOF:  make([]bool, cfg.Channels),
		standCh: make(chan standAck, cfg.Channels),
		stopCh:  make(chan struct{}),
		doneCh:  make(chan struct{}),
	}

	// Load the strict shared journal first; reconcile each channel's
	// position against its checkpoint afterwards.
	var err error
	m.log, err = openMuxLog(cfg.StateDir)
	if err != nil {
		return nil, err
	}

	// Per-channel shape derived strictly from journal order: the lowest
	// and one-past-highest sequence, whether a FIN was seen, and strict
	// contiguity. A gap, a repeat or a frame after a FIN would mean a
	// skipped or doubled frame and recovery must refuse to replay it.
	type span struct {
		min, max uint32
		have     bool
		fin      bool
	}
	spans := make([]span, cfg.Channels)
	for _, rec := range m.log.recs {
		if int(rec.ch) >= cfg.Channels {
			return nil, fmt.Errorf("codec: mux journal carries channel %d beyond [0,%d): %w",
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
					"codec: mux journal ch %d has seq %d after FIN: %w",
					rec.ch, rec.seq, ErrChecksum)
			}
		} else {
			sp.min = rec.seq
		}
		sp.have = true
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
			// The checkpoint confirms the whole retained tail.
			c.acked, c.next = cpAck, cpAck
		default:
			// The journal reaches further back than the checkpoint (a
			// crash between checkpoint and prefix discard). Resume at the
			// EARLIER position: the overlap is resent and the receiver
			// deduplicates it; starting later would skip an
			// unacknowledged frame.
			c.acked, c.next = sp.min, sp.max+1
		}
		if sp.fin {
			c.closed = true
		}
	}

	// Rebuild the send queue from the journal in arrival order, dropping
	// entries at or behind their channel's resumed position.
	m.queue = make([]*queuedFrame, 0, len(m.log.recs))
	for i := range m.log.recs {
		rec := m.log.recs[i]
		if rec.seq < m.sc[rec.ch].acked {
			continue
		}
		qf := rec.frame()
		m.queue = append(m.queue, &qf)
	}

	// Start the pump only after every error-return path is past: on a
	// failed NewMux no session (and no pump goroutine) exists.
	go m.pumpAck()
	return m, nil
}

// WriteFrame enqueues f on f.Channel with that channel's next sequence
// and journals it; nothing reaches the wire before Flush.
//
// It returns ErrTooLarge for a payload larger than MuxMaxPayload, the
// established checksum value for a channel number outside the session,
// ErrWindowFull when the channel already holds Window unacknowledged
// entries, and ErrChannelClosed when the channel's write side has been
// closed. A rejected frame is neither queued nor journaled and consumes
// no sequence, so it takes no quota.
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

	c := &m.sc[f.Channel]
	if c.closed {
		return ErrChannelClosed
	}
	if c.next-c.acked >= uint32(m.cfg.Window) {
		return ErrWindowFull
	}

	qf := queuedFrame{
		channel: f.Channel,
		seq:     c.next,
		flags:   f.Flags,
		kind:    f.Kind,
		payload: append([]byte(nil), f.Payload...),
	}
	if err := m.log.append(qf); err != nil {
		return err
	}
	c.next++
	m.queue = append(m.queue, &qf)
	return nil
}

// CloseChannel closes the write side of ch. The FIN is itself a numbered
// sequence entry: it rides the stream after the channel's queued data
// and consumes one in-flight slot, so a full quota makes CloseChannel
// return ErrWindowFull and the caller retries once acknowledgements
// arrive. Closing a channel that was never written opens it with a FIN
// alone, so an unused channel still delivers a clean io.EOF to the peer.
// Closing (or writing to) an already closed channel returns
// ErrChannelClosed.
func (m *Mux) CloseChannel(ch uint16) error {
	if int(ch) >= m.cfg.Channels {
		return fmt.Errorf("codec: mux channel %d out of range [0,%d): %w",
			ch, m.cfg.Channels, ErrChecksum)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	c := &m.sc[ch]
	if c.closed {
		return ErrChannelClosed
	}
	if c.next-c.acked >= uint32(m.cfg.Window) {
		return ErrWindowFull
	}

	qf := queuedFrame{channel: ch, seq: c.next, fin: true}
	if err := m.log.append(qf); err != nil {
		return err
	}
	c.closed = true
	c.next++
	m.queue = append(m.queue, &qf)
	return nil
}

// Flush sends the queued entries with transmission opportunities handed
// out round-robin across channels: starting where the previous Flush
// stopped, every pass takes at most one queued entry per channel, so a
// continuously busy channel can never starve the others and an entry
// can never overtake an earlier entry of its own channel. Every data
// entry piggybacks the sender's current receive position for its
// channel.
//
// When the lower reliable window is full, enqueueing stops there; the
// accepted frames already handed down are still flushed and the rest
// stay queued for a later Flush (backpressure, not a protocol error).
// After a restart the queue was reloaded from the journal, so Flush is
// also the retransmit operation; the receiver deduplicates overlaps.
func (m *Mux) Flush() error {
	// Snapshot receive positions before taking the send lock. The read
	// path takes mu from under rmu (rmu-then-mu order), so mu must never
	// be held while waiting on rmu.
	m.rmu.Lock()
	acks := make([]uint32, m.cfg.Channels)
	copy(acks, m.bases)
	m.rmu.Unlock()

	m.mu.Lock()
	out, cursor := m.buildFlushLocked(acks)
	m.mu.Unlock()

	for i := range out {
		if err := m.rel.WriteFrame(out[i]); errors.Is(err, ErrWindowFull) {
			break
		} else if err != nil {
			return err
		}
	}
	if err := m.rel.Flush(); err != nil {
		return err
	}

	// Advance the round-robin cursor only after the run was handed down:
	// a failed Flush retries starting at the same channel.
	m.mu.Lock()
	m.rr = cursor
	m.mu.Unlock()
	return nil
}

// buildFlushLocked returns one Flush's entries in wire order and the
// cursor the next Flush starts at. It never mutates the queue. The
// caller holds mu.
func (m *Mux) buildFlushLocked(acks []uint32) ([]ReliableFrame, int) {
	pending := make([]*queuedFrame, len(m.queue))
	copy(pending, m.queue)

	out := make([]ReliableFrame, 0, len(pending))
	cursor := m.rr
	for {
		served := false
		last := -1
		// One pass: visit every channel once, beginning at cursor, and
		// take its oldest queued entry.
		for step := 0; step < m.cfg.Channels; step++ {
			ch := uint16((cursor + step) % m.cfg.Channels)
			idx := -1
			for i, qf := range pending {
				if qf.channel == ch {
					idx = i
					break
				}
			}
			if idx < 0 {
				continue
			}
			qf := pending[idx]
			pending = append(pending[:idx], pending[idx+1:]...)

			var fl uint8
			if qf.fin {
				fl = mhFlagFin
			}
			out = append(out, ReliableFrame{
				Flags:   qf.flags,
				Kind:    qf.kind,
				Payload: encodeMuxHeader(qf.channel, qf.seq, fl, acks[qf.channel], qf.payload),
			})
			served = true
			last = int(ch)
		}
		if !served {
			break
		}
		// The next pass starts after this pass's last served channel,
		// which keeps a continuously busy channel from taking two turns
		// before any other channel gets one.
		cursor = (last + 1) % m.cfg.Channels
	}
	return out, cursor
}

// ReadFrame returns the next deliverable frame across all channels.
// Channels are scanned independently: a channel parked on a gap is
// skipped, so it never stalls delivery on another channel. Within one
// channel entries are delivered in write order exactly once;
// out-of-order arrivals are parked until their gap closes.
//
// When a channel's FIN is reached - after every earlier entry of that
// channel was delivered - the call returns (ChannelFrame{Channel: ch},
// io.EOF); reads on other channels continue unaffected.
//
// The following are refused with the established ErrChecksum and stop
// delivery stickily at the current positions: a channel number outside
// the session's reach, a residual frame older than one channel-window,
// a frame more than one channel-window ahead, an acknowledgement past
// the send frontier, a same-sequence retransmission carrying different
// content, an unknown flag, a malformed acknowledgement, a FIN carrying
// payload, and a damaged mux header (a header too short to parse is
// ErrShortFrame). Reliable-layer results (the transport's io.EOF,
// ErrShortFrame, outer checksum/version/size and reader errors) pass
// through unchanged. Complete frames already buffered are delivered
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
		// the layer below: sends and acknowledgement processing must keep
		// moving while this waits for bytes.
		outer, rerr := m.rel.ReadFrame()

		m.rmu.Lock()
		if m.rerr != nil {
			err := m.rerr
			m.rmu.Unlock()
			return ChannelFrame{}, err
		}
		if rerr != nil {
			// One lower-layer read can carry its final complete frame and
			// its terminal result together at the session boundary; parsed
			// entries parked in held are buffered complete frames. Drain
			// them across every channel first, then surface the terminal
			// result; it is sticky and returns again on later calls.
			if cf, eof, ok := m.nextDeliverableLocked(); ok {
				m.rmu.Unlock()
				if eof {
					return cf, io.EOF
				}
				return cf, nil
			}
			return m.failReadLocked(rerr)
		}

		ch, seq, fl, ack, payload, err := decodeMuxHeader(outer.Payload)
		if err != nil {
			return m.failReadLocked(err)
		}
		if int(ch) >= m.cfg.Channels {
			return m.failReadLocked(fmt.Errorf(
				"codec: mux channel %d out of range [0,%d): %w",
				ch, m.cfg.Channels, ErrChecksum))
		}
		if fl&^uint8(mhFlagFin|mhFlagAck) != 0 {
			return m.failReadLocked(fmt.Errorf(
				"codec: mux ch %d seq %d unknown flags %#x: %w",
				ch, seq, fl, ErrChecksum))
		}
		isAck := fl&mhFlagAck != 0
		isFin := fl&mhFlagFin != 0
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
		// channel; one past anything this side ever sent is impossible in
		// a well-behaved stream and is refused before anything is parked
		// or delivered.
		m.mu.Lock()
		frontier := m.sc[ch].next
		overFrontier := ack > frontier
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

		fr := inboundFrame{
			cf: ChannelFrame{
				Channel: ch,
				Flags:   outer.Flags,
				Kind:    outer.Kind,
				Payload: payload,
			},
			fin: isFin,
		}
		key := muxSeq{ch: ch, seq: seq}
		base := m.bases[ch]
		switch classify(seq, base, m.cfg.Window) {
		case seqInWindow:
			if prev, dup := m.held[key]; dup {
				// A retransmission of a parked entry must describe the
				// same entry; a same-sequence conflict is refused
				// instead of guessed, and delivery stops here.
				if !sameInbound(prev, fr) {
					return m.failReadLocked(fmt.Errorf(
						"codec: mux ch %d seq %d retransmission conflicts with parked entry: %w",
						ch, seq, ErrChecksum))
				}
			} else if prev, seen := m.seen[key]; seen {
				if !sameInbound(prev, fr) {
					return m.failReadLocked(fmt.Errorf(
						"codec: mux ch %d seq %d retransmission conflicts with delivered entry: %w",
						ch, seq, ErrChecksum))
				}
			} else {
				m.held[key] = fr
			}
			// Restate the channel's cumulative position either way (a
			// fresh entry extends it only once delivered in order; a
			// duplicate needs it so the peer stops resending).
			m.signalAckLocked(ch)
			m.rmu.Unlock()
			continue // deliver held[base] from the top of the loop
		case seqDuplicate:
			// Behind the base but within one window width. A conflicting
			// re-delivery of an already delivered sequence is refused;
			// an identical one is absorbed and re-acknowledged.
			if prev, known := m.seen[key]; known && !sameInbound(prev, fr) {
				return m.failReadLocked(fmt.Errorf(
					"codec: mux ch %d seq %d duplicate conflicts with delivered entry: %w",
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

// sameInbound reports whether two arrivals claiming one sequence
// describe the same entry (FIN marking; otherwise flags, kind and
// payload).
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

// nextDeliverableLocked scans channels in number order and delivers the
// due entry of the first channel that has one parked. It records the
// delivery, maintains the duplicate-conflict memory and asks for an
// acknowledgement. It returns eof=true when the delivered entry is that
// channel's FIN; the caller then returns (ChannelFrame{Channel: ch},
// io.EOF). Caller holds rmu.
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

		// Remember the delivered entry for duplicate conflict checks and
		// forget sequences now outside the duplicate span, so the table
		// stays bounded by Channels*Window entries.
		m.seen[key] = fr
		base := m.bases[ch]
		for old := range m.seen {
			if old.ch == key.ch && base-old.seq > uint32(m.cfg.Window) {
				delete(m.seen, old)
			}
		}

		m.signalAckLocked(key.ch)
		if fr.fin {
			m.finEOF[ch] = true
			return ChannelFrame{Channel: key.ch}, true, true
		}
		return fr.cf, false, true
	}
	return ChannelFrame{}, false, false
}

// applyAck advances one channel's cumulative send position, releases the
// freed quota by dropping acknowledged entries from the queue and, once
// the new position is durable, discards the journal prefix. Positions
// that do not move the channel forward are ignored. Called with rmu
// held; takes mu (rmu-then-mu order).
func (m *Mux) applyAck(ch uint16, ack uint32) {
	m.mu.Lock()
	defer m.mu.Unlock()

	c := &m.sc[ch]
	if ack <= c.acked || ack > c.next {
		return
	}

	kept := m.queue[:0]
	for _, qf := range m.queue {
		if qf.channel == ch && qf.seq < ack {
			continue
		}
		kept = append(kept, qf)
	}
	m.queue = kept
	c.acked = ack

	// Make the position durable BEFORE discarding the journal prefix: a
	// crash in between only leaves extra entries to dedupe, never a gap.
	if err := saveMuxCheckpoint(m.cfg.StateDir, ch, ack); err == nil && m.log != nil {
		_ = m.log.prefix(m.sc)
	}
}

// signalAckLocked coalescingly asks the background pump to restate the
// channel's cumulative delivered position. It never blocks and never
// writes from under the read lock. Caller holds rmu.
func (m *Mux) signalAckLocked(ch uint16) {
	select {
	case m.standCh <- standAck{ch: ch, next: m.bases[ch]}:
	default:
	}
}

// pumpAck serializes standalone acknowledgement writes off the caller's
// read goroutine, which is what lets two peers read simultaneously
// without blocking each other. One poke sends at most one ack stating
// the latest position; the bounded channel coalesces bursts because acks
// are cumulative.
func (m *Mux) pumpAck() {
	defer close(m.doneCh)
	for {
		select {
		case <-m.stopCh:
			return
		case a := <-m.standCh:
			b := encodeMuxHeader(a.ch, 0, mhFlagAck, a.next, nil)
			// Best effort: a failed or blocked standalone ack is
			// harmless because every later data frame piggybacks the
			// same cumulative position.
			if err := m.rel.WriteFrame(ReliableFrame{Payload: b}); err == nil {
				_ = m.rel.Flush()
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

// Close stops the background acknowledgement pump; it is idempotent.
// Fully journaled but unacknowledged entries remain in the journal for a
// later restart to resend.
func (m *Mux) Close() error {
	select {
	case <-m.stopCh:
	default:
		close(m.stopCh)
	}
	return nil
}

// ----------------------------------------------------------------------
// Mux header encoding.
// ----------------------------------------------------------------------

// encodeMuxHeader serializes one mux message into reliable payload
// bytes.
func encodeMuxHeader(ch uint16, seq uint32, fl uint8, ack uint32, payload []byte) []byte {
	out := make([]byte, MuxHeaderSize+len(payload))
	binary.BigEndian.PutUint16(out[mhChanOff:mhSeqOff], ch)
	binary.BigEndian.PutUint32(out[mhSeqOff:mhFlagOff], seq)
	out[mhFlagOff] = fl
	binary.BigEndian.PutUint32(out[mhAckOff:mhCRCOff], ack)
	copy(out[mhBodyOff:], payload)
	h := crc32.NewIEEE()
	h.Write(out[:mhCRCOff])
	h.Write(out[mhBodyOff:])
	binary.BigEndian.PutUint32(out[mhCRCOff:mhBodyOff], h.Sum32())
	return out
}

// decodeMuxHeader parses one mux message. Bytes too short for the fixed
// header are reported with the established short-frame value; a damaged
// header or payload with the established checksum value; nothing is
// delivered from either.
func decodeMuxHeader(p []byte) (ch uint16, seq uint32, fl uint8, ack uint32, payload []byte, err error) {
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
	fl = p[mhFlagOff]
	ack = binary.BigEndian.Uint32(p[mhAckOff:mhCRCOff])
	body := make([]byte, len(p)-MuxHeaderSize)
	copy(body, p[mhBodyOff:])
	return ch, seq, fl, ack, body, nil
}

// ----------------------------------------------------------------------
// Durable state: one dual-slot, checksummed checkpoint file per channel
// and one strict, CRC'd replay journal shared by all channels.
// ----------------------------------------------------------------------

const (
	muxCheckpointPrefix = "mux"
	muxCheckpointSuffix = ".ckp"
	muxJournalName      = "mux.log"

	muxCPRecordSize = 4 + 4 + 4 // generation + ack + crc32
	muxCPSlotSize   = 2 * muxCPRecordSize

	// ch(2) + seq(4) + fin(1) + flags(2) + kind(1) + len(4) + crc(4).
	muxLogHdrSize = 2 + 4 + 1 + 2 + 1 + 4 + 4
)

// muxLogRecord is one journaled entry on disk.
type muxLogRecord struct {
	ch    uint16
	seq   uint32
	fin   bool
	flags uint16
	kind  uint8
	f     ChannelFrame
}

// frame rebuilds the queued entry carried by a journal record.
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

type muxLog struct {
	path string
	mu   sync.Mutex
	recs []muxLogRecord
}

func muxCheckpointPath(dir string, ch uint16) string {
	return filepath.Join(dir, fmt.Sprintf("%s%03d%s", muxCheckpointPrefix, ch, muxCheckpointSuffix))
}

// loadMuxCheckpoint returns a channel's newest valid checkpointed
// "next expected" position. A missing file, an empty file or records
// that all fail validation mean position 0: with nothing confirmed, the
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

// saveMuxCheckpoint writes one new record into the alternate slot of the
// channel's checkpoint file; the other slot is untouched, so at every
// durable instant at least the previous complete record survives.
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
		finByte := hdr[6]
		if finByte&^1 != 0 {
			return fmt.Errorf("codec: mux journal %s bad fin marker: %w", muxJournalName, ErrChecksum)
		}
		fin := finByte != 0
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
			ch:    ch,
			seq:   seq,
			fin:   fin,
			flags: flags,
			kind:  kind,
			f:     ChannelFrame{Channel: ch, Flags: flags, Kind: kind, Payload: payload},
		})
		off = end
	}
	return nil
}

// append journals one accepted entry as a single fully checksummed
// record.
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

	f, err := os.OpenFile(l.path, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	// One write of the whole record keeps the on-disk tail either absent
	// or complete from the point of view of a killing crash.
	if _, err := f.Write(append(hdr, qf.payload...)); err != nil {
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

	l.recs = append(l.recs, muxLogRecord{
		ch:    qf.channel,
		seq:   qf.seq,
		fin:   qf.fin,
		flags: qf.flags,
		kind:  qf.kind,
		f: ChannelFrame{
			Channel: qf.channel,
			Flags:   qf.flags,
			Kind:    qf.kind,
			Payload: qf.payload,
		},
	})
	return nil
}

// prefix discards every journaled DATA entry at or behind its channel's
// confirmed position, rewriting the journal atomically via temp file
// plus rename. A FIN entry is retained even after confirmation: it is
// the only durable record that the channel's write side closed, so the
// fact must survive a restart that dropped the checkpoint past it. At
// most one FIN per channel ever exists, so the retained tail stays
// bounded by the number of channels.
func (l *muxLog) prefix(sc []sendChannel) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	keep := l.recs[:0]
	for _, r := range l.recs {
		if !r.fin && int(r.ch) < len(sc) && r.seq < sc[r.ch].acked {
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
