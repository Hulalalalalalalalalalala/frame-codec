package codec

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

// ---- deterministic harness -------------------------------------------------

// feed is a blocking byte queue used as the underlying reader in
// unidirectional tests: Read blocks while it is empty and open.
type feed struct {
	mu     sync.Mutex
	cond   *sync.Cond
	data   []byte
	closed bool
}

// closedReader is a read end that is already at EOF, for send-only
// sessions whose inbound direction is never used.
type closedReader struct{}

func (closedReader) Read([]byte) (int, error) { return 0, io.EOF }

func newFeed() *feed {
	f := &feed{}
	f.cond = sync.NewCond(&f.mu)
	return f
}

func (f *feed) replace(p []byte) {
	f.mu.Lock()
	f.data = append(f.data[:0], p...)
	f.cond.Broadcast()
	f.mu.Unlock()
}

func (f *feed) closeFeed() {
	f.mu.Lock()
	f.closed = true
	f.cond.Broadcast()
	f.mu.Unlock()
}

func (f *feed) Read(p []byte) (int, error) {
	f.mu.Lock()
	for len(f.data) == 0 && !f.closed {
		f.cond.Wait()
	}
	if len(f.data) > 0 {
		n := copy(p, f.data)
		f.data = f.data[n:]
		f.mu.Unlock()
		return n, nil
	}
	f.mu.Unlock()
	return 0, io.EOF
}

// newSender builds a reliable writer whose flushed bytes land in buf.
// Its read side goes to io.Discard because these tests drive acks
// deterministically.
func newSender(t *testing.T, buf *bytes.Buffer, window int, dir string) *ReliableSession {
	t.Helper()
	rs, err := NewReliable(NewSession(buf, closedReader{}), ReliableConfig{Window: window, StateDir: dir})
	if err != nil {
		t.Fatalf("NewReliable sender: %v", err)
	}
	t.Cleanup(func() { rs.Close() })
	return rs
}

// newReceiver builds a reliable reader over f. Standalone acks from its
// pump go to io.Discard.
func newReceiver(t *testing.T, f *feed, window int, dir string) *ReliableSession {
	t.Helper()
	rs, err := NewReliable(NewSession(io.Discard, f), ReliableConfig{Window: window, StateDir: dir})
	if err != nil {
		t.Fatalf("NewReliable receiver: %v", err)
	}
	t.Cleanup(func() { rs.Close() })
	return rs
}

// envWire serializes reliable envelopes into a concatenated outer wire.
func envWire(t *testing.T, es ...envelope) []byte {
	t.Helper()
	var buf bytes.Buffer
	s := NewSession(&buf, nil)
	for _, e := range es {
		b, err := encodeEnvelope(e)
		if err != nil {
			t.Fatalf("encodeEnvelope: %v", err)
		}
		if err := s.WriteFrame(Frame{Kind: outerKind, Payload: b}); err != nil {
			t.Fatalf("WriteFrame: %v", err)
		}
	}
	if err := s.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	return buf.Bytes()
}

func dataEnv(seq uint32, ack uint32, f ReliableFrame) envelope {
	return envelope{seq: seq, ack: ack, typ: envTypeData, flags: f.Flags, kind: f.Kind, payload: f.Payload}
}

func drainReliable(t *testing.T, rs *ReliableSession, want int) []ReliableFrame {
	t.Helper()
	got := make([]ReliableFrame, 0, want)
	for i := 0; i < want; i++ {
		f, err := rs.ReadFrame()
		if err != nil {
			t.Fatalf("ReadFrame %d: %v", i, err)
		}
		got = append(got, f)
	}
	return got
}

func assertFrames(t *testing.T, got []ReliableFrame, want []ReliableFrame) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("got %d frames, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i].Flags != want[i].Flags || got[i].Kind != want[i].Kind ||
			!bytes.Equal(got[i].Payload, want[i].Payload) {
			t.Fatalf("frame %d = {flags=%d kind=%d payload=%q}, want {flags=%d kind=%d payload=%q}",
				i, got[i].Flags, got[i].Kind, got[i].Payload,
				want[i].Flags, want[i].Kind, want[i].Payload)
		}
	}
}

// parseWireEnvelopes decodes flushed sender bytes back into envelopes.
func parseWireEnvelopes(t *testing.T, wire []byte) []envelope {
	t.Helper()
	s := NewSession(nil, bytes.NewReader(wire))
	var out []envelope
	for {
		fr, err := s.ReadFrame()
		if errors.Is(err, io.EOF) {
			return out
		}
		if err != nil {
			t.Fatalf("outer ReadFrame: %v", err)
		}
		e, err := decodeEnvelope(fr.Payload)
		if err != nil {
			t.Fatalf("decodeEnvelope: %v", err)
		}
		out = append(out, e)
	}
}

func dataSeqs(es []envelope) []uint32 {
	var out []uint32
	for _, e := range es {
		if e.typ == envTypeData {
			out = append(out, e.seq)
		}
	}
	return out
}

// ---- requirement tests -----------------------------------------------------

func TestReliableOrderingFlagsKindPayload(t *testing.T) {
	dir := t.TempDir()
	var wire bytes.Buffer
	snd := newSender(t, &wire, 8, dir)

	in := []ReliableFrame{
		{Flags: 0x0001, Kind: 7, Payload: []byte("first")},
		{Flags: 0xBEEF, Kind: 200, Payload: nil}, // empty payload
		{Flags: 0, Kind: 0, Payload: []byte{}},   // explicit empty
		{Flags: 0x0002, Kind: 254, Payload: []byte("fourth")},
	}
	for _, f := range in {
		if err := snd.WriteFrame(f); err != nil {
			t.Fatalf("WriteFrame: %v", err)
		}
	}
	if err := snd.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	// The underlying bytes are ordinary codec frames: the wire encoding
	// of the lower layer is untouched, and data seqs are 0,1,2,3.
	if got := dataSeqs(parseWireEnvelopes(t, wire.Bytes())); fmt.Sprint(got) != "[0 1 2 3]" {
		t.Fatalf("data seqs = %v, want [0 1 2 3]", got)
	}

	f := newFeed()
	rcv := newReceiver(t, f, 8, t.TempDir())
	f.replace(wire.Bytes())
	got := drainReliable(t, rcv, len(in))
	assertFrames(t, got, in)

	// Clean end of the stream: no more frames, io.EOF, sticky.
	f.closeFeed()
	if _, err := rcv.ReadFrame(); !errors.Is(err, io.EOF) {
		t.Fatalf("err = %v, want io.EOF", err)
	}
	if _, err := rcv.ReadFrame(); !errors.Is(err, io.EOF) {
		t.Fatalf("second err = %v, want sticky io.EOF", err)
	}
}

func TestReliableEmptyStream(t *testing.T) {
	// Zero frames, zero bytes: reading simply stops at io.EOF.
	f := newFeed()
	rcv := newReceiver(t, f, 4, t.TempDir())
	f.closeFeed()
	if _, err := rcv.ReadFrame(); !errors.Is(err, io.EOF) {
		t.Fatalf("err = %v, want io.EOF on a zero-frame empty stream", err)
	}
}

func TestReliableWindowFullIsOverLimitAndDropsFrame(t *testing.T) {
	const W = 3
	dir := t.TempDir()
	var wire bytes.Buffer
	snd := newSender(t, &wire, W, dir)

	for i := 0; i < W; i++ {
		if err := snd.WriteFrame(ReliableFrame{Kind: uint8(i), Payload: []byte{byte('a' + i)}}); err != nil {
			t.Fatalf("WriteFrame %d: %v", i, err)
		}
	}
	// Window exactly full: the next write returns the established
	// over-limit value (the same sentinel ErrTooLarge) and is dropped.
	err := snd.WriteFrame(ReliableFrame{Kind: 99, Payload: []byte("dropped")})
	if !errors.Is(err, ErrWindowFull) {
		t.Fatalf("err = %v, want ErrWindowFull", err)
	}
	if !errors.Is(err, ErrTooLarge) {
		t.Fatalf("ErrWindowFull must be the established over-limit value ErrTooLarge, got %v", err)
	}
	if err := snd.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	f := newFeed()
	rcv := newReceiver(t, f, W, t.TempDir())
	f.replace(wire.Bytes())
	got := drainReliable(t, rcv, W)
	if len(got) != W || got[W-1].Kind != uint8(W-1) {
		t.Fatalf("got %+v, want exactly the %d accepted frames", got, W)
	}

	// The rejected frame consumed no sequence: after the window drains
	// the next write numbers W and flows normally.
	snd.applyAck(W) // deterministic "all three delivered" acknowledgement
	if err := snd.WriteFrame(ReliableFrame{Kind: 99, Payload: []byte("after")}); err != nil {
		t.Fatalf("WriteFrame after ack: %v", err)
	}
	wire.Reset()
	if err := snd.Flush(); err != nil {
		t.Fatalf("Flush 2: %v", err)
	}
	for _, e := range parseWireEnvelopes(t, wire.Bytes()) {
		if e.typ == envTypeData && e.seq != uint32(W) {
			t.Fatalf("new frame seq = %d, want %d (rejected write consumed no seq)", e.seq, W)
		}
	}
}

func TestReliablePayloadLimits(t *testing.T) {
	dir := t.TempDir()
	var wire bytes.Buffer
	snd := newSender(t, &wire, 4, dir)

	if err := snd.WriteFrame(ReliableFrame{Payload: make([]byte, ReliableMaxPayload)}); err != nil {
		t.Fatalf("ReliableMaxPayload frame rejected: %v", err)
	}
	if err := snd.WriteFrame(ReliableFrame{Payload: make([]byte, ReliableMaxPayload+1)}); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("err = %v, want ErrTooLarge", err)
	}
	if err := snd.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	f := newFeed()
	rcv := newReceiver(t, f, 4, t.TempDir())
	f.replace(wire.Bytes())
	got, err := rcv.ReadFrame()
	if err != nil {
		t.Fatalf("ReadFrame: %v", err)
	}
	if len(got.Payload) != ReliableMaxPayload {
		t.Fatalf("payload len = %d, want %d", len(got.Payload), ReliableMaxPayload)
	}
}

func TestReliableDuplicateResendDeliveredOnce(t *testing.T) {
	// A lost acknowledgement makes the sender replay its whole window;
	// the same wire reaches the receiver twice. Every frame is still
	// delivered exactly once and in order.
	dir := t.TempDir()
	var wire bytes.Buffer
	snd := newSender(t, &wire, 8, dir)

	in := []ReliableFrame{
		{Kind: 1, Payload: []byte("a")},
		{Kind: 2, Payload: []byte("bb")},
		{Kind: 3, Payload: []byte("ccc")},
		{Kind: 4, Payload: nil}, // empty payload amid retransmits
	}
	for _, fr := range in {
		if err := snd.WriteFrame(fr); err != nil {
			t.Fatalf("WriteFrame: %v", err)
		}
	}
	if err := snd.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	f := newFeed()
	rcv := newReceiver(t, f, 8, t.TempDir())
	dup := append(append([]byte(nil), wire.Bytes()...), wire.Bytes()...) // original + resend
	f.replace(dup)
	got := drainReliable(t, rcv, len(in))
	assertFrames(t, got, in)

	// Exactly one delivery per sequence even though each arrived twice:
	// reading on must hit EOF, not an extra frame.
	f.closeFeed()
	if _, err := rcv.ReadFrame(); !errors.Is(err, io.EOF) {
		t.Fatalf("after duplicates err = %v, want io.EOF, got an extra delivery", err)
	}
}

func TestReliableOutOfOrderParkedThenDelivered(t *testing.T) {
	frames := []ReliableFrame{
		{Kind: 1, Payload: []byte("zero")},
		{Kind: 2, Payload: []byte("one")},
		{Kind: 3, Payload: []byte("two")},
	}
	// Arrival order 2, 0, 1; delivery order must be 0, 1, 2.
	wire := envWire(t,
		dataEnv(2, 0, frames[2]),
		dataEnv(0, 0, frames[0]),
		dataEnv(1, 0, frames[1]),
	)
	f := newFeed()
	rcv := newReceiver(t, f, 8, t.TempDir())
	f.replace(wire)
	got := drainReliable(t, rcv, 3)
	assertFrames(t, got, frames)
}

func TestReliableRetransmissionOfParkedFrameDeliveredOnce(t *testing.T) {
	// seq0 is missing; seq1 arrives and is parked, then is resent
	// (duplicate while parked), then seq0 closes the gap: seq1 is
	// delivered exactly once.
	f0 := ReliableFrame{Kind: 0, Payload: []byte("zero")}
	f1 := ReliableFrame{Kind: 1, Payload: []byte("one")}
	wire := envWire(t,
		dataEnv(1, 0, f1),
		dataEnv(1, 0, f1), // resend while parked
		dataEnv(0, 0, f0),
	)
	f := newFeed()
	rcv := newReceiver(t, f, 8, t.TempDir())
	f.replace(wire)
	got := drainReliable(t, rcv, 2)
	assertFrames(t, got, []ReliableFrame{f0, f1})
	f.closeFeed()
	if _, err := rcv.ReadFrame(); !errors.Is(err, io.EOF) {
		t.Fatalf("err = %v, want io.EOF (parked resend delivered once)", err)
	}
}

func TestReliableRejectBeyondWindowAndAncientResidual(t *testing.T) {
	const W = 4
	// After frame 0 is delivered recvBase is 1; a frame one window width
	// past that new position (seq 1+W) is refused and delivery stops at
	// the current position.
	wire := envWire(t,
		dataEnv(0, 0, ReliableFrame{Kind: 0, Payload: []byte("ok0")}),
		dataEnv(1+W, 0, ReliableFrame{Kind: 1 + W, Payload: []byte("too-far")}),
	)
	f := newFeed()
	rcv := newReceiver(t, f, W, t.TempDir())
	f.replace(wire)
	got, err := rcv.ReadFrame()
	if err != nil || string(got.Payload) != "ok0" {
		t.Fatalf("frame 0 = %+v, %v", got, err)
	}
	if _, err := rcv.ReadFrame(); !errors.Is(err, ErrChecksum) {
		t.Fatalf("err = %v, want ErrChecksum for seq %d past window at recvBase 1 (width %d)", err, 1+W, W)
	}
	// Sticky: later bytes are never delivered, even a now-valid frame.
	f.replace(envWire(t, dataEnv(1, 0, ReliableFrame{Kind: 1, Payload: []byte("next")})))
	if _, err := rcv.ReadFrame(); !errors.Is(err, ErrChecksum) {
		t.Fatalf("second err = %v, want sticky ErrChecksum", err)
	}

	// A declared sequence exactly one width ahead with nothing delivered
	// is refused immediately as well.
	early := envWire(t, dataEnv(W, 0, ReliableFrame{Kind: W, Payload: []byte("far")}))
	fe := newFeed()
	rcve := newReceiver(t, fe, W, t.TempDir())
	fe.replace(early)
	if _, err := rcve.ReadFrame(); !errors.Is(err, ErrChecksum) {
		t.Fatalf("early too-far seq err = %v, want ErrChecksum", err)
	}

	// An ancient residual, older than one window width, is refused too.
	old := envWire(t, dataEnv(0, 0, ReliableFrame{Kind: 0, Payload: []byte("ancient")}))
	f2 := newFeed()
	rcv2 := newReceiver(t, f2, W, t.TempDir())
	rcv2.recvBase.Store(8) // window [8,12); duplicate span [4,8); seq0 older
	f2.replace(old)
	if _, err := rcv2.ReadFrame(); !errors.Is(err, ErrChecksum) {
		t.Fatalf("ancient seq err = %v, want ErrChecksum", err)
	}

	// But legitimate retransmits inside the duplicate span (seq 4..7)
	// are silently absorbed, not refused, and delivered zero times.
	good := newFeed()
	rcv3 := newReceiver(t, good, W, t.TempDir())
	rcv3.recvBase.Store(8)
	good.replace(envWire(t,
		dataEnv(4, 0, ReliableFrame{Kind: 4}),
		dataEnv(7, 0, ReliableFrame{Kind: 7}),
	))
	good.closeFeed()
	if _, err := rcv3.ReadFrame(); !errors.Is(err, io.EOF) {
		t.Fatalf("in-span duplicates err = %v, want io.EOF with no delivery", err)
	}
}

func TestReliableSequenceWraparound(t *testing.T) {
	const W = 4
	f := newFeed()
	rcv := newReceiver(t, f, W, t.TempDir())
	rcv.recvBase.Store(0xFFFFFFFE)
	// Three in-order frames across the boundary.
	f.replace(envWire(t,
		dataEnv(0xFFFFFFFE, 0, ReliableFrame{Kind: 0xFE, Payload: []byte("fe")}),
		dataEnv(0xFFFFFFFF, 0, ReliableFrame{Kind: 0xFF, Payload: []byte("ff")}),
		dataEnv(0x00000000, 0, ReliableFrame{Kind: 0x00, Payload: []byte("00")}),
	))
	g0, err := rcv.ReadFrame()
	if err != nil || g0.Kind != 0xFE {
		t.Fatalf("wrap frame FFFFFFFE = %+v, %v", g0, err)
	}
	g1, err := rcv.ReadFrame()
	if err != nil || g1.Kind != 0xFF {
		t.Fatalf("wrap frame FFFFFFFF = %+v, %v", g1, err)
	}
	g2, err := rcv.ReadFrame()
	if err != nil || g2.Kind != 0x00 {
		t.Fatalf("wrap frame 00000000 = %+v, %v", g2, err)
	}

	// Old frame residual from before the wrap: recvBase is now 1,
	// FFFFFFFA is 7 behind, beyond the window width -> refuse.
	f.replace(envWire(t,
		dataEnv(0xFFFFFFFA, 0, ReliableFrame{Kind: 0xFA, Payload: []byte("ancient")}),
	))
	if _, err := rcv.ReadFrame(); !errors.Is(err, ErrChecksum) {
		t.Fatalf("pre-wrap residual err = %v, want ErrChecksum", err)
	}
	// Delivery stopped; the refusal is sticky even for a valid frame.
	f.replace(envWire(t, dataEnv(1, 0, ReliableFrame{Kind: 1})))
	if _, err := rcv.ReadFrame(); !errors.Is(err, ErrChecksum) {
		t.Fatalf("sticky err = %v, want ErrChecksum", err)
	}
}

func TestReliableEnvelopeBitflipStopsDelivery(t *testing.T) {
	wire := envWire(t,
		dataEnv(0, 0, ReliableFrame{Kind: 1, Payload: []byte("one")}),
		dataEnv(1, 0, ReliableFrame{Kind: 2, Payload: []byte("two")}),
		dataEnv(2, 0, ReliableFrame{Kind: 3, Payload: []byte("three")}),
	)
	// Each outer frame is HeaderSize+EnvelopeSize+payloadLen bytes:
	// 10+16+3 = 29 for frame 0. Flip the first envelope-payload byte of
	// frame 1 deterministically.
	frame1 := HeaderSize + EnvelopeSize + 3
	idx := frame1 + HeaderSize + EnvelopeSize // 't' of "two"
	wire[idx] ^= 0x01

	f := newFeed()
	rcv := newReceiver(t, f, 8, t.TempDir())
	f.replace(wire)
	g, err := rcv.ReadFrame()
	if err != nil || string(g.Payload) != "one" {
		t.Fatalf("frame before corruption = %+v, %v", g, err)
	}
	if _, err := rcv.ReadFrame(); !errors.Is(err, ErrChecksum) {
		t.Fatalf("bit-flip err = %v, want ErrChecksum", err)
	}
	// No frame past the corrupt one is delivered, error sticks.
	if _, err := rcv.ReadFrame(); !errors.Is(err, ErrChecksum) {
		t.Fatalf("follow-up err = %v, want sticky ErrChecksum", err)
	}
}

// ---- crash recovery --------------------------------------------------------

func TestReliableRestartResumesAfterLastAcked(t *testing.T) {
	dir := t.TempDir()
	cfg := ReliableConfig{Window: 4, StateDir: dir}

	// Phase 1: four frames are written and flushed. The receiver gets
	// frames 0 and 1 before the "connection" drops.
	var wire1 bytes.Buffer
	s1, err := NewReliable(NewSession(&wire1, closedReader{}), cfg)
	if err != nil {
		t.Fatalf("NewReliable: %v", err)
	}
	for i := 0; i < 4; i++ {
		if err := s1.WriteFrame(ReliableFrame{Kind: uint8(i + 1), Payload: []byte(fmt.Sprintf("frame%d", i))}); err != nil {
			t.Fatalf("WriteFrame %d: %v", i, err)
		}
	}
	if err := s1.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	f := newFeed()
	rcv := newReceiver(t, f, cfg.Window, t.TempDir())
	f.replace(wire1.Bytes())
	first := drainReliable(t, rcv, 2)
	if string(first[0].Payload) != "frame0" || string(first[1].Payload) != "frame1" {
		t.Fatalf("phase 1 frames = %+v", first)
	}

	// Cumulative ack (next expected = 2) reaches the sender: the window
	// slides and the position is checkpointed.
	s1.applyAck(2)
	if err := s1.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Phase 2: the sender restarts with the same state dir on a new
	// connection. It resumes strictly after the last acked position:
	// seq 2 and 3 go out, 0 and 1 are never re-sent.
	var wire2 bytes.Buffer
	s2, err := NewReliable(NewSession(&wire2, closedReader{}), cfg)
	if err != nil {
		t.Fatalf("restart NewReliable: %v", err)
	}
	if s2.sendBase != 2 || s2.sendNext != 4 {
		t.Fatalf("restart positions = base %d next %d, want 2/4", s2.sendBase, s2.sendNext)
	}
	if err := s2.Flush(); err != nil {
		t.Fatalf("restart Flush: %v", err)
	}
	for _, e := range parseWireEnvelopes(t, wire2.Bytes()) {
		if e.typ == envTypeData && (e.seq < 2 || e.seq > 3) {
			t.Fatalf("resent seq %d, want only 2 and 3 (acked frames never resent)", e.seq)
		}
	}

	// The same receiver continues on the new connection: frames 2 and 3
	// complete the original sequence exactly once.
	f.replace(wire2.Bytes())
	rest := drainReliable(t, rcv, 2)
	all := append(first, rest...)
	want := []ReliableFrame{
		{Kind: 1, Payload: []byte("frame0")},
		{Kind: 2, Payload: []byte("frame1")},
		{Kind: 3, Payload: []byte("frame2")},
		{Kind: 4, Payload: []byte("frame3")},
	}
	assertFrames(t, all, want)

	// New writes after recovery continue numbering at 4.
	if err := s2.WriteFrame(ReliableFrame{Kind: 5, Payload: []byte("frame4")}); err != nil {
		t.Fatalf("post-restart WriteFrame: %v", err)
	}
	if s2.sendNext != 5 {
		t.Fatalf("sendNext after recovery write = %d, want 5", s2.sendNext)
	}
	s2.Close()
}

func TestReliableRestartBacklogWiderThanWindowDrains(t *testing.T) {
	// A replay backlog wider than the window is reloaded one window at a
	// time and tail slots are refilled from the log as acks advance.
	dir := t.TempDir()

	var wb bytes.Buffer
	big, err := NewReliable(NewSession(&wb, closedReader{}), ReliableConfig{Window: 8, StateDir: dir})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		if err := big.WriteFrame(ReliableFrame{Kind: uint8(i), Payload: []byte{byte(i)}}); err != nil {
			t.Fatal(err)
		}
	}
	big.Close()

	var wire bytes.Buffer
	s2, err := NewReliable(NewSession(&wire, closedReader{}), ReliableConfig{Window: 2, StateDir: dir})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if err := s2.Flush(); err != nil {
		t.Fatal(err)
	}
	if got := dataSeqs(parseWireEnvelopes(t, wire.Bytes())); fmt.Sprint(got) != "[0 1]" {
		t.Fatalf("first window after reopen = %v, want [0 1]", got)
	}

	s2.applyAck(2)
	wire.Reset()
	if err := s2.Flush(); err != nil {
		t.Fatal(err)
	}
	if got := dataSeqs(parseWireEnvelopes(t, wire.Bytes())); fmt.Sprint(got) != "[2 3]" {
		t.Fatalf("second window = %v, want [2 3]", got)
	}

	s2.applyAck(4)
	wire.Reset()
	if err := s2.Flush(); err != nil {
		t.Fatal(err)
	}
	if got := dataSeqs(parseWireEnvelopes(t, wire.Bytes())); fmt.Sprint(got) != "[4]" {
		t.Fatalf("third window = %v, want [4]", got)
	}
	s2.Close()
}

func TestReliableCheckpointTornFallsBack(t *testing.T) {
	dir := t.TempDir()

	// Two complete checkpoints in alternating slots: gen1 ack5, gen2 ack10.
	if err := saveCheckpoint(dir, 5); err != nil {
		t.Fatal(err)
	}
	if err := saveCheckpoint(dir, 10); err != nil {
		t.Fatal(err)
	}
	if ack, ok, err := loadCheckpoint(dir); err != nil || !ok || ack != 10 {
		t.Fatalf("load = %d/%v/%v, want 10", ack, ok, err)
	}

	// Crash while writing the third checkpoint: the next slot to be
	// rewritten is slot 0, so leave it half-written. The previous
	// complete record in slot 1 must win.
	path := filepath.Join(dir, checkpointName)
	if err := os.WriteFile(path, append([]byte{0xDE, 0xAD, 0xBE, 0xEF, 0x00, 0x11},
		make([]byte, cpRecordSize-6)...), 0o644); err != nil {
		t.Fatal(err)
	}
	// That overwrote the whole file; reconstruct the realistic case:
	// slot 0 garbage, slot 1 still the gen2/ack10 record.
	good := marshalCPRecord(cpRecord{generation: 2, ack: 10})
	if err := os.WriteFile(path, append(make([]byte, cpRecordSize), good...), 0o644); err != nil {
		t.Fatal(err)
	}
	if ack, ok, err := loadCheckpoint(dir); err != nil || !ok || ack != 10 {
		t.Fatalf("torn load = %d/%v/%v, want fallback ack 10", ack, ok, err)
	}

	// The next save rewrites the torn slot and the position advances.
	if err := saveCheckpoint(dir, 15); err != nil {
		t.Fatal(err)
	}
	if ack, ok, err := loadCheckpoint(dir); err != nil || !ok || ack != 15 {
		t.Fatalf("post-fallback load = %d/%v/%v, want 15", ack, ok, err)
	}

	// Neither slot valid means "nothing confirmed": safe replay from the
	// earliest position, never trust garbage.
	if err := os.WriteFile(path, make([]byte, checkpointSize), 0o644); err != nil {
		t.Fatal(err)
	}
	if ack, ok, err := loadCheckpoint(dir); err != nil || ok || ack != 0 {
		t.Fatalf("all-invalid load = %d/%v/%v, want 0/false/nil", ack, ok, err)
	}
}

func TestReliableReplayLogTruncatedOrFlipped(t *testing.T) {
	for _, damage := range []string{"truncate", "bitflip"} {
		t.Run(damage, func(t *testing.T) {
			dir := t.TempDir()
			var wire bytes.Buffer
			snd := newSender(t, &wire, 8, dir)
			for i := 0; i < 4; i++ {
				if err := snd.WriteFrame(ReliableFrame{Kind: uint8(i), Payload: []byte(fmt.Sprintf("payload-%d", i))}); err != nil {
					t.Fatal(err)
				}
			}
			snd.Close()

			path := filepath.Join(dir, logName)
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			switch damage {
			case "truncate":
				data = data[:len(data)-3] // cut through the final record
			case "bitflip":
				data[logHdrSize+2] ^= 0x01 // flip one payload byte of record 0
			}
			if err := os.WriteFile(path, data, 0o644); err != nil {
				t.Fatal(err)
			}

			// Recovery refuses to start: the established checksum value,
			// so no frame is replayed from damaged state.
			var buf bytes.Buffer
			_, err = NewReliable(NewSession(&buf, closedReader{}), ReliableConfig{Window: 8, StateDir: dir})
			if !errors.Is(err, ErrChecksum) {
				t.Fatalf("damaged log (%s) err = %v, want ErrChecksum", damage, err)
			}
		})
	}
}

func TestReliableCheckpointLogMismatchUsesEarlierPosition(t *testing.T) {
	// Log holds seq 0..9; the checkpoint claims ack 6 while the log
	// prefix was never discarded. Reconciliation resumes from the
	// EARLIER position (the log base 0): nothing is skipped, and a
	// receiver that already consumed 0..5 dedupes the overlap.
	dir := t.TempDir()
	cfg := ReliableConfig{Window: 16, StateDir: dir}

	var journal bytes.Buffer
	s, err := NewReliable(NewSession(&journal, closedReader{}), cfg)
	if err != nil {
		t.Fatal(err)
	}
	frames := make([]ReliableFrame, 10)
	for i := range frames {
		frames[i] = ReliableFrame{Kind: uint8(i), Payload: []byte(fmt.Sprintf("f%d", i))}
		if err := s.WriteFrame(frames[i]); err != nil {
			t.Fatal(err)
		}
	}
	s.Close()
	if err := saveCheckpoint(dir, 6); err != nil {
		t.Fatal(err)
	}

	var wire bytes.Buffer
	restarted, err := NewReliable(NewSession(&wire, closedReader{}), cfg)
	if err != nil {
		t.Fatalf("restart: %v", err)
	}
	if restarted.sendBase != 0 || restarted.sendNext != 10 {
		t.Fatalf("resume positions = %d/%d, want 0/10 (earlier confirmed position)",
			restarted.sendBase, restarted.sendNext)
	}
	if err := restarted.Flush(); err != nil {
		t.Fatal(err)
	}
	if got := dataSeqs(parseWireEnvelopes(t, wire.Bytes())); fmt.Sprint(got) != "[0 1 2 3 4 5 6 7 8 9]" {
		t.Fatalf("resent seqs = %v, want 0..9, no skip", got)
	}

	// Receiver already delivered 0..5 before the restart: 0..5 are
	// duplicates, 6..9 arrive fresh, each exactly once overall.
	f := newFeed()
	rcv := newReceiver(t, f, 16, t.TempDir())
	rcv.recvBase.Store(6)
	f.replace(wire.Bytes())
	got := drainReliable(t, rcv, 4)
	assertFrames(t, got, frames[6:])
	f.closeFeed()
	if _, err := rcv.ReadFrame(); !errors.Is(err, io.EOF) {
		t.Fatalf("overlap produced a duplicate delivery: %v", err)
	}
	restarted.Close()
}

// ---- full duplex over a real byte stream -----------------------------------

func TestReliableRestartCheckpointAheadOfLogTail(t *testing.T) {
	// The log retains frames seq 2..5 (prefix discarded by an earlier
	// checkpoint ack 2) and the current checkpoint says ack 6 while the
	// prefix(6) never happened (crash window). Every retained record is
	// older than the checkpointed position: recovery must resume at 6,
	// not renumber a new frame onto a seq the peer already delivered.
	dir := t.TempDir()
	cfg := ReliableConfig{Window: 8, StateDir: dir}

	var wb bytes.Buffer
	s, err := NewReliable(NewSession(&wb, closedReader{}), cfg)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 6; i++ {
		if err := s.WriteFrame(ReliableFrame{Kind: uint8(i), Payload: []byte(fmt.Sprintf("f%d", i))}); err != nil {
			t.Fatal(err)
		}
	}
	s.Close()
	// Durable checkpoint ack 2 followed by prefix(2): simulate the
	// normal ack-2 cleanup, then a later checkpoint ack 6 with no prefix.
	if err := saveCheckpoint(dir, 2); err != nil {
		t.Fatal(err)
	}
	l, err := openReplayLog(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := l.prefix(2); err != nil {
		t.Fatal(err)
	}
	if err := saveCheckpoint(dir, 6); err != nil {
		t.Fatal(err)
	}

	var wire bytes.Buffer
	r, err := NewReliable(NewSession(&wire, closedReader{}), cfg)
	if err != nil {
		t.Fatalf("restart: %v", err)
	}
	if r.sendBase != 6 || r.sendNext != 6 {
		t.Fatalf("positions = %d/%d, want 6/6 (checkpoint at/beyond log tail)", r.sendBase, r.sendNext)
	}
	if err := r.WriteFrame(ReliableFrame{Kind: 99, Payload: []byte("fresh")}); err != nil {
		t.Fatalf("write after restart: %v", err)
	}
	if err := r.Flush(); err != nil {
		t.Fatal(err)
	}
	for _, e := range parseWireEnvelopes(t, wire.Bytes()) {
		if e.typ == envTypeData && e.seq != 6 {
			t.Fatalf("new frame seq = %d, want 6 (no collision with delivered seqs)", e.seq)
		}
	}
	r.Close()
}

func TestReliableTornCheckpointDuringRestart(t *testing.T) {
	// End-to-end torn checkpoint: frames 0..4 are acked through 2 on
	// connection 1; a crash leaves the newest checkpoint record
	// half-written. Restart falls back to the previous complete record
	// (ack 2), replays the retained log from 0, and a receiver that
	// already has 0..2 dedupes the overlap before taking 3 and 4.
	dir := t.TempDir()
	cfg := ReliableConfig{Window: 8, StateDir: dir}

	var wire1 bytes.Buffer
	s1, err := NewReliable(NewSession(&wire1, closedReader{}), cfg)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		if err := s1.WriteFrame(ReliableFrame{Kind: uint8(i + 1), Payload: []byte(fmt.Sprintf("p%d", i))}); err != nil {
			t.Fatal(err)
		}
	}
	if err := s1.Flush(); err != nil {
		t.Fatal(err)
	}
	s1.applyAck(2) // durably checkpoints ack 2 (slot 0) and prefixes
	s1.Close()

	// A later checkpoint (ack 4) is torn mid-write in its slot (slot 1).
	if err := saveCheckpoint(dir, 4); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, checkpointName)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	// Corrupt the just-written slot 1 record (bytes 12..23).
	for i := cpRecordSize; i < checkpointSize-1; i++ {
		data[i] = 0x77
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
	if ack, ok, _ := loadCheckpoint(dir); !ok || ack != 2 {
		t.Fatalf("fallback ack = %d/ok=%v, want 2/true", ack, ok)
	}

	// Receiver already delivered 0..2 on connection 1.
	f := newFeed()
	rcv := newReceiver(t, f, 8, t.TempDir())
	first := drainReliableFromWire(t, rcv, f, wire1.Bytes(), 3)

	var wire2 bytes.Buffer
	s2, err := NewReliable(NewSession(&wire2, closedReader{}), cfg)
	if err != nil {
		t.Fatalf("restart: %v", err)
	}
	// Fallback ack 2 + log (prefix discarded to 2): resume at 2,
	// re-send 2..4; 2 is a duplicate on the receiver, 3 and 4 fresh.
	if s2.sendBase != 2 {
		t.Fatalf("resume base = %d, want 2 from the previous complete checkpoint", s2.sendBase)
	}
	if err := s2.Flush(); err != nil {
		t.Fatal(err)
	}
	f.replace(wire2.Bytes())
	rest := drainReliable(t, rcv, 2)
	all := append(first, rest...)
	wantPayloads := []string{"p0", "p1", "p2", "p3", "p4"}
	if len(all) != len(wantPayloads) {
		t.Fatalf("delivered %d frames, want %d", len(all), len(wantPayloads))
	}
	for i := range wantPayloads {
		if string(all[i].Payload) != wantPayloads[i] {
			t.Fatalf("frame %d = %q, want %q: restart changed the sequence",
				i, all[i].Payload, wantPayloads[i])
		}
	}
	f.closeFeed()
	if _, err := rcv.ReadFrame(); !errors.Is(err, io.EOF) {
		t.Fatalf("overlap after torn-checkpoint restart duplicated a frame: %v", err)
	}
	s2.Close()
}

// drainReliableFromWire feeds wire into f (which rcv already reads) and
// pulls n in-order frames.
func drainReliableFromWire(t *testing.T, rcv *ReliableSession, f *feed, wire []byte, n int) []ReliableFrame {
	t.Helper()
	f.replace(wire)
	return drainReliable(t, rcv, n)
}

// TestReliableConcurrentReadersExactlyOnce mirrors the lower-layer
// guarantee: with several goroutines sharing one receiver, every frame
// is handed out exactly once and in sequence.
func TestReliableConcurrentReadersExactlyOnce(t *testing.T) {
	const n = 400
	frames := make([]ReliableFrame, n)
	for i := range frames {
		frames[i] = ReliableFrame{Kind: uint8(i), Payload: []byte(fmt.Sprintf("%08d", i))}
	}
	envs := make([]envelope, n)
	for i := range frames {
		envs[i] = dataEnv(uint32(i), 0, frames[i])
	}
	wire := envWire(t, envs...)

	f := newFeed()
	rcv := newReceiver(t, f, n, t.TempDir())
	f.replace(wire)

	const readers = 8
	var mu sync.Mutex
	got := make([]ReliableFrame, 0, n)
	var wg sync.WaitGroup
	for r := 0; r < readers; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				fr, err := rcv.ReadFrame()
				if err != nil {
					return
				}
				mu.Lock()
				if len(got) < n {
					got = append(got, fr)
				}
				mu.Unlock()
			}
		}()
	}
	// Wait until n frames are out, then close the feed so readers drain
	// at EOF.
	for {
		mu.Lock()
		done := len(got) == n
		mu.Unlock()
		if done {
			break
		}
	}
	f.closeFeed()
	wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	if len(got) != n {
		t.Fatalf("delivered %d frames, want %d", len(got), n)
	}
	// Concurrent delivery order is arbitrary, but the SET of delivered
	// frames must be exactly the input: no loss, no duplication.
	seen := make(map[string]int, n)
	for _, fr := range got {
		seen[string(fr.Payload)]++
	}
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("%08d", i)
		if seen[key] != 1 {
			t.Fatalf("frame %q delivered %d times, want exactly 1", key, seen[key])
		}
	}
}

func TestReliableFullDuplexOrdered(t *testing.T) {
	a, b := net.Pipe()
	sa, err := NewReliable(NewSession(a, a), ReliableConfig{Window: 128, StateDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	sb, err := NewReliable(NewSession(b, b), ReliableConfig{Window: 128, StateDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}

	const n = 60
	got := make([]ReliableFrame, 0, n)
	var gotMu sync.Mutex
	receivedAll := make(chan struct{})

	// A has no inbound data; its reader only consumes B's acks so the
	// pipe and B's ack pump never block.
	go func() {
		for {
			if _, err := sa.ReadFrame(); err != nil {
				return
			}
		}
	}()

	// B receives every data frame exactly once, in write order.
	go func() {
		for {
			f, err := sb.ReadFrame()
			if err != nil {
				return
			}
			gotMu.Lock()
			got = append(got, f)
			done := len(got) == n
			gotMu.Unlock()
			if done {
				close(receivedAll)
				return
			}
		}
	}()

	// Write in batches with flushes in between, like a pipelined caller.
	for i := 0; i < n; i++ {
		if err := sa.WriteFrame(ReliableFrame{
			Flags: uint16(i), Kind: uint8(i % 250), Payload: []byte(fmt.Sprintf("p%04d", i)),
		}); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
		if i%7 == 0 {
			if err := sa.Flush(); err != nil {
				t.Fatalf("flush %d: %v", i, err)
			}
		}
	}
	if err := sa.Flush(); err != nil {
		t.Fatalf("final flush: %v", err)
	}

	<-receivedAll
	a.Close()
	b.Close()
	sa.Close()
	sb.Close()

	gotMu.Lock()
	defer gotMu.Unlock()
	if len(got) != n {
		t.Fatalf("got %d frames, want %d", len(got), n)
	}
	for i := range got {
		want := fmt.Sprintf("p%04d", i)
		if string(got[i].Payload) != want {
			t.Fatalf("frame %d = %q, want %q: ordering changed or a duplicate", i, got[i].Payload, want)
		}
	}
}

// ---- envelope layout guard -------------------------------------------------

func TestReliableEnvelopeLayout(t *testing.T) {
	if EnvelopeSize != 16 {
		t.Fatalf("EnvelopeSize = %d, want 16", EnvelopeSize)
	}
	if ReliableMaxPayload != MaxPayload-16 {
		t.Fatalf("ReliableMaxPayload = %d, want %d", ReliableMaxPayload, MaxPayload-16)
	}
	b, err := encodeEnvelope(envelope{
		seq: 0x01020304, ack: 0x05060708, typ: 1, flags: 0x090A, kind: 0x0B, payload: []byte("xy"),
	})
	if err != nil {
		t.Fatal(err)
	}
	wantPrefix := []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 1, 0x09, 0x0A, 0x0B}
	if !bytes.Equal(b[:12], wantPrefix) {
		t.Fatalf("envelope header = % x, want % x", b[:12], wantPrefix)
	}
	if !bytes.Equal(b[16:], []byte("xy")) {
		t.Fatalf("envelope body = %x", b[16:])
	}
	// A flipped header byte is covered by the envelope CRC.
	b[3] ^= 0x80
	if _, err := decodeEnvelope(b); !errors.Is(err, ErrChecksum) {
		t.Fatalf("flipped envelope err = %v, want ErrChecksum", err)
	}
}
