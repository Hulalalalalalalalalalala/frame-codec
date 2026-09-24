package codec

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// frameWire is a concurrency-safe accumulation of wire bytes: one side
// appends while the test drains and resets it from another goroutine.
type frameWire struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (w *frameWire) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.Write(p)
}

// transfer parses whole underlying frames out of the accumulated wire,
// optionally drops one, appends it to dst and resets the wire.
func (w *frameWire) transfer(t *testing.T, dst *blockingFeed, drop func(Frame) bool) int {
	t.Helper()
	w.mu.Lock()
	data := append([]byte(nil), w.buf.Bytes()...)
	w.buf.Reset()
	w.mu.Unlock()

	p := NewSession(nil, bytes.NewReader(data))
	n := 0
	for {
		f, err := p.ReadFrame()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("forward parse: %v", err)
		}
		if drop != nil && drop(f) {
			continue
		}
		b, err := Encode(f)
		if err != nil {
			t.Fatalf("forward encode: %v", err)
		}
		dst.append(b)
		n++
	}
	return n
}

func (w *frameWire) snapshot() []byte {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]byte(nil), w.buf.Bytes()...)
}

func (w *frameWire) len() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.Len()
}

// emitWhole parses complete frames from data and appends each one whole
// to dst, optionally duplicating each frame duplicateCount times.
func emitWhole(t *testing.T, data []byte, dst *blockingFeed, duplicateCount int) {
	t.Helper()
	ps := NewSession(nil, bytes.NewReader(data))
	for {
		f, err := ps.ReadFrame()
		if errors.Is(err, io.EOF) {
			return
		}
		if err != nil {
			t.Fatalf("emit parse: %v", err)
		}
		b, err := Encode(f)
		if err != nil {
			t.Fatal(err)
		}
		for i := 0; i < duplicateCount; i++ {
			dst.append(b)
		}
	}
}

// --- deterministic in-memory network ---------------------------------

// reliablePair wires two ReliableSessions through frame-forwarding
// buffers, so whole underlying frames (never half bytes) can be dropped
// or duplicated to simulate loss.
type reliablePair struct {
	a, b *ReliableSession
	aIn  *blockingFeed // ACKs arriving at A
	bIn  *blockingFeed // DATA arriving at B
	aOut *frameWire    // DATA bytes written by A
	bOut *frameWire    // ACK bytes written by B
}

func newReliablePair(t *testing.T, window int) *reliablePair {
	t.Helper()
	p := &reliablePair{
		aIn:  &blockingFeed{},
		bIn:  &blockingFeed{},
		aOut: &frameWire{},
		bOut: &frameWire{},
	}
	sa := NewSession(p.aOut, p.aIn)
	sb := NewSession(p.bOut, p.bIn)
	var err error
	p.a, err = NewReliableSession(sa, window, "", "")
	if err != nil {
		t.Fatalf("NewReliableSession a: %v", err)
	}
	p.b, err = NewReliableSession(sb, window, "", "")
	if err != nil {
		t.Fatalf("NewReliableSession b: %v", err)
	}
	return p
}

func (p *reliablePair) dataToB(t *testing.T, drop func(Frame) bool) int {
	return p.aOut.transfer(t, p.bIn, drop)
}

func (p *reliablePair) acksToA(t *testing.T, drop func(Frame) bool) int {
	return p.bOut.transfer(t, p.aIn, drop)
}

func (p *reliablePair) close() {
	p.aIn.closeFeed()
	p.bIn.closeFeed()
	_ = p.a.Close()
	_ = p.b.Close()
}

// pumpAcks runs A's read side until its input feed closes; ACKs are
// consumed internally and advance its window.
func pumpAcks(t *testing.T, p *reliablePair) func() {
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			if _, err := p.a.ReadFrame(); err != nil {
				return
			}
		}
	}()
	return func() {
		p.aIn.closeFeed()
		<-done
	}
}

func waitBase(t *testing.T, p *reliablePair, want uint64) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		p.a.wmu.Lock()
		got := p.a.base
		p.a.wmu.Unlock()
		if got == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("sender base never reached %d", want)
}

func mkFrame(i int) Frame {
	return Frame{
		Flags:   uint16(0x100 + i),
		Kind:    uint8(i),
		Payload: []byte(fmt.Sprintf("frame-%04d-payload", i)),
	}
}

// --- tests -----------------------------------------------------------

func TestReliableInOrderDeliverySingleFlush(t *testing.T) {
	const n = 50
	// One pre-flush burst needs the whole run inside the send window;
	// ACKs only start flowing after the flush.
	p := newReliablePair(t, n)
	defer p.close()
	stopPump := pumpAcks(t, p)
	defer stopPump()

	in := make([]Frame, n)
	for i := 0; i < n; i++ {
		in[i] = mkFrame(i)
		if err := p.a.WriteFrame(in[i]); err != nil {
			t.Fatalf("WriteFrame %d: %v", i, err)
		}
	}
	// All frames become visible from one Flush; none earlier.
	if p.aOut.len() != 0 {
		t.Fatalf("%d bytes out before Flush", p.aOut.len())
	}
	if err := p.a.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	p.dataToB(t, nil)

	for i := 0; i < n; i++ {
		f, err := p.b.ReadFrame()
		if err != nil {
			t.Fatalf("ReadFrame %d: %v", i, err)
		}
		if !framesEqual(f, in[i]) {
			t.Fatalf("frame %d = %+v, want %+v", i, f, in[i])
		}
	}
	p.acksToA(t, nil)
	waitBase(t, p, n)
}

func TestReliableEmptyPayloadAndNilPayload(t *testing.T) {
	p := newReliablePair(t, 4)
	defer p.close()

	in := []Frame{
		{Kind: 1, Payload: nil},
		{Kind: 2, Payload: []byte{}},
		{Kind: 3, Payload: []byte("x")},
		{Kind: 4, Payload: nil},
	}
	for _, f := range in {
		if err := p.a.WriteFrame(f); err != nil {
			t.Fatalf("WriteFrame: %v", err)
		}
	}
	if err := p.a.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	p.dataToB(t, nil)
	for i, want := range in {
		got, err := p.b.ReadFrame()
		if err != nil {
			t.Fatalf("ReadFrame %d: %v", i, err)
		}
		if got.Kind != want.Kind || len(got.Payload) != len(want.Payload) {
			t.Fatalf("frame %d = %+v, want kind %d len %d", i, got, want.Kind, len(want.Payload))
		}
	}
}

func TestReliableZeroFrameEmptyStream(t *testing.T) {
	in := &blockingFeed{}
	s := NewSession(io.Discard, in)
	r, err := NewReliableSession(s, 4, "", "")
	if err != nil {
		t.Fatal(err)
	}
	in.closeFeed()
	if _, err := r.ReadFrame(); !errors.Is(err, io.EOF) {
		t.Fatalf("empty stream err = %v, want io.EOF", err)
	}
	if _, err := r.ReadFrame(); !errors.Is(err, io.EOF) {
		t.Fatalf("second err = %v, want sticky io.EOF", err)
	}
}

func TestReliableWindowFullRejectsAndDrops(t *testing.T) {
	const w = 3
	p := newReliablePair(t, w)
	defer p.close()
	stopPump := pumpAcks(t, p)
	defer stopPump()

	for i := 0; i < w; i++ {
		if err := p.a.WriteFrame(mkFrame(i)); err != nil {
			t.Fatalf("WriteFrame %d: %v", i, err)
		}
	}
	// Exactly full: the next write uses the existing overflow value and
	// is dropped without taking a sequence slot.
	if err := p.a.WriteFrame(mkFrame(99)); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("full window err = %v, want ErrTooLarge", err)
	}
	if err := p.a.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	p.dataToB(t, nil)
	for i := 0; i < w; i++ {
		if _, err := p.b.ReadFrame(); err != nil {
			t.Fatalf("ReadFrame %d: %v", i, err)
		}
	}
	p.acksToA(t, nil)
	waitBase(t, p, w)

	// After the window slides, the rejected content can be written as
	// the next frame and takes seq == w (the dropped frame left no gap).
	next := mkFrame(w)
	if err := p.a.WriteFrame(next); err != nil {
		t.Fatalf("WriteFrame after window slide: %v", err)
	}
	if err := p.a.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	p.dataToB(t, nil)
	got, err := p.b.ReadFrame()
	if err != nil || !framesEqual(got, next) {
		t.Fatalf("frame after slide = %+v, %v", got, err)
	}
}

func TestReliableOversizedPayloadUsesExistingError(t *testing.T) {
	p := newReliablePair(t, 2)
	defer p.close()
	big := make([]byte, reliableMaxDataPayload+1)
	if err := p.a.WriteFrame(Frame{Payload: big}); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("err = %v, want ErrTooLarge", err)
	}
	// The maximum inner payload still fits the unchanged outer limit.
	if err := p.a.WriteFrame(Frame{Payload: make([]byte, reliableMaxDataPayload)}); err != nil {
		t.Fatalf("max inner payload rejected: %v", err)
	}
}

func TestReliableAckLostRetransmitDeliversOnce(t *testing.T) {
	p := newReliablePair(t, 4)
	defer p.close()
	stopPump := pumpAcks(t, p)
	defer stopPump()

	// B is served by a background reader: after a duplicate it writes a
	// fresh ACK and then blocks until the next DATA frame arrives.
	got := make(chan Frame, 4)
	go func() {
		for {
			f, err := p.b.ReadFrame()
			if err != nil {
				return
			}
			got <- f
		}
	}()

	f0, f1 := mkFrame(0), mkFrame(1)
	if err := p.a.WriteFrame(f0); err != nil {
		t.Fatal(err)
	}
	if err := p.a.Flush(); err != nil {
		t.Fatal(err)
	}

	// Lose the first ACK: B has f0 and delivered it exactly once.
	p.dataToB(t, nil)
	if f := <-got; !framesEqual(f, f0) {
		t.Fatalf("f0 = %+v", f)
	}
	lost := 0
	p.acksToA(t, func(Frame) bool {
		lost++
		return lost == 1
	})
	p.a.wmu.Lock()
	if p.a.base != 0 {
		t.Fatalf("base = %d, want 0 after lost ACK", p.a.base)
	}
	p.a.wmu.Unlock()

	// Retransmit with the original seq: B must not deliver f0 twice; the
	// background reader's fresh ACK finally slides A's window.
	if err := p.a.Retransmit(); err != nil {
		t.Fatalf("Retransmit: %v", err)
	}
	p.dataToB(t, nil)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		p.acksToA(t, nil)
		p.a.wmu.Lock()
		done := p.a.base == 1
		p.a.wmu.Unlock()
		if done {
			break
		}
		time.Sleep(time.Millisecond)
	}
	p.a.wmu.Lock()
	if p.a.base != 1 {
		t.Fatalf("base = %d, want 1 after the duplicate's ACK", p.a.base)
	}
	p.a.wmu.Unlock()

	// The next frame follows in order; B's delivered sequence is f0,f1.
	if err := p.a.WriteFrame(f1); err != nil {
		t.Fatal(err)
	}
	if err := p.a.Flush(); err != nil {
		t.Fatal(err)
	}
	p.dataToB(t, nil)
	p.acksToA(t, nil)
	select {
	case f := <-got:
		if !framesEqual(f, f1) {
			t.Fatalf("f1 = %+v", f)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("f1 never delivered")
	}
}

func TestReliableDuplicateDataNeverDeliveredTwice(t *testing.T) {
	// Every DATA frame is duplicated by the network (as after a broad
	// ACK loss); the receiver still delivers each content exactly once.
	const n = 20
	p := newReliablePair(t, n+2)
	defer p.close()
	stopPump := pumpAcks(t, p)
	defer stopPump()

	for i := 0; i < n; i++ {
		if err := p.a.WriteFrame(mkFrame(i)); err != nil {
			t.Fatal(err)
		}
	}
	if err := p.a.Flush(); err != nil {
		t.Fatal(err)
	}
	wire := p.aOut.snapshot()

	// Emit every whole frame twice into B's input.
	emitWhole(t, wire, p.bIn, 2)

	for i := 0; i < n; i++ {
		f, err := p.b.ReadFrame()
		if err != nil {
			t.Fatalf("ReadFrame %d: %v", i, err)
		}
		if !framesEqual(f, mkFrame(i)) {
			t.Fatalf("frame %d = %+v", i, f)
		}
	}
	p.acksToA(t, nil)
	waitBase(t, p, n)
}

func TestReliableOutOfOrderParkedThenReleased(t *testing.T) {
	// Feed seq1, seq2 (parked), then seq0: all three come out 0,1,2.
	in := &blockingFeed{}
	var ackOut bytes.Buffer
	r, err := NewReliableSession(NewSession(&ackOut, in), 4, "", "")
	if err != nil {
		t.Fatal(err)
	}
	frames := []Frame{mkFrame(0), mkFrame(1), mkFrame(2)}
	w := func(seq uint64, f Frame) {
		b, err := Encode(buildDataFrame(seq, reliableEntry{f.Flags, f.Kind, f.Payload}))
		if err != nil {
			t.Fatal(err)
		}
		in.append(b)
	}
	w(1, frames[1])
	w(2, frames[2])
	w(0, frames[0])
	for i := 0; i < 3; i++ {
		got, err := r.ReadFrame()
		if err != nil {
			t.Fatalf("ReadFrame %d: %v", i, err)
		}
		if !framesEqual(got, frames[i]) {
			t.Fatalf("frame %d = %+v, want %+v", i, got, frames[i])
		}
	}
	// One cumulative ACK, ackSeq == 2, goes out after seq0 releases the
	// parked run.
	ps := NewSession(nil, bytes.NewReader(ackOut.Bytes()))
	af, err := ps.ReadFrame()
	if err != nil {
		t.Fatalf("ACK: %v", err)
	}
	if af.Kind != reliableKindAck || len(af.Payload) != 8 {
		t.Fatalf("bad ACK frame %+v", af)
	}
	if got := uint64FromBytes(af.Payload); got != 2 {
		t.Fatalf("ackSeq = %d, want 2", got)
	}
}

func TestReliableWindowBoundaries(t *testing.T) {
	const w = 3
	in := &blockingFeed{}
	var ackOut bytes.Buffer
	r, err := NewReliableSession(NewSession(&ackOut, in), w, "", "")
	if err != nil {
		t.Fatal(err)
	}
	feed := func(seq uint64) {
		b, _ := Encode(buildDataFrame(seq, reliableEntry{kind: uint8(seq), payload: []byte{byte(seq)}}))
		in.append(b)
	}

	// Deliver 0,1,2 in order -> deliverNext == 3.
	for seq := uint64(0); seq < 3; seq++ {
		feed(seq)
		if _, err := r.ReadFrame(); err != nil {
			t.Fatalf("deliver %d: %v", seq, err)
		}
	}

	// Forward boundary: seq == deliverNext+w is past the window and is
	// rejected; delivery must stop at the current position.
	feed(3 + w)
	if _, err := r.ReadFrame(); !errors.Is(err, ErrChecksum) {
		t.Fatalf("seq %d err = %v, want ErrChecksum", 3+w, err)
	}
	if _, err := r.ReadFrame(); !errors.Is(err, ErrChecksum) {
		t.Fatalf("reject not sticky: %v", err)
	}
}

func TestReliableStaleResidueRejected(t *testing.T) {
	const w = 3
	in := &blockingFeed{}
	var ackOut bytes.Buffer
	r, err := NewReliableSession(NewSession(&ackOut, in), w, "", "")
	if err != nil {
		t.Fatal(err)
	}
	feed := func(seq uint64) {
		b, _ := Encode(buildDataFrame(seq, reliableEntry{kind: uint8(seq), payload: []byte{byte(seq)}}))
		in.append(b)
	}
	for seq := uint64(0); seq < 6; seq++ {
		feed(seq)
		if _, err := r.ReadFrame(); err != nil {
			t.Fatalf("deliver %d: %v", seq, err)
		}
	}
	// deliverNext == 6. seq == 6-w == 3 is inside the recent duplicate
	// window (re-ACKed, not delivered); seq == 2 is older than the
	// window: a pre-wrap residue, rejected.
	feed(3)
	feed(2)
	if _, err := r.ReadFrame(); !errors.Is(err, ErrChecksum) {
		t.Fatalf("stale seq err = %v, want ErrChecksum", err)
	}
	if _, err := r.ReadFrame(); !errors.Is(err, ErrChecksum) {
		t.Fatalf("stale reject not sticky: %v", err)
	}
}

func TestReliableMalformedInnerFrames(t *testing.T) {
	cases := map[string]Frame{
		"short data":   {Kind: reliableKindData, Payload: []byte{1, 2, 3}},
		"bad length":   corruptInnerLength(),
		"unknown kind": {Kind: 0x01, Payload: []byte("hello")},
		"bad ack":      {Kind: reliableKindAck, Payload: []byte{1, 2, 3}},
	}
	for name, uf := range cases {
		t.Run(name, func(t *testing.T) {
			in := &blockingFeed{}
			r, err := NewReliableSession(NewSession(io.Discard, in), 4, "", "")
			if err != nil {
				t.Fatal(err)
			}
			b, err := Encode(uf)
			if err != nil {
				t.Fatal(err)
			}
			in.append(b)
			if _, err := r.ReadFrame(); !errors.Is(err, ErrChecksum) {
				t.Fatalf("err = %v, want ErrChecksum", err)
			}
			if _, err := r.ReadFrame(); !errors.Is(err, ErrChecksum) {
				t.Fatalf("error not sticky: %v", err)
			}
		})
	}
}

func corruptInnerLength() Frame {
	// Tamper with the inner length field, then re-encode the outer frame
	// so the outer checksum stays valid and the rejection happens in the
	// reliable layer itself.
	f := buildDataFrame(0, reliableEntry{kind: 1, payload: []byte("abc")})
	f.Payload[14] = 99 // declared 99 bytes, only 3 are carried
	b, err := Encode(f)
	if err != nil {
		panic(err)
	}
	dec, _ := Decode(b)
	return dec
}

func TestReliableWireCorruptionStopsDelivery(t *testing.T) {
	good0, _ := Encode(buildDataFrame(0, reliableEntry{kind: 1, payload: []byte("zero")}))
	bad, _ := Encode(buildDataFrame(1, reliableEntry{kind: 2, payload: []byte("bad-data")}))
	bad[HeaderSize] ^= 0xFF // outer checksum now mismatches
	good2, _ := Encode(buildDataFrame(2, reliableEntry{kind: 3, payload: []byte("two")}))

	in := &blockingFeed{}
	r, err := NewReliableSession(NewSession(io.Discard, in), 4, "", "")
	if err != nil {
		t.Fatal(err)
	}
	in.append(good0)
	in.append(bad)
	in.append(good2)

	f, err := r.ReadFrame()
	if err != nil || string(f.Payload) != "zero" {
		t.Fatalf("first frame = %+v, %v", f, err)
	}
	if _, err := r.ReadFrame(); !errors.Is(err, ErrChecksum) {
		t.Fatalf("corrupt frame err = %v, want ErrChecksum", err)
	}
	// The frame after the corrupt one is never delivered.
	if _, err := r.ReadFrame(); !errors.Is(err, ErrChecksum) {
		t.Fatalf("follow-up err = %v, want sticky ErrChecksum", err)
	}
}

// --- persistence / crash recovery ------------------------------------

func fileSender(t *testing.T, cpPath, logPath string, window int) (*ReliableSession, *bytes.Buffer, *blockingFeed) {
	t.Helper()
	out := &bytes.Buffer{}
	in := &blockingFeed{}
	sess := NewSession(out, in)
	r, err := NewReliableSession(sess, window, cpPath, logPath)
	if err != nil {
		t.Fatalf("NewReliableSession: %v", err)
	}
	return r, out, in
}

func TestReliableRestartResumesAfterAcks(t *testing.T) {
	dir := t.TempDir()
	cpPath := filepath.Join(dir, "cp")
	logPath := filepath.Join(dir, "log")
	const total, window = 10, 16

	// First process life: accept and buffer all ten frames, crash
	// without processing any ACK.
	s1, out1, _ := fileSender(t, cpPath, logPath, window)
	in := make([]Frame, total)
	for i := 0; i < total; i++ {
		in[i] = mkFrame(i)
		if err := s1.WriteFrame(in[i]); err != nil {
			t.Fatalf("WriteFrame %d: %v", i, err)
		}
	}
	if err := s1.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := s1.Close(); err != nil {
		t.Fatal(err)
	}

	// The peer receives the first five and acknowledges them.
	peerIn := &blockingFeed{}
	var peerAck bytes.Buffer
	peer, err := NewReliableSession(NewSession(&peerAck, peerIn), window, "", "")
	if err != nil {
		t.Fatal(err)
	}
	first := NewSession(nil, bytes.NewReader(out1.Bytes()))
	for i := 0; i < 5; i++ {
		uf, err := first.ReadFrame()
		if err != nil {
			t.Fatalf("read wire %d: %v", i, err)
		}
		b, _ := Encode(uf)
		peerIn.append(b)
	}
	for i := 0; i < 5; i++ {
		f, err := peer.ReadFrame()
		if err != nil || !framesEqual(f, in[i]) {
			t.Fatalf("peer frame %d = %+v, %v", i, f, err)
		}
	}

	// Second life: reopen the files and pump the five ACKs through the
	// read side. The checkpoint must end at base == 5.
	s2, _, s2in := fileSender(t, cpPath, logPath, window)
	s2in.append(peerAck.Bytes())
	go func() {
		for {
			if _, err := s2.ReadFrame(); err != nil {
				return
			}
		}
	}()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		s2.wmu.Lock()
		done := s2.base == 5
		s2.wmu.Unlock()
		if done {
			break
		}
		time.Sleep(time.Millisecond)
	}
	s2.wmu.Lock()
	if s2.base != 5 {
		t.Fatalf("base = %d, want 5 after replayed ACKs", s2.base)
	}
	s2.wmu.Unlock()
	s2in.closeFeed()
	if err := s2.Close(); err != nil {
		t.Fatal(err)
	}

	// Crash simulation: a half-published NEWER checkpoint is left beside
	// the committed one. Restart must discard it and use base == 5.
	if err := os.WriteFile(cpPath+".tmp", []byte("half-checkpoint"), 0o644); err != nil {
		t.Fatal(err)
	}
	s3, out3, _ := fileSender(t, cpPath, logPath, window)
	s3.wmu.Lock()
	base, next := s3.base, s3.nextSeq
	s3.wmu.Unlock()
	if base != 5 || next != total {
		t.Fatalf("recovered base,next = %d,%d, want 5,%d", base, next, total)
	}
	if err := s3.Flush(); err != nil {
		t.Fatal(err)
	}

	// The resumed wire must start at seq 5: acked frames are not resent.
	p3 := NewSession(nil, bytes.NewReader(out3.Bytes()))
	uf, err := p3.ReadFrame()
	if err != nil {
		t.Fatalf("resumed wire: %v", err)
	}
	if got := uint64FromBytes(uf.Payload); got != 5 {
		t.Fatalf("first resumed seq = %d, want 5", got)
	}
	// Deliver the rest to the same peer: exactly once, in order.
	rest := NewSession(nil, bytes.NewReader(out3.Bytes()))
	for i := 5; i < total; i++ {
		uf, err := rest.ReadFrame()
		if err != nil {
			t.Fatalf("resumed read %d: %v", i, err)
		}
		b, _ := Encode(uf)
		peerIn.append(b)
	}
	for i := 5; i < total; i++ {
		f, err := peer.ReadFrame()
		if err != nil || !framesEqual(f, in[i]) {
			t.Fatalf("peer frame %d = %+v, %v", i, f, err)
		}
	}
}

func TestReliableRestartNoCheckpointReplaysAndDedups(t *testing.T) {
	dir := t.TempDir()
	cpPath := filepath.Join(dir, "cp")
	logPath := filepath.Join(dir, "log")
	const total, window = 10, 16

	s1, out1, _ := fileSender(t, cpPath, logPath, window)
	in := make([]Frame, total)
	for i := 0; i < total; i++ {
		in[i] = mkFrame(i)
		if err := s1.WriteFrame(in[i]); err != nil {
			t.Fatal(err)
		}
	}
	if err := s1.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := s1.Close(); err != nil {
		t.Fatal(err)
	}

	// Peer got the first five (their ACKs were all lost in the crash).
	peerIn := &blockingFeed{}
	peer, err := NewReliableSession(NewSession(io.Discard, peerIn), window, "", "")
	if err != nil {
		t.Fatal(err)
	}
	first := NewSession(nil, bytes.NewReader(out1.Bytes()))
	for i := 0; i < 5; i++ {
		uf, _ := first.ReadFrame()
		b, _ := Encode(uf)
		peerIn.append(b)
	}
	for i := 0; i < 5; i++ {
		if _, err := peer.ReadFrame(); err != nil {
			t.Fatalf("peer deliver %d: %v", i, err)
		}
	}

	// Restart with no checkpoint: replay starts at the earliest logged
	// position, 0. Frames 0..4 are resent and must be deduplicated.
	s2, out2, _ := fileSender(t, cpPath, logPath, window)
	s2.wmu.Lock()
	base := s2.base
	s2.wmu.Unlock()
	if base != 0 {
		t.Fatalf("base = %d, want 0 with no checkpoint", base)
	}
	if err := s2.Flush(); err != nil {
		t.Fatal(err)
	}
	rest := NewSession(nil, bytes.NewReader(out2.Bytes()))
	for {
		uf, err := rest.ReadFrame()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		b, _ := Encode(uf)
		peerIn.append(b)
	}
	for i := 5; i < total; i++ {
		f, err := peer.ReadFrame()
		if err != nil || !framesEqual(f, in[i]) {
			t.Fatalf("peer frame %d = %+v, %v", i, f, err)
		}
	}
	// Nothing more should be deliverable: feed is now empty of new seqs.
	// (An extra ReadFrame blocks, so instead assert receive state.)
	peer.rmu.Lock()
	delivered := peer.deliverNext
	parked := len(peer.recv)
	peer.rmu.Unlock()
	if delivered != total || parked != 0 {
		t.Fatalf("deliverNext=%d parked=%d, want %d and 0", delivered, parked, total)
	}
}

func TestReliableTornCheckpointIgnored(t *testing.T) {
	dir := t.TempDir()
	cpPath := filepath.Join(dir, "cp")
	logPath := filepath.Join(dir, "log")

	// Seed one committed checkpoint at base=2,next=4 plus a log holding
	// records 0..3.
	var rec bytes.Buffer
	for seq := uint64(0); seq < 4; seq++ {
		rec.Write(appendLogRecordBytes(nil, seq, reliableEntry{kind: uint8(seq), payload: []byte(fmt.Sprintf("p%d", seq))}))
	}
	if err := os.WriteFile(logPath, rec.Bytes(), 0o644); err != nil {
		t.Fatal(err)
	}
	var cp [reliableCPRecord]byte
	copy(cp[0:4], reliableCPMagic)
	putU64(cp[4:12], 2)
	putU64(cp[12:20], 4)
	putU64(cp[20:28], 1)
	putU16(cp[28:30], checksum(cp[0:28]))
	if err := os.WriteFile(cpPath, cp[:], 0o644); err != nil {
		t.Fatal(err)
	}
	// A newer checkpoint half-written: garbage of arbitrary length.
	if err := os.WriteFile(cpPath+".tmp", []byte{0, 1, 2, 3, 4, 5, 6}, 0o644); err != nil {
		t.Fatal(err)
	}

	r, _, _ := fileSender(t, cpPath, logPath, 8)
	r.wmu.Lock()
	base, next := r.base, r.nextSeq
	pending := len(r.pending)
	r.wmu.Unlock()
	// Checkpoint says 2, log starts at 0: the earlier logged position
	// wins, so 0,1 are replayed once and deduped at the peer.
	if base != 0 || next != 4 || pending != 4 {
		t.Fatalf("base,next,pending = %d,%d,%d, want 0,4,4", base, next, pending)
	}
}

func TestReliableCorruptCheckpointFails(t *testing.T) {
	dir := t.TempDir()
	cpPath := filepath.Join(dir, "cp")
	logPath := filepath.Join(dir, "log")

	var cp [reliableCPRecord]byte
	copy(cp[0:4], reliableCPMagic)
	putU64(cp[4:12], 0)
	putU64(cp[12:20], 1)
	putU64(cp[20:28], 0)
	putU16(cp[28:30], checksum(cp[0:28]))
	cp[10] ^= 0x01 // bit flip in the middle of the checkpoint
	if err := os.WriteFile(cpPath, cp[:], 0o644); err != nil {
		t.Fatal(err)
	}
	sess := NewSession(io.Discard, &blockingFeed{})
	if _, err := NewReliableSession(sess, 4, cpPath, logPath); !errors.Is(err, ErrChecksum) {
		t.Fatalf("err = %v, want ErrChecksum", err)
	}
}

func TestReliableTruncatedAndFlippedLogFail(t *testing.T) {
	cases := map[string]func([]byte) []byte{
		"truncated": func(b []byte) []byte { return b[:len(b)-3] },
		"bit flip":  func(b []byte) []byte { b[len(b)/2] ^= 0x80; return b },
	}
	for name, mut := range cases {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			cpPath := filepath.Join(dir, "cp")
			logPath := filepath.Join(dir, "log")
			rec := appendLogRecordBytes(nil, 0, reliableEntry{kind: 1, payload: []byte("payload-bytes")})
			rec = appendLogRecordBytes(rec, 1, reliableEntry{kind: 2, payload: []byte("more-payload")})
			if err := os.WriteFile(logPath, mut(rec), 0o644); err != nil {
				t.Fatal(err)
			}
			sess := NewSession(io.Discard, &blockingFeed{})
			_, err := NewReliableSession(sess, 4, cpPath, logPath)
			if !errors.Is(err, ErrChecksum) {
				t.Fatalf("err = %v, want ErrChecksum", err)
			}
		})
	}
}

func TestReliableCheckpointLogMismatchEarlierWins(t *testing.T) {
	dir := t.TempDir()
	cpPath := filepath.Join(dir, "cp")
	logPath := filepath.Join(dir, "log")

	// Log holds 3..9 while a checkpoint claims base=5,next=10: resume at
	// the earlier position 3; 3,4 are replayed once and deduped.
	var rec bytes.Buffer
	for seq := uint64(3); seq < 10; seq++ {
		rec.Write(appendLogRecordBytes(nil, seq, reliableEntry{kind: uint8(seq), payload: []byte{byte(seq)}}))
	}
	if err := os.WriteFile(logPath, rec.Bytes(), 0o644); err != nil {
		t.Fatal(err)
	}
	var cp [reliableCPRecord]byte
	copy(cp[0:4], reliableCPMagic)
	putU64(cp[4:12], 5)
	putU64(cp[12:20], 10)
	putU64(cp[20:28], 4)
	putU16(cp[28:30], checksum(cp[0:28]))
	if err := os.WriteFile(cpPath, cp[:], 0o644); err != nil {
		t.Fatal(err)
	}

	r, out, _ := fileSender(t, cpPath, logPath, 16)
	r.wmu.Lock()
	base, next := r.base, r.nextSeq
	r.wmu.Unlock()
	if base != 3 || next != 10 {
		t.Fatalf("base,next = %d,%d, want 3,10", base, next)
	}
	if err := r.Flush(); err != nil {
		t.Fatal(err)
	}
	p := NewSession(nil, bytes.NewReader(out.Bytes()))
	for want := uint64(3); want < 10; want++ {
		uf, err := p.ReadFrame()
		if err != nil {
			t.Fatalf("read %d: %v", want, err)
		}
		if got := uint64FromBytes(uf.Payload); got != want {
			t.Fatalf("resent seq = %d, want %d", got, want)
		}
	}
}

func TestReliableCheckpointLogHoleFails(t *testing.T) {
	dir := t.TempDir()
	cpPath := filepath.Join(dir, "cp")
	logPath := filepath.Join(dir, "log")

	// Checkpoint base=5 but the log starts at 7: frames 5,6 are neither
	// acked nor replayable; exact continuation is impossible.
	var rec bytes.Buffer
	for _, seq := range []uint64{7, 8, 9} {
		rec.Write(appendLogRecordBytes(nil, seq, reliableEntry{kind: uint8(seq)}))
	}
	if err := os.WriteFile(logPath, rec.Bytes(), 0o644); err != nil {
		t.Fatal(err)
	}
	var cp [reliableCPRecord]byte
	copy(cp[0:4], reliableCPMagic)
	putU64(cp[4:12], 5)
	putU64(cp[12:20], 10)
	putU64(cp[20:28], 4)
	putU16(cp[28:30], checksum(cp[0:28]))
	if err := os.WriteFile(cpPath, cp[:], 0o644); err != nil {
		t.Fatal(err)
	}
	sess := NewSession(io.Discard, &blockingFeed{})
	if _, err := NewReliableSession(sess, 16, cpPath, logPath); !errors.Is(err, ErrChecksum) {
		t.Fatalf("err = %v, want ErrChecksum", err)
	}
}

func TestReliablePipelineWindowSlidesOnAcks(t *testing.T) {
	// Far more frames than the window: delivery and returning ACKs keep
	// the write side open, and the final delivered sequence is exactly
	// the write sequence.
	const total, window = 60, 4
	p := newReliablePair(t, window)
	defer p.close()
	stopPump := pumpAcks(t, p)
	defer stopPump()

	got := make(chan Frame, total)
	go func() {
		for {
			f, err := p.b.ReadFrame()
			if err != nil {
				return
			}
			got <- f
		}
	}()

	for i := 0; i < total; i++ {
		// Wait for window capacity, write exactly one frame, then push it
		// and cycle the returning ACKs.
		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			p.a.wmu.Lock()
			full := p.a.nextSeq >= p.a.base+p.a.window
			p.a.wmu.Unlock()
			if !full {
				break
			}
			p.dataToB(t, nil)
			p.acksToA(t, nil)
			time.Sleep(time.Millisecond)
		}
		if err := p.a.WriteFrame(mkFrame(i)); err != nil {
			t.Fatalf("WriteFrame %d: %v", i, err)
		}
		if err := p.a.Flush(); err != nil {
			t.Fatalf("Flush %d: %v", i, err)
		}
		p.dataToB(t, nil)
		p.acksToA(t, nil)
	}
	for i := 0; i < total; i++ {
		select {
		case f := <-got:
			if !framesEqual(f, mkFrame(i)) {
				t.Fatalf("frame %d = %+v", i, f)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("frame %d never delivered", i)
		}
	}
	p.acksToA(t, nil)
	waitBase(t, p, total)
}

func TestReliableAckBeyondFrontierRejected(t *testing.T) {
	// An ACK naming a seq the sender never produced must not clear its
	// pending set; the read side stops with the existing checksum error.
	in := &blockingFeed{}
	var out bytes.Buffer
	r, err := NewReliableSession(NewSession(&out, in), 4, "", "")
	if err != nil {
		t.Fatal(err)
	}
	var ack [8]byte
	putU64(ack[:], 99)
	b, _ := Encode(Frame{Kind: reliableKindAck, Payload: ack[:]})
	in.append(b)
	if _, err := r.ReadFrame(); !errors.Is(err, ErrChecksum) {
		t.Fatalf("err = %v, want ErrChecksum", err)
	}
}

func TestReliableRetransmitUsesOriginalSeqs(t *testing.T) {
	// After three writes with no ACKs, Retransmit re-emits seq 0,1,2.
	p := newReliablePair(t, 8)
	defer p.close()
	for i := 0; i < 3; i++ {
		if err := p.a.WriteFrame(mkFrame(i)); err != nil {
			t.Fatal(err)
		}
	}
	p.aOut.transfer(t, &blockingFeed{}, nil) // discard the first emission
	if err := p.a.Retransmit(); err != nil {
		t.Fatal(err)
	}
	wire := p.aOut.snapshot()
	ps := NewSession(nil, bytes.NewReader(wire))
	for want := uint64(0); want < 3; want++ {
		f, err := ps.ReadFrame()
		if err != nil {
			t.Fatalf("retransmit %d: %v", want, err)
		}
		if f.Kind != reliableKindData || uint64FromBytes(f.Payload) != want {
			t.Fatalf("retransmitted seq = %d, want %d", uint64FromBytes(f.Payload), want)
		}
	}
}

func TestReliableSequenceSpaceWraparound(t *testing.T) {
	// Place the receiver right at the 2^64 boundary. Unsigned distance
	// arithmetic must keep the in-window post-wrap run ordered while
	// rejecting anything farther away than one window.
	const w = 3
	in := &blockingFeed{}
	r, err := NewReliableSession(NewSession(io.Discard, in), w, "", "")
	if err != nil {
		t.Fatal(err)
	}
	// Frontier at 2^64-2: the live window is {MaxU64-2, MaxU64-1, MaxU64}.
	r.rmu.Lock()
	r.deliverNext = ^uint64(0) - (w - 1)
	r.rmu.Unlock()

	feed := func(seq uint64) {
		var pl [8]byte
		putU64(pl[:], seq)
		b, _ := Encode(buildDataFrame(seq, reliableEntry{kind: 7, payload: pl[:]}))
		in.append(b)
	}
	// Arrive out of order: the two ahead frames park, the in-order
	// boundary frame closes the gap and releases the whole run.
	feed(^uint64(0))
	feed(^uint64(0) - 1)
	feed(^uint64(0) - 2)

	want := []uint64{^uint64(0) - 2, ^uint64(0) - 1, ^uint64(0)}
	for i, wseq := range want {
		f, err := r.ReadFrame()
		if err != nil {
			t.Fatalf("deliver %d: %v", i, err)
		}
		if got := uint64FromBytes(f.Payload); got != wseq {
			t.Fatalf("frame %d seq = %d, want %d", i, got, wseq)
		}
	}
	r.rmu.Lock()
	dn := r.deliverNext
	r.rmu.Unlock()
	if dn != 0 {
		t.Fatalf("deliverNext = %d, want 0 after wrap", dn)
	}

	// A residue from before the wrap is more than a window behind the
	// new frontier; the unsigned distance rule rejects and stops.
	feed(^uint64(0) - 5)
	if _, err := r.ReadFrame(); !errors.Is(err, ErrChecksum) {
		t.Fatalf("pre-wrap residue err = %v, want ErrChecksum", err)
	}
	if _, err := r.ReadFrame(); !errors.Is(err, ErrChecksum) {
		t.Fatalf("wrap reject not sticky: %v", err)
	}
}

// --- small helpers (test-local encoding) -----------------------------

func uint64FromBytes(b []byte) uint64 {
	return uint64(b[0])<<56 | uint64(b[1])<<48 | uint64(b[2])<<40 | uint64(b[3])<<32 |
		uint64(b[4])<<24 | uint64(b[5])<<16 | uint64(b[6])<<8 | uint64(b[7])
}

func putU64(b []byte, v uint64) {
	for i := 7; i >= 0; i-- {
		b[i] = byte(v)
		v >>= 8
	}
}

func putU16(b []byte, v uint16) {
	b[0] = byte(v >> 8)
	b[1] = byte(v)
}
