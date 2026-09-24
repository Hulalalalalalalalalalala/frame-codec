package codec

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"
)

// ---- deterministic harness -------------------------------------------------

// newMuxSender builds a mux writer whose flushed reliable frames land in
// buf. Its read side is never used (the tests drive acks directly).
func newMuxSender(t *testing.T, buf *bytes.Buffer, channels, window, relWindow int) *Mux {
	t.Helper()
	relDir, muxDir := t.TempDir(), t.TempDir()
	rs, err := NewReliable(NewSession(buf, closedReader{}), ReliableConfig{
		Window:   relWindow,
		StateDir: relDir,
	})
	if err != nil {
		t.Fatalf("NewReliable sender: %v", err)
	}
	mx, err := NewMux(rs, MuxConfig{
		Channels: channels,
		Window:   window,
		StateDir: muxDir,
	})
	if err != nil {
		t.Fatalf("NewMux sender: %v", err)
	}
	t.Cleanup(func() {
		mx.Close()
		rs.Close()
		waitDone(mx.doneCh, rs.doneCh)
	})
	return mx
}

// newMuxReceiver builds a mux reader over f. Its standalone acks (and the
// reliable pump's) go to io.Discard.
func newMuxReceiver(t *testing.T, f *feed, channels, window int) *Mux {
	t.Helper()
	relDir, muxDir := t.TempDir(), t.TempDir()
	rs, err := NewReliable(NewSession(io.Discard, f), ReliableConfig{
		Window:   256,
		StateDir: relDir,
	})
	if err != nil {
		t.Fatalf("NewReliable receiver: %v", err)
	}
	mx, err := NewMux(rs, MuxConfig{
		Channels: channels,
		Window:   window,
		StateDir: muxDir,
	})
	if err != nil {
		t.Fatalf("NewMux receiver: %v", err)
	}
	t.Cleanup(func() {
		mx.Close()
		rs.Close()
		waitDone(mx.doneCh, rs.doneCh)
	})
	return mx
}

// waitDone blocks until every ack pump has exited, bounded, so a state
// directory is never removed while a pump may still be writing it.
func waitDone(chans ...chan struct{}) {
	for _, ch := range chans {
		select {
		case <-ch:
		case <-time.After(2 * time.Second):
		}
	}
}

// muxHdr is one parsed multiplexed message off the wire.
type muxHdr struct {
	relSeq uint32
	ch     uint16
	seq    uint32
	fl     uint8
	ack    uint32
	flags  uint16
	kind   uint8
	body   []byte
}

// parseMuxWire decodes flushed bytes into reliable envelopes and then
// into mux messages.
func parseMuxWire(t *testing.T, wire []byte) []muxHdr {
	t.Helper()
	es := parseWireEnvelopes(t, wire)
	out := make([]muxHdr, 0, len(es))
	for _, e := range es {
		if e.typ == envTypeAck {
			continue // the reliable layer's pure ack is not a mux message
		}
		ch, seq, fl, ack, body, err := decodeMuxHeader(e.payload)
		if err != nil {
			t.Fatalf("decodeMuxHeader: %v", err)
		}
		out = append(out, muxHdr{
			relSeq: e.seq, ch: ch, seq: seq, fl: fl, ack: ack,
			flags: e.flags, kind: e.kind, body: body,
		})
	}
	return out
}

// muxEnvWire wraps mux payloads in reliable data envelopes, reliable
// sequences 0..n-1 (the mux layer's own sequences are independent).
func muxEnvWire(t *testing.T, outer []ReliableFrame, payloads ...[]byte) []byte {
	t.Helper()
	es := make([]envelope, len(payloads))
	for i, p := range payloads {
		var of ReliableFrame
		if i < len(outer) {
			of = outer[i]
		}
		of.Payload = p
		es[i] = dataEnv(uint32(i), 0, of)
	}
	return envWire(t, es...)
}

func muxData(ch uint16, seq uint32, ack uint32, f ChannelFrame) []byte {
	return encodeMuxHeader(ch, seq, 0, ack, f.Payload)
}

func drainMux(t *testing.T, m *Mux, want int) []ChannelFrame {
	t.Helper()
	got := make([]ChannelFrame, 0, want)
	for i := 0; i < want; i++ {
		cf, err := m.ReadFrame()
		if err != nil {
			t.Fatalf("ReadFrame %d: %v", i, err)
		}
		got = append(got, cf)
	}
	return got
}

// ---- ordering and channel independence --------------------------------------

func TestMuxFramesCarryChannelAndPerChannelOrder(t *testing.T) {
	// Reliable arrival (rel seq 0..3): ch1/0, ch0/1, ch1/1, ch0/0.
	// ch1 never waits for ch0's gap; ch0 flushes in order once it closes.
	p1 := muxData(1, 0, 0, ChannelFrame{Kind: 10, Payload: []byte("c1-a")})
	p2 := muxData(0, 1, 0, ChannelFrame{Kind: 20, Payload: []byte("c0-b")})
	p3 := muxData(1, 1, 0, ChannelFrame{Kind: 11, Payload: []byte("c1-b")})
	p4 := muxData(0, 0, 0, ChannelFrame{Kind: 19, Payload: []byte("c0-a")})
	wire := muxEnvWire(t,
		[]ReliableFrame{{Kind: 10}, {Kind: 20}, {Kind: 11}, {Kind: 19}},
		p1, p2, p3, p4)
	f := newFeed()
	rcv := newMuxReceiver(t, f, 3, 8)
	f.replace(wire)
	got := drainMux(t, rcv, 4)
	want := []ChannelFrame{
		{Channel: 1, Kind: 10, Payload: []byte("c1-a")},
		{Channel: 1, Kind: 11, Payload: []byte("c1-b")},
		{Channel: 0, Kind: 19, Payload: []byte("c0-a")},
		{Channel: 0, Kind: 20, Payload: []byte("c0-b")},
	}
	if len(got) != len(want) {
		t.Fatalf("got %d frames, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i].Channel != want[i].Channel || got[i].Kind != want[i].Kind ||
			!bytes.Equal(got[i].Payload, want[i].Payload) {
			t.Fatalf("frame %d = %+v, want %+v", i, got[i], want[i])
		}
	}

	// Clean end of the transport: io.EOF, sticky.
	f.closeFeed()
	if _, err := rcv.ReadFrame(); !errors.Is(err, io.EOF) {
		t.Fatalf("err = %v, want io.EOF", err)
	}
	if _, err := rcv.ReadFrame(); !errors.Is(err, io.EOF) {
		t.Fatalf("second err = %v, want sticky io.EOF", err)
	}
}

func TestMuxZeroFrameEmptyStream(t *testing.T) {
	f := newFeed()
	rcv := newMuxReceiver(t, f, 4, 4)
	f.closeFeed()
	if _, err := rcv.ReadFrame(); !errors.Is(err, io.EOF) {
		t.Fatalf("err = %v, want io.EOF on a zero-frame empty stream", err)
	}
}

func TestMuxEmptyPayloadsAndFlagsKindSurvive(t *testing.T) {
	var buf bytes.Buffer
	snd := newMuxSender(t, &buf, 2, 8, 32)
	in := []ChannelFrame{
		{Channel: 0, Flags: 0x0001, Kind: 7, Payload: nil},
		{Channel: 0, Flags: 0xBEEF, Kind: 200, Payload: []byte{}},
		{Channel: 1, Flags: 0x0002, Kind: 254, Payload: []byte("x")},
	}
	for _, cf := range in {
		if err := snd.WriteFrame(cf); err != nil {
			t.Fatalf("WriteFrame: %v", err)
		}
	}
	if err := snd.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	// Round-robin emission reorders the single Flush into ch0/0, ch1/0,
	// ch0/1; independent channel delivery follows that arrival order.
	want := []ChannelFrame{in[0], in[2], in[1]}
	f := newFeed()
	rcv := newMuxReceiver(t, f, 2, 8)
	f.replace(buf.Bytes())
	got := drainMux(t, rcv, 3)
	for i := range want {
		if got[i].Channel != want[i].Channel || got[i].Flags != want[i].Flags ||
			got[i].Kind != want[i].Kind || !bytes.Equal(got[i].Payload, want[i].Payload) {
			t.Fatalf("frame %d = %+v, want %+v", i, got[i], want[i])
		}
	}
}

// ---- round robin ------------------------------------------------------------

func TestMuxFlushIsRoundRobin(t *testing.T) {
	var buf bytes.Buffer
	snd := newMuxSender(t, &buf, 3, 32, 64)
	for i := 0; i < 5; i++ {
		if err := snd.WriteFrame(ChannelFrame{Channel: 0, Payload: []byte(fmt.Sprintf("a%d", i))}); err != nil {
			t.Fatal(err)
		}
	}
	if err := snd.WriteFrame(ChannelFrame{Channel: 1, Payload: []byte("b")}); err != nil {
		t.Fatal(err)
	}
	if err := snd.WriteFrame(ChannelFrame{Channel: 2, Payload: []byte("c")}); err != nil {
		t.Fatal(err)
	}
	if err := snd.Flush(); err != nil {
		t.Fatal(err)
	}
	hdrs := parseMuxWire(t, buf.Bytes())
	got := make([]int, len(hdrs))
	seqs := make([][]uint32, 3)
	for i, h := range hdrs {
		got[i] = int(h.ch)
		seqs[h.ch] = append(seqs[h.ch], h.seq)
	}
	want := []int{0, 1, 2, 0, 0, 0, 0}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("channel turn order = %v, want %v", got, want)
	}
	if fmt.Sprint(seqs[0]) != "[0 1 2 3 4]" || fmt.Sprint(seqs[1]) != "[0]" || fmt.Sprint(seqs[2]) != "[0]" {
		t.Fatalf("per-channel seqs = %v", seqs)
	}
}

func TestMuxRoundRobinCursorAdvancesAcrossFlushes(t *testing.T) {
	var buf bytes.Buffer
	snd := newMuxSender(t, &buf, 2, 32, 64)
	if err := snd.WriteFrame(ChannelFrame{Channel: 0, Payload: []byte("a0")}); err != nil {
		t.Fatal(err)
	}
	if err := snd.Flush(); err != nil {
		t.Fatal(err)
	}
	// The first Flush served ch0 last, so the cursor parks at ch1 even
	// though no data for ch1 existed yet.
	if snd.rr != 1 {
		t.Fatalf("cursor after flush 1 = %d, want 1", snd.rr)
	}
	if err := snd.WriteFrame(ChannelFrame{Channel: 0, Payload: []byte("a1")}); err != nil {
		t.Fatal(err)
	}
	if err := snd.WriteFrame(ChannelFrame{Channel: 1, Payload: []byte("b0")}); err != nil {
		t.Fatal(err)
	}

	// Inspect the mux layer's own emission order directly: the lower
	// reliable Flush also re-sends its unacked window, so raw wire bytes
	// would contain an extra a0 duplicate the receiver dedupes.
	snd.rmu.Lock()
	acks := append([]uint32(nil), snd.bases...)
	snd.rmu.Unlock()
	snd.mu.Lock()
	out, cursor := snd.buildFlushLocked(acks)
	snd.mu.Unlock()
	got := make([][2]uint32, len(out))
	for i, rf := range out {
		ch, seq, _, _, _, err := decodeMuxHeader(rf.Payload)
		if err != nil {
			t.Fatal(err)
		}
		got[i] = [2]uint32{uint32(ch), seq}
	}
	want := [][2]uint32{{1, 0}, {0, 0}, {0, 1}}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("flush 2 emission order = %v, want %v (b0 first, re-sent a0, then a1)", got, want)
	}
	if cursor != 1 {
		t.Fatalf("cursor after flush 2 = %d, want 1 (pass ended on ch0)", cursor)
	}
}

// ---- per-channel quota ------------------------------------------------------

func TestMuxQuotaPerChannelAndRejectedFrameTakesNoSeq(t *testing.T) {
	const W = 2
	var buf bytes.Buffer
	snd := newMuxSender(t, &buf, 2, W, 16)

	for i := 0; i < W; i++ {
		if err := snd.WriteFrame(ChannelFrame{Channel: 0, Payload: []byte{byte('a' + i)}}); err != nil {
			t.Fatalf("WriteFrame %d: %v", i, err)
		}
	}
	// Quota exactly full: the established over-limit sentinel.
	err := snd.WriteFrame(ChannelFrame{Channel: 0, Payload: []byte("dropped")})
	if !errors.Is(err, ErrWindowFull) || !errors.Is(err, ErrTooLarge) {
		t.Fatalf("full-quota write err = %v, want ErrWindowFull == ErrTooLarge", err)
	}
	// The other channel has its own independent quota.
	if err := snd.WriteFrame(ChannelFrame{Channel: 1, Payload: []byte("b0")}); err != nil {
		t.Fatalf("other channel blocked by full quota: %v", err)
	}

	// One ack frees one slot; the rejected frame consumed no sequence.
	snd.applyAck(0, 1)
	if err := snd.WriteFrame(ChannelFrame{Channel: 0, Kind: 99, Payload: []byte("after")}); err != nil {
		t.Fatalf("write after ack: %v", err)
	}
	if err := snd.Flush(); err != nil {
		t.Fatal(err)
	}
	byCh := map[uint16][]uint32{}
	for _, h := range parseMuxWire(t, buf.Bytes()) {
		byCh[h.ch] = append(byCh[h.ch], h.seq)
	}
	if fmt.Sprint(byCh[0]) != "[1 2]" {
		t.Fatalf("ch0 seqs = %v, want [1 2] (acked seq0 dropped, rejected write took no seq)", byCh[0])
	}
	if fmt.Sprint(byCh[1]) != "[0]" {
		t.Fatalf("ch1 seqs = %v, want [0]", byCh[1])
	}
}

func TestMuxPayloadLimits(t *testing.T) {
	var buf bytes.Buffer
	snd := newMuxSender(t, &buf, 1, 4, 8)
	if err := snd.WriteFrame(ChannelFrame{Payload: make([]byte, MuxMaxPayload)}); err != nil {
		t.Fatalf("MuxMaxPayload frame rejected: %v", err)
	}
	if err := snd.WriteFrame(ChannelFrame{Payload: make([]byte, MuxMaxPayload+1)}); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("err = %v, want ErrTooLarge", err)
	}
	if err := snd.Flush(); err != nil {
		t.Fatal(err)
	}
	f := newFeed()
	rcv := newMuxReceiver(t, f, 1, 4)
	f.replace(buf.Bytes())
	cf, err := rcv.ReadFrame()
	if err != nil {
		t.Fatalf("ReadFrame: %v", err)
	}
	if len(cf.Payload) != MuxMaxPayload {
		t.Fatalf("payload len = %d, want %d", len(cf.Payload), MuxMaxPayload)
	}
}

// ---- channel number range ---------------------------------------------------

func TestMuxChannelNumberCritical(t *testing.T) {
	var buf bytes.Buffer
	snd := newMuxSender(t, &buf, 2, 4, 8)

	// Highest valid channel works.
	if err := snd.WriteFrame(ChannelFrame{Channel: 1, Payload: []byte("top")}); err != nil {
		t.Fatalf("WriteFrame top channel: %v", err)
	}
	if err := snd.CloseChannel(1); err != nil {
		t.Fatalf("CloseChannel top: %v", err)
	}
	// One past the top: the established checksum value, write and close.
	if err := snd.WriteFrame(ChannelFrame{Channel: 2, Payload: []byte("x")}); !errors.Is(err, ErrChecksum) {
		t.Fatalf("write ch2 err = %v, want ErrChecksum", err)
	}
	if err := snd.CloseChannel(2); !errors.Is(err, ErrChecksum) {
		t.Fatalf("close ch2 err = %v, want ErrChecksum", err)
	}
	if err := snd.Flush(); err != nil {
		t.Fatal(err)
	}

	// Receive side: an out-of-range channel is refused and delivery
	// stops stickily.
	wire := muxEnvWire(t, nil,
		muxData(0, 0, 0, ChannelFrame{Payload: []byte("ok")}),
		muxData(2, 0, 0, ChannelFrame{Payload: []byte("bad")}),
		muxData(0, 1, 0, ChannelFrame{Payload: []byte("never")}),
	)
	f := newFeed()
	rcv := newMuxReceiver(t, f, 2, 4)
	f.replace(wire)
	cf, err := rcv.ReadFrame()
	if err != nil || string(cf.Payload) != "ok" {
		t.Fatalf("frame before bad channel = %+v, %v", cf, err)
	}
	if _, err := rcv.ReadFrame(); !errors.Is(err, ErrChecksum) {
		t.Fatalf("bad channel err = %v, want ErrChecksum", err)
	}
	if _, err := rcv.ReadFrame(); !errors.Is(err, ErrChecksum) {
		t.Fatalf("sticky err = %v, want ErrChecksum", err)
	}
}

// ---- close / FIN / EOF ------------------------------------------------------

func TestMuxCloseChannelDeliversEOFAfterRemainingFrames(t *testing.T) {
	var buf bytes.Buffer
	snd := newMuxSender(t, &buf, 2, 8, 32)
	if err := snd.WriteFrame(ChannelFrame{Channel: 0, Payload: []byte("a0")}); err != nil {
		t.Fatal(err)
	}
	if err := snd.WriteFrame(ChannelFrame{Channel: 0, Payload: []byte("a1")}); err != nil {
		t.Fatal(err)
	}
	if err := snd.WriteFrame(ChannelFrame{Channel: 1, Payload: []byte("b0")}); err != nil {
		t.Fatal(err)
	}
	if err := snd.CloseChannel(0); err != nil {
		t.Fatalf("CloseChannel: %v", err)
	}
	// Write to, or close again, the closed channel: ErrChannelClosed.
	if err := snd.WriteFrame(ChannelFrame{Channel: 0, Payload: []byte("late")}); !errors.Is(err, ErrChannelClosed) {
		t.Fatalf("write after close err = %v, want ErrChannelClosed", err)
	}
	if err := snd.CloseChannel(0); !errors.Is(err, ErrChannelClosed) {
		t.Fatalf("reclose err = %v, want ErrChannelClosed", err)
	}
	if err := snd.Flush(); err != nil {
		t.Fatal(err)
	}

	f := newFeed()
	rcv := newMuxReceiver(t, f, 2, 8)
	f.replace(buf.Bytes())

	// Wire order is a0, b0, a1, FIN; the read side scans channels
	// independently, so b0 is delivered as soon as it arrives even
	// though a1 is still in flight - channel stalls never cross.
	got := drainMux(t, rcv, 3)
	wantPayloads := []string{"a0", "b0", "a1"}
	wantChans := []uint16{0, 1, 0}
	for i := range wantPayloads {
		if got[i].Channel != wantChans[i] || string(got[i].Payload) != wantPayloads[i] {
			t.Fatalf("delivery %d = ch%d %q, want ch%d %q",
				i, got[i].Channel, got[i].Payload, wantChans[i], wantPayloads[i])
		}
	}
	// Then the FIN surfaces as io.EOF naming channel 0.
	eof, err := rcv.ReadFrame()
	if !errors.Is(err, io.EOF) || eof.Channel != 0 {
		t.Fatalf("FIN read = %+v, %v, want channel 0 + io.EOF", eof, err)
	}
	// The channel EOF is not repeated. With the transport still open the
	// read blocks; closing it yields the transport's own io.EOF.
	f.closeFeed()
	if _, err := rcv.ReadFrame(); !errors.Is(err, io.EOF) {
		t.Fatalf("err = %v, want transport io.EOF (no repeated channel EOF)", err)
	}
}

func TestMuxCloseNeverOpenedChannelDeliversBareEOF(t *testing.T) {
	var buf bytes.Buffer
	snd := newMuxSender(t, &buf, 2, 8, 16)
	if err := snd.CloseChannel(1); err != nil {
		t.Fatalf("CloseChannel unused: %v", err)
	}
	if err := snd.Flush(); err != nil {
		t.Fatal(err)
	}
	hdrs := parseMuxWire(t, buf.Bytes())
	if len(hdrs) != 1 || hdrs[0].ch != 1 || hdrs[0].seq != 0 || hdrs[0].fl != mhFlagFin || len(hdrs[0].body) != 0 {
		t.Fatalf("bare FIN wire = %+v, want ch1 seq0 FIN no body", hdrs)
	}

	f := newFeed()
	rcv := newMuxReceiver(t, f, 2, 8)
	f.replace(buf.Bytes())
	cf, err := rcv.ReadFrame()
	if !errors.Is(err, io.EOF) || cf.Channel != 1 {
		t.Fatalf("bare FIN read = %+v, %v, want ch1 io.EOF", cf, err)
	}
}

func TestMuxFINConsumesQuota(t *testing.T) {
	const W = 2
	var buf bytes.Buffer
	snd := newMuxSender(t, &buf, 1, W, 8)
	for i := 0; i < W; i++ {
		if err := snd.WriteFrame(ChannelFrame{Payload: []byte{byte(i)}}); err != nil {
			t.Fatal(err)
		}
	}
	// Quota full: even the FIN is refused with the over-limit value.
	if err := snd.CloseChannel(0); !errors.Is(err, ErrWindowFull) {
		t.Fatalf("FIN at full quota err = %v, want ErrWindowFull", err)
	}
	// The failed close neither closed the channel nor took a sequence:
	// another data write is refused only because the quota is still full.
	if err := snd.WriteFrame(ChannelFrame{Payload: []byte("still-open")}); !errors.Is(err, ErrWindowFull) {
		t.Fatalf("channel should still be open but full, got %v", err)
	}
	// One confirmation frees one slot; the FIN now numbers seq 2.
	snd.applyAck(0, 1)
	if err := snd.CloseChannel(0); err != nil {
		t.Fatalf("FIN after ack: %v", err)
	}
	if err := snd.WriteFrame(ChannelFrame{Payload: []byte("late")}); !errors.Is(err, ErrChannelClosed) {
		t.Fatalf("write after FIN err = %v, want ErrChannelClosed", err)
	}
	if err := snd.Flush(); err != nil {
		t.Fatal(err)
	}

	// The wire holds seq1 data and the seq2 FIN (seq0 was confirmed and
	// dropped). A receiver already positioned past seq0 takes the data
	// and then gets exactly one channel EOF.
	f := newFeed()
	rcv := newMuxReceiver(t, f, 1, W)
	rcv.bases[0] = 1
	f.replace(buf.Bytes())
	cf, err := rcv.ReadFrame()
	if err != nil || string(cf.Payload) != "\x01" {
		t.Fatalf("seq1 data = %+v, %v", cf, err)
	}
	eof, err := rcv.ReadFrame()
	if !errors.Is(err, io.EOF) || eof.Channel != 0 {
		t.Fatalf("FIN = %+v, %v, want ch0 io.EOF", eof, err)
	}
}

// ---- receive-side validation ------------------------------------------------

func TestMuxWindowReachAndWrapResidual(t *testing.T) {
	const W = 4
	// One window width ahead of the base is refused.
	wire := muxEnvWire(t,
		[]ReliableFrame{{Kind: 0}, {Kind: 1}},
		muxData(0, 0, 0, ChannelFrame{Payload: []byte("ok0")}),
		muxData(0, uint32(W+1), 0, ChannelFrame{Payload: []byte("far")}),
	)
	f := newFeed()
	rcv := newMuxReceiver(t, f, 2, W)
	f.replace(wire)
	if cf, err := rcv.ReadFrame(); err != nil || string(cf.Payload) != "ok0" {
		t.Fatalf("ok0 = %+v, %v", cf, err)
	}
	if _, err := rcv.ReadFrame(); !errors.Is(err, ErrChecksum) {
		t.Fatalf("one-width-ahead err = %v, want ErrChecksum", err)
	}
	f.replace(muxEnvWire(t, nil, muxData(0, 1, 0, ChannelFrame{Payload: []byte("next")})))
	if _, err := rcv.ReadFrame(); !errors.Is(err, ErrChecksum) {
		t.Fatalf("sticky err = %v, want ErrChecksum", err)
	}

	// Exactly one width ahead with nothing delivered: refused at once.
	early := muxEnvWire(t, nil, muxData(0, uint32(W), 0, ChannelFrame{Payload: []byte("far")}))
	fe := newFeed()
	rcve := newMuxReceiver(t, fe, 2, W)
	fe.replace(early)
	if _, err := rcve.ReadFrame(); !errors.Is(err, ErrChecksum) {
		t.Fatalf("early too-far err = %v, want ErrChecksum", err)
	}

	// Wraparound: three in-order frames across the boundary.
	fw := newFeed()
	rcvw := newMuxReceiver(t, fw, 2, W)
	rcvw.bases[0] = 0xFFFFFFFE
	fw.replace(muxEnvWire(t,
		[]ReliableFrame{{Kind: 0xFE}, {Kind: 0xFF}, {Kind: 0x00}},
		muxData(0, 0xFFFFFFFE, 0, ChannelFrame{Kind: 0xFE, Payload: []byte("fe")}),
		muxData(0, 0xFFFFFFFF, 0, ChannelFrame{Kind: 0xFF, Payload: []byte("ff")}),
		muxData(0, 0x00000000, 0, ChannelFrame{Kind: 0x00, Payload: []byte("00")}),
	))
	for i, kind := range []uint8{0xFE, 0xFF, 0x00} {
		cf, err := rcvw.ReadFrame()
		if err != nil || cf.Kind != kind {
			t.Fatalf("wrap frame %d = %+v, %v (kind %d)", i, cf, err, kind)
		}
	}
	// Pre-wrap residual older than one mux-window width: refused,
	// sticky. It must ride a FRESH reliable sequence - reliable seq 0 is
	// already behind this reliable receiver's base and would be absorbed
	// below the mux layer.
	residual := envWire(t, dataEnv(3, 0, ReliableFrame{
		Kind: 0xFA, Payload: muxData(0, 0xFFFFFFFA, 0, ChannelFrame{Payload: []byte("ancient")}),
	}))
	fw.replace(residual)
	if _, err := rcvw.ReadFrame(); !errors.Is(err, ErrChecksum) {
		t.Fatalf("pre-wrap residual err = %v, want ErrChecksum", err)
	}
	next := envWire(t, dataEnv(4, 0, ReliableFrame{
		Kind: 1, Payload: muxData(0, 1, 0, ChannelFrame{Payload: []byte("x")}),
	}))
	fw.replace(next)
	if _, err := rcvw.ReadFrame(); !errors.Is(err, ErrChecksum) {
		t.Fatalf("sticky after residual err = %v, want ErrChecksum", err)
	}
}

func TestMuxDuplicateSameContentDeliveredOnce(t *testing.T) {
	frame := ChannelFrame{Kind: 1, Payload: []byte("once")}
	wire := muxEnvWire(t, nil,
		muxData(0, 0, 0, frame),
		muxData(0, 0, 0, frame), // identical retransmission
	)
	f := newFeed()
	rcv := newMuxReceiver(t, f, 1, 8)
	f.replace(wire)
	got := drainMux(t, rcv, 1)
	if string(got[0].Payload) != "once" {
		t.Fatalf("got %+v", got[0])
	}
	f.closeFeed()
	if _, err := rcv.ReadFrame(); !errors.Is(err, io.EOF) {
		t.Fatalf("identical duplicate delivered twice: %v", err)
	}
}

func TestMuxSameSeqConflictRefused(t *testing.T) {
	wire := muxEnvWire(t, nil,
		muxData(0, 0, 0, ChannelFrame{Payload: []byte("first")}),
		muxData(0, 0, 0, ChannelFrame{Payload: []byte("CONFLICT")}),
	)
	f := newFeed()
	rcv := newMuxReceiver(t, f, 1, 8)
	f.replace(wire)
	if cf, err := rcv.ReadFrame(); err != nil || string(cf.Payload) != "first" {
		t.Fatalf("first = %+v, %v", cf, err)
	}
	if _, err := rcv.ReadFrame(); !errors.Is(err, ErrChecksum) {
		t.Fatalf("conflict err = %v, want ErrChecksum", err)
	}
	if _, err := rcv.ReadFrame(); !errors.Is(err, ErrChecksum) {
		t.Fatalf("sticky conflict err = %v, want ErrChecksum", err)
	}
}

func TestMuxAckPastSendFrontierRefused(t *testing.T) {
	// This side never sent anything: its frontier for ch0 is 0, so an
	// inbound ack of 1 is impossible and refused.
	wire := muxEnvWire(t, nil,
		muxData(0, 0, 1, ChannelFrame{Payload: []byte("d")}),
	)
	f := newFeed()
	rcv := newMuxReceiver(t, f, 1, 8)
	f.replace(wire)
	if _, err := rcv.ReadFrame(); !errors.Is(err, ErrChecksum) {
		t.Fatalf("future ack err = %v, want ErrChecksum", err)
	}

	// An ack equal to the frontier (0) on a data frame is fine.
	ok := muxEnvWire(t, nil, muxData(0, 0, 0, ChannelFrame{Payload: []byte("d")}))
	f2 := newFeed()
	rcv2 := newMuxReceiver(t, f2, 1, 8)
	f2.replace(ok)
	if cf, err := rcv2.ReadFrame(); err != nil || string(cf.Payload) != "d" {
		t.Fatalf("frontier-equal ack = %+v, %v", cf, err)
	}
}

func TestMuxMalformedHeadersRefused(t *testing.T) {
	cases := []struct {
		name string
		b    []byte
	}{
		{"unknown flag", encodeMuxHeader(0, 0, 0x80, 0, nil)},
		{"fin and ack", encodeMuxHeader(0, 0, mhFlagFin|mhFlagAck, 0, nil)},
		{"ack with payload", encodeMuxHeader(0, 0, mhFlagAck, 0, []byte("x"))},
		{"ack with seq", encodeMuxHeader(0, 7, mhFlagAck, 0, nil)},
		{"fin with payload", encodeMuxHeader(0, 0, mhFlagFin, 0, []byte("x"))},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newFeed()
			rcv := newMuxReceiver(t, f, 2, 8)
			f.replace(muxEnvWire(t, nil, tc.b))
			if _, err := rcv.ReadFrame(); !errors.Is(err, ErrChecksum) {
				t.Fatalf("%s: err = %v, want ErrChecksum", tc.name, err)
			}
		})
	}

	// A damaged mux header is the established checksum value; a header
	// too short to parse is the established short-frame value.
	good := encodeMuxHeader(0, 0, 0, 0, []byte("xy"))
	good[mhBodyOff] ^= 0x01
	f := newFeed()
	rcv := newMuxReceiver(t, f, 2, 8)
	f.replace(muxEnvWire(t, nil, good))
	if _, err := rcv.ReadFrame(); !errors.Is(err, ErrChecksum) {
		t.Fatalf("bitflip err = %v, want ErrChecksum", err)
	}

	short := envWire(t, dataEnv(0, 0, ReliableFrame{Payload: []byte{1, 2, 3}}))
	fs := newFeed()
	rcvs := newMuxReceiver(t, fs, 2, 8)
	fs.replace(short)
	if _, err := rcvs.ReadFrame(); !errors.Is(err, ErrShortFrame) {
		t.Fatalf("short mux header err = %v, want ErrShortFrame", err)
	}
}

// ---- lower-layer pass-through and buffer draining ---------------------------

func TestMuxLowerLayerResultsPassThrough(t *testing.T) {
	good := envWire(t,
		dataEnv(0, 0, ReliableFrame{Kind: 10, Payload: muxData(0, 0, 0, ChannelFrame{Payload: []byte("one")})}),
		dataEnv(1, 0, ReliableFrame{Kind: 11, Payload: muxData(1, 0, 0, ChannelFrame{Payload: []byte("two")})}),
		dataEnv(2, 0, ReliableFrame{Kind: 12, Payload: muxData(0, 1, 0, ChannelFrame{Payload: []byte("three")})}),
	)

	// Truncated trailing outer frame at end of input: the three complete
	// frames are delivered first, then the session's ErrShortFrame
	// passes through unchanged and sticks.
	truncated := append(append([]byte(nil), good...), good[:HeaderSize+3]...)
	f := newFeed()
	rcv := newMuxReceiver(t, f, 2, 8)
	f.replace(truncated)
	f.closeFeed()
	drainMux(t, rcv, 3)
	if _, err := rcv.ReadFrame(); !errors.Is(err, ErrShortFrame) {
		t.Fatalf("truncated tail err = %v, want ErrShortFrame", err)
	}
	if _, err := rcv.ReadFrame(); !errors.Is(err, ErrShortFrame) {
		t.Fatalf("sticky err = %v, want ErrShortFrame", err)
	}

	// A damaged reliable envelope at a fresh seq fails below this layer;
	// the mux passes the established checksum value through byte-for-byte
	// (the outer codec frame stays valid, so the failure is the
	// envelope's own CRC).
	b, err := encodeEnvelope(dataEnv(3, 0, ReliableFrame{
		Kind: 13, Payload: muxData(0, 2, 0, ChannelFrame{Payload: []byte("four")}),
	}))
	if err != nil {
		t.Fatal(err)
	}
	b[envCRCOff] ^= 0xFF // break the envelope CRC; outer frame is rebuilt below
	outer, err := Encode(Frame{Kind: outerKind, Payload: b})
	if err != nil {
		t.Fatal(err)
	}
	damaged := append(append([]byte(nil), good...), outer...)
	fd := newFeed()
	rcvd := newMuxReceiver(t, fd, 2, 8)
	fd.replace(damaged)
	drainMux(t, rcvd, 3)
	if _, err := rcvd.ReadFrame(); !errors.Is(err, ErrChecksum) {
		t.Fatalf("outer envelope corruption err = %v, want ErrChecksum passed through", err)
	}
}

func TestMuxBufferedFramesDeliveredBeforeTransportEOF(t *testing.T) {
	// ch1's complete frame is parked while ch0 has no gap-filler; it is
	// still a complete buffered frame and goes out before the transport's
	// terminal EOF.
	wire := muxEnvWire(t, nil,
		muxData(1, 0, 0, ChannelFrame{Payload: []byte("alone")}),
	)
	f := newFeed()
	rcv := newMuxReceiver(t, f, 2, 8)
	f.replace(wire)
	f.closeFeed()
	cf, err := rcv.ReadFrame()
	if err != nil || string(cf.Payload) != "alone" {
		t.Fatalf("buffered frame = %+v, %v", cf, err)
	}
	if _, err := rcv.ReadFrame(); !errors.Is(err, io.EOF) {
		t.Fatalf("terminal err = %v, want io.EOF after buffered frame", err)
	}
}

// ---- restart recovery -------------------------------------------------------

func newMuxSenderAt(t *testing.T, buf *bytes.Buffer, dir, relSub string, channels, window, relWindow int) (*Mux, func()) {
	t.Helper()
	rs, err := NewReliable(NewSession(buf, closedReader{}), ReliableConfig{
		Window:   relWindow,
		StateDir: filepath.Join(dir, relSub),
	})
	if err != nil {
		t.Fatalf("NewReliable: %v", err)
	}
	mx, err := NewMux(rs, MuxConfig{Channels: channels, Window: window, StateDir: filepath.Join(dir, "mux")})
	if err != nil {
		t.Fatalf("NewMux: %v", err)
	}
	return mx, func() {
		mx.Close()
		rs.Close()
		waitDone(mx.doneCh, rs.doneCh)
	}
}

// flushLoop periodically flushes m so acknowledgements and data keep
// moving over a blocking full-duplex pipe.
func flushLoop(t *testing.T, m *Mux) chan struct{} {
	t.Helper()
	stop := make(chan struct{})
	go func() {
		for {
			select {
			case <-stop:
				return
			default:
				_ = m.Flush()
				time.Sleep(2 * time.Millisecond)
			}
		}
	}()
	return stop
}

func TestMuxRestartResumesAfterConfirmedPositions(t *testing.T) {
	dir := t.TempDir()
	var wire1 bytes.Buffer
	s1, done1 := newMuxSenderAt(t, &wire1, dir, "rel1", 2, 4, 16)
	for i := 0; i < 4; i++ {
		if err := s1.WriteFrame(ChannelFrame{Channel: 0, Payload: []byte(fmt.Sprintf("a%d", i))}); err != nil {
			t.Fatal(err)
		}
	}
	if err := s1.WriteFrame(ChannelFrame{Channel: 1, Payload: []byte("b0")}); err != nil {
		t.Fatal(err)
	}
	if err := s1.WriteFrame(ChannelFrame{Channel: 1, Payload: []byte("b1")}); err != nil {
		t.Fatal(err)
	}
	if err := s1.Flush(); err != nil {
		t.Fatal(err)
	}
	// Receiver confirms ch0 through 2 and ch1 through 1.
	s1.applyAck(0, 2)
	s1.applyAck(1, 1)
	done1()

	var wire2 bytes.Buffer
	s2, done2 := newMuxSenderAt(t, &wire2, dir, "rel2", 2, 4, 16)
	defer done2()
	if s2.sc[0].acked != 2 || s2.sc[0].next != 4 || s2.sc[1].acked != 1 || s2.sc[1].next != 2 {
		t.Fatalf("resume positions = ch0 %d/%d ch1 %d/%d, want 2/4 and 1/2",
			s2.sc[0].acked, s2.sc[0].next, s2.sc[1].acked, s2.sc[1].next)
	}
	if err := s2.Flush(); err != nil {
		t.Fatal(err)
	}
	got := map[[2]uint32]int{}
	for _, h := range parseMuxWire(t, wire2.Bytes()) {
		got[[2]uint32{uint32(h.ch), h.seq}]++
	}
	// ch0/2, ch0/3 were never confirmed; ch1/1 sits past the checkpointed
	// position in the retained journal, so it is re-sent as overlap the
	// receiver dedupes. The earlier position is the safe one.
	for _, key := range [][2]uint32{{0, 2}, {0, 3}, {1, 1}} {
		if got[key] != 1 {
			t.Fatalf("resent key %v count = %d, want 1", key, got[key])
		}
	}
	for _, key := range [][2]uint32{{0, 0}, {0, 1}, {1, 0}} {
		if got[key] != 0 {
			t.Fatalf("already-confirmed key %v resent (count %d)", key, got[key])
		}
	}

	// A new write after recovery continues ch0 numbering at 4.
	if err := s2.WriteFrame(ChannelFrame{Channel: 0, Payload: []byte("a4")}); err != nil {
		t.Fatal(err)
	}
	if s2.sc[0].next != 5 {
		t.Fatalf("next after recovery write = %d, want 5", s2.sc[0].next)
	}
}

func TestMuxRestartResendsOverlapDeduped(t *testing.T) {
	dir := t.TempDir()
	var wire1 bytes.Buffer
	s1, done1 := newMuxSenderAt(t, &wire1, dir, "rel1", 1, 8, 16)
	frames := []ChannelFrame{
		{Channel: 0, Kind: 1, Payload: []byte("f0")},
		{Channel: 0, Kind: 2, Payload: []byte("f1")},
		{Channel: 0, Kind: 3, Payload: []byte("f2")},
	}
	for _, cf := range frames {
		if err := s1.WriteFrame(cf); err != nil {
			t.Fatal(err)
		}
	}
	if err := s1.Flush(); err != nil {
		t.Fatal(err)
	}
	done1() // no acknowledgements: the whole journal replays

	f := newFeed()
	rcv := newMuxReceiver(t, f, 1, 8)
	f.replace(wire1.Bytes())
	got := drainMux(t, rcv, 3)
	for i := range frames {
		if got[i].Kind != frames[i].Kind {
			t.Fatalf("frame %d = %+v, want %+v", i, got[i], frames[i])
		}
	}

	var wire2 bytes.Buffer
	s2, done2 := newMuxSenderAt(t, &wire2, dir, "rel2", 1, 8, 16)
	defer done2()
	if err := s2.Flush(); err != nil {
		t.Fatal(err)
	}
	// The receiver already holds every sequence: the overlap delivers
	// nothing twice.
	f.replace(wire2.Bytes())
	f.closeFeed()
	if _, err := rcv.ReadFrame(); !errors.Is(err, io.EOF) {
		t.Fatalf("replayed overlap duplicated a frame: %v", err)
	}
}

func TestMuxRestartCheckpointBeyondTail(t *testing.T) {
	dir := t.TempDir()
	var wire1 bytes.Buffer
	s1, done1 := newMuxSenderAt(t, &wire1, dir, "rel1", 1, 8, 16)
	for i := 0; i < 3; i++ {
		if err := s1.WriteFrame(ChannelFrame{Payload: []byte(fmt.Sprintf("f%d", i))}); err != nil {
			t.Fatal(err)
		}
	}
	if err := s1.Flush(); err != nil {
		t.Fatal(err)
	}
	done1()
	// Checkpoint claims a position beyond the retained journal tail.
	if err := saveMuxCheckpoint(filepath.Join(dir, "mux"), 0, 6); err != nil {
		t.Fatal(err)
	}
	var wire2 bytes.Buffer
	s2, done2 := newMuxSenderAt(t, &wire2, dir, "rel2", 1, 8, 16)
	defer done2()
	if s2.sc[0].acked != 6 || s2.sc[0].next != 6 {
		t.Fatalf("positions = %d/%d, want 6/6", s2.sc[0].acked, s2.sc[0].next)
	}
	if err := s2.WriteFrame(ChannelFrame{Payload: []byte("fresh")}); err != nil {
		t.Fatal(err)
	}
	if err := s2.Flush(); err != nil {
		t.Fatal(err)
	}
	for _, h := range parseMuxWire(t, wire2.Bytes()) {
		if h.seq != 6 {
			t.Fatalf("new frame seq = %d, want 6 (no collision with delivered seqs)", h.seq)
		}
	}
}

func TestMuxDamagedJournalFailsRecovery(t *testing.T) {
	for _, damage := range []string{"truncate", "bitflip", "gap", "channel"} {
		t.Run(damage, func(t *testing.T) {
			dir := t.TempDir()
			mdir := filepath.Join(dir, "mux")
			var wire bytes.Buffer
			s, done := newMuxSenderAt(t, &wire, dir, "rel1", 2, 8, 16)
			for i := 0; i < 3; i++ {
				if err := s.WriteFrame(ChannelFrame{Channel: 0, Payload: []byte(fmt.Sprintf("p%d", i))}); err != nil {
					t.Fatal(err)
				}
			}
			done()

			switch damage {
			case "truncate", "bitflip":
				path := filepath.Join(mdir, muxJournalName)
				data, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				if damage == "truncate" {
					data = data[:len(data)-3]
				} else {
					data[muxLogHdrSize+2] ^= 0x01
				}
				if err := os.WriteFile(path, data, 0o644); err != nil {
					t.Fatal(err)
				}
			case "gap":
				l, err := openMuxLog(mdir)
				if err != nil {
					t.Fatal(err)
				}
				if err := l.append(queuedFrame{channel: 0, seq: 9}); err != nil {
					t.Fatal(err)
				}
			case "channel":
				l, err := openMuxLog(mdir)
				if err != nil {
					t.Fatal(err)
				}
				if err := l.append(queuedFrame{channel: 5, seq: 0}); err != nil {
					t.Fatal(err)
				}
			}

			var buf bytes.Buffer
			rs, err := NewReliable(NewSession(&buf, closedReader{}), ReliableConfig{
				Window: 16, StateDir: filepath.Join(dir, "rel2"),
			})
			if err != nil {
				t.Fatal(err)
			}
			_, err = NewMux(rs, MuxConfig{Channels: 2, Window: 8, StateDir: mdir})
			if !errors.Is(err, ErrChecksum) {
				t.Fatalf("damaged journal (%s) err = %v, want ErrChecksum", damage, err)
			}
		})
	}
}

// ---- full duplex over a real byte stream ------------------------------------

func newMuxPair(t *testing.T, channels, window, relWindow int) (a, b *Mux) {
	t.Helper()
	// Create every TempDir BEFORE registering the teardown cleanup, so
	// LIFO order closes the sessions (and lets their pumps exit) before
	// the directories they write into are removed.
	dirAR, dirAM, dirBR, dirBM := t.TempDir(), t.TempDir(), t.TempDir(), t.TempDir()
	pa, pb := net.Pipe()
	rsa, err := NewReliable(NewSession(pa, pa), ReliableConfig{Window: relWindow, StateDir: dirAR})
	if err != nil {
		t.Fatal(err)
	}
	rsb, err := NewReliable(NewSession(pb, pb), ReliableConfig{Window: relWindow, StateDir: dirBR})
	if err != nil {
		t.Fatal(err)
	}
	a, err = NewMux(rsa, MuxConfig{Channels: channels, Window: window, StateDir: dirAM})
	if err != nil {
		t.Fatal(err)
	}
	b, err = NewMux(rsb, MuxConfig{Channels: channels, Window: window, StateDir: dirBM})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		a.Close()
		b.Close()
		rsa.Close()
		rsb.Close()
		// Closing the pipes unblocks any pump stuck writing an ack; then
		// wait for all four pumps to have exited before the state
		// directories go away.
		pa.Close()
		pb.Close()
		for _, ch := range []chan struct{}{a.doneCh, b.doneCh, rsa.doneCh, rsb.doneCh} {
			select {
			case <-ch:
			case <-time.After(2 * time.Second):
			}
		}
	})
	return a, b
}

func writeAccepted(m *Mux, cf ChannelFrame) error {
	deadline := time.Now().Add(5 * time.Second)
	for {
		err := m.WriteFrame(cf)
		if err == nil {
			return nil
		}
		if errors.Is(err, ErrWindowFull) {
			if time.Now().After(deadline) {
				return fmt.Errorf("WriteFrame stayed full: %w", err)
			}
			time.Sleep(time.Millisecond)
			continue
		}
		return err
	}
}

func writeUntilAccepted(t *testing.T, m *Mux, cf ChannelFrame) {
	t.Helper()
	if err := writeAccepted(m, cf); err != nil {
		t.Fatalf("WriteFrame: %v", err)
	}
}

func closeAccepted(m *Mux, ch uint16) error {
	deadline := time.Now().Add(5 * time.Second)
	for {
		err := m.CloseChannel(ch)
		if err == nil {
			return nil
		}
		if errors.Is(err, ErrWindowFull) {
			if time.Now().After(deadline) {
				return fmt.Errorf("CloseChannel stayed full: %w", err)
			}
			time.Sleep(time.Millisecond)
			continue
		}
		return err
	}
}

func closeUntilAccepted(t *testing.T, m *Mux, ch uint16) {
	t.Helper()
	if err := closeAccepted(m, ch); err != nil {
		t.Fatalf("CloseChannel: %v", err)
	}
}

func TestMuxFullDuplexPerChannelExactlyOnce(t *testing.T) {
	const channels = 4
	const perChannel = 60
	a, b := newMuxPair(t, channels, 8, 256)

	stopFlush := flushLoop(t, a)
	defer close(stopFlush)

	// A's inbound direction carries only B's acknowledgements; drain it
	// so A's windows keep advancing.
	go func() {
		for {
			if _, err := a.ReadFrame(); err != nil {
				return
			}
		}
	}()

	var mu sync.Mutex
	perCh := make([][]int, channels)
	eofs := make(map[uint16]int)
	allDone := make(chan struct{})

	// Single reader on B: per-channel order must match write order; each
	// channel ends with exactly one io.EOF naming it.
	go func() {
		for {
			cf, err := b.ReadFrame()
			if errors.Is(err, io.EOF) {
				mu.Lock()
				eofs[cf.Channel]++
				done := len(eofs) == channels
				mu.Unlock()
				if done {
					close(allDone)
					return
				}
				continue
			}
			if err != nil {
				return
			}
			var chN, idx int
			if _, err := fmt.Sscanf(string(cf.Payload), "c%df%d", &chN, &idx); err != nil {
				return
			}
			mu.Lock()
			perCh[cf.Channel] = append(perCh[cf.Channel], idx)
			mu.Unlock()
		}
	}()

	for ch := 0; ch < channels; ch++ {
		for i := 0; i < perChannel; i++ {
			writeUntilAccepted(t, a, ChannelFrame{
				Channel: uint16(ch),
				Kind:    uint8(ch),
				Payload: []byte(fmt.Sprintf("c%df%04d", ch, i)),
			})
		}
	}
	for ch := 0; ch < channels; ch++ {
		closeUntilAccepted(t, a, uint16(ch))
	}
	if err := a.Flush(); err != nil {
		t.Fatalf("final flush: %v", err)
	}

	select {
	case <-allDone:
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for all frames and FINs")
	}

	mu.Lock()
	defer mu.Unlock()
	for ch := 0; ch < channels; ch++ {
		got := perCh[ch]
		if len(got) != perChannel {
			t.Fatalf("ch%d got %d frames, want %d", ch, len(got), perChannel)
		}
		for i, idx := range got {
			if idx != i {
				t.Fatalf("ch%d delivery order broken at %d: got %d, want %d", ch, i, idx, i)
			}
		}
		if eofs[uint16(ch)] != 1 {
			t.Fatalf("ch%d EOF count = %d, want 1", ch, eofs[uint16(ch)])
		}
	}
}

func TestMuxConcurrentWritersSameChannelNoLoss(t *testing.T) {
	const channels = 2
	const writers = 8
	const per = 50
	a, b := newMuxPair(t, channels, 4, 256)

	stopFlush := flushLoop(t, a)
	defer close(stopFlush)

	go func() {
		for {
			if _, err := a.ReadFrame(); err != nil {
				return
			}
		}
	}()

	var wg sync.WaitGroup
	errCh := make(chan error, writers)
	for g := 0; g < writers; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < per; i++ {
				if err := writeAccepted(a, ChannelFrame{
					Channel: uint16(g % channels),
					Kind:    uint8(g),
					Payload: []byte(fmt.Sprintf("g%02di%04d", g, i)),
				}); err != nil {
					errCh <- err
					return
				}
			}
		}(g)
	}

	expected := writers * per
	counts := make(map[string]int)
	var cmu sync.Mutex
	readerDone := make(chan struct{})
	signaled := false
	// B keeps draining until the pipe is torn down: exiting early would
	// leave A's acknowledgement pump blocked on the unread pipe while
	// holding the write lock, deadlocking A's own Flush.
	go func() {
		for {
			cf, err := b.ReadFrame()
			if err != nil {
				return
			}
			cmu.Lock()
			counts[string(cf.Payload)]++
			n := len(counts)
			if !signaled && n == expected {
				signaled = true
				close(readerDone)
			}
			cmu.Unlock()
		}
	}()

	wg.Wait()
	select {
	case err := <-errCh:
		t.Fatalf("writer: %v", err)
	default:
	}
	// Flush in the background too: it must not block shutdown once all
	// data is already confirmed (the background flush loop is the
	// primary driver; this just pushes the final queued tail promptly).
	flushErr := make(chan error, 1)
	go func() { flushErr <- a.Flush() }()
	select {
	case <-readerDone:
	case <-time.After(10 * time.Second):
		t.Fatal("timed out; frames lost or interleaved")
	}
	select {
	case err := <-errCh:
		t.Fatalf("writer: %v", err)
	default:
	}
	select {
	case err := <-flushErr:
		if err != nil {
			t.Fatalf("final flush: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("final flush did not return")
	}

	cmu.Lock()
	defer cmu.Unlock()
	if len(counts) != expected {
		t.Fatalf("distinct frames = %d, want %d", len(counts), expected)
	}
	keys := make([]string, 0, expected)
	for k := range counts {
		keys = append(keys, k)
		if counts[k] != 1 {
			t.Fatalf("frame %q delivered %d times", k, counts[k])
		}
	}
	sort.Strings(keys)
	_ = keys
}

// ---- layout and constants guard ---------------------------------------------

func TestMuxHeaderLayout(t *testing.T) {
	if MuxHeaderSize != 15 {
		t.Fatalf("MuxHeaderSize = %d, want 15", MuxHeaderSize)
	}
	if MuxMaxPayload != ReliableMaxPayload-15 {
		t.Fatalf("MuxMaxPayload = %d, want %d", MuxMaxPayload, ReliableMaxPayload-15)
	}
	b := encodeMuxHeader(0x0102, 0x03040506, 0x07, 0x08090A0B, []byte("xy"))
	wantPrefix := []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B}
	if !bytes.Equal(b[:11], wantPrefix) {
		t.Fatalf("mux header prefix = % x, want % x", b[:11], wantPrefix)
	}
	if !bytes.Equal(b[15:], []byte("xy")) {
		t.Fatalf("mux body = %x", b[15:])
	}
	// A flipped header byte is covered by the mux CRC.
	b[2] ^= 0x80
	if _, _, _, _, _, err := decodeMuxHeader(b); !errors.Is(err, ErrChecksum) {
		t.Fatalf("flipped header err = %v, want ErrChecksum", err)
	}
}

func TestMuxCloseIdempotent(t *testing.T) {
	var buf bytes.Buffer
	mx := newMuxSender(t, &buf, 1, 2, 4)
	if err := mx.Close(); err != nil {
		t.Fatalf("first close: %v", err)
	}
	if err := mx.Close(); err != nil {
		t.Fatalf("second close: %v", err)
	}
}

func TestMuxClosedChannelStaysClosedAfterRestart(t *testing.T) {
	dir := t.TempDir()
	var wire bytes.Buffer
	s1, done1 := newMuxSenderAt(t, &wire, dir, "rel1", 2, 8, 16)
	if err := s1.WriteFrame(ChannelFrame{Channel: 1, Payload: []byte("b0")}); err != nil {
		t.Fatal(err)
	}
	if err := s1.CloseChannel(1); err != nil {
		t.Fatal(err)
	}
	if err := s1.WriteFrame(ChannelFrame{Channel: 0, Payload: []byte("a0")}); err != nil {
		t.Fatal(err)
	}
	s1.applyAck(1, 2) // confirm b0 and the FIN
	s1.applyAck(0, 1)
	if err := s1.Flush(); err != nil {
		t.Fatal(err)
	}
	done1()

	var wire2 bytes.Buffer
	s2, done2 := newMuxSenderAt(t, &wire2, dir, "rel2", 2, 8, 16)
	defer done2()
	if !s2.sc[1].closed {
		t.Fatal("channel 1 closed state did not survive restart")
	}
	if err := s2.WriteFrame(ChannelFrame{Channel: 1, Payload: []byte("late")}); !errors.Is(err, ErrChannelClosed) {
		t.Fatalf("write to restarted-closed channel err = %v, want ErrChannelClosed", err)
	}
	if err := s2.CloseChannel(1); !errors.Is(err, ErrChannelClosed) {
		t.Fatalf("reclose after restart err = %v, want ErrChannelClosed", err)
	}
	// The other channel keeps numbering where it left off.
	if err := s2.WriteFrame(ChannelFrame{Channel: 0, Payload: []byte("a1")}); err != nil {
		t.Fatalf("open channel after restart: %v", err)
	}
	if s2.sc[0].next != 2 {
		t.Fatalf("ch0 next = %d, want 2", s2.sc[0].next)
	}
}

func TestMuxBufferBoundedByChannelsTimesWindow(t *testing.T) {
	const C, W = 3, 4
	var buf bytes.Buffer
	snd := newMuxSender(t, &buf, C, W, 64)
	// Fill every channel to its quota; the next writes are rejected and
	// leave no queued trace behind.
	for ch := 0; ch < C; ch++ {
		for i := 0; i < W; i++ {
			if err := snd.WriteFrame(ChannelFrame{Channel: uint16(ch), Payload: []byte{byte(i)}}); err != nil {
				t.Fatalf("fill ch%d i%d: %v", ch, i, err)
			}
		}
	}
	for ch := 0; ch < C; ch++ {
		if err := snd.WriteFrame(ChannelFrame{Channel: uint16(ch), Payload: []byte("x")}); !errors.Is(err, ErrWindowFull) {
			t.Fatalf("over-quota ch%d err = %v, want ErrWindowFull", ch, err)
		}
	}
	snd.mu.Lock()
	queued := len(snd.queue)
	snd.mu.Unlock()
	if queued != C*W {
		t.Fatalf("queued entries = %d, want bounded %d", queued, C*W)
	}
}
