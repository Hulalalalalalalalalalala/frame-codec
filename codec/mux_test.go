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

// ---- deterministic mux harness ---------------------------------------------

// sideCfg splits one test config's state directory into independent
// per-side mux and reliable directories, so a sender's journal is never
// read as its peer's.
func sideCfg(base MuxConfig, side string) (MuxConfig, ReliableConfig) {
	dir := filepath.Join(base.StateDir, side)
	mc := MuxConfig{Channels: base.Channels, Window: base.Window, StateDir: dir}
	rc := ReliableConfig{Window: base.Channels * base.Window, StateDir: filepath.Join(dir, "rel")}
	return mc, rc
}

// newMuxSender builds a mux whose flushed reliable bytes land in buf.
func newMuxSender(t *testing.T, buf *bytes.Buffer, c MuxConfig) *Mux {
	t.Helper()
	mc, rc := sideCfg(c, "snd")
	rel, err := NewReliable(NewSession(buf, closedReader{}), rc)
	if err != nil {
		t.Fatalf("NewReliable sender: %v", err)
	}
	m, err := NewMux(rel, mc)
	if err != nil {
		t.Fatalf("NewMux sender: %v", err)
	}
	t.Cleanup(func() { m.Close(); rel.Close() })
	return m
}

// newMuxReceiver builds a mux reader over f; its standalone acks go to
// io.Discard (deterministic tests drive sender acks from parsed wire).
func newMuxReceiver(t *testing.T, f *feed, c MuxConfig) *Mux {
	t.Helper()
	mc, rc := sideCfg(c, "rcv")
	rel, err := NewReliable(NewSession(io.Discard, f), rc)
	if err != nil {
		t.Fatalf("NewReliable receiver: %v", err)
	}
	m, err := NewMux(rel, mc)
	if err != nil {
		t.Fatalf("NewMux receiver: %v", err)
	}
	t.Cleanup(func() { m.Close(); rel.Close() })
	return m
}

func muxCfg(t *testing.T, channels, window int) MuxConfig {
	t.Helper()
	return MuxConfig{Channels: channels, Window: window, StateDir: t.TempDir()}
}

// muxWireFrame is one parsed message off the wire.
type muxWireFrame struct {
	ch      uint16
	seq     uint32
	flags   uint8
	ack     uint32
	payload []byte
}

// muxMsg wraps one mux message in a reliable envelope with an explicit
// reliable sequence (the receiver places and dedupes on it), then a
// codec frame.
func muxMsg(relSeq uint32, ch uint16, seq uint32, flags uint8, ack uint32, payload []byte) envelope {
	return envelope{
		seq:     relSeq,
		ack:     0,
		typ:     envTypeData,
		kind:    outerKind,
		payload: encodeMuxHeader(ch, seq, flags, ack, payload),
	}
}

func muxDataEnv(relSeq, muxSeq uint32, ch uint16, payload []byte) envelope {
	return muxMsg(relSeq, ch, muxSeq, 0, 0, payload)
}

func muxFinEnv(relSeq, muxSeq uint32, ch uint16) envelope {
	return muxMsg(relSeq, ch, muxSeq, mhFlagFin, 0, nil)
}

func muxAckEnv(relSeq uint32, ch uint16, ack uint32) envelope {
	return muxMsg(relSeq, ch, 0, mhFlagAck, ack, nil)
}

// parseMuxWire decodes flushed sender bytes into mux data messages,
// keeping only frames carried by reliable sequences >= baseSeq: a
// send-only deterministic sender gets no reliable-layer acks, so each
// Flush retransmits its whole reliable window and the new frames are
// exactly those with fresh reliable sequences.
func parseMuxWire(t *testing.T, wire []byte, baseSeq uint32) []muxWireFrame {
	t.Helper()
	out := make([]muxWireFrame, 0)
	for _, e := range parseWireEnvelopes(t, wire) {
		if e.typ != envTypeData || e.seq < baseSeq {
			continue
		}
		ch, seq, flags, ack, payload, err := decodeMuxHeader(e.payload)
		if err != nil {
			t.Fatalf("decodeMuxHeader: %v", err)
		}
		if flags&mhFlagAck != 0 {
			continue
		}
		out = append(out, muxWireFrame{ch: ch, seq: seq, flags: flags, ack: ack, payload: payload})
	}
	return out
}

func muxChansSeq(ws []muxWireFrame) []string {
	out := make([]string, len(ws))
	for i, w := range ws {
		tag := ""
		if w.flags&mhFlagFin != 0 {
			tag = "F"
		}
		out[i] = fmt.Sprintf("%d:%d%s", w.ch, w.seq, tag)
	}
	return out
}

// drainMuxData pulls until n data frames have been delivered; per
// -channel EOF returns (which name a channel but do not stop the other
// channels) are skipped.
func drainMuxData(t *testing.T, m *Mux, n int) []ChannelFrame {
	t.Helper()
	got := make([]ChannelFrame, 0, n)
	for len(got) < n {
		cf, err := m.ReadFrame()
		if errors.Is(err, io.EOF) {
			continue
		}
		if err != nil {
			t.Fatalf("ReadFrame after %d data frames: %v", len(got), err)
		}
		got = append(got, cf)
	}
	return got
}

// drainMux pulls frames until n data frames have been delivered.
func drainMux(t *testing.T, m *Mux, n int) []ChannelFrame {
	t.Helper()
	return drainMuxData(t, m, n)
}

// ---- requirement tests -----------------------------------------------------

func TestMuxRoundTripMultipleChannels(t *testing.T) {
	cfg := muxCfg(t, 3, 8)
	var wire bytes.Buffer
	snd := newMuxSender(t, &wire, cfg)

	in := map[uint16][]ChannelFrame{
		0: {{Channel: 0, Kind: 1, Payload: []byte("c0-a")}, {Channel: 0, Kind: 2, Payload: []byte("c0-b")}},
		1: {{Channel: 1, Kind: 3, Payload: []byte("c1-a")}},
		2: {{Channel: 2, Kind: 4, Payload: nil}, {Channel: 2, Kind: 5, Payload: []byte("c2-b")}},
	}
	// Interleave writes across channels: arrival order is global,
	// delivery order must be per-channel write order.
	if err := snd.WriteFrame(in[0][0]); err != nil {
		t.Fatal(err)
	}
	if err := snd.WriteFrame(in[1][0]); err != nil {
		t.Fatal(err)
	}
	if err := snd.WriteFrame(in[2][0]); err != nil {
		t.Fatal(err)
	}
	if err := snd.WriteFrame(in[0][1]); err != nil {
		t.Fatal(err)
	}
	if err := snd.WriteFrame(in[2][1]); err != nil {
		t.Fatal(err)
	}
	for ch := uint16(0); ch < 3; ch++ {
		if err := snd.CloseChannel(ch); err != nil {
			t.Fatal(err)
		}
	}
	if wire.Len() != 0 {
		t.Fatalf("%d bytes on the wire before Flush", wire.Len())
	}
	if err := snd.Flush(); err != nil {
		t.Fatal(err)
	}

	f := newFeed()
	rcv := newMuxReceiver(t, f, cfg)
	f.replace(wire.Bytes())

	// EOF events interleave with other channels' data (a closed channel
	// reports EOF as soon as its own FIN arrives, regardless of other
	// channels), so collect all 5 data frames and 3 EOF events together.
	byCh := map[uint16][]ChannelFrame{}
	eofCh := map[uint16]bool{}
	for len(eofCh) < 3 {
		cf, err := rcv.ReadFrame()
		if errors.Is(err, io.EOF) {
			if eofCh[cf.Channel] {
				t.Fatalf("channel %d reported EOF twice", cf.Channel)
			}
			eofCh[cf.Channel] = true
			continue
		}
		if err != nil {
			t.Fatalf("ReadFrame: %v", err)
		}
		byCh[cf.Channel] = append(byCh[cf.Channel], cf)
	}
	if len(eofCh) != 3 {
		t.Fatalf("EOF channels = %v, want all three", eofCh)
	}
	for ch, want := range in {
		if len(byCh[ch]) != len(want) {
			t.Fatalf("ch %d got %d frames, want %d", ch, len(byCh[ch]), len(want))
		}
		for i := range want {
			g := byCh[ch][i]
			if g.Kind != want[i].Kind || !bytes.Equal(g.Payload, want[i].Payload) {
				t.Fatalf("ch %d frame %d = {kind=%d payload=%q}, want {kind=%d payload=%q}",
					ch, i, g.Kind, g.Payload, want[i].Kind, want[i].Payload)
			}
		}
	}
}

func TestMuxEmptyStream(t *testing.T) {
	cfg := muxCfg(t, 2, 4)
	f := newFeed()
	rcv := newMuxReceiver(t, f, cfg)
	f.closeFeed()
	if _, err := rcv.ReadFrame(); !errors.Is(err, io.EOF) {
		t.Fatalf("err = %v, want io.EOF on a zero-frame stream", err)
	}
}

func TestMuxEmptyChannelDeliversEOF(t *testing.T) {
	cfg := muxCfg(t, 2, 4)
	var wire bytes.Buffer
	snd := newMuxSender(t, &wire, cfg)
	// Channel 0 never written, still closable: the peer gets a clean
	// EOF; channel 1 carries one frame.
	if err := snd.WriteFrame(ChannelFrame{Channel: 1, Kind: 7, Payload: []byte("hi")}); err != nil {
		t.Fatal(err)
	}
	if err := snd.CloseChannel(0); err != nil {
		t.Fatal(err)
	}
	if err := snd.CloseChannel(1); err != nil {
		t.Fatal(err)
	}
	if err := snd.Flush(); err != nil {
		t.Fatal(err)
	}

	f := newFeed()
	rcv := newMuxReceiver(t, f, cfg)
	f.replace(wire.Bytes())

	var data *ChannelFrame
	eof := map[uint16]bool{}
	for len(eof) < 2 {
		cf, err := rcv.ReadFrame()
		if errors.Is(err, io.EOF) {
			eof[cf.Channel] = true
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		cp := cf
		data = &cp
	}
	if data == nil || data.Channel != 1 || string(data.Payload) != "hi" {
		t.Fatalf("data = %+v, want ch1/hi", data)
	}
	if !eof[0] || !eof[1] {
		t.Fatalf("EOF map = %v, want both channels closed", eof)
	}
}

func TestMuxQuotaFullIsOverLimitAndReleasesOnAck(t *testing.T) {
	const W = 3
	cfg := muxCfg(t, 2, W)
	var wire bytes.Buffer
	snd := newMuxSender(t, &wire, cfg)

	for i := 0; i < W; i++ {
		if err := snd.WriteFrame(ChannelFrame{Channel: 0, Kind: uint8(i), Payload: []byte{byte('a' + i)}}); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}
	// Quota exactly full: the established over-limit sentinel, same
	// value as ErrTooLarge; frame dropped, no sequence consumed.
	err := snd.WriteFrame(ChannelFrame{Channel: 0, Kind: 99, Payload: []byte("drop")})
	if !errors.Is(err, ErrWindowFull) || !errors.Is(err, ErrTooLarge) {
		t.Fatalf("err = %v, want ErrWindowFull == ErrTooLarge", err)
	}
	// Another channel is unaffected by the full quota of channel 0.
	if err := snd.WriteFrame(ChannelFrame{Channel: 1, Kind: 5, Payload: []byte("other")}); err != nil {
		t.Fatalf("ch1 write while ch0 full: %v", err)
	}
	if err := snd.Flush(); err != nil {
		t.Fatal(err)
	}

	ws := parseMuxWire(t, wire.Bytes(), 0)
	if fmt.Sprint(muxChansSeq(ws)) != "[0:0 1:0 0:1 0:2]" {
		t.Fatalf("wire order = %v, want round-robin [0:0 1:0 0:1 0:2]", muxChansSeq(ws))
	}

	// Ack two ch0 frames at the mux layer: two quota slots free,
	// numbering continues at 3. The send-only reliable layer is driven
	// directly too (as the reliable layer's own tests do), freeing its
	// window so the resend plus the new frame all go out.
	snd.applyAck(0, 2)
	snd.rel.applyAck(4)
	if err := snd.WriteFrame(ChannelFrame{Channel: 0, Kind: 99, Payload: []byte("after")}); err != nil {
		t.Fatalf("write after ack: %v", err)
	}
	wire.Reset()
	if err := snd.Flush(); err != nil {
		t.Fatal(err)
	}
	var ch0 []uint32
	for _, w := range parseMuxWire(t, wire.Bytes(), 0) {
		if w.ch == 0 {
			ch0 = append(ch0, w.seq)
		}
	}
	// Acked 0,1 never reappear; 2 is the still-in-flight retransmit and
	// 3 is the new frame, proving the rejected write consumed no seq.
	if fmt.Sprint(ch0) != "[2 3]" {
		t.Fatalf("ch0 seqs = %v, want [2 3]", ch0)
	}
}

func TestMuxQuotaExactlyFullBoundary(t *testing.T) {
	const W = 4
	cfg := muxCfg(t, 1, W)
	var wire bytes.Buffer
	snd := newMuxSender(t, &wire, cfg)
	for i := 0; i < W; i++ {
		if err := snd.WriteFrame(ChannelFrame{Channel: 0, Payload: []byte{byte(i)}}); err != nil {
			t.Fatal(err)
		}
	}
	if err := snd.WriteFrame(ChannelFrame{Channel: 0, Payload: []byte("x")}); !errors.Is(err, ErrWindowFull) {
		t.Fatalf("W+1 err = %v, want ErrWindowFull", err)
	}
	// FIN also competes for a quota slot.
	if err := snd.CloseChannel(0); !errors.Is(err, ErrWindowFull) {
		t.Fatalf("close at full quota err = %v, want ErrWindowFull", err)
	}
	snd.applyAck(0, 1)
	if err := snd.CloseChannel(0); err != nil {
		t.Fatalf("close after one ack: %v", err)
	}
}

func TestMuxPayloadLimit(t *testing.T) {
	cfg := muxCfg(t, 1, 4)
	var wire bytes.Buffer
	snd := newMuxSender(t, &wire, cfg)
	if err := snd.WriteFrame(ChannelFrame{Channel: 0, Payload: make([]byte, MuxMaxPayload)}); err != nil {
		t.Fatalf("max payload rejected: %v", err)
	}
	if err := snd.WriteFrame(ChannelFrame{Channel: 0, Payload: make([]byte, MuxMaxPayload+1)}); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("err = %v, want ErrTooLarge", err)
	}
	if err := snd.Flush(); err != nil {
		t.Fatal(err)
	}
	f := newFeed()
	rcv := newMuxReceiver(t, f, cfg)
	f.replace(wire.Bytes())
	cf, err := rcv.ReadFrame()
	if err != nil {
		t.Fatal(err)
	}
	if len(cf.Payload) != MuxMaxPayload {
		t.Fatalf("payload len = %d, want %d", len(cf.Payload), MuxMaxPayload)
	}
}

func TestMuxChannelClosedSentinel(t *testing.T) {
	cfg := muxCfg(t, 2, 4)
	var wire bytes.Buffer
	snd := newMuxSender(t, &wire, cfg)
	if err := snd.CloseChannel(1); err != nil {
		t.Fatal(err)
	}
	if err := snd.WriteFrame(ChannelFrame{Channel: 1, Payload: []byte("x")}); !errors.Is(err, ErrChannelClosed) {
		t.Fatalf("write after close err = %v, want ErrChannelClosed", err)
	}
	if err := snd.CloseChannel(1); !errors.Is(err, ErrChannelClosed) {
		t.Fatalf("second close err = %v, want ErrChannelClosed", err)
	}
	// Channel 0 is untouched and still writable.
	if err := snd.WriteFrame(ChannelFrame{Channel: 0, Payload: []byte("y")}); err != nil {
		t.Fatalf("ch0 write: %v", err)
	}
}

func TestMuxChannelNumberBoundaries(t *testing.T) {
	cfg := muxCfg(t, 4, 4) // valid channels 0..3
	var wire bytes.Buffer
	snd := newMuxSender(t, &wire, cfg)
	if err := snd.WriteFrame(ChannelFrame{Channel: 3, Payload: []byte("last")}); err != nil {
		t.Fatalf("boundary channel write: %v", err)
	}
	if err := snd.WriteFrame(ChannelFrame{Channel: 4, Payload: []byte("bad")}); !errors.Is(err, ErrChecksum) {
		t.Fatalf("channel 4 write err = %v, want ErrChecksum", err)
	}
	if err := snd.CloseChannel(4); !errors.Is(err, ErrChecksum) {
		t.Fatalf("close channel 4 err = %v, want ErrChecksum", err)
	}
	if err := snd.Flush(); err != nil {
		t.Fatal(err)
	}

	// A frame on the wire claiming channel 4 is refused stickily.
	f := newFeed()
	rcv := newMuxReceiver(t, f, cfg)
	f.replace(envWire(t, muxDataEnv(0, 0, 4, []byte("wild"))))
	if _, err := rcv.ReadFrame(); !errors.Is(err, ErrChecksum) {
		t.Fatalf("wire channel 4 err = %v, want ErrChecksum", err)
	}
	// A valid frame arriving after the refusal is never delivered.
	f.replace(envWire(t, muxDataEnv(1, 0, 0, []byte("ok"))))
	if _, err := rcv.ReadFrame(); !errors.Is(err, ErrChecksum) {
		t.Fatalf("after bad channel err = %v, want sticky ErrChecksum", err)
	}
}

func TestMuxRoundRobinNoStarvation(t *testing.T) {
	cfg := muxCfg(t, 3, 8)
	var wire bytes.Buffer
	snd := newMuxSender(t, &wire, cfg)
	// ch0 saturates its quota; ch1 and ch2 interleave smaller streams.
	for i := 0; i < 8; i++ {
		if err := snd.WriteFrame(ChannelFrame{Channel: 0, Payload: []byte(fmt.Sprintf("a%d", i))}); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 3; i++ {
		if err := snd.WriteFrame(ChannelFrame{Channel: 1, Payload: []byte(fmt.Sprintf("b%d", i))}); err != nil {
			t.Fatal(err)
		}
	}
	if err := snd.WriteFrame(ChannelFrame{Channel: 2, Payload: []byte("c0")}); err != nil {
		t.Fatal(err)
	}
	if err := snd.Flush(); err != nil {
		t.Fatal(err)
	}
	want := "[0:0 1:0 2:0 0:1 1:1 0:2 1:2 0:3 0:4 0:5 0:6 0:7]"
	if got := fmt.Sprint(muxChansSeq(parseMuxWire(t, wire.Bytes(), 0))); got != want {
		t.Fatalf("wire = %s\nwant %s", got, want)
	}
	// Per-channel order is strictly ascending even globally.
	last := map[uint16]uint32{}
	for _, w := range parseMuxWire(t, wire.Bytes(), 0) {
		if s, seen := last[w.ch]; seen && w.seq != s+1 {
			t.Fatalf("ch %d seq %d followed %d", w.ch, w.seq, s)
		}
		last[w.ch] = w.seq
	}
}

func TestMuxChannelsDeliverIndependently(t *testing.T) {
	cfg := muxCfg(t, 3, 8)
	// Only ch2 has frames; a silent ch0/ch1 must not block it.
	wire := envWire(t,
		muxDataEnv(0, 0, 2, []byte("x")),
		muxDataEnv(1, 1, 2, []byte("y")),
	)
	f := newFeed()
	rcv := newMuxReceiver(t, f, cfg)
	f.replace(wire)
	got := drainMux(t, rcv, 2)
	if got[0].Channel != 2 || string(got[0].Payload) != "x" ||
		got[1].Channel != 2 || string(got[1].Payload) != "y" {
		t.Fatalf("got %+v", got)
	}
}

func TestMuxPerChannelOutOfOrderParked(t *testing.T) {
	cfg := muxCfg(t, 2, 8)
	// ch0 arrives 1,0; ch1 interleaves and does not disturb it.
	wire := envWire(t,
		muxDataEnv(0, 1, 0, []byte("one")),
		muxDataEnv(1, 0, 1, []byte("B-zero")),
		muxDataEnv(2, 0, 0, []byte("zero")),
	)
	f := newFeed()
	rcv := newMuxReceiver(t, f, cfg)
	f.replace(wire)
	got := drainMux(t, rcv, 3)
	// ch1's frame goes first (nothing blocks it), then ch0 in order.
	want := []string{"1:B-zero", "0:zero", "0:one"}
	for i, w := range want {
		g := fmt.Sprintf("%d:%s", got[i].Channel, got[i].Payload)
		if g != w {
			t.Fatalf("frame %d = %s, want %s", i, g, w)
		}
	}
}

func TestMuxDuplicatesDeliveredOnce(t *testing.T) {
	cfg := muxCfg(t, 1, 8)
	// The whole mux run is resent in later reliable frames: same mux
	// (ch,seq), new reliable sequences, so the reliable layer passes
	// them and the mux layer must dedupe.
	wire := envWire(t,
		muxDataEnv(0, 0, 0, []byte("a")),
		muxDataEnv(1, 1, 0, []byte("bb")),
		muxDataEnv(2, 0, 0, []byte("a")),
		muxDataEnv(3, 1, 0, []byte("bb")),
	)
	f := newFeed()
	rcv := newMuxReceiver(t, f, cfg)
	f.replace(wire)
	got := drainMux(t, rcv, 2)
	if string(got[0].Payload) != "a" || string(got[1].Payload) != "bb" {
		t.Fatalf("got %+v", got)
	}
	f.closeFeed()
	if _, err := rcv.ReadFrame(); !errors.Is(err, io.EOF) {
		t.Fatalf("err = %v, want io.EOF with no duplicate delivery", err)
	}
}

func TestMuxRejectBeyondWindowAndAncient(t *testing.T) {
	const W = 4
	cfg := muxCfg(t, 1, W)

	// Exactly one window width ahead: refused.
	wire := envWire(t, muxDataEnv(0, W, 0, []byte("far")))
	f := newFeed()
	rcv := newMuxReceiver(t, f, cfg)
	f.replace(wire)
	if _, err := rcv.ReadFrame(); !errors.Is(err, ErrChecksum) {
		t.Fatalf("too-far err = %v, want ErrChecksum", err)
	}

	// Ancient residual older than one window width after progress.
	f2 := newFeed()
	rcv2 := newMuxReceiver(t, f2, cfg)
	rcv2.bases[0] = 2 * W
	old := envWire(t, muxDataEnv(0, 0, 0, []byte("old")))
	f2.replace(old)
	if _, err := rcv2.ReadFrame(); !errors.Is(err, ErrChecksum) {
		t.Fatalf("ancient err = %v, want ErrChecksum", err)
	}

	// A duplicate inside the span is absorbed silently.
	f3 := newFeed()
	rcv3 := newMuxReceiver(t, f3, cfg)
	rcv3.bases[0] = uint32(W)
	dup := envWire(t, muxDataEnv(0, 1, 0, []byte("dup")))
	f3.replace(dup)
	f3.closeFeed()
	if _, err := rcv3.ReadFrame(); !errors.Is(err, io.EOF) {
		t.Fatalf("in-span duplicate err = %v, want io.EOF with no delivery", err)
	}
}

func TestMuxAckPastSendFrontier(t *testing.T) {
	cfg := muxCfg(t, 1, 4)
	var wire bytes.Buffer
	snd := newMuxSender(t, &wire, cfg)
	if err := snd.WriteFrame(ChannelFrame{Channel: 0, Payload: []byte("a")}); err != nil {
		t.Fatal(err)
	}
	if err := snd.Flush(); err != nil {
		t.Fatal(err)
	}
	f := newFeed()
	rcv := newMuxReceiver(t, f, cfg)
	// This side has only ever accepted seq 0; an inbound ack of 5 from a
	// confused peer is impossible and must be refused.
	bad := envWire(t, muxAckEnv(0, 0, 5))
	f.replace(bad)
	if _, err := rcv.ReadFrame(); !errors.Is(err, ErrChecksum) {
		t.Fatalf("frontier ack err = %v, want ErrChecksum", err)
	}
}

func TestMuxSameSequenceConflict(t *testing.T) {
	cfg := muxCfg(t, 1, 8)
	// Same mux seq 0, two different payloads (distinct reliable seqs so
	// the lower layer delivers both): refuse at the conflicting
	// retransmit and stop at the current position.
	wire := envWire(t,
		muxDataEnv(0, 0, 0, []byte("first")),
		muxDataEnv(1, 0, 0, []byte("OTHER")),
	)
	f := newFeed()
	rcv := newMuxReceiver(t, f, cfg)
	f.replace(wire)
	cf, err := rcv.ReadFrame()
	if err != nil || string(cf.Payload) != "first" {
		t.Fatalf("first frame = %+v, %v", cf, err)
	}
	if _, err := rcv.ReadFrame(); !errors.Is(err, ErrChecksum) {
		t.Fatalf("conflict err = %v, want ErrChecksum", err)
	}
	// Conflict after the frame was already delivered (retransmit
	// arrives behind the base) is refused too.
	wire2 := envWire(t,
		muxDataEnv(0, 0, 0, []byte("a")),
		muxDataEnv(1, 1, 0, []byte("b")),
		muxDataEnv(2, 0, 0, []byte("XXX")),
	)
	f2 := newFeed()
	rcv2 := newMuxReceiver(t, f2, cfg)
	f2.replace(wire2)
	drainMux(t, rcv2, 2)
	if _, err := rcv2.ReadFrame(); !errors.Is(err, ErrChecksum) {
		t.Fatalf("late conflict err = %v, want ErrChecksum", err)
	}
}

func TestMuxHeaderBitflipStopsDelivery(t *testing.T) {
	cfg := muxCfg(t, 1, 8)
	wire := envWire(t,
		muxDataEnv(0, 0, 0, []byte("one")),
		muxDataEnv(1, 1, 0, []byte("two")),
	)
	// Flip the first payload byte of the second mux frame.
	firstFrame := HeaderSize + EnvelopeSize + MuxHeaderSize + 3
	idx := firstFrame + HeaderSize + EnvelopeSize + MuxHeaderSize
	wire[idx] ^= 0x01
	f := newFeed()
	rcv := newMuxReceiver(t, f, cfg)
	f.replace(wire)
	cf, err := rcv.ReadFrame()
	if err != nil || string(cf.Payload) != "one" {
		t.Fatalf("frame before flip = %+v, %v", cf, err)
	}
	if _, err := rcv.ReadFrame(); !errors.Is(err, ErrChecksum) {
		t.Fatalf("bitflip err = %v, want ErrChecksum", err)
	}
	if _, err := rcv.ReadFrame(); !errors.Is(err, ErrChecksum) {
		t.Fatalf("sticky err = %v, want ErrChecksum", err)
	}
}

func TestMuxBufferedFramesDeliveredBeforeEOF(t *testing.T) {
	cfg := muxCfg(t, 1, 8)
	wire := envWire(t,
		muxDataEnv(0, 0, 0, []byte("a")),
		muxDataEnv(1, 1, 0, []byte("bb")),
	)
	// Feed the whole run and close it in one go: the lower layer's
	// terminal read carries trailing complete frames; both must come
	// out before io.EOF.
	f := newFeed()
	rcv := newMuxReceiver(t, f, cfg)
	f.replace(wire)
	f.closeFeed()
	got := drainMux(t, rcv, 2)
	if string(got[0].Payload) != "a" || string(got[1].Payload) != "bb" {
		t.Fatalf("got %+v", got)
	}
	if _, err := rcv.ReadFrame(); !errors.Is(err, io.EOF) {
		t.Fatalf("err = %v, want io.EOF after the buffered run", err)
	}
}

// ---- crash recovery --------------------------------------------------------

func TestMuxRestartResumesFromEarliestPosition(t *testing.T) {
	dir := t.TempDir()
	cfg := MuxConfig{Channels: 2, Window: 4, StateDir: dir}
	relCfg := ReliableConfig{Window: 32, StateDir: filepath.Join(dir, "rel")}

	var wire1 bytes.Buffer
	rel1, err := NewReliable(NewSession(&wire1, closedReader{}), relCfg)
	if err != nil {
		t.Fatal(err)
	}
	s1, err := NewMux(rel1, cfg)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 4; i++ {
		if err := s1.WriteFrame(ChannelFrame{Channel: 0, Kind: uint8(i), Payload: []byte(fmt.Sprintf("a%d", i))}); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 2; i++ {
		if err := s1.WriteFrame(ChannelFrame{Channel: 1, Kind: uint8(i), Payload: []byte(fmt.Sprintf("b%d", i))}); err != nil {
			t.Fatal(err)
		}
	}
	if err := s1.Flush(); err != nil {
		t.Fatal(err)
	}

	f := newFeed()
	rcvCfg := MuxConfig{Channels: 2, Window: 4, StateDir: filepath.Join(dir, "rcv")}
	rcvRelCfg := ReliableConfig{Window: 32, StateDir: filepath.Join(dir, "rcv", "rel")}
	rcvR, err := NewReliable(NewSession(io.Discard, f), rcvRelCfg)
	if err != nil {
		t.Fatal(err)
	}
	rcv, err := NewMux(rcvR, rcvCfg)
	if err != nil {
		t.Fatal(err)
	}
	f.replace(wire1.Bytes())
	// Peer confirms a0,a1 and b0 before the drop.
	first := drainMux(t, rcv, 3)
	if len(first) != 3 {
		t.Fatalf("got %d frames, want 3", len(first))
	}
	s1.applyAck(0, 2)
	s1.applyAck(1, 1)
	if err := s1.Close(); err != nil {
		t.Fatal(err)
	}
	if err := rel1.Close(); err != nil {
		t.Fatal(err)
	}

	// Restart with the same state directory: only unacked frames go out
	// (ch0 2,3; ch1 1), round-robin from channel 0.
	var wire2 bytes.Buffer
	rel2, err := NewReliable(NewSession(&wire2, closedReader{}), relCfg)
	if err != nil {
		t.Fatal(err)
	}
	s2, err := NewMux(rel2, cfg)
	if err != nil {
		t.Fatalf("restart: %v", err)
	}
	if s2.sc[0].acked != 2 || s2.sc[0].next != 4 || s2.sc[1].acked != 1 || s2.sc[1].next != 2 {
		t.Fatalf("positions = ch0 %d/%d ch1 %d/%d, want 2/4 and 1/2",
			s2.sc[0].acked, s2.sc[0].next, s2.sc[1].acked, s2.sc[1].next)
	}
	if err := s2.Flush(); err != nil {
		t.Fatal(err)
	}
	rest := parseMuxWire(t, wire2.Bytes(), 6)
	if fmt.Sprint(muxChansSeq(rest)) != "[0:2 1:1 0:3]" {
		t.Fatalf("resent = %v, want [0:2 1:1 0:3]", muxChansSeq(rest))
	}

	// New writes continue numbering past the journal tail per channel.
	if err := s2.WriteFrame(ChannelFrame{Channel: 0, Payload: []byte("a4")}); err != nil {
		t.Fatal(err)
	}
	if s2.sc[0].next != 5 {
		t.Fatalf("next = %d, want 5", s2.sc[0].next)
	}

	// Receiver finishes the stream on the new connection.
	f.replace(wire2.Bytes())
	got := drainMux(t, rcv, 3)
	all := append(first, got...)
	// Round-robin delivery across channels: only the SET per channel is
	// order-sensitive, so compare grouped.
	gotByCh := map[uint16][]string{}
	for _, cf := range all {
		gotByCh[cf.Channel] = append(gotByCh[cf.Channel], string(cf.Payload))
	}
	if fmt.Sprint(gotByCh[0]) != "[a0 a1 a2 a3]" || fmt.Sprint(gotByCh[1]) != "[b0 b1]" {
		t.Fatalf("grouped delivery = %v", gotByCh)
	}
}

func TestMuxRestartOverlapDeduped(t *testing.T) {
	// Checkpoint claims a position the journal prefix was never trimmed
	// past: recovery resends from the earlier position and the receiver
	// dedupes; nothing is skipped or delivered twice.
	dir := t.TempDir()
	cfg := MuxConfig{Channels: 1, Window: 8, StateDir: dir}

	var wb bytes.Buffer
	relW, err := NewReliable(NewSession(&wb, closedReader{}), ReliableConfig{
		Window: 32, StateDir: filepath.Join(dir, "rel")})
	if err != nil {
		t.Fatal(err)
	}
	s, err := NewMux(relW, cfg)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		if err := s.WriteFrame(ChannelFrame{Channel: 0, Kind: uint8(i), Payload: []byte(fmt.Sprintf("f%d", i))}); err != nil {
			t.Fatal(err)
		}
	}
	s.Close()
	relW.Close()
	if err := saveMuxCheckpoint(dir, 0, 3); err != nil {
		t.Fatal(err)
	}

	var wire bytes.Buffer
	rel2, err := NewReliable(NewSession(&wire, closedReader{}), ReliableConfig{
		Window: 32, StateDir: filepath.Join(dir, "rel2")})
	if err != nil {
		t.Fatal(err)
	}
	rs, err := NewMux(rel2, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if rs.sc[0].acked != 0 || rs.sc[0].next != 5 {
		t.Fatalf("positions = %d/%d, want 0/5 (earlier confirmed position)", rs.sc[0].acked, rs.sc[0].next)
	}
	if err := rs.Flush(); err != nil {
		t.Fatal(err)
	}
	if got := fmt.Sprint(muxChansSeq(parseMuxWire(t, wire.Bytes(), 0))); got != "[0:0 0:1 0:2 0:3 0:4]" {
		t.Fatalf("resent = %s, want 0..4", got)
	}

	f := newFeed()
	rcvDir := filepath.Join(dir, "rcv")
	rcvR, err := NewReliable(NewSession(io.Discard, f), ReliableConfig{
		Window: 32, StateDir: filepath.Join(rcvDir, "rel")})
	if err != nil {
		t.Fatal(err)
	}
	rcv, err := NewMux(rcvR, MuxConfig{Channels: 1, Window: 8, StateDir: rcvDir})
	if err != nil {
		t.Fatal(err)
	}
	rcv.bases[0] = 3 // peer already delivered 0..2 on the old connection
	f.replace(wire.Bytes())
	got := drainMux(t, rcv, 2)
	if string(got[0].Payload) != "f3" || string(got[1].Payload) != "f4" {
		t.Fatalf("fresh delivery = %+v, want f3 f4", got)
	}
	f.closeFeed()
	if _, err := rcv.ReadFrame(); !errors.Is(err, io.EOF) {
		t.Fatalf("overlap duplicated a frame: %v", err)
	}
}

func TestMuxRestartClosedChannelStaysClosed(t *testing.T) {
	dir := t.TempDir()
	cfg := MuxConfig{Channels: 1, Window: 4, StateDir: dir}
	relCfg := ReliableConfig{Window: 16, StateDir: filepath.Join(dir, "rel")}

	var wb bytes.Buffer
	rel1, err := NewReliable(NewSession(&wb, closedReader{}), relCfg)
	if err != nil {
		t.Fatal(err)
	}
	s1, err := NewMux(rel1, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := s1.WriteFrame(ChannelFrame{Channel: 0, Payload: []byte("only")}); err != nil {
		t.Fatal(err)
	}
	if err := s1.CloseChannel(0); err != nil {
		t.Fatal(err)
	}
	s1.Close()
	rel1.Close()

	var wire bytes.Buffer
	rel2, err := NewReliable(NewSession(&wire, closedReader{}), relCfg)
	if err != nil {
		t.Fatal(err)
	}
	s2, err := NewMux(rel2, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if !s2.sc[0].closed {
		t.Fatal("channel did not reopen closed after restart")
	}
	if err := s2.WriteFrame(ChannelFrame{Channel: 0, Payload: []byte("late")}); !errors.Is(err, ErrChannelClosed) {
		t.Fatalf("write after restart err = %v, want ErrChannelClosed", err)
	}
	if err := s2.Flush(); err != nil {
		t.Fatal(err)
	}
	if got := fmt.Sprint(muxChansSeq(parseMuxWire(t, wire.Bytes(), 0))); got != "[0:0 0:1F]" {
		t.Fatalf("wire = %s, want data then FIN", got)
	}
}

func TestMuxJournalTruncatedOrFlipped(t *testing.T) {
	for _, damage := range []string{"truncate", "bitflip"} {
		t.Run(damage, func(t *testing.T) {
			dir := t.TempDir()
			cfg := MuxConfig{Channels: 1, Window: 8, StateDir: dir}
			var buf bytes.Buffer
			rel, err := NewReliable(NewSession(&buf, closedReader{}), ReliableConfig{
				Window: 16, StateDir: filepath.Join(dir, "rel")})
			if err != nil {
				t.Fatal(err)
			}
			s, err := NewMux(rel, cfg)
			if err != nil {
				t.Fatal(err)
			}
			for i := 0; i < 3; i++ {
				if err := s.WriteFrame(ChannelFrame{Channel: 0, Payload: []byte(fmt.Sprintf("p%d", i))}); err != nil {
					t.Fatal(err)
				}
			}
			s.Close()
			rel.Close()

			path := filepath.Join(dir, muxJournalName)
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			switch damage {
			case "truncate":
				data = data[:len(data)-2]
			case "bitflip":
				data[muxLogHdrSize+1] ^= 0x01
			}
			if err := os.WriteFile(path, data, 0o644); err != nil {
				t.Fatal(err)
			}

			var out bytes.Buffer
			rel2, err := NewReliable(NewSession(&out, closedReader{}), ReliableConfig{
				Window: 16, StateDir: filepath.Join(dir, "rel2")})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := NewMux(rel2, cfg); !errors.Is(err, ErrChecksum) {
				t.Fatalf("damaged journal (%s) err = %v, want ErrChecksum", damage, err)
			}
		})
	}
}

// ---- concurrency and full duplex -------------------------------------------

func TestMuxConcurrentWritersDifferentChannels(t *testing.T) {
	const C, W, per = 4, 400, 200
	cfg := muxCfg(t, C, W)
	var wire bytes.Buffer
	snd := newMuxSender(t, &wire, cfg)

	var wg sync.WaitGroup
	for g := 0; g < C; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < per; i++ {
				payload := []byte(fmt.Sprintf("g%di%d", g, i))
				for {
					err := snd.WriteFrame(ChannelFrame{
						Channel: uint16(g), Kind: uint8(g), Payload: payload,
					})
					if err == nil {
						break
					}
					if !errors.Is(err, ErrWindowFull) {
						t.Errorf("write: %v", err)
						return
					}
					// No acks move in this send-only test; once a
					// channel fills, stop: bounded buffer verified
					// elsewhere. Just make the accepted count check
					// below possible by writing per<=W.
					return
				}
			}
		}(g)
	}
	wg.Wait()
	if err := snd.Flush(); err != nil {
		t.Fatal(err)
	}

	// Every frame decodes whole (no interleaving), per-channel seqs are
	// contiguous from 0 and counts match accepted writes.
	ws := parseMuxWire(t, wire.Bytes(), 0)
	counts := map[uint16]int{}
	seqs := map[uint16]map[uint32]bool{}
	for _, w := range ws {
		counts[w.ch]++
		if seqs[w.ch] == nil {
			seqs[w.ch] = map[uint32]bool{}
		}
		if seqs[w.ch][w.seq] {
			t.Fatalf("ch %d seq %d sent twice", w.ch, w.seq)
		}
		seqs[w.ch][w.seq] = true
	}
	for g := 0; g < C; g++ {
		if counts[uint16(g)] != per {
			t.Fatalf("ch %d sent %d frames, want %d", g, counts[uint16(g)], per)
		}
		for i := uint32(0); i < per; i++ {
			if !seqs[uint16(g)][i] {
				t.Fatalf("ch %d missing seq %d", g, i)
			}
		}
	}
}

func TestMuxFullDuplex(t *testing.T) {
	a, b := net.Pipe()
	dir := t.TempDir()
	mk := func(conn net.Conn, sub string) *Mux {
		rel, err := NewReliable(NewSession(conn, conn), ReliableConfig{
			Window: 256, StateDir: filepath.Join(dir, sub, "rel")})
		if err != nil {
			t.Fatal(err)
		}
		m, err := NewMux(rel, MuxConfig{Channels: 3, Window: 64, StateDir: filepath.Join(dir, sub)})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { m.Close(); rel.Close() })
		return m
	}
	ma := mk(a, "a")
	mb := mk(b, "b")

	const perCh = 40
	got := make(map[uint16][]string)
	var gotMu sync.Mutex
	allDone := make(chan struct{})

	// A reads only acks.
	go func() {
		for {
			if _, err := ma.ReadFrame(); err != nil {
				return
			}
		}
	}()

	// B collects data by channel until perCh frames on each of 3
	// channels, plus the three EOFs.
	go func() {
		defer close(allDone)
		for {
			cf, err := mb.ReadFrame()
			if err != nil {
				if errors.Is(err, io.EOF) {
					gotMu.Lock()
					tag := "EOF:" + fmt.Sprint(cf.Channel)
					got[99] = append(got[99], tag)
					done := len(got[0]) == perCh && len(got[1]) == perCh && len(got[2]) == perCh && len(got[99]) == 3
					gotMu.Unlock()
					if done {
						return
					}
					continue
				}
				return
			}
			gotMu.Lock()
			got[cf.Channel] = append(got[cf.Channel], string(cf.Payload))
			gotMu.Unlock()
		}
	}()

	for ch := 0; ch < 3; ch++ {
		for i := 0; i < perCh; i++ {
			if err := ma.WriteFrame(ChannelFrame{
				Channel: uint16(ch), Kind: uint8(ch), Payload: []byte(fmt.Sprintf("c%d-%04d", ch, i)),
			}); err != nil {
				t.Fatalf("write ch%d i%d: %v", ch, i, err)
			}
			if i%9 == 0 {
				if err := ma.Flush(); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
	for ch := 0; ch < 3; ch++ {
		if err := ma.CloseChannel(uint16(ch)); err != nil {
			t.Fatal(err)
		}
	}
	if err := ma.Flush(); err != nil {
		t.Fatal(err)
	}

	<-allDone
	a.Close()
	b.Close()

	gotMu.Lock()
	defer gotMu.Unlock()
	for ch := 0; ch < 3; ch++ {
		if len(got[uint16(ch)]) != perCh {
			t.Fatalf("ch %d got %d frames, want %d", ch, len(got[uint16(ch)]), perCh)
		}
		for i, p := range got[uint16(ch)] {
			if want := fmt.Sprintf("c%d-%04d", ch, i); p != want {
				t.Fatalf("ch %d frame %d = %q, want %q (order/dup changed)", ch, i, p, want)
			}
		}
	}
}

func TestMuxHeaderLayout(t *testing.T) {
	if MuxHeaderSize != 15 {
		t.Fatalf("MuxHeaderSize = %d, want 15", MuxHeaderSize)
	}
	if MuxMaxPayload != ReliableMaxPayload-15 {
		t.Fatalf("MuxMaxPayload = %d, want %d", MuxMaxPayload, ReliableMaxPayload-15)
	}
	b := encodeMuxHeader(0x0102, 0x03040506, 0x07, 0x08090A0B, []byte("xy"))
	want := []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B}
	if !bytes.Equal(b[:11], want) {
		t.Fatalf("header prefix = % x, want % x", b[:11], want)
	}
	if !bytes.Equal(b[15:], []byte("xy")) {
		t.Fatalf("body = %x", b[15:])
	}
	b[3] ^= 0x80
	if _, _, _, _, _, err := decodeMuxHeader(b); !errors.Is(err, ErrChecksum) {
		t.Fatalf("flipped header err = %v, want ErrChecksum", err)
	}
}

func TestMuxClosedChannelStaysClosedAfterFinAckedAndRestart(t *testing.T) {
	// After a FIN is acknowledged the journal prefix is discarded; the
	// durable close marker must keep the channel closed across restart.
	dir := t.TempDir()
	cfg := MuxConfig{Channels: 1, Window: 4, StateDir: dir}
	relCfg := ReliableConfig{Window: 16, StateDir: filepath.Join(dir, "rel")}

	var wb bytes.Buffer
	rel1, err := NewReliable(NewSession(&wb, closedReader{}), relCfg)
	if err != nil {
		t.Fatal(err)
	}
	s1, err := NewMux(rel1, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := s1.WriteFrame(ChannelFrame{Channel: 0, Payload: []byte("x")}); err != nil {
		t.Fatal(err)
	}
	if err := s1.CloseChannel(0); err != nil {
		t.Fatal(err)
	}
	if err := s1.Flush(); err != nil {
		t.Fatal(err)
	}
	// Everything (data seq 0 and FIN seq 1) is acknowledged: checkpoint
	// to 2, which prefixes both records out of the journal.
	s1.applyAck(0, 2)
	if n := len(s1.queue); n != 0 {
		t.Fatalf("queue len = %d, want 0 after full ack", n)
	}
	s1.Close()
	rel1.Close()

	rel2, err := NewReliable(NewSession(&bytes.Buffer{}, closedReader{}), relCfg)
	if err != nil {
		t.Fatal(err)
	}
	s2, err := NewMux(rel2, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if !s2.sc[0].closed {
		t.Fatal("channel reopened after FIN prefix was discarded")
	}
	if err := s2.WriteFrame(ChannelFrame{Channel: 0, Payload: []byte("late")}); !errors.Is(err, ErrChannelClosed) {
		t.Fatalf("write after FIN+ack+restart err = %v, want ErrChannelClosed", err)
	}
	if err := s2.CloseChannel(0); !errors.Is(err, ErrChannelClosed) {
		t.Fatalf("reclose err = %v, want ErrChannelClosed", err)
	}
}

func TestMuxPerChannelSequenceWraparound(t *testing.T) {
	cfg := muxCfg(t, 2, 4)
	f := newFeed()
	rcv := newMuxReceiver(t, f, cfg)
	rcv.bases[0] = 0xFFFFFFFE
	// ch0 crosses the uint32 boundary; ch1 with independent numbering
	// flows in between and must be unaffected.
	f.replace(envWire(t,
		muxDataEnv(0, 0xFFFFFFFE, 0, []byte("fe")),
		muxDataEnv(1, 0, 1, []byte("ch1-zero")),
		muxDataEnv(2, 0xFFFFFFFF, 0, []byte("ff")),
		muxDataEnv(3, 0x00000000, 0, []byte("00")),
	))
	g0, err := rcv.ReadFrame()
	if err != nil || string(g0.Payload) != "fe" {
		t.Fatalf("wrap fe = %+v %v", g0, err)
	}
	g1, err := rcv.ReadFrame()
	if err != nil || g1.Channel != 1 || string(g1.Payload) != "ch1-zero" {
		t.Fatalf("ch1 = %+v %v", g1, err)
	}
	g2, err := rcv.ReadFrame()
	if err != nil || string(g2.Payload) != "ff" {
		t.Fatalf("wrap ff = %+v %v", g2, err)
	}
	g3, err := rcv.ReadFrame()
	if err != nil || string(g3.Payload) != "00" {
		t.Fatalf("wrap 00 = %+v %v", g3, err)
	}

	// Pre-wrap residual on ch0 beyond the window span is refused; the
	// other channel is not implicated.
	f.replace(envWire(t, muxDataEnv(4, 0xFFFFFFF9, 0, []byte("ancient"))))
	if _, err := rcv.ReadFrame(); !errors.Is(err, ErrChecksum) {
		t.Fatalf("pre-wrap residual err = %v, want ErrChecksum", err)
	}
}

func TestMuxMalformedFrames(t *testing.T) {
	cfg := muxCfg(t, 1, 4)

	// Ack frame carrying payload.
	f := newFeed()
	rcv := newMuxReceiver(t, f, cfg)
	f.replace(envWire(t, muxMsg(0, 0, 0, mhFlagAck, 0, []byte("x"))))
	if _, err := rcv.ReadFrame(); !errors.Is(err, ErrChecksum) {
		t.Fatalf("ack-with-payload err = %v, want ErrChecksum", err)
	}

	// FIN carrying payload.
	f2 := newFeed()
	rcv2 := newMuxReceiver(t, f2, cfg)
	f2.replace(envWire(t, muxMsg(0, 0, 0, mhFlagFin, 0, []byte("x"))))
	if _, err := rcv2.ReadFrame(); !errors.Is(err, ErrChecksum) {
		t.Fatalf("fin-with-payload err = %v, want ErrChecksum", err)
	}

	// Unknown flag bit.
	f3 := newFeed()
	rcv3 := newMuxReceiver(t, f3, cfg)
	f3.replace(envWire(t, muxMsg(0, 0, 0, 0x80, 0, nil)))
	if _, err := rcv3.ReadFrame(); !errors.Is(err, ErrChecksum) {
		t.Fatalf("unknown-flag err = %v, want ErrChecksum", err)
	}

	// New frame arriving after the channel's FIN was delivered.
	f4 := newFeed()
	rcv4 := newMuxReceiver(t, f4, cfg)
	f4.replace(envWire(t,
		muxDataEnv(0, 0, 0, []byte("a")),
		muxFinEnv(1, 1, 0),
		muxDataEnv(2, 2, 0, []byte("late")),
	))
	if cf, err := rcv4.ReadFrame(); err != nil || string(cf.Payload) != "a" {
		t.Fatalf("data = %+v %v", cf, err)
	}
	if cf, err := rcv4.ReadFrame(); !errors.Is(err, io.EOF) || cf.Channel != 0 {
		t.Fatalf("FIN = %+v %v, want ch0 EOF", cf, err)
	}
	if _, err := rcv4.ReadFrame(); !errors.Is(err, ErrChecksum) {
		t.Fatalf("post-FIN data err = %v, want ErrChecksum", err)
	}
}

func TestMuxNewConfigValidation(t *testing.T) {
	rel, err := NewReliable(NewSession(&bytes.Buffer{}, closedReader{}),
		ReliableConfig{Window: 4, StateDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer rel.Close()
	if _, err := NewMux(rel, MuxConfig{Channels: 0, Window: 4, StateDir: t.TempDir()}); err == nil {
		t.Fatal("zero channels: want error")
	}
	if _, err := NewMux(rel, MuxConfig{Channels: 2, Window: 0, StateDir: t.TempDir()}); err == nil {
		t.Fatal("zero window: want error")
	}
	if _, err := NewMux(rel, MuxConfig{Channels: 2, Window: 4, StateDir: ""}); err == nil {
		t.Fatal("empty state dir: want error")
	}
}
