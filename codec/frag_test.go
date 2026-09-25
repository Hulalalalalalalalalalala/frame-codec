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
	"time"
)

// ---- deterministic frag harness ----------------------------------------------

// newFragSender builds a frag session whose flushed bytes land in buf.
func newFragSender(t *testing.T, buf *bytes.Buffer, c MuxConfig, maxMsg int) *FragSession {
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
	fs, err := NewFrag(m, FragConfig{MaxMessage: maxMsg, StateDir: filepath.Join(mc.StateDir, "frag")})
	if err != nil {
		t.Fatalf("NewFrag sender: %v", err)
	}
	t.Cleanup(func() { fs.Close(); rel.Close() })
	return fs
}

// newFragReceiver builds a frag reader over f; its standalone acks go to
// io.Discard (deterministic tests drive acks from parsed wire).
func newFragReceiver(t *testing.T, f *feed, c MuxConfig, maxMsg int) *FragSession {
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
	fs, err := NewFrag(m, FragConfig{MaxMessage: maxMsg, StateDir: filepath.Join(mc.StateDir, "frag")})
	if err != nil {
		t.Fatalf("NewFrag receiver: %v", err)
	}
	t.Cleanup(func() { fs.Close(); rel.Close() })
	return fs
}

// fragDataEnv wraps one fragment in a mux message and a reliable
// envelope with explicit sequences.
func fragDataEnv(relSeq, muxSeq uint32, ch uint16, msg, idx, total uint32, fl uint8, ack uint32, chunk []byte) envelope {
	return muxMsg(relSeq, ch, muxSeq, 0, 0, encodeFragHeader(msg, idx, total, fl, ack, chunk))
}

// fragWire is one parsed fragment off the wire.
type fragWire struct {
	ch     uint16
	muxSeq uint32
	msg    uint32
	idx    uint32
	total  uint32
	flags  uint8
	chunk  []byte
}

// parseFragWire decodes flushed sender bytes into fragments, skipping
// mux FIN frames and frag-level standalone acks.
func parseFragWire(t *testing.T, wire []byte, baseSeq uint32) []fragWire {
	t.Helper()
	var out []fragWire
	for _, w := range parseMuxWire(t, wire, baseSeq) {
		if w.flags&mhFlagFin != 0 {
			continue
		}
		msg, idx, total, fl, _, chunk, err := decodeFragHeader(w.payload)
		if err != nil {
			t.Fatalf("decodeFragHeader: %v", err)
		}
		if fl&fhFlagAck != 0 {
			continue
		}
		out = append(out, fragWire{ch: w.ch, muxSeq: w.seq, msg: msg, idx: idx, total: total, flags: fl, chunk: chunk})
	}
	return out
}

// fragPattern returns n bytes of a deterministic pattern.
func fragPattern(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i * 31)
	}
	return b
}

// waitForFragState polls until the channel's persisted reassembly buffer
// holds want bytes (a ReadFrame goroutine persists asynchronously).
func waitForFragState(t *testing.T, dir string, ch uint16, want int) {
	t.Helper()
	for i := 0; i < 400; i++ {
		_, rc, err := loadFragState(dir, ch, 1<<30)
		if err == nil && len(rc.buf) == want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("state file did not reach %d buffered bytes", want)
}

// ---- wire format --------------------------------------------------------------

func TestFragHeaderLayout(t *testing.T) {
	if FragHeaderSize != 21 {
		t.Fatalf("FragHeaderSize = %d, want 21", FragHeaderSize)
	}
	if FragMaxPayload != MuxMaxPayload-21 {
		t.Fatalf("FragMaxPayload = %d, want %d", FragMaxPayload, MuxMaxPayload-21)
	}
	b := encodeFragHeader(0x01020304, 0x05060708, 0x090A0B0C, 0x01, 0x0D0E0F10, []byte("xy"))
	want := []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
		0x09, 0x0A, 0x0B, 0x0C, 0x01, 0x0D, 0x0E, 0x0F, 0x10}
	if !bytes.Equal(b[:17], want) {
		t.Fatalf("header prefix = % x, want % x", b[:17], want)
	}
	if !bytes.Equal(b[21:], []byte("xy")) {
		t.Fatalf("chunk = %x", b[21:])
	}
	msg, idx, total, fl, ack, chunk, err := decodeFragHeader(b)
	if err != nil {
		t.Fatalf("decodeFragHeader: %v", err)
	}
	if msg != 0x01020304 || idx != 0x05060708 || total != 0x090A0B0C ||
		fl != 0x01 || ack != 0x0D0E0F10 || !bytes.Equal(chunk, []byte("xy")) {
		t.Fatalf("round trip = %x %x %x %x %x %q", msg, idx, total, fl, ack, chunk)
	}
	b[3] ^= 0x80
	if _, _, _, _, _, _, err := decodeFragHeader(b); !errors.Is(err, ErrChecksum) {
		t.Fatalf("flipped header err = %v, want ErrChecksum", err)
	}
	if _, _, _, _, _, _, err := decodeFragHeader(b[:10]); !errors.Is(err, ErrChecksum) {
		t.Fatalf("short header err = %v, want ErrChecksum", err)
	}
}

func TestFragConfigValidation(t *testing.T) {
	rel, err := NewReliable(NewSession(&bytes.Buffer{}, closedReader{}),
		ReliableConfig{Window: 4, StateDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer rel.Close()
	m, err := NewMux(rel, MuxConfig{Channels: 1, Window: 4, StateDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	if _, err := NewFrag(m, FragConfig{MaxMessage: 0, StateDir: t.TempDir()}); err == nil {
		t.Fatal("zero max message: want error")
	}
	if _, err := NewFrag(m, FragConfig{MaxMessage: 10, StateDir: ""}); err == nil {
		t.Fatal("empty state dir: want error")
	}
}

// ---- round trips --------------------------------------------------------------

func TestFragRoundTripEmptyAndSingle(t *testing.T) {
	cfg := muxCfg(t, 1, 4)
	var wire bytes.Buffer
	snd := newFragSender(t, &wire, cfg, 1<<20)

	if err := snd.WriteFrame(FragFrame{Channel: 0, Kind: 1}); err != nil {
		t.Fatal(err)
	}
	if err := snd.WriteFrame(FragFrame{Channel: 0, Kind: 2, Payload: []byte("hello")}); err != nil {
		t.Fatal(err)
	}
	if err := snd.CloseChannel(0); err != nil {
		t.Fatal(err)
	}
	if err := snd.Flush(); err != nil {
		t.Fatal(err)
	}

	// The empty payload is exactly one fragment declaring total 0.
	ws := parseFragWire(t, wire.Bytes(), 0)
	if len(ws) != 2 {
		t.Fatalf("fragments = %d, want 2", len(ws))
	}
	if ws[0].msg != 0 || ws[0].idx != 0 || ws[0].total != 0 || ws[0].flags != fhFlagLast || len(ws[0].chunk) != 0 {
		t.Fatalf("empty fragment = %+v", ws[0])
	}
	if ws[1].msg != 1 || ws[1].total != 5 || ws[1].flags != fhFlagLast {
		t.Fatalf("single fragment = %+v", ws[1])
	}

	f := newFeed()
	rcv := newFragReceiver(t, f, cfg, 1<<20)
	f.replace(wire.Bytes())

	g0, err := rcv.ReadFrame()
	if err != nil || g0.Kind != 1 || len(g0.Payload) != 0 {
		t.Fatalf("empty message = %+v, %v", g0, err)
	}
	g1, err := rcv.ReadFrame()
	if err != nil || g1.Kind != 2 || string(g1.Payload) != "hello" {
		t.Fatalf("single message = %+v, %v", g1, err)
	}
	if _, err := rcv.ReadFrame(); !errors.Is(err, io.EOF) {
		t.Fatalf("err = %v, want io.EOF after FIN", err)
	}
}

func TestFragInterleavedChannels(t *testing.T) {
	cfg := muxCfg(t, 2, 4)
	var wire bytes.Buffer
	snd := newFragSender(t, &wire, cfg, 1<<20)

	big := fragPattern(FragMaxPayload + 10) // two fragments on ch0
	if err := snd.WriteFrame(FragFrame{Channel: 0, Kind: 1, Payload: big}); err != nil {
		t.Fatal(err)
	}
	if err := snd.WriteFrame(FragFrame{Channel: 1, Kind: 2, Payload: []byte("one")}); err != nil {
		t.Fatal(err)
	}
	if err := snd.Flush(); err != nil {
		t.Fatal(err)
	}

	// Round-robin interleaves ch0's fragments with ch1's frame.
	if got := fmt.Sprint(muxChansSeq(parseMuxWire(t, wire.Bytes(), 0))); got != "[0:0 1:0 0:1]" {
		t.Fatalf("wire = %s, want [0:0 1:0 0:1]", got)
	}

	f := newFeed()
	rcv := newFragReceiver(t, f, cfg, 1<<20)
	f.replace(wire.Bytes())

	// ch1's complete message is delivered while ch0's is still
	// mid-assembly: a stalled channel never blocks another.
	g1, err := rcv.ReadFrame()
	if err != nil || g1.Channel != 1 || string(g1.Payload) != "one" {
		t.Fatalf("first delivery = %+v, %v, want ch1/one", g1, err)
	}
	g0, err := rcv.ReadFrame()
	if err != nil || g0.Channel != 0 || !bytes.Equal(g0.Payload, big) {
		t.Fatalf("second delivery = ch%d len %d, %v, want ch0/%d bytes",
			g0.Channel, len(g0.Payload), err, len(big))
	}
}

func TestFragRoundTripLargeMultiWindow(t *testing.T) {
	a, b := net.Pipe()
	dir := t.TempDir()
	mk := func(conn net.Conn, sub string) *FragSession {
		rel, err := NewReliable(NewSession(conn, conn), ReliableConfig{
			Window: 64, StateDir: filepath.Join(dir, sub, "rel")})
		if err != nil {
			t.Fatal(err)
		}
		m, err := NewMux(rel, MuxConfig{Channels: 2, Window: 2, StateDir: filepath.Join(dir, sub)})
		if err != nil {
			t.Fatal(err)
		}
		fs, err := NewFrag(m, FragConfig{MaxMessage: 16 << 20, StateDir: filepath.Join(dir, sub, "frag")})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { fs.Close(); rel.Close() })
		return fs
	}
	fa := mk(a, "a")
	fb := mk(b, "b")

	// Six fragments through a window of two.
	big := fragPattern(5*FragMaxPayload + 12345)
	const smalls = 20

	// A reads only acks.
	go func() {
		for {
			if _, err := fa.ReadFrame(); err != nil {
				return
			}
		}
	}()

	type result struct {
		big    []byte
		smalls []string
		eofs   []uint16
		err    error
	}
	resCh := make(chan result, 1)
	go func() {
		var res result
		for res.err == nil {
			if res.big != nil && len(res.smalls) == smalls && len(res.eofs) == 2 {
				break
			}
			ff, err := fb.ReadFrame()
			if errors.Is(err, io.EOF) {
				res.eofs = append(res.eofs, ff.Channel)
				continue
			}
			if err != nil {
				res.err = err
				break
			}
			if ff.Channel == 0 {
				res.big = append([]byte(nil), ff.Payload...)
			} else {
				res.smalls = append(res.smalls, string(ff.Payload))
			}
		}
		resCh <- res
	}()

	// All flushing happens on one goroutine: once the collector is done
	// the pipes are closed, which unwinds any blocked flush.
	flushStop := make(chan struct{})
	flusherDone := make(chan struct{})
	go func() {
		defer close(flusherDone)
		for {
			select {
			case <-flushStop:
				return
			default:
			}
			if err := fa.Flush(); err != nil {
				return
			}
			time.Sleep(time.Millisecond)
		}
	}()

	// The big message is accepted in one call even though its fragments
	// span several windows.
	if err := fa.WriteFrame(FragFrame{Channel: 0, Kind: 7, Payload: big}); err != nil {
		t.Fatalf("big write: %v", err)
	}
	waitQuota := func(step string) {
		t.Helper()
		for {
			select {
			case res := <-resCh:
				t.Fatalf("collector exited during %s: err=%v big=%d smalls=%d eofs=%v",
					step, res.err, len(res.big), len(res.smalls), res.eofs)
			case <-time.After(10 * time.Second):
				t.Fatalf("timed out during %s", step)
			default:
			}
			var err error
			switch step {
			case "close0":
				err = fa.CloseChannel(0)
			case "close1":
				err = fa.CloseChannel(1)
			}
			if err == nil {
				return
			}
			if !errors.Is(err, ErrWindowFull) {
				t.Fatalf("%s: %v", step, err)
			}
			time.Sleep(time.Millisecond)
		}
	}
	for i := 0; i < smalls; i++ {
		payload := []byte(fmt.Sprintf("s%02d", i))
		for {
			select {
			case res := <-resCh:
				t.Fatalf("collector exited during small %d: err=%v", i, res.err)
			case <-time.After(10 * time.Second):
				t.Fatalf("timed out during small %d", i)
			default:
			}
			err := fa.WriteFrame(FragFrame{Channel: 1, Kind: uint8(i), Payload: payload})
			if err == nil {
				break
			}
			if !errors.Is(err, ErrWindowFull) {
				t.Fatalf("small write %d: %v", i, err)
			}
			time.Sleep(time.Millisecond)
		}
	}
	waitQuota("close0")
	waitQuota("close1")

	res := <-resCh
	a.Close()
	b.Close()
	close(flushStop)
	<-flusherDone

	if res.err != nil {
		t.Fatalf("collector read error: %v", res.err)
	}
	if !bytes.Equal(res.big, big) {
		t.Fatalf("big message: got %d bytes, equal=%v", len(res.big), bytes.Equal(res.big, big))
	}
	if len(res.smalls) != smalls {
		t.Fatalf("smalls = %d, want %d", len(res.smalls), smalls)
	}
	for i, s := range res.smalls {
		if want := fmt.Sprintf("s%02d", i); s != want {
			t.Fatalf("small %d = %q, want %q", i, s, want)
		}
	}
	if len(res.eofs) != 2 {
		t.Fatalf("eofs = %v, want 2", res.eofs)
	}
}

// ---- send-side limits -----------------------------------------------------------

func TestFragOverCapWriteRejected(t *testing.T) {
	cfg := muxCfg(t, 1, 4)
	var wire bytes.Buffer
	snd := newFragSender(t, &wire, cfg, 10)

	// Directly comparable with the established over-limit value.
	if err := snd.WriteFrame(FragFrame{Channel: 0, Payload: make([]byte, 11)}); err != ErrTooLarge {
		t.Fatalf("over-cap err = %v, want ErrTooLarge", err)
	}
	if err := snd.WriteFrame(FragFrame{Channel: 0, Payload: []byte("hello")}); err != nil {
		t.Fatal(err)
	}
	if err := snd.Flush(); err != nil {
		t.Fatal(err)
	}
	// The rejected write consumed no message sequence.
	ws := parseFragWire(t, wire.Bytes(), 0)
	if len(ws) != 1 || ws[0].msg != 0 || ws[0].total != 5 {
		t.Fatalf("wire = %+v, want one message with msg 0 total 5", ws)
	}
}

func TestFragQuotaFullRejected(t *testing.T) {
	const W = 2
	cfg := muxCfg(t, 1, W)
	var wire bytes.Buffer
	snd := newFragSender(t, &wire, cfg, 1<<20)

	// Two fragments fill the per-channel quota exactly.
	if err := snd.WriteFrame(FragFrame{Channel: 0, Payload: fragPattern(FragMaxPayload + 1)}); err != nil {
		t.Fatal(err)
	}
	err := snd.WriteFrame(FragFrame{Channel: 0, Payload: []byte("drop")})
	if err != ErrWindowFull || err != ErrTooLarge {
		t.Fatalf("quota err = %v, want ErrWindowFull == ErrTooLarge", err)
	}
	// Acks free the quota; the retried write takes the next message
	// sequence, proving the rejected one consumed none.
	snd.mux.applyAck(0, 2)
	snd.mux.rel.applyAck(2)
	if err := snd.WriteFrame(FragFrame{Channel: 0, Payload: []byte("keep")}); err != nil {
		t.Fatalf("write after ack: %v", err)
	}
	if err := snd.Flush(); err != nil {
		t.Fatal(err)
	}
	ws := parseFragWire(t, wire.Bytes(), 0)
	if len(ws) != 1 || ws[0].msg != 1 || ws[0].muxSeq != 2 || string(ws[0].chunk) != "keep" {
		t.Fatalf("wire = %+v, want msg 1 at mux seq 2", ws)
	}
}

// ---- receive-side refusals ------------------------------------------------------

func TestFragDeclaredTotalOverCapDropped(t *testing.T) {
	cfg := muxCfg(t, 1, 8)
	f := newFeed()
	rcv := newFragReceiver(t, f, cfg, 10)

	// A message declaring 100 bytes against a cap of 10 is dropped with
	// the established over-limit value; the stream continues.
	f.replace(envWire(t, fragDataEnv(0, 0, 0, 0, 0, 100, 0, 0, nil)))
	if _, err := rcv.ReadFrame(); err != ErrTooLarge {
		t.Fatalf("over-cap declaration err = %v, want ErrTooLarge", err)
	}
	f.replace(envWire(t,
		fragDataEnv(1, 1, 0, 0, 1, 100, fhFlagLast, 0, nil),
		fragDataEnv(2, 2, 0, 1, 0, 3, fhFlagLast, 0, []byte("abc")),
	))
	got, err := rcv.ReadFrame()
	if err != nil || string(got.Payload) != "abc" {
		t.Fatalf("after drop = %+v, %v, want abc", got, err)
	}
}

func TestFragOldResidualBeyondReachAndGap(t *testing.T) {
	cfg := muxCfg(t, 1, 8)

	// A fragment of an already delivered message is an old-generation
	// residual: refused, and the refusal is sticky.
	f := newFeed()
	rcv := newFragReceiver(t, f, cfg, 1<<20)
	f.replace(envWire(t, fragDataEnv(0, 0, 0, 0, 0, 1, fhFlagLast, 0, []byte("z"))))
	if got, err := rcv.ReadFrame(); err != nil || string(got.Payload) != "z" {
		t.Fatalf("first = %+v, %v", got, err)
	}
	f.replace(envWire(t, fragDataEnv(1, 1, 0, 0, 0, 1, fhFlagLast, 0, []byte("z"))))
	if _, err := rcv.ReadFrame(); err != ErrChecksum {
		t.Fatalf("old residual err = %v, want ErrChecksum", err)
	}
	if _, err := rcv.ReadFrame(); err != ErrChecksum {
		t.Fatalf("sticky err = %v, want ErrChecksum", err)
	}

	// A message beyond the current receive position is refused.
	f2 := newFeed()
	rcv2 := newFragReceiver(t, f2, cfg, 1<<20)
	f2.replace(envWire(t, fragDataEnv(0, 0, 0, 3, 0, 1, fhFlagLast, 0, []byte("z"))))
	if _, err := rcv2.ReadFrame(); err != ErrChecksum {
		t.Fatalf("beyond reach err = %v, want ErrChecksum", err)
	}

	// A message starting past its first fragment is refused.
	f3 := newFeed()
	rcv3 := newFragReceiver(t, f3, cfg, 1<<20)
	f3.replace(envWire(t, fragDataEnv(0, 0, 0, 0, 1,
		uint32(2*FragMaxPayload), fhFlagLast, 0, fragPattern(FragMaxPayload))))
	if _, err := rcv3.ReadFrame(); err != ErrChecksum {
		t.Fatalf("gap at start err = %v, want ErrChecksum", err)
	}
}

func TestFragDuplicateFragment(t *testing.T) {
	cfg := muxCfg(t, 1, 8)
	total := uint32(FragMaxPayload + 7)
	c0 := fragPattern(FragMaxPayload)
	c1 := fragPattern(7)

	// An identical retransmission is absorbed silently.
	f := newFeed()
	rcv := newFragReceiver(t, f, cfg, 1<<20)
	f.replace(envWire(t,
		fragDataEnv(0, 0, 0, 0, 0, total, 0, 0, c0),
		fragDataEnv(1, 1, 0, 0, 0, total, 0, 0, c0),
		fragDataEnv(2, 2, 0, 0, 1, total, fhFlagLast, 0, c1),
	))
	got, err := rcv.ReadFrame()
	if err != nil || !bytes.Equal(got.Payload, append(append([]byte(nil), c0...), c1...)) {
		t.Fatalf("assembled = %d bytes, %v", len(got.Payload), err)
	}
	f.closeFeed()
	if _, err := rcv.ReadFrame(); !errors.Is(err, io.EOF) {
		t.Fatalf("err = %v, want io.EOF with no duplicate delivery", err)
	}

	// A same-position duplicate with different content is refused.
	other := append([]byte(nil), c0...)
	other[0] ^= 0xFF
	f2 := newFeed()
	rcv2 := newFragReceiver(t, f2, cfg, 1<<20)
	f2.replace(envWire(t,
		fragDataEnv(0, 0, 0, 0, 0, total, 0, 0, c0),
		fragDataEnv(1, 1, 0, 0, 0, total, 0, 0, other),
	))
	if _, err := rcv2.ReadFrame(); err != ErrChecksum {
		t.Fatalf("conflicting duplicate err = %v, want ErrChecksum", err)
	}
	if _, err := rcv2.ReadFrame(); err != ErrChecksum {
		t.Fatalf("sticky err = %v, want ErrChecksum", err)
	}
}

func TestFragDamagedHeaderStopsDelivery(t *testing.T) {
	cfg := muxCfg(t, 1, 8)

	// A bit-flipped fragment header (valid mux CRC, broken frag CRC).
	bad := encodeFragHeader(0, 0, 3, fhFlagLast, 0, []byte("abc"))
	bad[1] ^= 0x01
	f := newFeed()
	rcv := newFragReceiver(t, f, cfg, 1<<20)
	f.replace(envWire(t, muxDataEnv(0, 0, 0, bad)))
	if _, err := rcv.ReadFrame(); err != ErrChecksum {
		t.Fatalf("bitflip err = %v, want ErrChecksum", err)
	}
	if _, err := rcv.ReadFrame(); err != ErrChecksum {
		t.Fatalf("sticky err = %v, want ErrChecksum", err)
	}

	// A payload too short for the fragment header.
	f2 := newFeed()
	rcv2 := newFragReceiver(t, f2, cfg, 1<<20)
	f2.replace(envWire(t, muxDataEnv(0, 0, 0, []byte("tiny"))))
	if _, err := rcv2.ReadFrame(); err != ErrChecksum {
		t.Fatalf("short err = %v, want ErrChecksum", err)
	}

	// An unknown flag bit.
	f3 := newFeed()
	rcv3 := newFragReceiver(t, f3, cfg, 1<<20)
	f3.replace(envWire(t, fragDataEnv(0, 0, 0, 0, 0, 1, 0x80|fhFlagLast, 0, []byte("z"))))
	if _, err := rcv3.ReadFrame(); err != ErrChecksum {
		t.Fatalf("unknown flag err = %v, want ErrChecksum", err)
	}
}

func TestFragAckRefusals(t *testing.T) {
	cfg := muxCfg(t, 1, 8)

	// An acknowledgement past the send frontier on a data fragment.
	f := newFeed()
	rcv := newFragReceiver(t, f, cfg, 1<<20)
	f.replace(envWire(t, fragDataEnv(0, 0, 0, 0, 0, 1, fhFlagLast, 7, []byte("z"))))
	if _, err := rcv.ReadFrame(); err != ErrChecksum {
		t.Fatalf("frontier ack err = %v, want ErrChecksum", err)
	}

	// The same on a standalone acknowledgement.
	f2 := newFeed()
	rcv2 := newFragReceiver(t, f2, cfg, 1<<20)
	f2.replace(envWire(t, muxDataEnv(0, 0, 0, encodeFragHeader(0, 0, 0, fhFlagAck, 7, nil))))
	if _, err := rcv2.ReadFrame(); err != ErrChecksum {
		t.Fatalf("standalone frontier ack err = %v, want ErrChecksum", err)
	}

	// A standalone acknowledgement carrying a chunk.
	f3 := newFeed()
	rcv3 := newFragReceiver(t, f3, cfg, 1<<20)
	f3.replace(envWire(t, muxDataEnv(0, 0, 0, encodeFragHeader(0, 0, 0, fhFlagAck, 0, []byte("x")))))
	if _, err := rcv3.ReadFrame(); err != ErrChecksum {
		t.Fatalf("ack-with-chunk err = %v, want ErrChecksum", err)
	}

	// LAST and ACK combined.
	f4 := newFeed()
	rcv4 := newFragReceiver(t, f4, cfg, 1<<20)
	f4.replace(envWire(t, muxDataEnv(0, 0, 0, encodeFragHeader(0, 0, 0, fhFlagAck|fhFlagLast, 0, nil))))
	if _, err := rcv4.ReadFrame(); err != ErrChecksum {
		t.Fatalf("combined flags err = %v, want ErrChecksum", err)
	}

	// A well-formed standalone acknowledgement is absorbed silently.
	f5 := newFeed()
	rcv5 := newFragReceiver(t, f5, cfg, 1<<20)
	f5.replace(envWire(t, muxDataEnv(0, 0, 0, encodeFragHeader(0, 0, 0, fhFlagAck, 0, nil))))
	f5.closeFeed()
	if _, err := rcv5.ReadFrame(); !errors.Is(err, io.EOF) {
		t.Fatalf("valid ack err = %v, want io.EOF with no delivery", err)
	}
}

// ---- restart and recovery -------------------------------------------------------

func TestFragRestartResumesAndContinues(t *testing.T) {
	dir := t.TempDir()
	mcfg := MuxConfig{Channels: 1, Window: 8, StateDir: filepath.Join(dir, "snd")}
	rcfg := ReliableConfig{Window: 32, StateDir: filepath.Join(dir, "snd", "rel")}
	fcfg := FragConfig{MaxMessage: 8 << 20, StateDir: filepath.Join(dir, "snd", "frag")}

	var wire1 bytes.Buffer
	rel1, err := NewReliable(NewSession(&wire1, closedReader{}), rcfg)
	if err != nil {
		t.Fatal(err)
	}
	m1, err := NewMux(rel1, mcfg)
	if err != nil {
		t.Fatal(err)
	}
	s1, err := NewFrag(m1, fcfg)
	if err != nil {
		t.Fatal(err)
	}

	big := fragPattern(2*FragMaxPayload + 100)
	if err := s1.WriteFrame(FragFrame{Channel: 0, Kind: 7, Payload: big}); err != nil {
		t.Fatal(err)
	}
	if err := s1.WriteFrame(FragFrame{Channel: 0, Kind: 8, Payload: []byte("second")}); err != nil {
		t.Fatal(err)
	}
	if err := s1.Flush(); err != nil {
		t.Fatal(err)
	}

	f := newFeed()
	rrel, err := NewReliable(NewSession(io.Discard, f),
		ReliableConfig{Window: 32, StateDir: filepath.Join(dir, "rcv", "rel")})
	if err != nil {
		t.Fatal(err)
	}
	rmux, err := NewMux(rrel, MuxConfig{Channels: 1, Window: 8, StateDir: filepath.Join(dir, "rcv")})
	if err != nil {
		t.Fatal(err)
	}
	rcv, err := NewFrag(rmux, FragConfig{MaxMessage: 8 << 20, StateDir: filepath.Join(dir, "rcv", "frag")})
	if err != nil {
		t.Fatal(err)
	}
	f.replace(wire1.Bytes())

	g0, err := rcv.ReadFrame()
	if err != nil || g0.Kind != 7 || !bytes.Equal(g0.Payload, big) {
		t.Fatalf("big = %d bytes kind %d, %v", len(g0.Payload), g0.Kind, err)
	}
	g1, err := rcv.ReadFrame()
	if err != nil || string(g1.Payload) != "second" {
		t.Fatalf("second = %+v, %v", g1, err)
	}

	// All four mux frames confirmed; the journal prefix is discarded.
	m1.applyAck(0, 4)
	if err := s1.Close(); err != nil {
		t.Fatal(err)
	}
	if err := rel1.Close(); err != nil {
		t.Fatal(err)
	}

	// Restart with the same state: message numbering resumes at 2 even
	// though the journal is empty, so a new message can never collide
	// with one the peer already received.
	var wire2 bytes.Buffer
	rel2, err := NewReliable(NewSession(&wire2, closedReader{}), rcfg)
	if err != nil {
		t.Fatal(err)
	}
	m2, err := NewMux(rel2, mcfg)
	if err != nil {
		t.Fatal(err)
	}
	s2, err := NewFrag(m2, fcfg)
	if err != nil {
		t.Fatalf("restart: %v", err)
	}
	if s2.sendNext[0] != 2 {
		t.Fatalf("sendNext = %d, want 2", s2.sendNext[0])
	}
	if err := s2.WriteFrame(FragFrame{Channel: 0, Kind: 9, Payload: []byte("third")}); err != nil {
		t.Fatal(err)
	}
	if err := s2.Flush(); err != nil {
		t.Fatal(err)
	}

	f.replace(wire2.Bytes())
	g2, err := rcv.ReadFrame()
	if err != nil || g2.Kind != 9 || string(g2.Payload) != "third" {
		t.Fatalf("third = %+v, %v", g2, err)
	}
	f.closeFeed()
	if _, err := rcv.ReadFrame(); !errors.Is(err, io.EOF) {
		t.Fatalf("err = %v, want io.EOF with nothing delivered twice", err)
	}
}

func TestFragRestartPartialMessageResume(t *testing.T) {
	dir := t.TempDir()
	mcfg := MuxConfig{Channels: 1, Window: 8, StateDir: filepath.Join(dir, "snd")}
	rcfg := ReliableConfig{Window: 32, StateDir: filepath.Join(dir, "snd", "rel")}
	fcfg := FragConfig{MaxMessage: 8 << 20, StateDir: filepath.Join(dir, "snd", "frag")}

	var wire1 bytes.Buffer
	rel1, err := NewReliable(NewSession(&wire1, closedReader{}), rcfg)
	if err != nil {
		t.Fatal(err)
	}
	m1, err := NewMux(rel1, mcfg)
	if err != nil {
		t.Fatal(err)
	}
	s1, err := NewFrag(m1, fcfg)
	if err != nil {
		t.Fatal(err)
	}

	big := fragPattern(2*FragMaxPayload + 100) // three fragments
	if err := s1.WriteFrame(FragFrame{Channel: 0, Kind: 7, Payload: big}); err != nil {
		t.Fatal(err)
	}
	if err := s1.Flush(); err != nil {
		t.Fatal(err)
	}

	// The receiver consumes only the first two of three fragments.
	var dataEnvs []envelope
	for _, e := range parseWireEnvelopes(t, wire1.Bytes()) {
		if e.typ == envTypeData {
			dataEnvs = append(dataEnvs, e)
		}
	}
	if len(dataEnvs) != 3 {
		t.Fatalf("wire1 carries %d data frames, want 3", len(dataEnvs))
	}
	f := newFeed()
	rrel, err := NewReliable(NewSession(io.Discard, f),
		ReliableConfig{Window: 32, StateDir: filepath.Join(dir, "rcv", "rel")})
	if err != nil {
		t.Fatal(err)
	}
	rmux, err := NewMux(rrel, MuxConfig{Channels: 1, Window: 8, StateDir: filepath.Join(dir, "rcv")})
	if err != nil {
		t.Fatal(err)
	}
	rcvFragDir := filepath.Join(dir, "rcv", "frag")
	rcv, err := NewFrag(rmux, FragConfig{MaxMessage: 8 << 20, StateDir: rcvFragDir})
	if err != nil {
		t.Fatal(err)
	}
	f.replace(envWire(t, dataEnvs[0], dataEnvs[1]))
	got := make(chan FragFrame, 1)
	go func() {
		if ff, err := rcv.ReadFrame(); err == nil {
			got <- ff
		}
	}()
	// The interrupted assembly is durable before the restart.
	waitForFragState(t, rcvFragDir, 0, 2*FragMaxPayload)

	// The first two fragments are confirmed at the sender; it restarts
	// and resends only the unconfirmed third.
	m1.applyAck(0, 2)
	if err := s1.Close(); err != nil {
		t.Fatal(err)
	}
	if err := rel1.Close(); err != nil {
		t.Fatal(err)
	}
	var wire2 bytes.Buffer
	rel2, err := NewReliable(NewSession(&wire2, closedReader{}), rcfg)
	if err != nil {
		t.Fatal(err)
	}
	m2, err := NewMux(rel2, mcfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := NewFrag(m2, fcfg); err != nil {
		t.Fatalf("restart: %v", err)
	}
	if err := m2.Flush(); err != nil {
		t.Fatal(err)
	}
	f.replace(wire2.Bytes())

	select {
	case ff := <-got:
		if !bytes.Equal(ff.Payload, big) {
			t.Fatalf("reassembled %d bytes, content mismatch", len(ff.Payload))
		}
	case <-time.After(5 * time.Second):
		t.Fatal("message not reassembled after restart")
	}
}

func TestFragReceiverStateRecovery(t *testing.T) {
	dir := t.TempDir()
	fragDir := filepath.Join(dir, "frag")
	total := uint32(2*FragMaxPayload + 50)
	c0 := bytes.Repeat([]byte{0x11}, FragMaxPayload)
	c1 := bytes.Repeat([]byte{0x22}, FragMaxPayload)
	c2 := bytes.Repeat([]byte{0x33}, 50)

	// First incarnation: two of three fragments arrive and are persisted.
	f1 := newFeed()
	rrel1, err := NewReliable(NewSession(io.Discard, f1),
		ReliableConfig{Window: 16, StateDir: filepath.Join(dir, "r1", "rel")})
	if err != nil {
		t.Fatal(err)
	}
	rmux1, err := NewMux(rrel1, MuxConfig{Channels: 1, Window: 8, StateDir: filepath.Join(dir, "r1", "mux")})
	if err != nil {
		t.Fatal(err)
	}
	r1, err := NewFrag(rmux1, FragConfig{MaxMessage: 8 << 20, StateDir: fragDir})
	if err != nil {
		t.Fatal(err)
	}
	f1.replace(envWire(t,
		fragDataEnv(0, 0, 0, 0, 0, total, 0, 0, c0),
		fragDataEnv(1, 1, 0, 0, 1, total, 0, 0, c1),
	))
	blocked := make(chan error, 1)
	go func() { _, err := r1.ReadFrame(); blocked <- err }()
	waitForFragState(t, fragDir, 0, 2*FragMaxPayload)
	f1.closeFeed()
	if err := <-blocked; !errors.Is(err, io.EOF) {
		t.Fatalf("first incarnation err = %v, want io.EOF", err)
	}
	r1.Close()
	rrel1.Close()

	// Second incarnation over a fresh connection: the assembly resumes
	// from the persisted position; retransmitted fragments are deduped.
	f2 := newFeed()
	rrel2, err := NewReliable(NewSession(io.Discard, f2),
		ReliableConfig{Window: 16, StateDir: filepath.Join(dir, "r2", "rel")})
	if err != nil {
		t.Fatal(err)
	}
	rmux2, err := NewMux(rrel2, MuxConfig{Channels: 1, Window: 8, StateDir: filepath.Join(dir, "r2", "mux")})
	if err != nil {
		t.Fatal(err)
	}
	r2, err := NewFrag(rmux2, FragConfig{MaxMessage: 8 << 20, StateDir: fragDir})
	if err != nil {
		t.Fatalf("recovery: %v", err)
	}
	f2.replace(envWire(t,
		fragDataEnv(0, 0, 0, 0, 0, total, 0, 0, c0), // retransmit of idx 0
		fragDataEnv(1, 1, 0, 0, 1, total, 0, 0, c1), // retransmit of idx 1
		fragDataEnv(2, 2, 0, 0, 2, total, fhFlagLast, 0, c2),
	))
	got, err := r2.ReadFrame()
	if err != nil {
		t.Fatalf("ReadFrame after recovery: %v", err)
	}
	want := append(append(append([]byte(nil), c0...), c1...), c2...)
	if !bytes.Equal(got.Payload, want) {
		t.Fatalf("reassembled %d bytes, content mismatch", len(got.Payload))
	}
	r2.Close()
	rrel2.Close()
}

func TestFragStateFileCorrupt(t *testing.T) {
	for _, damage := range []string{"truncate", "bitflip"} {
		t.Run(damage, func(t *testing.T) {
			dir := t.TempDir()
			fragDir := filepath.Join(dir, "frag")

			var buf bytes.Buffer
			rel, err := NewReliable(NewSession(&buf, closedReader{}),
				ReliableConfig{Window: 8, StateDir: filepath.Join(dir, "rel")})
			if err != nil {
				t.Fatal(err)
			}
			m, err := NewMux(rel, MuxConfig{Channels: 1, Window: 4, StateDir: filepath.Join(dir, "mux")})
			if err != nil {
				t.Fatal(err)
			}
			fs, err := NewFrag(m, FragConfig{MaxMessage: 1 << 20, StateDir: fragDir})
			if err != nil {
				t.Fatal(err)
			}
			if err := fs.WriteFrame(FragFrame{Channel: 0, Payload: []byte("x")}); err != nil {
				t.Fatal(err)
			}
			fs.Close()
			rel.Close()

			path := filepath.Join(fragDir, "frag000.state")
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			switch damage {
			case "truncate":
				data = data[:len(data)-2]
			case "bitflip":
				data[2] ^= 0x01
			}
			if err := os.WriteFile(path, data, 0o644); err != nil {
				t.Fatal(err)
			}

			rel2, err := NewReliable(NewSession(&bytes.Buffer{}, closedReader{}),
				ReliableConfig{Window: 8, StateDir: filepath.Join(dir, "rel2")})
			if err != nil {
				t.Fatal(err)
			}
			defer rel2.Close()
			m2, err := NewMux(rel2, MuxConfig{Channels: 1, Window: 4, StateDir: filepath.Join(dir, "mux2")})
			if err != nil {
				t.Fatal(err)
			}
			defer m2.Close()
			if _, err := NewFrag(m2, FragConfig{MaxMessage: 1 << 20, StateDir: fragDir}); !errors.Is(err, ErrChecksum) {
				t.Fatalf("damaged state (%s) err = %v, want ErrChecksum", damage, err)
			}
		})
	}
}

// ---- close semantics ------------------------------------------------------------

func TestFragCloseChannelSemantics(t *testing.T) {
	cfg := muxCfg(t, 3, 8)
	var wire bytes.Buffer
	snd := newFragSender(t, &wire, cfg, 1<<20)

	if err := snd.WriteFrame(FragFrame{Channel: 0, Kind: 3, Payload: []byte("data")}); err != nil {
		t.Fatal(err)
	}
	if err := snd.CloseChannel(0); err != nil {
		t.Fatal(err)
	}
	if err := snd.CloseChannel(1); err != nil { // empty channel
		t.Fatal(err)
	}
	// Refusals compare equal to the established values directly.
	if err := snd.CloseChannel(0); err != ErrChannelClosed {
		t.Fatalf("second close err = %v, want ErrChannelClosed", err)
	}
	if err := snd.WriteFrame(FragFrame{Channel: 0, Payload: []byte("z")}); err != ErrChannelClosed {
		t.Fatalf("write on closed err = %v, want ErrChannelClosed", err)
	}
	if err := snd.CloseChannel(9); err != ErrChecksum {
		t.Fatalf("out-of-range close err = %v, want ErrChecksum", err)
	}
	if err := snd.Flush(); err != nil {
		t.Fatal(err)
	}

	f := newFeed()
	rcv := newFragReceiver(t, f, cfg, 1<<20)
	f.replace(wire.Bytes())
	got, err := rcv.ReadFrame()
	if err != nil || string(got.Payload) != "data" {
		t.Fatalf("data = %+v, %v", got, err)
	}
	eofs := map[uint16]bool{}
	for len(eofs) < 2 {
		cf, err := rcv.ReadFrame()
		if !errors.Is(err, io.EOF) {
			t.Fatalf("err = %v, want io.EOF", err)
		}
		eofs[cf.Channel] = true
	}
	if !eofs[0] || !eofs[1] {
		t.Fatalf("eofs = %v, want channels 0 and 1", eofs)
	}

	// Concurrent closes of one channel do not panic; exactly one wins
	// and the rest get the established closed value.
	const n = 6
	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			errs[i] = snd.CloseChannel(2)
		}(i)
	}
	wg.Wait()
	ok := 0
	for _, err := range errs {
		if err == nil {
			ok++
			continue
		}
		if err != ErrChannelClosed {
			t.Fatalf("concurrent close err = %v, want ErrChannelClosed", err)
		}
	}
	if ok != 1 {
		t.Fatalf("%d closes succeeded, want exactly 1", ok)
	}

	// After Close, a repeated close still returns the established value
	// without hanging on the stopped pump.
	snd.Close()
	if err := snd.CloseChannel(2); err != ErrChannelClosed {
		t.Fatalf("close after Close err = %v, want ErrChannelClosed", err)
	}
}

// ---- concurrency ------------------------------------------------------------------

func TestFragConcurrentWritersSameChannel(t *testing.T) {
	cfg := muxCfg(t, 1, 64)
	var wire bytes.Buffer
	snd := newFragSender(t, &wire, cfg, 8<<20)

	const writers, per = 2, 4
	var wg sync.WaitGroup
	for g := 0; g < writers; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < per; i++ {
				payload := bytes.Repeat([]byte{byte(g*per + i)}, FragMaxPayload+13)
				if err := snd.WriteFrame(FragFrame{
					Channel: 0, Kind: uint8(g*per + i), Payload: payload,
				}); err != nil {
					t.Errorf("write: %v", err)
					return
				}
			}
		}(g)
	}
	wg.Wait()
	if err := snd.Flush(); err != nil {
		t.Fatal(err)
	}

	// Fragments of one message are contiguous and ordered on the wire:
	// concurrent writers never interleave two messages of one channel.
	ws := parseFragWire(t, wire.Bytes(), 0)
	if len(ws) != writers*per*2 {
		t.Fatalf("fragments = %d, want %d", len(ws), writers*per*2)
	}
	seen := map[uint32]int{}
	lastMsg := uint32(0)
	started := false
	for _, w := range ws {
		if started && w.msg != lastMsg && seen[w.msg] > 0 {
			t.Fatalf("message %d interleaved with another", w.msg)
		}
		started, lastMsg = true, w.msg
		seen[w.msg]++
		if w.idx != uint32(seen[w.msg]-1) {
			t.Fatalf("msg %d fragment idx %d out of order", w.msg, w.idx)
		}
	}
	for msg, nfrags := range seen {
		if nfrags != 2 {
			t.Fatalf("msg %d has %d fragments, want 2", msg, nfrags)
		}
	}
}
