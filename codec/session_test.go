package codec

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sort"
	"sync"
	"testing"
)

// chunkReader returns its data in fixed-size chunks, simulating a byte
// stream whose block boundaries are unrelated to frame boundaries.
type chunkReader struct {
	data []byte
	n    int // bytes returned so far
	size int // chunk size
}

func (c *chunkReader) Read(p []byte) (int, error) {
	if c.n >= len(c.data) {
		return 0, io.EOF
	}
	end := c.n + c.size
	if end > len(c.data) {
		end = len(c.data)
	}
	n := copy(p, c.data[c.n:end])
	c.n += n
	return n, nil
}

func encodeFrames(t *testing.T, fs []Frame) []byte {
	t.Helper()
	var buf bytes.Buffer
	for _, f := range fs {
		b, err := Encode(f)
		if err != nil {
			t.Fatalf("Encode: %v", err)
		}
		buf.Write(b)
	}
	return buf.Bytes()
}

func framesEqual(a, b Frame) bool {
	// Encode always writes Version regardless of the input field, so a
	// decoded frame carries Version; compare b's other fields only.
	return a.Version == Version && a.Flags == b.Flags && a.Kind == b.Kind && bytes.Equal(a.Payload, b.Payload)
}

func drain(t *testing.T, s *Session) ([]Frame, error) {
	t.Helper()
	var got []Frame
	for {
		f, err := s.ReadFrame()
		if err != nil {
			return got, err
		}
		got = append(got, f)
	}
}

func TestSessionRoundTripChunks(t *testing.T) {
	in := []Frame{
		{Flags: 1, Kind: 2, Payload: nil},
		{Flags: 3, Kind: 4, Payload: []byte("hello, frame")},
		{Flags: 0xBEEF, Kind: 0x2A, Payload: bytes.Repeat([]byte{'x'}, 5000)},
		{Flags: 99, Kind: 7, Payload: []byte{}},
	}
	wire := encodeFrames(t, in)

	// Every chunk size from 1 byte up to the full stream length must
	// yield the same frame sequence: frame boundaries may land exactly
	// on read-block boundaries, a block may contain several full frames
	// plus a half frame, and the stream may start with empty input.
	for _, size := range []int{1, 2, 3, 7, 10, 11, 13, 64, 4096, len(wire)} {
		t.Run(fmt.Sprintf("chunk=%d", size), func(t *testing.T) {
			s := NewSession(io.Discard, &chunkReader{data: wire, size: size})
			got, err := drain(t, s)
			if !errors.Is(err, io.EOF) {
				t.Fatalf("terminal err = %v, want io.EOF", err)
			}
			if len(got) != len(in) {
				t.Fatalf("got %d frames, want %d", len(got), len(in))
			}
			for i := range in {
				if !framesEqual(got[i], in[i]) {
					t.Fatalf("frame %d mismatch: got %+v want %+v", i, got[i], in[i])
				}
			}
		})
	}
}

func TestSessionEmptyInput(t *testing.T) {
	s := NewSession(io.Discard, &chunkReader{data: nil, size: 4})
	if _, err := s.ReadFrame(); !errors.Is(err, io.EOF) {
		t.Fatalf("err = %v, want io.EOF on empty input", err)
	}
	// After a clean end, reads keep returning io.EOF without a new error.
	if _, err := s.ReadFrame(); !errors.Is(err, io.EOF) {
		t.Fatalf("second err = %v, want io.EOF", err)
	}
}

func TestSessionWriteOrderingAndFlush(t *testing.T) {
	var out bytes.Buffer
	s := NewSession(&out, nil)

	in := []Frame{
		{Flags: 1, Kind: 1, Payload: []byte("first")},
		{Flags: 2, Kind: 2, Payload: []byte("second")},
		{Flags: 3, Kind: 3, Payload: []byte("third")},
	}
	for _, f := range in {
		if err := s.WriteFrame(f); err != nil {
			t.Fatalf("WriteFrame: %v", err)
		}
	}
	// Nothing reaches the writer before flush.
	if out.Len() != 0 {
		t.Fatalf("%d bytes written before Flush", out.Len())
	}
	if err := s.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if !bytes.Equal(out.Bytes(), encodeFrames(t, in)) {
		t.Fatalf("flushed bytes differ from concatenated single frames")
	}

	// Buffer reuse: frames appended after a flush must not carry bytes
	// from the previous run.
	more := []Frame{{Flags: 4, Kind: 4, Payload: []byte("after")}}
	for _, f := range more {
		if err := s.WriteFrame(f); err != nil {
			t.Fatalf("WriteFrame: %v", err)
		}
	}
	if err := s.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	got, err := drain(t, NewSession(nil, bytes.NewReader(out.Bytes())))
	if err != io.EOF && !errors.Is(err, io.EOF) {
		t.Fatalf("drain: %v", err)
	}
	want := append(in, more...)
	if len(got) != len(want) {
		t.Fatalf("got %d frames, want %d", len(got), len(want))
	}
	for i := range want {
		if !framesEqual(got[i], want[i]) {
			t.Fatalf("frame %d = %+v, want %+v", i, got[i], want[i])
		}
	}
}

func TestSessionFlushPropagatesToBufferedWriter(t *testing.T) {
	var raw bytes.Buffer
	bw := bufio.NewWriter(&raw)
	s := NewSession(bw, nil)
	f := Frame{Payload: []byte("buffered")}
	if err := s.WriteFrame(f); err != nil {
		t.Fatalf("WriteFrame: %v", err)
	}
	if err := s.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if raw.Len() == 0 {
		t.Fatal("Flush did not flush the underlying *bufio.Writer")
	}
}

func TestSessionFlushRetriesAfterShortWrite(t *testing.T) {
	fw := &faultyWriter{limit: 3, fail: errFaultyWrite}
	s := NewSession(fw, nil)
	in := []Frame{
		{Kind: 1, Payload: []byte("first-frame")},
		{Kind: 2, Payload: []byte("second-frame")},
	}
	for _, f := range in {
		if err := s.WriteFrame(f); err != nil {
			t.Fatalf("WriteFrame: %v", err)
		}
	}
	if err := s.Flush(); !errors.Is(err, errFaultyWrite) {
		t.Fatalf("Flush err = %v, want errFaultyWrite", err)
	}

	// The unsent tail must survive; once the writer recovers the next
	// Flush completes the ordered stream with no gap or duplication.
	fw.limit = 1 << 20
	fw.fail = nil
	if err := s.Flush(); err != nil {
		t.Fatalf("retry Flush: %v", err)
	}
	got, err := drain(t, NewSession(nil, bytes.NewReader(fw.buf.Bytes())))
	if !errors.Is(err, io.EOF) {
		t.Fatalf("drain: %v", err)
	}
	if len(got) != len(in) {
		t.Fatalf("got %d frames, want %d", len(got), len(in))
	}
	for i := range in {
		if !framesEqual(got[i], in[i]) {
			t.Fatalf("frame %d = %+v, want %+v", i, got[i], in[i])
		}
	}
}

func TestSessionWriteTooLarge(t *testing.T) {
	var out bytes.Buffer
	s := NewSession(&out, nil)

	good := Frame{Kind: 1, Payload: []byte("ok")}
	if err := s.WriteFrame(good); err != nil {
		t.Fatalf("WriteFrame good: %v", err)
	}
	// Oversized payload is rejected before any of its bytes are buffered.
	if err := s.WriteFrame(Frame{Kind: 2, Payload: make([]byte, MaxPayload+1)}); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("err = %v, want ErrTooLarge", err)
	}
	if err := s.WriteFrame(good); err != nil {
		t.Fatalf("WriteFrame good after reject: %v", err)
	}
	if err := s.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	got, err := drain(t, NewSession(nil, bytes.NewReader(out.Bytes())))
	if !errors.Is(err, io.EOF) {
		t.Fatalf("drain: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("got %d frames, want 2 (rejected frame left no half frame)", len(got))
	}
	for _, g := range got {
		if !framesEqual(g, good) {
			t.Fatalf("got %+v, want the two accepted frames", g)
		}
	}
}

func TestSessionIncrementalArrival(t *testing.T) {
	// Feed a half frame, then the rest plus a full frame, then close:
	// the residual must wait without error and be completed by later
	// bytes.
	f1 := Frame{Kind: 1, Payload: []byte("abcdef")}
	f2 := Frame{Kind: 2, Payload: []byte("gh")}
	w1, err := Encode(f1)
	if err != nil {
		t.Fatal(err)
	}
	w2, err := Encode(f2)
	if err != nil {
		t.Fatal(err)
	}
	wire := append(append([]byte(nil), w1...), w2...)

	r := &blockingFeed{}
	s := NewSession(io.Discard, r)

	got := make(chan Frame, 2)
	errc := make(chan error, 2)
	go func() {
		for {
			f, err := s.ReadFrame()
			if err != nil {
				errc <- err
				return
			}
			got <- f
		}
	}()

	// Half of frame 1: no frame, no error yet.
	r.append(wire[:HeaderSize+2])
	r.append(wire[HeaderSize+2:]) // rest of frame 1 + all of frame 2
	r.closeFeed()

	first := <-got
	second := <-got
	if !framesEqual(first, f1) || !framesEqual(second, f2) {
		t.Fatalf("frames = %+v, %+v", first, second)
	}
	if err := <-errc; !errors.Is(err, io.EOF) {
		t.Fatalf("terminal err = %v, want io.EOF", err)
	}
}

func TestSessionTruncatedTailAtEOF(t *testing.T) {
	good := Frame{Kind: 1, Payload: []byte("complete")}
	w, err := Encode(good)
	if err != nil {
		t.Fatal(err)
	}
	// A complete frame followed by a half frame of the next one.
	half := w[:HeaderSize+1]
	stream := append(append([]byte(nil), w...), half...)

	s := NewSession(io.Discard, bytes.NewReader(stream))
	f, err := s.ReadFrame()
	if err != nil {
		t.Fatalf("first frame: %v", err)
	}
	if !framesEqual(f, good) {
		t.Fatalf("first frame = %+v", f)
	}
	_, err = s.ReadFrame()
	if !errors.Is(err, ErrShortFrame) {
		t.Fatalf("truncated tail err = %v, want ErrShortFrame", err)
	}
	// The half frame is not delivered as a frame; the short-frame error
	// is sticky on subsequent reads.
	_, err = s.ReadFrame()
	if !errors.Is(err, ErrShortFrame) {
		t.Fatalf("second err = %v, want sticky ErrShortFrame", err)
	}
}

func TestSessionBadFrameStopsWithPriorFrames(t *testing.T) {
	good1 := Frame{Kind: 1, Payload: []byte("one")}
	good2 := Frame{Kind: 2, Payload: []byte("two")}
	good3 := Frame{Kind: 3, Payload: []byte("three")}
	w1, _ := Encode(good1)
	w2, _ := Encode(good2)
	bad, _ := Encode(Frame{Kind: 9, Payload: []byte("bad")})
	bad[HeaderSize] ^= 0xFF // payload corruption -> checksum mismatch
	w3, _ := Encode(good3)
	stream := append(append(append(append([]byte(nil), w1...), w2...), bad...), w3...)

	s := NewSession(io.Discard, bytes.NewReader(stream))
	f1, err := s.ReadFrame()
	if err != nil || !framesEqual(f1, good1) {
		t.Fatalf("frame 1 = %+v, %v", f1, err)
	}
	f2, err := s.ReadFrame()
	if err != nil || !framesEqual(f2, good2) {
		t.Fatalf("frame 2 = %+v, %v", f2, err)
	}
	_, err = s.ReadFrame()
	if !errors.Is(err, ErrChecksum) {
		t.Fatalf("bad frame err = %v, want ErrChecksum", err)
	}
	// Frames after the corrupt one are not delivered; the error sticks.
	_, err = s.ReadFrame()
	if !errors.Is(err, ErrChecksum) {
		t.Fatalf("follow-up err = %v, want sticky ErrChecksum", err)
	}
}

func TestSessionBadVersionAndTooLarge(t *testing.T) {
	// Bad version mid-stream.
	bad, _ := Encode(Frame{Payload: []byte("x")})
	bad[4] = 2
	s := NewSession(io.Discard, bytes.NewReader(bad))
	if _, err := s.ReadFrame(); !errors.Is(err, ErrBadVersion) {
		t.Fatalf("bad version err = %v, want ErrBadVersion", err)
	}

	// Declared length over the limit, full frame present: the streaming
	// side returns the same ErrTooLarge Decode returns.
	payload := make([]byte, MaxPayload+1)
	big := make([]byte, HeaderSize+len(payload))
	binary.BigEndian.PutUint32(big[0:4], uint32(len(payload)))
	big[4] = Version
	sum := checksum(big[0:8]) + checksum(big[HeaderSize:])
	binary.BigEndian.PutUint16(big[8:10], sum)

	// Deliver it in small chunks so the declared length is known long
	// before the bytes arrive.
	s = NewSession(io.Discard, &chunkReader{data: big, size: 4096})
	if _, err := s.ReadFrame(); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("oversize err = %v, want ErrTooLarge", err)
	}
}

func TestSessionChecksumPrecedenceOverTooLarge(t *testing.T) {
	// Mirrors TestDecodeErrorPrecedence on the streaming side: an
	// oversize declared length with a broken checksum must surface
	// ErrChecksum, even when delivered one byte at a time.
	big := make([]byte, HeaderSize+MaxPayload+1)
	binary.BigEndian.PutUint32(big[0:4], uint32(MaxPayload+1))
	big[4] = Version
	big[HeaderSize] = 1 // breaks the (zero) checksum
	s := NewSession(io.Discard, &chunkReader{data: big, size: 1})
	if _, err := s.ReadFrame(); !errors.Is(err, ErrChecksum) {
		t.Fatalf("err = %v, want ErrChecksum", err)
	}
}

func TestSessionManyFramesUnbounded(t *testing.T) {
	// Well past any fixed in-memory frame table: frame count is unbounded.
	const n = 5000
	var buf bytes.Buffer
	s := NewSession(&buf, nil)
	for i := 0; i < n; i++ {
		if err := s.WriteFrame(Frame{Kind: uint8(i), Payload: []byte{byte(i), byte(i >> 8)}}); err != nil {
			t.Fatalf("WriteFrame %d: %v", i, err)
		}
	}
	if err := s.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	r := NewSession(nil, &chunkReader{data: buf.Bytes(), size: 3})
	for i := 0; i < n; i++ {
		f, err := r.ReadFrame()
		if err != nil {
			t.Fatalf("frame %d: %v", i, err)
		}
		if f.Kind != uint8(i) || len(f.Payload) != 2 ||
			f.Payload[0] != byte(i) || f.Payload[1] != byte(i>>8) {
			t.Fatalf("frame %d = %+v", i, f)
		}
	}
	if _, err := r.ReadFrame(); !errors.Is(err, io.EOF) {
		t.Fatalf("terminal err = %v, want io.EOF", err)
	}
}

func TestSessionMaxPayloadAccepted(t *testing.T) {
	var buf bytes.Buffer
	s := NewSession(&buf, nil)
	if err := s.WriteFrame(Frame{Payload: make([]byte, MaxPayload)}); err != nil {
		t.Fatalf("MaxPayload frame rejected: %v", err)
	}
	if err := s.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	f, err := NewSession(nil, bytes.NewReader(buf.Bytes())).ReadFrame()
	if err != nil {
		t.Fatalf("ReadFrame: %v", err)
	}
	if len(f.Payload) != MaxPayload {
		t.Fatalf("payload len = %d, want %d", len(f.Payload), MaxPayload)
	}
}

func TestSessionConcurrentWritesNoInterleave(t *testing.T) {
	var out lockedBuffer
	s := NewSession(&out, nil)

	const writers = 16
	const per = 200
	var wg sync.WaitGroup
	for g := 0; g < writers; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < per; i++ {
				payload := []byte(fmt.Sprintf("g%di%d", g, i))
				if err := s.WriteFrame(Frame{Kind: uint8(g), Payload: payload}); err != nil {
					t.Errorf("WriteFrame: %v", err)
					return
				}
			}
		}(g)
	}
	wg.Wait()
	if err := s.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	// Every frame must decode cleanly whole: interleaved bytes would
	// corrupt a header or checksum.
	r := NewSession(nil, bytes.NewReader(out.bytes()))
	counts := map[int]int{}
	total := 0
	for {
		f, err := r.ReadFrame()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("frame %d corrupted by interleaving: %v", total, err)
		}
		counts[int(f.Kind)]++
		var gg, ii int
		if _, err := fmt.Sscanf(string(f.Payload), "g%di%d", &gg, &ii); err != nil {
			t.Fatalf("bad payload %q", f.Payload)
		}
		if gg != int(f.Kind) || ii < 0 || ii >= per {
			t.Fatalf("payload/header mismatch: kind %d payload %q", f.Kind, f.Payload)
		}
		total++
	}
	if total != writers*per {
		t.Fatalf("decoded %d frames, want %d", total, writers*per)
	}
	for g := 0; g < writers; g++ {
		if counts[g] != per {
			t.Fatalf("group %d produced %d frames, want %d", g, counts[g], per)
		}
	}
}

func TestSessionConcurrentReadersEachFrameOnce(t *testing.T) {
	const n = 2000
	frames := make([]Frame, n)
	for i := range frames {
		frames[i] = Frame{Kind: uint8(i), Payload: []byte(fmt.Sprintf("%08d", i))}
	}
	wire := encodeFrames(t, frames)

	s := NewSession(io.Discard, bytes.NewReader(wire))
	const readers = 8
	var mu sync.Mutex
	seen := make(map[string]int)
	var wg sync.WaitGroup
	for r := 0; r < readers; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				f, err := s.ReadFrame()
				if errors.Is(err, io.EOF) {
					return
				}
				if err != nil {
					t.Errorf("ReadFrame: %v", err)
					return
				}
				key := string(f.Payload)
				mu.Lock()
				seen[key]++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	if len(seen) != n {
		t.Fatalf("delivered %d distinct frames, want %d", len(seen), n)
	}
	ids := make([]int, 0, n)
	for k, c := range seen {
		if c != 1 {
			t.Fatalf("frame %q delivered %d times", k, c)
		}
		var id int
		fmt.Sscanf(k, "%08d", &id)
		ids = append(ids, id)
	}
	sort.Ints(ids)
	for i, id := range ids {
		if id != i {
			t.Fatalf("missing/duplicated frame at index %d (got %d)", i, id)
		}
	}
}

func TestSessionConcurrentReadersAndWriters(t *testing.T) {
	pr, pw := io.Pipe()
	s := NewSession(pw, pr)

	const n = 500
	var wg sync.WaitGroup
	wg.Add(2)

	// Writer side: multiple goroutines append frames, one flusher pushes.
	go func() {
		defer wg.Done()
		var fw sync.WaitGroup
		for g := 0; g < 4; g++ {
			fw.Add(1)
			go func(g int) {
				defer fw.Done()
				for i := 0; i < n/4; i++ {
					if err := s.WriteFrame(Frame{Kind: uint8(g), Payload: []byte(fmt.Sprintf("g%d-%d", g, i))}); err != nil {
						t.Errorf("WriteFrame: %v", err)
					}
				}
			}(g)
		}
		fw.Wait()
		if err := s.Flush(); err != nil {
			t.Errorf("Flush: %v", err)
		}
		pw.Close()
	}()

	// Reader side: multiple goroutines share the read end.
	got := make(chan Frame, n)
	go func() {
		defer wg.Done()
		var fr sync.WaitGroup
		for r := 0; r < 4; r++ {
			fr.Add(1)
			go func() {
				defer fr.Done()
				for {
					f, err := s.ReadFrame()
					if errors.Is(err, io.EOF) {
						return
					}
					if err != nil {
						t.Errorf("ReadFrame: %v", err)
						return
					}
					got <- f
				}
			}()
		}
		fr.Wait()
	}()
	wg.Wait()
	close(got)

	count := 0
	for range got {
		count++
	}
	if count != n {
		t.Fatalf("got %d frames through concurrent session, want %d", count, n)
	}
}

func TestSessionReaderErrorSurfaces(t *testing.T) {
	sentinel := errors.New("boom")
	s := NewSession(io.Discard, &errReader{err: sentinel})
	if _, err := s.ReadFrame(); !errors.Is(err, sentinel) {
		t.Fatalf("err = %v, want underlying reader error", err)
	}
}

// --- helpers ---

type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) bytes() []byte {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Bytes()
}

type errReader struct{ err error }

func (e *errReader) Read([]byte) (int, error) { return 0, e.err }

var errFaultyWrite = errors.New("faulty write")

// faultyWriter accepts at most limit bytes per Write and reports fail
// alongside them, simulating a short write at the transport.
type faultyWriter struct {
	buf   bytes.Buffer
	limit int
	fail  error
}

func (w *faultyWriter) Write(p []byte) (int, error) {
	n := len(p)
	if w.limit < n {
		n = w.limit
	}
	w.buf.Write(p[:n])
	if w.fail != nil {
		return n, w.fail
	}
	return n, nil
}

// blockingFeed is a reader whose data arrives incrementally; Read blocks
// while no chunk is available and returns EOF only after closeFeed.
type blockingFeed struct {
	mu     sync.Mutex
	cond   *sync.Cond
	chunks [][]byte
	closed bool
}

func (b *blockingFeed) append(p []byte) {
	b.mu.Lock()
	b.chunks = append(b.chunks, append([]byte(nil), p...))
	if b.cond != nil {
		b.cond.Broadcast()
	}
	b.mu.Unlock()
}

func (b *blockingFeed) closeFeed() {
	b.mu.Lock()
	b.closed = true
	if b.cond != nil {
		b.cond.Broadcast()
	}
	b.mu.Unlock()
}

func (b *blockingFeed) Read(p []byte) (int, error) {
	b.mu.Lock()
	if b.cond == nil {
		b.cond = sync.NewCond(&b.mu)
	}
	for len(b.chunks) == 0 && !b.closed {
		b.cond.Wait()
	}
	if len(b.chunks) > 0 {
		n := copy(p, b.chunks[0])
		b.chunks = b.chunks[1:]
		b.mu.Unlock()
		return n, nil
	}
	b.mu.Unlock()
	return 0, io.EOF
}
