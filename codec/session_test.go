package codec

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"sync"
	"testing"
)

// chunkReader yields data in fixed-size chunks to simulate arbitrary
// network fragmentation. A chunk size of 0 behaves like io.Reader
// returning everything at once.
type chunkReader struct {
	data []byte
	n    int
}

func (c *chunkReader) Read(p []byte) (int, error) {
	if len(c.data) == 0 {
		return 0, io.EOF
	}
	n := c.n
	if n <= 0 || n > len(c.data) {
		n = len(c.data)
	}
	if n > len(p) {
		n = len(p)
	}
	copy(p, c.data[:n])
	c.data = c.data[n:]
	return n, nil
}

func sampleFrames() []Frame {
	return []Frame{
		{Flags: 0, Kind: 1, Payload: []byte("first")},
		{Flags: 0xBEEF, Kind: 0x2A, Payload: []byte{}},
		{Flags: 7, Kind: 3, Payload: bytes.Repeat([]byte("ab"), 500)},
		{Flags: 1, Kind: 9, Payload: []byte("last")},
	}
}

func encodeAll(t *testing.T, frames []Frame) []byte {
	t.Helper()
	var want []byte
	for _, f := range frames {
		data, err := Encode(f)
		if err != nil {
			t.Fatalf("Encode: %v", err)
		}
		want = append(want, data...)
	}
	return want
}

func readAll(t *testing.T, s *Session) []Frame {
	t.Helper()
	var got []Frame
	for {
		f, err := s.ReadFrame()
		if err == io.EOF {
			return got
		}
		if err != nil {
			t.Fatalf("ReadFrame: %v", err)
		}
		got = append(got, f)
	}
}

func assertFramesEqual(t *testing.T, got, want []Frame) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("got %d frames, want %d", len(got), len(want))
	}
	for i := range want {
		g, w := got[i], want[i]
		if g.Version != Version || g.Flags != w.Flags || g.Kind != w.Kind ||
			!bytes.Equal(g.Payload, w.Payload) {
			t.Fatalf("frame %d: got %+v, want flags=%#x kind=%#x payload=%q",
				i, g, w.Flags, w.Kind, w.Payload)
		}
	}
}

func TestSessionRoundTrip(t *testing.T) {
	frames := sampleFrames()
	var buf bytes.Buffer
	s := NewSession(&buf, &buf)
	for _, f := range frames {
		if err := s.WriteFrame(f); err != nil {
			t.Fatalf("WriteFrame: %v", err)
		}
	}
	if err := s.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	assertFramesEqual(t, readAll(t, s), frames)
}

func TestSessionWireCompatibleWithEncode(t *testing.T) {
	frames := sampleFrames()
	var buf bytes.Buffer
	s := NewSession(&buf, nil)
	for _, f := range frames {
		if err := s.WriteFrame(f); err != nil {
			t.Fatalf("WriteFrame: %v", err)
		}
	}
	if err := s.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if want := encodeAll(t, frames); !bytes.Equal(buf.Bytes(), want) {
		t.Fatalf("stream mismatch:\n got %x\nwant %x", buf.Bytes(), want)
	}
}

func TestSessionArbitraryChunking(t *testing.T) {
	frames := sampleFrames()
	stream := encodeAll(t, frames)
	// Chunk sizes: 1-byte drip, odd sizes, exactly the stream, and a
	// size that splits a header and lands mid-payload.
	for _, n := range []int{0, 1, 2, 3, 7, HeaderSize - 1, HeaderSize, 64, len(stream)} {
		t.Run(fmt.Sprintf("chunk=%d", n), func(t *testing.T) {
			s := NewSession(nil, &chunkReader{data: stream, n: n})
			assertFramesEqual(t, readAll(t, s), frames)
		})
	}
}

func TestSessionEmptyInput(t *testing.T) {
	s := NewSession(nil, &chunkReader{})
	if _, err := s.ReadFrame(); err != io.EOF {
		t.Fatalf("err = %v, want io.EOF", err)
	}
	// Still io.EOF on repeat, never a spurious error.
	if _, err := s.ReadFrame(); err != io.EOF {
		t.Fatalf("second read err = %v, want io.EOF", err)
	}
}

func TestSessionCleanBoundaryEOF(t *testing.T) {
	frames := sampleFrames()
	s := NewSession(nil, bytes.NewReader(encodeAll(t, frames)))
	got := readAll(t, s)
	assertFramesEqual(t, got, frames)
	// Buffer drained exactly: further reads keep returning io.EOF.
	if _, err := s.ReadFrame(); err != io.EOF {
		t.Fatalf("err = %v, want io.EOF", err)
	}
}

func TestSessionTruncatedTail(t *testing.T) {
	frames := sampleFrames()
	stream := encodeAll(t, frames)
	// Keep the first frames whole, cut the last one in half.
	cut := len(stream) - (HeaderSize+len(frames[len(frames)-1].Payload))/2 - 1
	s := NewSession(nil, bytes.NewReader(stream[:cut]))

	for i := 0; i < len(frames)-1; i++ {
		if _, err := s.ReadFrame(); err != nil {
			t.Fatalf("frame %d: %v", i, err)
		}
	}
	if _, err := s.ReadFrame(); !errors.Is(err, ErrShortFrame) {
		t.Fatalf("err = %v, want ErrShortFrame", err)
	}
	// Sticky: the truncated tail is not retried as a new frame.
	if _, err := s.ReadFrame(); !errors.Is(err, ErrShortFrame) {
		t.Fatalf("second read err = %v, want ErrShortFrame", err)
	}
}

func TestSessionBadFrameStopsStream(t *testing.T) {
	good, err := Encode(Frame{Flags: 1, Kind: 2, Payload: []byte("ok")})
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	bad, err := Encode(Frame{Payload: []byte("corrupt")})
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	bad[4] = 99 // bad version
	more, err := Encode(Frame{Payload: []byte("never seen")})
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}

	stream := append(append(append([]byte{}, good...), bad...), more...)
	s := NewSession(nil, bytes.NewReader(stream))

	f, err := s.ReadFrame()
	if err != nil {
		t.Fatalf("first frame: %v", err)
	}
	if string(f.Payload) != "ok" {
		t.Fatalf("first payload = %q", f.Payload)
	}
	if _, err := s.ReadFrame(); !errors.Is(err, ErrBadVersion) {
		t.Fatalf("err = %v, want ErrBadVersion", err)
	}
	// Sticky: frames after the bad one are never emitted.
	if _, err := s.ReadFrame(); !errors.Is(err, ErrBadVersion) {
		t.Fatalf("after bad frame err = %v, want ErrBadVersion", err)
	}
}

func TestSessionBadChecksumStopsStream(t *testing.T) {
	bad, err := Encode(Frame{Payload: []byte("corrupt")})
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	bad[HeaderSize] ^= 0xFF
	s := NewSession(nil, bytes.NewReader(bad))
	if _, err := s.ReadFrame(); !errors.Is(err, ErrChecksum) {
		t.Fatalf("err = %v, want ErrChecksum", err)
	}
}

func TestSessionWriteTooLarge(t *testing.T) {
	var buf bytes.Buffer
	s := NewSession(&buf, nil)
	err := s.WriteFrame(Frame{Payload: make([]byte, MaxPayload+1)})
	if !errors.Is(err, ErrTooLarge) {
		t.Fatalf("err = %v, want ErrTooLarge", err)
	}
	if err := s.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if buf.Len() != 0 {
		t.Fatalf("rejected frame leaked %d bytes into the stream", buf.Len())
	}
	// The session is still usable for valid frames.
	if err := s.WriteFrame(Frame{Payload: []byte("fine")}); err != nil {
		t.Fatalf("WriteFrame after error: %v", err)
	}
	if err := s.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if _, err := Decode(buf.Bytes()); err != nil {
		t.Fatalf("stream after recovery does not decode: %v", err)
	}
}

func TestSessionFlushReuse(t *testing.T) {
	frames := sampleFrames()
	var buf bytes.Buffer
	s := NewSession(&buf, nil)
	// Interleave writes and flushes; no residue may leak between flushes.
	for _, f := range frames {
		if err := s.WriteFrame(f); err != nil {
			t.Fatalf("WriteFrame: %v", err)
		}
		if err := s.Flush(); err != nil {
			t.Fatalf("Flush: %v", err)
		}
	}
	if want := encodeAll(t, frames); !bytes.Equal(buf.Bytes(), want) {
		t.Fatalf("stream mismatch after repeated flushes")
	}
	// Flushing with an empty queue is a no-op.
	if err := s.Flush(); err != nil {
		t.Fatalf("empty Flush: %v", err)
	}
}

func TestSessionConcurrent(t *testing.T) {
	const writers = 8
	const framesPerWriter = 50

	pr, pw := io.Pipe()
	s := NewSession(pw, pr)

	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < framesPerWriter; i++ {
				payload := []byte(fmt.Sprintf("w%02d-frame%03d", w, i))
				if err := s.WriteFrame(Frame{Flags: uint16(w), Kind: uint8(i), Payload: payload}); err != nil {
					t.Errorf("WriteFrame: %v", err)
					return
				}
				if i%7 == 0 {
					if err := s.Flush(); err != nil {
						t.Errorf("Flush: %v", err)
						return
					}
				}
			}
		}(w)
	}
	go func() {
		wg.Wait()
		if err := s.Flush(); err != nil {
			t.Errorf("final Flush: %v", err)
		}
		pw.Close()
	}()

	// Concurrent readers share the stream; every frame must arrive whole
	// and exactly once.
	want := make(map[string]bool, writers*framesPerWriter)
	for w := 0; w < writers; w++ {
		for i := 0; i < framesPerWriter; i++ {
			want[fmt.Sprintf("w%02d-frame%03d", w, i)] = true
		}
	}
	var mu sync.Mutex
	var rwg sync.WaitGroup
	for r := 0; r < 3; r++ {
		rwg.Add(1)
		go func() {
			defer rwg.Done()
			for {
				f, err := s.ReadFrame()
				if err == io.EOF {
					return
				}
				if err != nil {
					t.Errorf("ReadFrame: %v", err)
					return
				}
				mu.Lock()
				key := string(f.Payload)
				if !want[key] {
					t.Errorf("unexpected or duplicate frame %q", key)
				}
				delete(want, key)
				mu.Unlock()
			}
		}()
	}
	rwg.Wait()
	if len(want) != 0 {
		t.Fatalf("%d frames never arrived", len(want))
	}
}
