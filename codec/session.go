package codec

import (
	"encoding/binary"
	"io"
	"sync"
)

// flusher is implemented by buffered writers such as *bufio.Writer.
type flusher interface {
	Flush() error
}

// Session is a concurrency-safe multi-frame layer sitting on top of one
// byte writer and one byte reader. It does not change the wire format:
// every written frame is serialized with Encode and frames are simply
// concatenated, so the byte stream stays byte-for-byte compatible with
// the single-frame Encode/Decode entries.
//
// The two directions are independent and may be used concurrently:
//   - WriteFrame appends one whole encoded frame to an internal write
//     buffer; Flush writes everything buffered so far, in write order,
//     to the underlying writer.
//   - ReadFrame pulls bytes from the underlying reader in whatever
//     chunk sizes they arrive, keeps an incomplete trailing frame in an
//     internal buffer and returns one complete Frame per call.
//
// The total number of frames in a session is unbounded; the per-frame
// payload limit is MaxPayload, enforced at write time.
type Session struct {
	w io.Writer
	r io.Reader

	wmu  sync.Mutex
	wbuf []byte

	rmu  sync.Mutex
	rbuf []byte
	// terminal error of the read side: a bad frame's error, io.EOF or
	// an error reported by the underlying reader.
	rerr error
}

// NewSession returns a session writing to w and reading from r.
func NewSession(w io.Writer, r io.Reader) *Session {
	return &Session{w: w, r: r}
}

// WriteFrame encodes f with the single-frame encoder and appends the
// whole frame to the session's write buffer. Because Encode rejects an
// oversized payload before any byte is buffered, an ErrTooLarge result
// never leaves a partial frame behind, and concurrent writes never
// interleave the bytes of one frame with another. Writes become visible
// to the reader in call-arrival order once Flush runs.
func (s *Session) WriteFrame(f Frame) error {
	b, err := Encode(f)
	if err != nil {
		return err
	}
	s.wmu.Lock()
	s.wbuf = append(s.wbuf, b...)
	s.wmu.Unlock()
	return nil
}

// Flush writes every buffered frame to the underlying writer as one
// ordered run and then, if the writer implements Flush itself (for
// example *bufio.Writer), flushes it. Bytes already appended by
// WriteFrame are never reordered or split across frames. If the
// underlying writer errors after a short write, the unsent tail is
// retained and a later Flush retries it.
func (s *Session) Flush() error {
	s.wmu.Lock()
	defer s.wmu.Unlock()

	pending := s.wbuf
	for len(pending) > 0 {
		n, err := s.w.Write(pending)
		pending = pending[n:]
		if err != nil {
			// Keep the unsent tail at the front of the buffer so a
			// later WriteFrame/Flush continues from exactly this point.
			copy(s.wbuf, pending)
			s.wbuf = s.wbuf[:len(pending)]
			return err
		}
	}
	s.wbuf = s.wbuf[:0]

	if f, ok := s.w.(flusher); ok {
		return f.Flush()
	}
	return nil
}

// ReadFrame returns the next complete frame, reading more bytes from the
// underlying reader only when the buffer cannot yet supply one. A
// truncated frame at the end of the input is buffered without error
// until more bytes arrive; it is reported as ErrShortFrame only when the
// reader signals end of input with bytes still outstanding. A clean end
// of input with no residual bytes returns io.EOF, i.e. reading simply
// stops.
//
// When a complete frame is corrupt, the error is the same value Decode
// would return for that frame (ErrBadVersion, ErrChecksum or
// ErrTooLarge); frame delivery stops there and the error becomes
// sticky, while every frame returned earlier already belongs to the
// caller. Concurrent ReadFrame calls are serialized, so each frame is
// delivered exactly once.
func (s *Session) ReadFrame() (Frame, error) {
	s.rmu.Lock()
	defer s.rmu.Unlock()

	if s.rerr != nil {
		return Frame{}, s.rerr
	}

	var scratch [4096]byte

	for {
		// A frame can be handed to Decode only once its declared length
		// is fully buffered; Decode then decides every error with exactly
		// the same precedence the single-frame entry uses.
		if len(s.rbuf) >= HeaderSize {
			frameLen := uint64(HeaderSize) + uint64(binary.BigEndian.Uint32(s.rbuf[0:4]))
			if uint64(len(s.rbuf)) >= frameLen {
				f, err := Decode(s.rbuf[:int(frameLen)])
				if err != nil {
					return s.failRead(err)
				}
				// Drop the consumed frame and compact the residual bytes
				// of later frames to the front; Decode copied the payload
				// out, so overlapping storage is safe.
				rest := len(s.rbuf) - int(frameLen)
				copy(s.rbuf, s.rbuf[int(frameLen):])
				s.rbuf = s.rbuf[:rest]
				return f, nil
			}
		}

		n, err := s.r.Read(scratch[:])
		if n > 0 {
			s.rbuf = append(s.rbuf, scratch[:n]...)
		}
		if err == nil {
			// A (0, nil) read delivers no information; retry instead of
			// mistaking it for end of input.
			continue
		}
		if err == io.EOF {
			if len(s.rbuf) > 0 {
				return s.failRead(ErrShortFrame)
			}
			return s.failRead(io.EOF)
		}
		return s.failRead(err)
	}
}

// failRead records the terminal read-side error and returns it with the
// zero frame.
func (s *Session) failRead(err error) (Frame, error) {
	s.rerr = err
	return Frame{}, err
}
