package codec

import (
	"bytes"
	"encoding/binary"
	"io"
	"sync"
)

// readChunk caps how many bytes ReadFrame pulls from the underlying
// reader in one Read call.
const readChunk = 64 << 10

// Session is a multi-frame conversation over a byte sink and a byte
// source. The write side buffers frames encoded with Encode and flushes
// them on demand; the read side incrementally parses frames with Decode
// from whatever byte chunks arrive.
//
// The wire stream is the plain concatenation of single-frame encodings,
// so it is byte-for-byte compatible with Encode/Decode.
//
// A Session is safe for concurrent use: concurrent WriteFrame calls are
// serialized in arrival order and one frame's bytes are never
// interleaved with another's; concurrent ReadFrame calls each receive
// whole, distinct frames. Writes and reads may also proceed concurrently.
type Session struct {
	w io.Writer
	r io.Reader

	// wmu serializes the write side; wbuf holds encoded frames not yet
	// flushed to w.
	wmu  sync.Mutex
	wbuf bytes.Buffer

	// rmu serializes the read side; rbuf holds bytes read from r but not
	// yet consumed by a decoded frame; rerr is the sticky read error.
	rmu  sync.Mutex
	rbuf []byte
	rerr error
}

// NewSession returns a Session that writes frames to w and reads frames
// from r. Either may be nil if only one direction is used.
func NewSession(w io.Writer, r io.Reader) *Session {
	return &Session{w: w, r: r}
}

// WriteFrame encodes f with Encode and queues the bytes for the next
// Flush. If f is invalid (e.g. its payload exceeds MaxPayload) the
// encoding error is returned and nothing is queued, so a failed call
// never emits a partial frame. Calls are queued in arrival order.
func (s *Session) WriteFrame(f Frame) error {
	s.wmu.Lock()
	defer s.wmu.Unlock()

	data, err := Encode(f)
	if err != nil {
		return err
	}
	s.wbuf.Write(data)
	return nil
}

// Flush writes every queued frame to the underlying writer, in the order
// they were written, and empties the queue. If the writer also
// implements Flush() error (e.g. bufio.Writer) it is flushed afterwards.
// On error the unwritten remainder stays queued so Flush can be retried.
func (s *Session) Flush() error {
	s.wmu.Lock()
	defer s.wmu.Unlock()

	if _, err := s.wbuf.WriteTo(s.w); err != nil {
		return err
	}
	if f, ok := s.w.(interface{ Flush() error }); ok {
		return f.Flush()
	}
	return nil
}

// ReadFrame returns the next complete frame from the read side,
// consuming exactly its bytes. Bytes of a frame that has only partially
// arrived stay buffered until the rest arrives; they are never reported
// as a frame and never discarded.
//
// The first corrupt frame ends the stream: ReadFrame returns the Decode
// error for that frame (ErrBadVersion, ErrChecksum or ErrTooLarge) and
// every later call returns the same error; frames decoded before it were
// already delivered to their callers. If the input ends with a partial
// frame in the buffer, ReadFrame returns ErrShortFrame. If the input
// ends on an exact frame boundary, ReadFrame returns io.EOF.
func (s *Session) ReadFrame() (Frame, error) {
	s.rmu.Lock()
	defer s.rmu.Unlock()

	if s.rerr != nil {
		return Frame{}, s.rerr
	}
	for {
		if f, n, err, ok := s.tryDecode(); ok {
			if err != nil {
				s.rerr = err
				return Frame{}, err
			}
			// Consume exactly this frame's bytes; Decode has already
			// copied the payload, so no residue leaks into the next frame.
			s.rbuf = s.rbuf[n:]
			return f, nil
		}

		if err := s.fill(); err != nil {
			s.rerr = err
			return Frame{}, err
		}
	}
}

// tryDecode attempts to decode one frame from the front of rbuf. ok is
// false when rbuf does not yet hold a complete frame. rmu must be held.
func (s *Session) tryDecode() (f Frame, n int, err error, ok bool) {
	if uint64(len(s.rbuf)) < HeaderSize {
		return Frame{}, 0, nil, false
	}
	payloadLen := binary.BigEndian.Uint32(s.rbuf[0:4])
	total := uint64(HeaderSize) + uint64(payloadLen)
	if uint64(len(s.rbuf)) < total {
		return Frame{}, 0, nil, false
	}
	f, err = Decode(s.rbuf[:total])
	if err != nil {
		return Frame{}, 0, err, true
	}
	return f, int(total), nil, true
}

// fill reads one more chunk from the underlying reader into rbuf. It
// maps a clean end of input to io.EOF when the buffer is empty and to
// ErrShortFrame when a partial frame remains. rmu must be held, and the
// buffer must not already hold a complete frame (tryDecode said so).
func (s *Session) fill() error {
	var need uint64 = 1
	if len(s.rbuf) < HeaderSize {
		need = uint64(HeaderSize - len(s.rbuf))
	} else {
		payloadLen := binary.BigEndian.Uint32(s.rbuf[0:4])
		need = uint64(HeaderSize) + uint64(payloadLen) - uint64(len(s.rbuf))
	}
	if need > readChunk {
		need = readChunk
	}

	tmp := make([]byte, need)
	n, err := s.r.Read(tmp)
	s.rbuf = append(s.rbuf, tmp[:n]...)
	if err == nil {
		return nil
	}
	if err == io.EOF {
		if len(s.rbuf) == 0 {
			return io.EOF
		}
		return ErrShortFrame
	}
	return err
}
