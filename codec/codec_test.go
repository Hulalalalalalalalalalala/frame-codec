package codec

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"
)

func roundTrip(t *testing.T, f Frame) Frame {
	t.Helper()
	data, err := Encode(f)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	got, err := Decode(data)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	return got
}

func TestRoundTrip(t *testing.T) {
	in := Frame{Version: Version, Flags: 0xBEEF, Kind: 0x2A, Payload: []byte("hello, frame")}
	got := roundTrip(t, in)
	if got.Version != in.Version || got.Flags != in.Flags || got.Kind != in.Kind {
		t.Fatalf("header mismatch: got %+v want %+v", got, in)
	}
	if !bytes.Equal(got.Payload, in.Payload) {
		t.Fatalf("payload mismatch: got %q want %q", got.Payload, in.Payload)
	}
}

func TestRoundTripEmptyPayload(t *testing.T) {
	got := roundTrip(t, Frame{Version: Version, Flags: 0, Kind: 0, Payload: []byte{}})
	if got.Version != Version || got.Flags != 0 || got.Kind != 0 || len(got.Payload) != 0 {
		t.Fatalf("bad empty round trip: %+v", got)
	}
}

func TestEncodeWritesVersionOne(t *testing.T) {
	data, err := Encode(Frame{Version: 99, Payload: []byte("x")})
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if data[4] != Version {
		t.Fatalf("version byte = %d, want %d", data[4], Version)
	}
}

func TestEncodeTooLarge(t *testing.T) {
	_, err := Encode(Frame{Payload: make([]byte, MaxPayload+1)})
	if !errors.Is(err, ErrTooLarge) {
		t.Fatalf("err = %v, want ErrTooLarge", err)
	}
}

func TestDecodeShortFrame(t *testing.T) {
	cases := [][]byte{
		nil,
		{1, 2, 3},
		make([]byte, HeaderSize-1),
	}
	for _, c := range cases {
		if _, err := Decode(c); !errors.Is(err, ErrShortFrame) {
			t.Fatalf("Decode(%d bytes) err = %v, want ErrShortFrame", len(c), err)
		}
	}

	// Header complete, declared payload missing.
	data, err := Encode(Frame{Payload: []byte("hello")})
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if _, err := Decode(data[:len(data)-2]); !errors.Is(err, ErrShortFrame) {
		t.Fatalf("truncated payload err = %v, want ErrShortFrame", err)
	}
}

func TestDecodeBadVersion(t *testing.T) {
	data, err := Encode(Frame{Payload: []byte("hello")})
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	data[4] = 2
	if _, err := Decode(data); !errors.Is(err, ErrBadVersion) {
		t.Fatalf("err = %v, want ErrBadVersion", err)
	}
}

func TestDecodeChecksum(t *testing.T) {
	data, err := Encode(Frame{Flags: 7, Kind: 3, Payload: []byte("hello")})
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	// Flip one payload byte; every single-byte corruption must be caught.
	for i := HeaderSize; i < len(data); i++ {
		bad := append([]byte(nil), data...)
		bad[i] ^= 0xFF
		if _, err := Decode(bad); !errors.Is(err, ErrChecksum) {
			t.Fatalf("corrupt byte %d: err = %v, want ErrChecksum", i, err)
		}
	}
	// Corrupt the stored checksum itself.
	bad := append([]byte(nil), data...)
	bad[8] ^= 0x01
	if _, err := Decode(bad); !errors.Is(err, ErrChecksum) {
		t.Fatalf("corrupt checksum field: err = %v, want ErrChecksum", err)
	}
}

func TestDecodeTooLarge(t *testing.T) {
	// Declared length over the limit, full frame bytes present, valid
	// version and checksum -> ErrTooLarge (size checked last).
	payload := make([]byte, MaxPayload+1)
	data := make([]byte, HeaderSize+len(payload))
	binary.BigEndian.PutUint32(data[0:4], uint32(len(payload)))
	data[4] = Version
	sum := checksum(data[0:8]) + checksum(data[HeaderSize:])
	binary.BigEndian.PutUint16(data[8:10], sum)

	if _, err := Decode(data); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("err = %v, want ErrTooLarge", err)
	}
}

func TestDecodeErrorPrecedence(t *testing.T) {
	// Short input wins over everything.
	if _, err := Decode([]byte{0}); !errors.Is(err, ErrShortFrame) {
		t.Fatalf("err = %v, want ErrShortFrame", err)
	}
	// Bad version wins over bad checksum.
	data, err := Encode(Frame{Payload: []byte("abc")})
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	data[4] = 9
	data[HeaderSize] ^= 0xFF
	if _, err := Decode(data); !errors.Is(err, ErrBadVersion) {
		t.Fatalf("err = %v, want ErrBadVersion", err)
	}
	// Bad checksum wins over oversize declared length.
	big := make([]byte, HeaderSize+MaxPayload+1)
	binary.BigEndian.PutUint32(big[0:4], uint32(MaxPayload+1))
	big[4] = Version
	big[HeaderSize] = 1 // breaks the (zero) checksum
	if _, err := Decode(big); !errors.Is(err, ErrChecksum) {
		t.Fatalf("err = %v, want ErrChecksum", err)
	}
}

func TestDecodeNoPartialFrame(t *testing.T) {
	data, err := Encode(Frame{Flags: 5, Kind: 6, Payload: []byte("abc")})
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	data[4] = 2
	f, err := Decode(data)
	if !errors.Is(err, ErrBadVersion) {
		t.Fatalf("err = %v, want ErrBadVersion", err)
	}
	if f.Version != 0 || f.Flags != 0 || f.Kind != 0 || f.Payload != nil {
		t.Fatalf("got partial frame %+v on error", f)
	}
}
