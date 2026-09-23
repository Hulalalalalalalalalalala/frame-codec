package codec

import (
	"encoding/binary"
	"errors"
	"testing"
)

func refChecksum(t *testing.T, data []byte, payloadLen int) uint16 {
	t.Helper()
	var sum uint32
	for _, b := range data[:8] {
		sum += uint32(b)
	}
	for _, b := range data[HeaderSize : HeaderSize+payloadLen] {
		sum += uint32(b)
	}
	return uint16(sum)
}

func TestEncodeKnownVector(t *testing.T) {
	frame := Frame{Version: 1, Flags: 0x1234, Kind: 0x78, Payload: []byte("OK")}
	got, err := Encode(frame)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	want := []byte{
		0x00, 0x00, 0x00, 0x02, // length
		0x01,       // version
		0x12, 0x34, // flags
		0x78,       // kind
		0x01, 0x5B, // checksum
		0x4F, 0x4B, // payload "OK"
	}
	if len(got) != len(want) {
		t.Fatalf("len = %d, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("byte %d = %#02x, want %#02x\nfull: % x", i, got[i], want[i], got)
		}
	}
}

func TestRoundTrip(t *testing.T) {
	cases := []Frame{
		{Version: 1, Flags: 0x0000, Kind: 0x00, Payload: nil},
		{Version: 1, Flags: 0x0000, Kind: 0x00, Payload: []byte{}},
		{Version: 1, Flags: 0xFFFF, Kind: 0xFF, Payload: []byte{0x00}},
		{Version: 1, Flags: 0xABCD, Kind: 0x42, Payload: []byte("the quick brown fox")},
	}
	for i, in := range cases {
		raw, err := Encode(in)
		if err != nil {
			t.Fatalf("case %d: Encode: %v", i, err)
		}
		if len(raw) != HeaderSize+len(in.Payload) {
			t.Fatalf("case %d: encoded len = %d", i, len(raw))
		}
		out, err := Decode(raw)
		if err != nil {
			t.Fatalf("case %d: Decode: %v", i, err)
		}
		if out.Version != 1 || out.Flags != in.Flags || out.Kind != in.Kind {
			t.Fatalf("case %d: header mismatch: %+v vs %+v", i, out, in)
		}
		if len(out.Payload) != len(in.Payload) {
			t.Fatalf("case %d: payload len = %d, want %d", i, len(out.Payload), len(in.Payload))
		}
		for j := range in.Payload {
			if out.Payload[j] != in.Payload[j] {
				t.Fatalf("case %d: payload byte %d mismatch", i, j)
			}
		}
		// Mutating the returned payload must not alias the input buffer.
		if len(out.Payload) > 0 {
			out.Payload[0] ^= 0xFF
			if raw[HeaderSize] == out.Payload[0] {
				t.Fatalf("case %d: decoded payload aliases input", i)
			}
		}
	}
}

func TestDecodeTrailingBytesIgnored(t *testing.T) {
	raw, err := Encode(Frame{Version: 1, Kind: 1, Payload: []byte("xy")})
	if err != nil {
		t.Fatal(err)
	}
	padded := append(raw, 0xAA, 0xBB, 0xCC)
	f, err := Decode(padded)
	if err != nil {
		t.Fatalf("Decode with trailing bytes: %v", err)
	}
	if string(f.Payload) != "xy" {
		t.Fatalf("payload = %q", f.Payload)
	}
}

func TestEncodeErrors(t *testing.T) {
	if _, err := Encode(Frame{Version: 2, Payload: nil}); !errors.Is(err, ErrBadVersion) {
		t.Fatalf("version 2: err = %v, want ErrBadVersion", err)
	}
	big := make([]byte, MaxPayload+1)
	if _, err := Encode(Frame{Version: 1, Payload: big}); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("oversize payload: err = %v, want ErrTooLarge", err)
	}
}

func TestEncodeMaxPayloadAccepted(t *testing.T) {
	p := make([]byte, MaxPayload)
	for i := range p {
		p[i] = byte(i * 7)
	}
	raw, err := Encode(Frame{Version: 1, Flags: 1, Kind: 2, Payload: p})
	if err != nil {
		t.Fatalf("Encode max payload: %v", err)
	}
	if len(raw) != HeaderSize+MaxPayload {
		t.Fatalf("len = %d", len(raw))
	}
	out, err := Decode(raw)
	if err != nil {
		t.Fatalf("Decode max payload: %v", err)
	}
	for i := range p {
		if out.Payload[i] != p[i] {
			t.Fatalf("byte %d mismatch", i)
		}
	}
}

// buildFrame assures a wire frame with the given fields, recomputing the
// checksum so the caller can then corrupt it deliberately.
func buildFrame(t *testing.T, version uint8, flags uint16, kind uint8, payload []byte) []byte {
	t.Helper()
	buf := make([]byte, HeaderSize+len(payload))
	binary.BigEndian.PutUint32(buf[0:4], uint32(len(payload)))
	buf[4] = version
	binary.BigEndian.PutUint16(buf[5:7], flags)
	buf[7] = kind
	copy(buf[HeaderSize:], payload)
	binary.BigEndian.PutUint16(buf[8:10], refChecksum(t, buf, len(payload)))
	return buf
}

func TestErrShortFrame(t *testing.T) {
	// Every prefix shorter than the header is a short frame.
	for n := 0; n < HeaderSize; n++ {
		data := make([]byte, n)
		if n >= 4 {
			binary.BigEndian.PutUint32(data[0:4], 0)
		}
		if _, err := Decode(data); !errors.Is(err, ErrShortFrame) {
			t.Fatalf("len %d: err = %v, want ErrShortFrame", n, err)
		}
	}
	// Full header declaring payload bytes that are not present.
	raw := buildFrame(t, 1, 0, 0, []byte("abc"))
	for n := HeaderSize; n < len(raw); n++ {
		if _, err := Decode(raw[:n]); !errors.Is(err, ErrShortFrame) {
			t.Fatalf("len %d of %d: err = %v, want ErrShortFrame", n, len(raw), err)
		}
	}
}

func TestErrBadVersion(t *testing.T) {
	payload := []byte("data")
	raw := buildFrame(t, 2, 0x0001, 0x02, payload)
	if _, err := Decode(raw); !errors.Is(err, ErrBadVersion) {
		t.Fatalf("version 2: err = %v, want ErrBadVersion", err)
	}
	if raw[4] != 2 {
		t.Fatalf("setup wrong")
	}
}

func TestErrChecksumBitFlips(t *testing.T) {
	payload := []byte("checksum me")
	base := buildFrame(t, 1, 0x8000, 0x09, payload)

	// Flip every single bit in the payload: each must be detected.
	for i := range payload {
		bad := append([]byte(nil), base...)
		bad[HeaderSize+i] ^= 0x01
		if _, err := Decode(bad); !errors.Is(err, ErrChecksum) {
			t.Fatalf("payload bit flip at %d: err = %v, want ErrChecksum", i, err)
		}
	}

	// Flip a bit in flags/kind: version stays 1 and the frame stays
	// complete, so corruption must surface as ErrChecksum. (Flipping the
	// version byte would be ErrBadVersion and the length byte
	// ErrShortFrame — both precede the checksum by design.)
	for _, off := range []int{5, 6, 7} {
		bad := append([]byte(nil), base...)
		bad[off] ^= 0x80
		if _, err := Decode(bad); !errors.Is(err, ErrChecksum) {
			t.Fatalf("header bit flip at %d: err = %v, want ErrChecksum", off, err)
		}
	}

	// A flipped bit in the length field changes the declared length too; when
	// enough bytes are present for the new declaration it must still be
	// detected as a checksum error rather than accepted.
	padCase := append([]byte(nil), base...)
	padCase[3] ^= 0x80 // declared length 0x0B -> 0x8B (139)
	padCase = append(padCase, make([]byte, 139-len(payload))...)
	if _, err := Decode(padCase); !errors.Is(err, ErrChecksum) {
		t.Fatalf("length-field flip with padding: err = %v, want ErrChecksum", err)
	}

	// Corrupt the stored checksum itself.
	bad := append([]byte(nil), base...)
	bad[8] ^= 0xFF
	if _, err := Decode(bad); !errors.Is(err, ErrChecksum) {
		t.Fatalf("corrupt checksum field: err = %v", err)
	}
}

func TestErrTooLargeOnDecode(t *testing.T) {
	// Declared length exceeds MaxPayload while the whole frame is present.
	payloadLen := MaxPayload + 1
	buf := make([]byte, HeaderSize+payloadLen)
	binary.BigEndian.PutUint32(buf[0:4], uint32(payloadLen))
	buf[4] = 1
	binary.BigEndian.PutUint16(buf[8:10], refChecksum(t, buf, payloadLen))
	if _, err := Decode(buf); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("err = %v, want ErrTooLarge", err)
	}
}

// TestErrorOrdering verifies the fixed precedence: length, version,
// checksum, size — the same bad bytes must always map to the same error.
func TestErrorOrdering(t *testing.T) {
	// 1. Short frame beats bad version (and everything else).
	buf := make([]byte, HeaderSize+4)
	binary.BigEndian.PutUint32(buf[0:4], 4)
	buf[4] = 9 // bad version
	// Drop the last payload byte: incomplete despite bad version.
	if _, err := Decode(buf[:len(buf)-1]); !errors.Is(err, ErrShortFrame) {
		t.Fatalf("short + bad version: err = %v, want ErrShortFrame", err)
	}

	// 2. Bad version beats bad checksum and oversize.
	oversize := MaxPayload + 1
	buf = make([]byte, HeaderSize+oversize)
	binary.BigEndian.PutUint32(buf[0:4], uint32(oversize))
	buf[4] = 9 // bad version; checksum left zero (also wrong)
	if _, err := Decode(buf); !errors.Is(err, ErrBadVersion) {
		t.Fatalf("bad version + bad checksum + oversize: err = %v, want ErrBadVersion", err)
	}

	// 3. Bad checksum beats oversize.
	buf = make([]byte, HeaderSize+oversize)
	binary.BigEndian.PutUint32(buf[0:4], uint32(oversize))
	buf[4] = 1
	// Checksum stays zero; recomputed value for all-zero payload/header is
	// 0x0001 (version byte), so it mismatches.
	if _, err := Decode(buf); !errors.Is(err, ErrChecksum) {
		t.Fatalf("bad checksum + oversize: err = %v, want ErrChecksum", err)
	}

	// 4. Oversize with a valid checksum reports ErrTooLarge (covered in
	// TestErrTooLargeOnDecode, but assert determinism by repeating).
	good := make([]byte, HeaderSize+oversize)
	binary.BigEndian.PutUint32(good[0:4], uint32(oversize))
	good[4] = 1
	binary.BigEndian.PutUint16(good[8:10], refChecksum(t, good, oversize))
	for i := 0; i < 3; i++ {
		if _, err := Decode(good); !errors.Is(err, ErrTooLarge) {
			t.Fatalf("iteration %d: err = %v, want ErrTooLarge", i, err)
		}
	}
}

func TestShortFrameHugeDeclaredLength(t *testing.T) {
	// A tiny input claiming a ~4 GiB payload must be ErrShortFrame, even on
	// 32-bit platforms where HeaderSize+length could wrap.
	buf := make([]byte, HeaderSize)
	binary.BigEndian.PutUint32(buf[0:4], 0xFFFFFFFF)
	buf[4] = 1
	if _, err := Decode(buf); !errors.Is(err, ErrShortFrame) {
		t.Fatalf("err = %v, want ErrShortFrame", err)
	}
}

func TestChecksumWraparound(t *testing.T) {
	// Payload whose byte sum exceeds 0xFFFF; only the low 16 bits survive.
	payload := make([]byte, 300)
	for i := range payload {
		payload[i] = 0xFF
	}
	raw := buildFrame(t, 1, 0xFFFF, 0xFF, payload)
	got := binary.BigEndian.Uint16(raw[8:10])
	// Independent sum over every byte the checksum covers:
	// length 00 00 01 2C -> 0+0+1+0x2C, version 0x01, flags 0xFF+0xFF,
	// kind 0xFF, plus 300 payload bytes of 0xFF. Low 16 bits only.
	want := uint32(0 + 0 + 1 + 0x2C + 0x01 + 0xFF + 0xFF + 0xFF + 300*0xFF)
	if got != uint16(want) {
		t.Fatalf("checksum = %#04x, want %#04x", got, uint16(want))
	}
	if _, err := Decode(raw); err != nil {
		t.Fatalf("Decode: %v", err)
	}
}
