// Package codec implements encoding and decoding of single binary frames
// in a length-prefixed wire format.
//
// Wire layout (all multi-byte fields big-endian):
//
//	+------------+---------+--------+------+----------+=========+
//	| length (4) | ver (1) | flags(2)| kind(1)| cksum(2) | payload |
//	+------------+---------+--------+------+----------+=========+
//
// The 4-byte length counts payload bytes only. The 10-byte header is
// length + version + flags + kind + checksum. The checksum is the low
// 16 bits of the byte-wise sum of all header bytes except the checksum
// field itself, plus every payload byte.
package codec

import (
	"encoding/binary"
	"errors"
)

const (
	// Version is the only protocol version this codec reads and writes.
	Version = 1

	// HeaderSize is the total header length in bytes:
	// 4 (length) + 1 (version) + 2 (flags) + 1 (kind) + 2 (checksum).
	HeaderSize = 10

	// MaxPayload is the maximum payload size in bytes a frame may carry.
	MaxPayload = 1 << 20 // 1 MiB
)

var (
	// ErrShortFrame is returned when the input is too short to contain a
	// complete frame.
	ErrShortFrame = errors.New("codec: short frame")
	// ErrBadVersion is returned when the frame version is not supported.
	ErrBadVersion = errors.New("codec: unsupported version")
	// ErrChecksum is returned when the checksum does not match the
	// recomputed value.
	ErrChecksum = errors.New("codec: checksum mismatch")
	// ErrTooLarge is returned when the payload exceeds MaxPayload.
	ErrTooLarge = errors.New("codec: payload too large")
)

// Frame is a single decoded frame, or the input to Encode.
type Frame struct {
	Version uint8
	Flags   uint16
	Kind    uint8
	Payload []byte
}

// checksum sums every byte of header-and-payload except the checksum
// field itself and keeps the low 16 bits.
func checksum(data []byte) uint16 {
	var sum uint32
	for _, b := range data {
		sum += uint32(b)
	}
	return uint16(sum)
}

// Encode serializes f into the wire format. The version written is
// always Version. It returns ErrTooLarge if the payload exceeds
// MaxPayload.
func Encode(f Frame) ([]byte, error) {
	if len(f.Payload) > MaxPayload {
		return nil, ErrTooLarge
	}

	out := make([]byte, HeaderSize+len(f.Payload))
	binary.BigEndian.PutUint32(out[0:4], uint32(len(f.Payload)))
	out[4] = Version
	binary.BigEndian.PutUint16(out[5:7], f.Flags)
	out[7] = f.Kind
	copy(out[HeaderSize:], f.Payload)

	// Sum the header bytes before the checksum field plus the payload.
	sum := checksum(out[0:8]) + checksum(out[HeaderSize:])
	binary.BigEndian.PutUint16(out[8:10], sum)
	return out, nil
}

// Decode parses one frame from data. On any error it returns the zero
// Frame and one of ErrShortFrame, ErrBadVersion, ErrChecksum or
// ErrTooLarge; checks run in that order so the same corrupt input always
// yields the same error.
func Decode(data []byte) (Frame, error) {
	var f Frame

	// 1. Length: enough bytes for the header, then for the declared payload.
	if len(data) < HeaderSize {
		return f, ErrShortFrame
	}
	payloadLen := binary.BigEndian.Uint32(data[0:4])
	if uint64(len(data)) < uint64(HeaderSize)+uint64(payloadLen) {
		return f, ErrShortFrame
	}

	// 2. Version.
	version := data[4]
	if version != Version {
		return f, ErrBadVersion
	}

	// 3. Checksum over header (minus the checksum field) and payload.
	want := binary.BigEndian.Uint16(data[8:10])
	got := checksum(data[0:8]) + checksum(data[HeaderSize:HeaderSize+int(payloadLen)])
	if got != want {
		return f, ErrChecksum
	}

	// 4. Size.
	if payloadLen > MaxPayload {
		return f, ErrTooLarge
	}

	payload := make([]byte, payloadLen)
	copy(payload, data[HeaderSize:HeaderSize+int(payloadLen)])

	f.Version = version
	f.Flags = binary.BigEndian.Uint16(data[5:7])
	f.Kind = data[7]
	f.Payload = payload
	return f, nil
}
