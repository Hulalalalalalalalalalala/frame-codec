// Package codec encodes and decodes single binary frames in memory.
//
// Wire format (all multi-byte fields are big-endian):
//
//	offset 0:  payload length, 4 bytes (payload only, header excluded)
//	offset 4:  version,        1 byte
//	offset 5:  flags,          2 bytes
//	offset 7:  kind,           1 byte
//	offset 8:  checksum,       2 bytes
//	offset 10: payload,        length bytes, written verbatim
//
// The header is therefore HeaderSize bytes. The checksum is the low 16 bits
// of the byte-wise sum of every header byte other than the checksum itself
// plus every payload byte.
package codec

import (
	"encoding/binary"
	"errors"
)

// HeaderSize is the fixed wire-format header size in bytes.
const HeaderSize = 10

// MaxPayload is the largest payload, in bytes, that a frame may carry.
const MaxPayload = 1 << 20

// supportedVersion is the only accepted wire-format version.
const supportedVersion uint8 = 1

// Sentinel errors returned by Encode and Decode. Decode applies its checks in
// a fixed order: frame completeness first, then version, then checksum, then
// the declared payload size.
var (
	// ErrShortFrame means the input does not hold enough bytes for the
	// complete frame declared by its length field.
	ErrShortFrame = errors.New("codec: short frame")

	// ErrBadVersion means the frame's version byte is not supported.
	ErrBadVersion = errors.New("codec: unsupported frame version")

	// ErrChecksum means the stored checksum does not match the one
	// recomputed from the header and payload.
	ErrChecksum = errors.New("codec: checksum mismatch")

	// ErrTooLarge means the payload is larger than MaxPayload.
	ErrTooLarge = errors.New("codec: payload too large")
)

// Frame is one in-memory frame.
type Frame struct {
	Version uint8
	Flags   uint16
	Kind    uint8
	Payload []byte
}

// checksum returns the low 16 bits of the byte-wise sum of the header bytes
// preceding the checksum field (data[0:8]) followed by the payload
// (data[HeaderSize : HeaderSize+payloadLen]).
func checksum(data []byte, payloadLen int) uint16 {
	var sum uint32
	for _, b := range data[:8] {
		sum += uint32(b)
	}
	for _, b := range data[HeaderSize : HeaderSize+payloadLen] {
		sum += uint32(b)
	}
	return uint16(sum)
}

// Encode returns the framed bytes for frame. It never touches the disk.
func Encode(frame Frame) ([]byte, error) {
	if frame.Version != supportedVersion {
		return nil, ErrBadVersion
	}
	if len(frame.Payload) > MaxPayload {
		return nil, ErrTooLarge
	}

	buf := make([]byte, HeaderSize+len(frame.Payload))
	binary.BigEndian.PutUint32(buf[0:4], uint32(len(frame.Payload)))
	buf[4] = frame.Version
	binary.BigEndian.PutUint16(buf[5:7], frame.Flags)
	buf[7] = frame.Kind
	copy(buf[HeaderSize:], frame.Payload)
	binary.BigEndian.PutUint16(buf[8:10], checksum(buf, len(frame.Payload)))

	return buf, nil
}

// Decode parses one frame from data and returns it. Bytes trailing the frame
// are ignored. No half-built frame is returned on error.
func Decode(data []byte) (Frame, error) {
	// Completeness first: the full header must be present.
	if len(data) < HeaderSize {
		return Frame{}, ErrShortFrame
	}
	payloadLen := int(binary.BigEndian.Uint32(data[0:4]))
	// Compare against len(data)-HeaderSize (both non-negative here) rather
	// than adding, so a declared length near 2^32 cannot overflow on 32-bit
	// platforms.
	if payloadLen > len(data)-HeaderSize {
		return Frame{}, ErrShortFrame
	}

	// Version before anything else in the received frame.
	if data[4] != supportedVersion {
		return Frame{}, ErrBadVersion
	}

	// Checksum before the declared-size limit.
	stored := binary.BigEndian.Uint16(data[8:10])
	if stored != checksum(data, payloadLen) {
		return Frame{}, ErrChecksum
	}

	// Size last: only when the whole declared frame is actually present.
	if payloadLen > MaxPayload {
		return Frame{}, ErrTooLarge
	}

	payload := make([]byte, payloadLen)
	copy(payload, data[HeaderSize:HeaderSize+payloadLen])

	return Frame{
		Version: data[4],
		Flags:   binary.BigEndian.Uint16(data[5:7]),
		Kind:    data[7],
		Payload: payload,
	}, nil
}
