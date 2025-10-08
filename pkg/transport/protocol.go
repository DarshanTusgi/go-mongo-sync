package transport

import (
	"encoding/binary"
	"fmt"
	"hash/crc64"
)

// Protocol constants
const (
	// Protocol version
	ProtocolVersion = 1

	// Frame header size (32 bytes)
	FrameHeaderSize = 32

	// Maximum frame size (64MB)
	MaxFrameSize = 64 * 1024 * 1024

	// Minimum frame size (header only)
	MinFrameSize = FrameHeaderSize
)

// Message types
const (
	MsgTypeDocBatch       uint8 = 0x01
	MsgTypeAck            uint8 = 0x02
	MsgTypeHeartbeat      uint8 = 0x03
	MsgTypeResumeRequest  uint8 = 0x04
	MsgTypeResumeResponse uint8 = 0x05
	MsgTypeControl        uint8 = 0x06
)

// Frame flags (bitfield)
const (
	FlagCompressed  uint8 = 0x01
	FlagControl     uint8 = 0x02
	FlagLastInBatch uint8 = 0x04
)

// Compression types
const (
	CompressionNone = 0
	CompressionZstd = 1
	CompressionLZ4  = 2
)

// FrameHeader represents the 32-byte fixed header
type FrameHeader struct {
	FrameLen        uint32  // Total frame length (header + payload)
	Flags           uint8   // Bitfield flags
	MsgType         uint8   // Message type
	Version         uint16  // Protocol version
	StreamID        uint64  // Stream identifier
	BatchSeq        uint64  // Batch sequence number
	PayloadChecksum uint64  // CRC64 checksum of payload
	Reserved        [8]byte // Reserved for future use
}

// Frame represents a complete protocol frame
type Frame struct {
	Header  FrameHeader
	Payload []byte
}

// EncodeHeader encodes the frame header to bytes
func (h *FrameHeader) EncodeHeader() []byte {
	buf := make([]byte, FrameHeaderSize)

	binary.BigEndian.PutUint32(buf[0:4], h.FrameLen)
	buf[4] = h.Flags
	buf[5] = h.MsgType
	binary.BigEndian.PutUint16(buf[6:8], h.Version)
	binary.BigEndian.PutUint64(buf[8:16], h.StreamID)
	binary.BigEndian.PutUint64(buf[16:24], h.BatchSeq)
	binary.BigEndian.PutUint64(buf[24:32], h.PayloadChecksum)
	// Reserved bytes are already zero

	return buf
}

// DecodeHeader decodes bytes to frame header
func DecodeHeader(buf []byte) (*FrameHeader, error) {
	if len(buf) < FrameHeaderSize {
		return nil, fmt.Errorf("insufficient data for header: got %d bytes, need %d", len(buf), FrameHeaderSize)
	}

	h := &FrameHeader{
		FrameLen:        binary.BigEndian.Uint32(buf[0:4]),
		Flags:           buf[4],
		MsgType:         buf[5],
		Version:         binary.BigEndian.Uint16(buf[6:8]),
		StreamID:        binary.BigEndian.Uint64(buf[8:16]),
		BatchSeq:        binary.BigEndian.Uint64(buf[16:24]),
		PayloadChecksum: binary.BigEndian.Uint64(buf[24:32]),
	}

	// Reserved bytes are already zero in the new struct

	return h, nil
}

// Validate checks if the header is valid
func (h *FrameHeader) Validate() error {
	if h.Version != ProtocolVersion {
		return fmt.Errorf("unsupported protocol version: %d", h.Version)
	}

	if h.FrameLen < FrameHeaderSize {
		return fmt.Errorf("invalid frame length: %d", h.FrameLen)
	}

	if h.FrameLen > MaxFrameSize {
		return fmt.Errorf("frame too large: %d bytes", h.FrameLen)
	}

	switch h.MsgType {
	case MsgTypeDocBatch, MsgTypeAck, MsgTypeHeartbeat, MsgTypeResumeRequest, MsgTypeResumeResponse, MsgTypeControl:
		// Valid message types
	default:
		return fmt.Errorf("unknown message type: 0x%02x", h.MsgType)
	}

	return nil
}

// PayloadSize returns the size of the payload
func (h *FrameHeader) PayloadSize() uint32 {
	if h.FrameLen < FrameHeaderSize {
		return 0
	}
	return h.FrameLen - FrameHeaderSize
}

// IsCompressed checks if the frame is compressed
func (h *FrameHeader) IsCompressed() bool {
	return h.Flags&FlagCompressed != 0
}

// IsControl checks if this is a control frame
func (h *FrameHeader) IsControl() bool {
	return h.Flags&FlagControl != 0
}

// IsLastInBatch checks if this is the last frame in a batch
func (h *FrameHeader) IsLastInBatch() bool {
	return h.Flags&FlagLastInBatch != 0
}

// NewFrame creates a new frame with the given parameters
func NewFrame(msgType uint8, streamID, batchSeq uint64, payload []byte) *Frame {
	checksum := crc64.Checksum(payload, crc64.MakeTable(crc64.ECMA))

	header := FrameHeader{
		FrameLen:        uint32(FrameHeaderSize + len(payload)),
		Flags:           0,
		MsgType:         msgType,
		Version:         ProtocolVersion,
		StreamID:        streamID,
		BatchSeq:        batchSeq,
		PayloadChecksum: checksum,
	}

	return &Frame{
		Header:  header,
		Payload: payload,
	}
}

// NewDocBatchFrame creates a new document batch frame
func NewDocBatchFrame(streamID, batchSeq uint64, payload []byte, compressed bool, lastInBatch bool) *Frame {
	frame := NewFrame(MsgTypeDocBatch, streamID, batchSeq, payload)

	if compressed {
		frame.Header.Flags |= FlagCompressed
	}
	if lastInBatch {
		frame.Header.Flags |= FlagLastInBatch
	}

	return frame
}

// NewAckFrame creates a new acknowledgment frame
func NewAckFrame(streamID, ackUpTo uint64) *Frame {
	payload := make([]byte, 16)
	binary.BigEndian.PutUint64(payload[0:8], streamID)
	binary.BigEndian.PutUint64(payload[8:16], ackUpTo)

	return NewFrame(MsgTypeAck, streamID, ackUpTo, payload)
}

// NewHeartbeatFrame creates a new heartbeat frame
func NewHeartbeatFrame() *Frame {
	return NewFrame(MsgTypeHeartbeat, 0, 0, nil)
}

// NewResumeRequestFrame creates a new resume request frame
func NewResumeRequestFrame(streamID, fromSeq uint64) *Frame {
	payload := make([]byte, 16)
	binary.BigEndian.PutUint64(payload[0:8], streamID)
	binary.BigEndian.PutUint64(payload[8:16], fromSeq)

	return NewFrame(MsgTypeResumeRequest, streamID, fromSeq, payload)
}

// NewResumeResponseFrame creates a new resume response frame
func NewResumeResponseFrame(streamID, fromSeq uint64, success bool) *Frame {
	payload := make([]byte, 17)
	binary.BigEndian.PutUint64(payload[0:8], streamID)
	binary.BigEndian.PutUint64(payload[8:16], fromSeq)
	if success {
		payload[16] = 1
	}

	return NewFrame(MsgTypeResumeResponse, streamID, fromSeq, payload)
}

// EncodeFrame encodes a complete frame to bytes
func (f *Frame) EncodeFrame() []byte {
	headerBytes := f.Header.EncodeHeader()
	if len(f.Payload) == 0 {
		return headerBytes
	}

	result := make([]byte, len(headerBytes)+len(f.Payload))
	copy(result, headerBytes)
	copy(result[len(headerBytes):], f.Payload)

	return result
}

// VerifyChecksum verifies the payload checksum
func (f *Frame) VerifyChecksum() bool {
	expected := crc64.Checksum(f.Payload, crc64.MakeTable(crc64.ECMA))
	return expected == f.Header.PayloadChecksum
}

// AckMessage represents an acknowledgment message payload
type AckMessage struct {
	StreamID uint64
	AckUpTo  uint64
}

// DecodeAckMessage decodes an ACK message from payload
func DecodeAckMessage(payload []byte) (*AckMessage, error) {
	if len(payload) < 16 {
		return nil, fmt.Errorf("invalid ACK payload size: %d", len(payload))
	}

	return &AckMessage{
		StreamID: binary.BigEndian.Uint64(payload[0:8]),
		AckUpTo:  binary.BigEndian.Uint64(payload[8:16]),
	}, nil
}

// ResumeMessage represents a resume request/response message payload
type ResumeMessage struct {
	StreamID uint64
	FromSeq  uint64
	Success  bool // Only used in response
}

// DecodeResumeMessage decodes a resume message from payload
func DecodeResumeMessage(payload []byte, isResponse bool) (*ResumeMessage, error) {
	minSize := 16
	if isResponse {
		minSize = 17
	}

	if len(payload) < minSize {
		return nil, fmt.Errorf("invalid resume payload size: %d", len(payload))
	}

	msg := &ResumeMessage{
		StreamID: binary.BigEndian.Uint64(payload[0:8]),
		FromSeq:  binary.BigEndian.Uint64(payload[8:16]),
	}

	if isResponse && len(payload) > 16 {
		msg.Success = payload[16] != 0
	}

	return msg, nil
}
