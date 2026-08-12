package protocol

import (
	"bytes"
	"encoding/binary"
)

// WrapWithMessageSize prepends the 4-byte size integer to any encoded payload
func WrapWithMessageSize(payload []byte) []byte {
	messageSize := uint32(len(payload))
	var finalResponse bytes.Buffer
	binary.Write(&finalResponse, binary.BigEndian, messageSize)
	finalResponse.Write(payload)
	return finalResponse.Bytes()
}
