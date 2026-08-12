package protocol

import (
	"encoding/binary"
	"fmt"
)

type RequestHeader struct {
	MessageSize   uint32
	RequestAPIKey uint16
	RequestAPIVer uint16
	CorrelationID uint32
}

// ParseHeader extracts the header info from the raw buffer
func ParseHeader(buff []byte) (RequestHeader, error) {
	if len(buff) < 12 {
		return RequestHeader{}, fmt.Errorf("buffer too short to parse request")
	}
	return RequestHeader{
		MessageSize:   binary.BigEndian.Uint32(buff[0:4]),
		RequestAPIKey: binary.BigEndian.Uint16(buff[4:6]),
		RequestAPIVer: binary.BigEndian.Uint16(buff[6:8]),
		CorrelationID: binary.BigEndian.Uint32(buff[8:12]),
	}, nil
}
