package protocol

import (
	"bytes"
	"encoding/binary"
)

type APIInfo struct {
	APIKey     int16
	MinVersion int16
	MaxVersion int16
}

var SupportedAPIs = []APIInfo{
	{APIKey: 18, MinVersion: 0, MaxVersion: 4}, // APIVersions
	{APIKey: 75, MinVersion: 0, MaxVersion: 0}, // DescribeTopicPartitions
}

type ApiVersionsResponse struct {
	CorrelationID uint32
	ErrorCode     int16
}

// Encode converts the ApiVersionsResponse struct into the Kafka wire protocol bytes
func (r *ApiVersionsResponse) Encode() []byte {
	var body bytes.Buffer

	// Header
	binary.Write(&body, binary.BigEndian, r.CorrelationID)

	// Body
	binary.Write(&body, binary.BigEndian, r.ErrorCode)
	binary.Write(&body, binary.BigEndian, int8(len(SupportedAPIs)+1)) // Compact array length

	for _, apiInfo := range SupportedAPIs {
		binary.Write(&body, binary.BigEndian, apiInfo.APIKey)
		binary.Write(&body, binary.BigEndian, apiInfo.MinVersion)
		binary.Write(&body, binary.BigEndian, apiInfo.MaxVersion)
		binary.Write(&body, binary.BigEndian, int8(0)) // TAG_BUFFER for element
	}

	binary.Write(&body, binary.BigEndian, int32(0)) // throttle_time_ms
	binary.Write(&body, binary.BigEndian, int8(0))  // TAG_BUFFER for response

	return body.Bytes()
}
