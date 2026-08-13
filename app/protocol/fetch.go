package protocol

import (
	"bytes"
	"encoding/binary"
)

type FetchResponse struct {
	CorrelationID uint32
}

func (r *FetchResponse) Encode() []byte {
	var body bytes.Buffer

	binary.Write(&body, binary.BigEndian, r.CorrelationID)
	body.WriteByte(0)

	// Fetch Response Body (v16)
	binary.Write(&body, binary.BigEndian, int32(0)) // throttle_time_ms: 0
	binary.Write(&body, binary.BigEndian, int16(0)) // error_code: 0 (No Error)
	binary.Write(&body, binary.BigEndian, int32(0)) // session_id: 0

	// Responses array (Compact Array format: Number of elements + 1)
	// Since the tester sends 0 topics, 0 + 1 = 1
	body.WriteByte(1)

	// TAG_BUFFER for response body
	body.WriteByte(0)

	return body.Bytes()
}
