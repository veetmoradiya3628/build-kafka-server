package protocol

import (
	"bytes"
	"encoding/binary"
)

type DescribeTopicPartitionsResponse struct {
	CorrelationID uint32
	TopicName     string
}

// ParseDescribeTopicPartitionsRequest extracts the topic name from the raw buffer
func ParseDescribeTopicPartitionsRequest(buff []byte) string {
	// The Client ID length is a standard 16-bit int at offset 12 in the request header
	clientIDLen := int(binary.BigEndian.Uint16(buff[12:14]))

	// Request Header v2 ends with a TAG_BUFFER (1 byte) after the Client ID string.
	// Body offset = 14 (Header bytes up to Client ID) + Client ID length + 1 (TAG_BUFFER)
	bodyOffset := 14 + clientIDLen + 1

	// The topic name length is at bodyOffset+1 (Compact String format: length + 1)
	topicNameLen := int(buff[bodyOffset+1]) - 1

	topicNameStart := bodyOffset + 2
	topicNameEnd := topicNameStart + topicNameLen

	return string(buff[topicNameStart:topicNameEnd])
}

// Encode converts the response into Kafka wire protocol bytes
func (r *DescribeTopicPartitionsResponse) Encode() []byte {
	var body bytes.Buffer

	// 1. Response Header v1
	binary.Write(&body, binary.BigEndian, r.CorrelationID)
	binary.Write(&body, binary.BigEndian, int8(0)) // TAG_BUFFER

	// 2. Response Body
	binary.Write(&body, binary.BigEndian, int32(0)) // throttle_time_ms
	binary.Write(&body, binary.BigEndian, int8(2))  // topics array length: 1 element (1 + 1)

	// --- Topic Element ---
	binary.Write(&body, binary.BigEndian, int16(3)) // error_code: 3 (UNKNOWN_TOPIC_OR_PARTITION)

	// Compact String: length + 1, followed by the string bytes
	body.WriteByte(byte(len(r.TopicName) + 1))
	body.WriteString(r.TopicName)

	// topic_id: 16 bytes of zeros (UUID)
	body.Write(make([]byte, 16))

	// is_internal: false (0)
	body.WriteByte(0)

	// partitions array: empty (length 0 + 1 = 1)
	body.WriteByte(1)

	// topic_authorized_operations
	binary.Write(&body, binary.BigEndian, int32(0))

	// TAG_BUFFER for topic element
	body.WriteByte(0)
	// ---------------------

	// next_cursor (use -1 for null, represented as 0xff in int8)
	binary.Write(&body, binary.BigEndian, int8(-1))

	// TAG_BUFFER for response body
	body.WriteByte(0)

	return body.Bytes()
}
