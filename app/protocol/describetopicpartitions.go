package protocol

import (
	"bytes"
	"encoding/binary"
)

type DescribeTopicPartitionsResponse struct {
	CorrelationID  uint32
	TopicName      string
	ErrorCode      int16
	TopicID        []byte
	PartitionIndex int32
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

func (r *DescribeTopicPartitionsResponse) Encode() []byte {
	var body bytes.Buffer

	// Response Header v1
	binary.Write(&body, binary.BigEndian, r.CorrelationID)
	body.WriteByte(0) // TAG_BUFFER

	// Response Body
	binary.Write(&body, binary.BigEndian, int32(0)) // throttle_time_ms
	body.WriteByte(2)                               // topics array length: 1 element (1 + 1)

	// --- Topic Element ---
	binary.Write(&body, binary.BigEndian, r.ErrorCode)

	body.WriteByte(byte(len(r.TopicName) + 1))
	body.WriteString(r.TopicName)

	body.Write(r.TopicID) // Write the actual 16-byte UUID

	body.WriteByte(0) // is_internal: false

	// --- Partitions Array ---
	if r.ErrorCode == 0 {
		// Topic found: 1 partition element (1 + 1 = 2)
		body.WriteByte(2)

		binary.Write(&body, binary.BigEndian, int16(0)) // error_code
		binary.Write(&body, binary.BigEndian, r.PartitionIndex)
		binary.Write(&body, binary.BigEndian, int32(1)) // leader_id
		binary.Write(&body, binary.BigEndian, int32(0)) // leader_epoch

		body.WriteByte(2) // replica_nodes: 1 element
		binary.Write(&body, binary.BigEndian, int32(1))

		body.WriteByte(2) // isr_nodes: 1 element
		binary.Write(&body, binary.BigEndian, int32(1))

		body.WriteByte(1) // eligible_leader_replicas: 0 elements
		body.WriteByte(1) // last_known_elr: 0 elements
		body.WriteByte(1) // offline_replicas: 0 elements
		body.WriteByte(0) // TAG_BUFFER
	} else {
		// Topic not found: Empty partitions array (0 + 1 = 1)
		body.WriteByte(1)
	}

	binary.Write(&body, binary.BigEndian, int32(0)) // topic_authorized_operations
	body.WriteByte(0)                               // TAG_BUFFER for topic element
	// ---------------------

	binary.Write(&body, binary.BigEndian, int8(-1)) // next_cursor
	body.WriteByte(0)                               // TAG_BUFFER for response body

	return body.Bytes()
}
