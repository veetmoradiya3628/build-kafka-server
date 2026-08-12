package protocol

import (
	"bytes"
	"encoding/binary"
)

type TopicResponse struct {
	ErrorCode  int16
	TopicName  string
	TopicID    []byte
	Partitions []int32
}

type DescribeTopicPartitionsResponse struct {
	CorrelationID uint32
	Topics        []TopicResponse // Now a slice of structs
}

// ParseDescribeTopicPartitionsRequest extracts the topic names from the raw buffer
func ParseDescribeTopicPartitionsRequest(buff []byte) []string {
	// The Client ID length is a standard 16-bit int at offset 12 in the request header
	clientIDLen := int(binary.BigEndian.Uint16(buff[12:14]))

	// Request Header v2 ends with a TAG_BUFFER (1 byte) after the Client ID string.
	// Body offset = 14 (Header bytes up to Client ID) + Client ID length + 1 (TAG_BUFFER)
	bodyOffset := 14 + clientIDLen + 1

	// Read the number of elements in the compact array (length - 1)
	numTopics := int(buff[bodyOffset]) - 1
	ptr := bodyOffset + 1

	var topics []string

	// Loop through the topics array
	for i := 0; i < numTopics; i++ {
		// Read compact string length (length - 1)
		topicNameLen := int(buff[ptr]) - 1
		ptr++

		// Extract string
		topicName := string(buff[ptr : ptr+topicNameLen])
		ptr += topicNameLen

		topics = append(topics, topicName)

		// Skip TAG_BUFFER for this topic element
		ptr++
	}

	return topics
}

func (r *DescribeTopicPartitionsResponse) Encode() []byte {
	var body bytes.Buffer

	// Response Header v1
	binary.Write(&body, binary.BigEndian, r.CorrelationID)
	body.WriteByte(0) // TAG_BUFFER

	// Response Body
	binary.Write(&body, binary.BigEndian, int32(0)) // throttle_time_ms

	// topics array length (Compact Array format: Number of elements + 1)
	body.WriteByte(byte(len(r.Topics) + 1))

	// --- Loop over Topics ---
	for _, topic := range r.Topics {
		binary.Write(&body, binary.BigEndian, topic.ErrorCode)

		// Topic Name (Compact String)
		body.WriteByte(byte(len(topic.TopicName) + 1))
		body.WriteString(topic.TopicName)

		body.Write(topic.TopicID) // 16-byte UUID
		body.WriteByte(0)         // is_internal: false

		// --- Partitions Array ---
		if topic.ErrorCode == 0 && len(topic.Partitions) > 0 {
			// Array length: Number of partitions + 1
			body.WriteByte(byte(len(topic.Partitions) + 1))

			// Loop through all discovered partitions for this specific topic
			for _, partitionID := range topic.Partitions {
				binary.Write(&body, binary.BigEndian, int16(0)) // error_code
				binary.Write(&body, binary.BigEndian, partitionID)
				binary.Write(&body, binary.BigEndian, int32(1)) // leader_id
				binary.Write(&body, binary.BigEndian, int32(0)) // leader_epoch

				body.WriteByte(2) // replica_nodes: 1 element
				binary.Write(&body, binary.BigEndian, int32(1))

				body.WriteByte(2) // isr_nodes: 1 element
				binary.Write(&body, binary.BigEndian, int32(1))

				body.WriteByte(1) // eligible_leader_replicas: 0
				body.WriteByte(1) // last_known_elr: 0
				body.WriteByte(1) // offline_replicas: 0
				body.WriteByte(0) // TAG_BUFFER
			}
		} else {
			// Topic not found or no partitions: Empty partitions array (0 + 1 = 1)
			body.WriteByte(1)
		}

		binary.Write(&body, binary.BigEndian, int32(0)) // topic_authorized_operations
		body.WriteByte(0)                               // TAG_BUFFER for topic element
	}
	// ---------------------

	binary.Write(&body, binary.BigEndian, int8(-1)) // next_cursor
	body.WriteByte(0)                               // TAG_BUFFER for response body

	return body.Bytes()
}
