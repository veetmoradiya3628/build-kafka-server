package protocol

import (
	"bytes"
	"encoding/binary"
)

type FetchResponse struct {
	CorrelationID uint32
	Responses     []FetchTopicResponse
}

type FetchTopicResponse struct {
	TopicID    []byte
	Partitions []FetchPartitionResponse
}

type FetchPartitionResponse struct {
	PartitionIndex int32
	ErrorCode      int16
}

// ParseFetchRequest extracts the topic IDs from the raw buffer safely
func ParseFetchRequest(buff []byte) [][]byte {
	if len(buff) < 14 {
		return nil
	}

	// Client ID length is a standard 16-bit int at offset 12 in the request header
	clientIDLen := int16(binary.BigEndian.Uint16(buff[12:14]))
	bodyOffset := 14

	// Handle valid vs nullable client IDs
	if clientIDLen > 0 {
		bodyOffset += int(clientIDLen)
	}
	bodyOffset++ // Request Header v2 ends with a TAG_BUFFER

	// Skip MaxWait (4), MinBytes (4), MaxBytes (4), IsolationLevel (1), SessionID (4), SessionEpoch (4) = 21 bytes
	ptr := bodyOffset + 21

	if ptr >= len(buff) {
		return nil
	}

	// Read the number of elements in the compact array (length - 1)
	numTopics := int(buff[ptr]) - 1
	ptr++

	var topicIDs [][]byte

	for i := 0; i < numTopics; i++ {
		if ptr+16 > len(buff) {
			break
		}

		// Extract 16-byte Topic UUID
		topicID := buff[ptr : ptr+16]
		topicIDs = append(topicIDs, topicID)
		ptr += 16

		if ptr >= len(buff) {
			break
		}
		numPartitions := int(buff[ptr]) - 1
		ptr++

		// Skip over the partitions block since we only need the topic ID for this stage
		for j := 0; j < numPartitions; j++ {
			// partition (4) + currentLeaderEpoch (4) + fetchOffset (8) + lastFetchedEpoch (4) + logStartOffset (8) + partitionMaxBytes (4) = 32 bytes
			ptr += 32
			ptr++ // TAG_BUFFER for partition
		}
		ptr++ // TAG_BUFFER for topic
	}

	return topicIDs
}

// Encode converts the FetchResponse struct into the Kafka wire protocol bytes (v16)
func (r *FetchResponse) Encode() []byte {
	var body bytes.Buffer

	// Response Header v1
	binary.Write(&body, binary.BigEndian, r.CorrelationID)
	body.WriteByte(0) // TAG_BUFFER

	// Fetch Response Body (v16)
	binary.Write(&body, binary.BigEndian, int32(0)) // throttle_time_ms: 0
	binary.Write(&body, binary.BigEndian, int16(0)) // error_code: 0 (No Error)
	binary.Write(&body, binary.BigEndian, int32(0)) // session_id: 0

	// Responses array (Compact Array format: Number of elements + 1)
	body.WriteByte(byte(len(r.Responses) + 1))

	// Loop over Topics
	for _, topic := range r.Responses {
		body.Write(topic.TopicID) // Echo back the exact 16-byte TopicID

		// Partitions array length
		body.WriteByte(byte(len(topic.Partitions) + 1))

		// Loop over Partitions
		for _, partition := range topic.Partitions {
			binary.Write(&body, binary.BigEndian, partition.PartitionIndex)
			binary.Write(&body, binary.BigEndian, partition.ErrorCode)
			binary.Write(&body, binary.BigEndian, int64(0)) // high_watermark
			binary.Write(&body, binary.BigEndian, int64(0)) // last_stable_offset
			binary.Write(&body, binary.BigEndian, int64(0)) // log_start_offset

			body.WriteByte(1)                               // aborted_transactions (Compact Array: 0 elements + 1)
			binary.Write(&body, binary.BigEndian, int32(0)) // preferred_read_replica

			// COMPACT_RECORDS (Null record batch is represented by a length of 0)
			body.WriteByte(0)

			body.WriteByte(0) // TAG_BUFFER for partition
		}

		body.WriteByte(0) // TAG_BUFFER for topic
	}

	// TAG_BUFFER for response body
	body.WriteByte(0)

	return body.Bytes()
}
