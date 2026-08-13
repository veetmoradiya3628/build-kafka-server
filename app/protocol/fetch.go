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
	Records        []byte // Added to hold the log file contents
}

// ParseFetchRequest extracts the topic IDs from the raw buffer safely
func ParseFetchRequest(buff []byte) [][]byte {
	// ... (Keep existing implementation exactly the same)
	if len(buff) < 14 {
		return nil
	}

	clientIDLen := int16(binary.BigEndian.Uint16(buff[12:14]))
	bodyOffset := 14

	if clientIDLen > 0 {
		bodyOffset += int(clientIDLen)
	}
	bodyOffset++

	ptr := bodyOffset + 21

	if ptr >= len(buff) {
		return nil
	}

	numTopics := int(buff[ptr]) - 1
	ptr++

	var topicIDs [][]byte

	for i := 0; i < numTopics; i++ {
		if ptr+16 > len(buff) {
			break
		}

		topicID := buff[ptr : ptr+16]
		topicIDs = append(topicIDs, topicID)
		ptr += 16

		if ptr >= len(buff) {
			break
		}
		numPartitions := int(buff[ptr]) - 1
		ptr++

		for j := 0; j < numPartitions; j++ {
			ptr += 32
			ptr++
		}
		ptr++
	}

	return topicIDs
}

// Encode converts the FetchResponse struct into the Kafka wire protocol bytes (v16)
func (r *FetchResponse) Encode() []byte {
	var body bytes.Buffer

	binary.Write(&body, binary.BigEndian, r.CorrelationID)
	body.WriteByte(0) // TAG_BUFFER

	binary.Write(&body, binary.BigEndian, int32(0)) // throttle_time_ms: 0
	binary.Write(&body, binary.BigEndian, int16(0)) // error_code: 0 (No Error)
	binary.Write(&body, binary.BigEndian, int32(0)) // session_id: 0

	body.WriteByte(byte(len(r.Responses) + 1))

	for _, topic := range r.Responses {
		body.Write(topic.TopicID)
		body.WriteByte(byte(len(topic.Partitions) + 1))

		for _, partition := range topic.Partitions {
			binary.Write(&body, binary.BigEndian, partition.PartitionIndex)
			binary.Write(&body, binary.BigEndian, partition.ErrorCode)
			binary.Write(&body, binary.BigEndian, int64(0)) // high_watermark
			binary.Write(&body, binary.BigEndian, int64(0)) // last_stable_offset
			binary.Write(&body, binary.BigEndian, int64(0)) // log_start_offset

			body.WriteByte(1)                               // aborted_transactions (Compact Array: 0 elements + 1)
			binary.Write(&body, binary.BigEndian, int32(0)) // preferred_read_replica

			// --- COMPACT_RECORDS ENCODING ---
			if len(partition.Records) == 0 {
				body.WriteByte(0) // Null record batch length
			} else {
				// Kafka compact records length is Uvarint(length + 1)
				varintBuf := make([]byte, binary.MaxVarintLen64)
				n := binary.PutUvarint(varintBuf, uint64(len(partition.Records)+1))
				body.Write(varintBuf[:n])
				// Write the raw bytes straight from disk
				body.Write(partition.Records)
			}

			body.WriteByte(0) // TAG_BUFFER for partition
		}

		body.WriteByte(0) // TAG_BUFFER for topic
	}

	body.WriteByte(0) // TAG_BUFFER for response body

	return body.Bytes()
}
