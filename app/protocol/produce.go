package protocol

import (
	"bytes"
	"encoding/binary"
)

type ProduceResponse struct {
	CorrelationID uint32
	Topics        []ProduceResponseTopic
}

type ProduceResponseTopic struct {
	Name       string
	Partitions []ProduceResponsePartition
}

type ProduceResponsePartition struct {
	Index          int32
	ErrorCode      int16
	BaseOffset     int64
	LogAppendTime  int64
	LogStartOffset int64
}

type ProduceRequestTopic struct {
	Name       string
	Partitions []int32
}

// ParseProduceRequest specifically extracts the first topic and partition
func ParseProduceRequest(buff []byte) []ProduceRequestTopic {
	if len(buff) < 14 {
		return nil
	}

	// Read Client ID
	clientIDLen := int16(binary.BigEndian.Uint16(buff[12:14]))
	ptr := 14
	if clientIDLen > 0 {
		ptr += int(clientIDLen)
	}
	ptr++ // TAG_BUFFER for Request Header v2

	// Read Transactional ID (Compact Nullable String)
	txIdLen := int(buff[ptr])
	ptr++
	if txIdLen > 0 {
		ptr += txIdLen - 1
	}

	ptr += 2 // acks
	ptr += 4 // timeout_ms

	// Read Topics Array Length
	topicsCount := int(buff[ptr]) - 1
	ptr++

	var topics []ProduceRequestTopic
	if topicsCount <= 0 {
		return topics
	}

	// We only need to parse the first topic and partition to fulfill the current requirements
	nameLen := int(buff[ptr]) - 1
	ptr++
	topicName := string(buff[ptr : ptr+nameLen])
	ptr += nameLen

	// Read Partitions Array Length
	partsCount := int(buff[ptr]) - 1
	ptr++

	var partitions []int32
	if partsCount > 0 {
		// Read just the Partition Index (the first 4 bytes of the partition block)
		partIndex := int32(binary.BigEndian.Uint32(buff[ptr : ptr+4]))
		partitions = append(partitions, partIndex)
	}

	topics = append(topics, ProduceRequestTopic{
		Name:       topicName,
		Partitions: partitions,
	})

	return topics
}

// Encode converts the ProduceResponse struct into the Kafka Produce Response v11 wire protocol bytes
func (r *ProduceResponse) Encode() []byte {
	var body bytes.Buffer

	// Response Header v1
	binary.Write(&body, binary.BigEndian, r.CorrelationID)
	body.WriteByte(0) // TAG_BUFFER

	// Responses Array (Compact Array format)
	body.WriteByte(byte(len(r.Topics) + 1))

	for _, topic := range r.Topics {
		// Topic Name (Compact String)
		body.WriteByte(byte(len(topic.Name) + 1))
		body.WriteString(topic.Name)

		// Partitions Array (Compact Array format)
		body.WriteByte(byte(len(topic.Partitions) + 1))
		for _, part := range topic.Partitions {
			binary.Write(&body, binary.BigEndian, part.Index)
			binary.Write(&body, binary.BigEndian, part.ErrorCode)
			binary.Write(&body, binary.BigEndian, part.BaseOffset)
			binary.Write(&body, binary.BigEndian, part.LogAppendTime)
			binary.Write(&body, binary.BigEndian, part.LogStartOffset)

			// record_errors (Compact Array): 0 elements -> length 1
			body.WriteByte(1)

			// error_message (Compact Nullable String): null -> length 0
			body.WriteByte(0)

			body.WriteByte(0) // TAG_BUFFER for partition
		}
		body.WriteByte(0) // TAG_BUFFER for topic
	}

	// throttle_time_ms
	binary.Write(&body, binary.BigEndian, int32(0))

	// TAG_BUFFER for response body
	body.WriteByte(0)

	return body.Bytes()
}
