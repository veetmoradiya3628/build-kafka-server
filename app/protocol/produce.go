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

type ProduceRequestPartition struct {
	Index   int32
	Records []byte // Slice to hold the raw RecordBatch
}

type ProduceRequestTopic struct {
	Name       string
	Partitions []ProduceRequestPartition
}

// ParseProduceRequest specifically extracts the first topic and partition
func ParseProduceRequest(buff []byte) []ProduceRequestTopic {
	if len(buff) < 14 {
		return nil
	}

	clientIDLen := int16(binary.BigEndian.Uint16(buff[12:14]))
	ptr := 14
	if clientIDLen > 0 {
		ptr += int(clientIDLen)
	}
	ptr++ // TAG_BUFFER for Request Header v2

	// Read Transactional ID (Compact Nullable String)
	txIdLen, n := binary.Uvarint(buff[ptr:])
	ptr += n
	if txIdLen > 0 {
		ptr += int(txIdLen - 1)
	}

	ptr += 2 // acks
	ptr += 4 // timeout_ms

	// Read Topics Array Length (Compact Array)
	topicsCount, n := binary.Uvarint(buff[ptr:])
	ptr += n

	var topics []ProduceRequestTopic
	if topicsCount <= 1 {
		return topics
	}

	for i := 0; i < int(topicsCount-1); i++ {
		// Read Topic Name (Compact String)
		nameLen, n := binary.Uvarint(buff[ptr:])
		ptr += n
		topicName := string(buff[ptr : ptr+int(nameLen-1)])
		ptr += int(nameLen - 1)

		// Read Partitions Array Length (Compact Array)
		partsCount, n := binary.Uvarint(buff[ptr:])
		ptr += n

		var partitions []ProduceRequestPartition
		for j := 0; j < int(partsCount-1); j++ {
			// Partition Index
			partIndex := int32(binary.BigEndian.Uint32(buff[ptr : ptr+4]))
			ptr += 4

			// Per KIP-482, the "records" type is strictly an INT32 length, not a Uvarint
			recordsLen := int32(binary.BigEndian.Uint32(buff[ptr : ptr+4]))
			ptr += 4

			var records []byte
			if recordsLen > 0 {
				records = buff[ptr : ptr+int(recordsLen)]
				ptr += int(recordsLen)
			}

			ptr++ // TAG_BUFFER for partition

			partitions = append(partitions, ProduceRequestPartition{
				Index:   partIndex,
				Records: records,
			})
		}
		ptr++ // TAG_BUFFER for topic

		topics = append(topics, ProduceRequestTopic{
			Name:       topicName,
			Partitions: partitions,
		})
	}

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
