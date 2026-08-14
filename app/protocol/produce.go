package protocol

import (
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

	if ptr >= len(buff) {
		return nil
	}
	_, n := binary.Uvarint(buff[ptr:]) // TAG_BUFFER for Request Header v2
	ptr += n

	if ptr >= len(buff) {
		return nil
	}
	// Read Transactional ID (Compact Nullable String)
	txIdLen, n := binary.Uvarint(buff[ptr:])
	ptr += n
	if txIdLen > 0 {
		ptr += int(txIdLen - 1)
	}

	if ptr+6 > len(buff) {
		return nil
	}
	ptr += 2 // acks
	ptr += 4 // timeout_ms

	if ptr >= len(buff) {
		return nil
	}
	// Read Topics Array Length (Compact Array)
	topicsCount, n := binary.Uvarint(buff[ptr:])
	ptr += n

	var topics []ProduceRequestTopic
	if topicsCount <= 1 {
		return topics
	}

	for i := 0; i < int(topicsCount-1); i++ {
		if ptr >= len(buff) {
			return topics
		}
		// Read Topic Name (Compact String)
		nameLen, n := binary.Uvarint(buff[ptr:])
		ptr += n

		if ptr+int(nameLen-1) > len(buff) {
			return topics
		}
		topicName := string(buff[ptr : ptr+int(nameLen-1)])
		ptr += int(nameLen - 1)

		if ptr >= len(buff) {
			return topics
		}
		// Read Partitions Array Length (Compact Array)
		partsCount, n := binary.Uvarint(buff[ptr:])
		ptr += n

		var partitions []ProduceRequestPartition
		for j := 0; j < int(partsCount-1); j++ {
			if ptr+4 > len(buff) {
				return topics
			}
			// Partition Index
			partIndex := int32(binary.BigEndian.Uint32(buff[ptr : ptr+4]))
			ptr += 4

			if ptr >= len(buff) {
				return topics
			}

			// FIX: COMPACT_RECORDS uses Uvarint(length + 1)
			recordsLenPlusOne, n := binary.Uvarint(buff[ptr:])
			ptr += n

			var records []byte
			if recordsLenPlusOne > 0 {
				recordsLen := int(recordsLenPlusOne - 1)
				if ptr+recordsLen <= len(buff) {
					records = buff[ptr : ptr+recordsLen]
					ptr += recordsLen
				} else {
					// Buffer is truncated, prevent panic
					records = buff[ptr:]
					ptr = len(buff)
				}
			}

			if ptr < len(buff) {
				_, n = binary.Uvarint(buff[ptr:]) // TAG_BUFFER for partition
				ptr += n
			}

			partitions = append(partitions, ProduceRequestPartition{
				Index:   partIndex,
				Records: records,
			})
		}

		if ptr < len(buff) {
			_, n = binary.Uvarint(buff[ptr:]) // TAG_BUFFER for topic
			ptr += n
		}

		topics = append(topics, ProduceRequestTopic{
			Name:       topicName,
			Partitions: partitions,
		})
	}

	return topics
}
