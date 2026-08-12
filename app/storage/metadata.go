package storage

import (
	"encoding/binary"
	"fmt"
	"os"
)

type TopicMetadata struct {
	ErrorCode  int16
	TopicID    []byte  // 16 bytes UUID
	Partitions []int32 // List of partition IDs for the topic
	// You can add LeaderID, ReplicaNodes, etc., here later
}

// LoadAllTopics parses the KRaft log and returns a map of TopicName -> TopicMetadata
func LoadAllTopics() (map[string]TopicMetadata, error) {
	fileData, err := os.ReadFile("/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log")
	if err != nil {
		return nil, fmt.Errorf("error reading metadata log: %w", err)
	}

	// State trackers
	uuidToName := make(map[string]string)
	uuidToPartitions := make(map[string][]int32)

	offset := 0
	for offset < len(fileData) {
		batchLength := int(binary.BigEndian.Uint32(fileData[offset+8 : offset+12]))
		batchData := fileData[offset : offset+batchLength+12]

		recordsCount := int(binary.BigEndian.Uint32(batchData[57:61]))
		recordOffset := 61

		for idx := 0; idx < recordsCount; idx++ {
			recordLen, bytesRead := ReadVarint(batchData[recordOffset:])
			currentRecordStart := recordOffset + bytesRead
			nextRecordOffset := currentRecordStart + int(recordLen)

			ptr := currentRecordStart

			// Skip Attributes, TimestampDelta, OffsetDelta
			_, n := ReadVarint(batchData[ptr:])
			ptr += n
			_, n = ReadVarint(batchData[ptr:])
			ptr += n
			_, n = ReadVarint(batchData[ptr:])
			ptr += n

			// Skip Key
			keyLen, n := ReadVarint(batchData[ptr:])
			ptr += n
			if keyLen > 0 {
				ptr += int(keyLen)
			}

			// Read Value
			valueLen, n := ReadVarint(batchData[ptr:])
			ptr += n
			if valueLen > 0 {
				messageBytes := batchData[ptr : ptr+int(valueLen)]
				messageType := messageBytes[1]
				msgPtr := 3

				switch messageType {
				case 2:
					// TopicRecord
					nameLen := int(messageBytes[msgPtr]) - 1
					msgPtr++

					topicName := string(messageBytes[msgPtr : msgPtr+nameLen])
					msgPtr += nameLen

					topicUUID := messageBytes[msgPtr : msgPtr+16]

					// Store the mapping of string(UUID) to Topic Name
					uuidToName[string(topicUUID)] = topicName

				case 3:
					// PartitionRecord
					partitionID := int32(binary.BigEndian.Uint32(messageBytes[msgPtr : msgPtr+4]))
					msgPtr += 4
					partitionUUID := messageBytes[msgPtr : msgPtr+16]

					// Append the partition ID to this UUID's slice of partitions
					uuidStr := string(partitionUUID)
					uuidToPartitions[uuidStr] = append(uuidToPartitions[uuidStr], partitionID)
				}
			}

			recordOffset = nextRecordOffset
		}
		offset += batchLength + 12
	}

	// Reconstruct the final map
	clusterState := make(map[string]TopicMetadata)
	for uuidStr, name := range uuidToName {
		clusterState[name] = TopicMetadata{
			ErrorCode:  0,
			TopicID:    []byte(uuidStr),
			Partitions: uuidToPartitions[uuidStr],
		}
	}

	return clusterState, nil
}

// ReadVarint decodes a zigzag-encoded variable length integer from the buffer.
// It returns the decoded integer and the number of bytes read.
func ReadVarint(buf []byte) (int64, int) {
	var value uint64
	var shift uint
	var bytesRead int

	for i := 0; i < len(buf); i++ {
		b := buf[i]
		bytesRead++
		value |= uint64(b&0x7F) << shift
		if b&0x80 == 0 {
			break
		}
		shift += 7
	}

	// Zigzag decode
	decoded := (int64(value) >> 1) ^ -(int64(value) & 1)
	return decoded, bytesRead
}
