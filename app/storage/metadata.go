package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
)

type TopicMetadata struct {
	ErrorCode      int16
	TopicID        []byte // 16 bytes UUID
	PartitionIndex int32
	// You can add LeaderID, ReplicaNodes, etc., here later
}

// LookupTopic reads the __cluster_metadata log file to find a specific topic
func LookupTopic(targetTopic string) (TopicMetadata, bool) {
	fileData, err := os.ReadFile("/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log")
	if err != nil {
		fmt.Println("Error reading metadata log:", err)
		return TopicMetadata{}, false
	}

	offset := 0
	var foundUUID []byte
	var foundPartition int32
	topicFound := false
	partitionFound := false

	for offset < len(fileData) {
		batchLength := int(binary.BigEndian.Uint32(fileData[offset+8 : offset+12]))
		batchData := fileData[offset : offset+batchLength+12]

		// The number of records is a 4-byte int at offset 57
		recordsCount := int(binary.BigEndian.Uint32(batchData[57:61]))

		// Records start immediately after the count (offset 61)
		recordOffset := 61

		for idx := 0; idx < recordsCount; idx++ {

			// Read Record Length (Varint) to know how far to jump for the NEXT record
			recordLen, bytesRead := ReadVarint(batchData[recordOffset:])
			currentRecordStart := recordOffset + bytesRead
			nextRecordOffset := currentRecordStart + int(recordLen)

			// We need to skip Attributes, TimestampDelta, OffsetDelta, and Key to get to the Value.
			// Because they are all varints, we must read them sequentially.
			ptr := currentRecordStart

			_, n := ReadVarint(batchData[ptr:])
			ptr += n // Skip Attributes
			_, n = ReadVarint(batchData[ptr:])
			ptr += n // Skip TimestampDelta
			_, n = ReadVarint(batchData[ptr:])
			ptr += n // Skip OffsetDelta

			keyLen, n := ReadVarint(batchData[ptr:])
			ptr += n // Read KeyLength
			if keyLen > 0 {
				ptr += int(keyLen) // Skip Key bytes if they exist
			}

			valueLen, n := ReadVarint(batchData[ptr:])
			ptr += n // Read ValueLength

			if valueLen > 0 {
				// We found the Metadata Message!
				messageBytes := batchData[ptr : ptr+int(valueLen)]

				// Frame Version is byte 0
				messageType := messageBytes[1] // Message Type is byte 1

				// Message Version is byte 2
				msgPtr := 3

				switch messageType {
				case 2: // TopicRecord
					// Compact String length: length + 1
					nameLen := int(messageBytes[msgPtr]) - 1
					msgPtr++

					parsedTopicName := string(messageBytes[msgPtr : msgPtr+nameLen])
					msgPtr += nameLen

					topicUUID := messageBytes[msgPtr : msgPtr+16]

					if parsedTopicName == targetTopic {
						foundUUID = append([]byte(nil), topicUUID...) // Make a copy of the slice
						topicFound = true
					}
				case 3: // PartitionRecord
					partitionPartitionID := int32(binary.BigEndian.Uint32(messageBytes[msgPtr : msgPtr+4]))
					msgPtr += 4
					partitionUUID := messageBytes[msgPtr : msgPtr+16]

					// If this partition belongs to the UUID we already found
					if topicFound && bytes.Equal(partitionUUID, foundUUID) {
						foundPartition = partitionPartitionID
						partitionFound = true
					}
				}
			}

			// Advance to the next record in the batch
			recordOffset = nextRecordOffset
		}

		offset += batchLength + 12
	}

	if topicFound && partitionFound {
		return TopicMetadata{
			TopicID:        foundUUID,
			PartitionIndex: foundPartition,
		}, true
	}

	return TopicMetadata{}, false
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
