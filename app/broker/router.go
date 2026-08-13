package broker

import (
	"bytes"
	"fmt"
	"os"
	"sort"

	"github.com/codecrafters-io/kafka-starter-go/app/protocol"
	"github.com/codecrafters-io/kafka-starter-go/app/storage"
)

// RouteRequest is the entry point for all incoming Kafka requests
func RouteRequest(buff []byte, n int) ([]byte, error) {
	header, err := protocol.ParseHeader(buff[:n])
	if err != nil {
		return nil, fmt.Errorf("error parsing header: %v", err)
	}

	fmt.Printf("Request received - API Key: %d, Version: %d, CorrelationID: %d\n",
		header.RequestAPIKey, header.RequestAPIVer, header.CorrelationID)

	var responseBytes []byte

	switch header.RequestAPIKey {
	case 1: // Fetch API
		responseBytes = handleFetch(header, buff[:n])
	case 18: // ApiVersions
		responseBytes = handleApiVersions(header)
	case 75: // DescribeTopicPartitions
		responseBytes = handleDescribeTopicPartitions(header, buff[:n])
	default:
		fmt.Printf("Unsupported API Key: %d\n", header.RequestAPIKey)
		// Depending on Kafka protocol, you might want to return a specific error response here
		return nil, fmt.Errorf("unsupported API key")
	}

	// Dynamically prepend the message size to the final response
	return protocol.WrapWithMessageSize(responseBytes), nil
}

// Add the handler for Fetch requests
func handleFetch(header protocol.RequestHeader, rawBuff []byte) []byte {
	// Extract topic IDs directly from the request buffer
	topicIDs := protocol.ParseFetchRequest(rawBuff)

	clusterState, err := storage.LoadAllTopics()
	if err != nil {
		fmt.Println("Failed to load metadata:", err)
	}

	resp := protocol.FetchResponse{
		CorrelationID: header.CorrelationID,
	}

	// For now, any topic we encounter is treated as unknown
	for _, topicID := range topicIDs {

		var errorCode int16 = 100 // Default to UNKNOWN_TOPIC_ID
		var topicName string
		var records []byte

		// Iterate over metadata to check if the requested topic ID exists
		for name, metadata := range clusterState {
			// Compare the 16-byte UUIDs
			if bytes.Equal(metadata.TopicID, topicID) {
				errorCode = 0 // Topic found, No Error
				topicName = name
				break
			}
		}

		// If the topic was found, attempt to read its partition log
		if errorCode == 0 {
			// We assume partition 0 for this stage. Adjust dynamically for multiple partitions later.
			logPath := fmt.Sprintf("/tmp/kraft-combined-logs/%s-0/00000000000000000000.log", topicName)
			if fileData, err := os.ReadFile(logPath); err == nil {
				records = fileData // Populate the struct slice with the log file bytes
			}
		}

		topicResp := protocol.FetchTopicResponse{
			TopicID: topicID,
			Partitions: []protocol.FetchPartitionResponse{
				{
					PartitionIndex: 0,
					ErrorCode:      errorCode, // UNKNOWN_TOPIC_ID Error Code
					Records:        records
				},
			},
		}

		resp.Responses = append(resp.Responses, topicResp)
	}

	return resp.Encode()
}

func handleApiVersions(header protocol.RequestHeader) []byte {
	var errorCode int16 = 0

	// Validate version support
	if header.RequestAPIVer < 0 || header.RequestAPIVer > 4 {
		errorCode = 35 // UNSUPPORTED_VERSION error code
	}

	resp := protocol.ApiVersionsResponse{
		CorrelationID: header.CorrelationID,
		ErrorCode:     errorCode,
	}

	return resp.Encode()
}

func handleDescribeTopicPartitions(header protocol.RequestHeader, rawBuff []byte) []byte {
	// Get the array of requested topics
	topicNames := protocol.ParseDescribeTopicPartitionsRequest(rawBuff)

	// Sort the topics alphabetically to satisfy the test constraints
	sort.Strings(topicNames)

	clusterState, err := storage.LoadAllTopics()
	if err != nil {
		fmt.Println("Failed to load metadata:", err)
	}

	resp := protocol.DescribeTopicPartitionsResponse{
		CorrelationID: header.CorrelationID,
	}

	// Process each sorted requested topic and map it to a response struct
	for _, topicName := range topicNames {
		topicResp := protocol.TopicResponse{
			TopicName: topicName,
		}

		// Check if the requested topic exists in our parsed map
		if metadata, exists := clusterState[topicName]; exists {
			topicResp.ErrorCode = 0
			topicResp.TopicID = metadata.TopicID
			topicResp.Partitions = metadata.Partitions
		} else {
			topicResp.ErrorCode = 3
			topicResp.TopicID = make([]byte, 16) // all zeros
			topicResp.Partitions = nil
		}

		// Append the formatted topic to the response payload
		resp.Topics = append(resp.Topics, topicResp)
	}

	return resp.Encode()
}
