package broker

import (
	"fmt"

	"github.com/codecrafters-io/kafka-starter-go/app/protocol"
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
	topicName := protocol.ParseDescribeTopicPartitionsRequest(rawBuff)

	resp := protocol.DescribeTopicPartitionsResponse{
		CorrelationID: header.CorrelationID,
		TopicName:     topicName,
	}

	return resp.Encode()
}
