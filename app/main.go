package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"net"
	"os"
)

type Request struct {
	MessageSize   uint32
	RequestAPIKey uint16
	RequestAPIVer uint16
	CorrelationID uint32
}

// Extract the correction_id from the request
// 0-3 [message size - 4 bytes]
// 4-5 [request_api_key - 2 bytes]
// 6-7 [request_api_version - 2 bytes]
// 8-11 [correlation_id - 4 bytes]
func parseRequest(buff []byte) (Request, error) {
	if len(buff) < 12 {
		return Request{}, fmt.Errorf("buffer too short to parse request")
	}
	return Request{
		MessageSize:   binary.BigEndian.Uint32(buff[0:4]),
		RequestAPIKey: binary.BigEndian.Uint16(buff[4:6]),
		RequestAPIVer: binary.BigEndian.Uint16(buff[6:8]),
		CorrelationID: binary.BigEndian.Uint32(buff[8:12]),
	}, nil
}

func handleConnection(conn net.Conn) {
	defer conn.Close()

	buff := make([]byte, 1024)
	n, err := conn.Read(buff)
	if err != nil {
		fmt.Println("Error reading from connection: ", err.Error())
		return
	}

	req, err := parseRequest(buff[:n])
	if err != nil {
		fmt.Println("Error parsing request: ", err.Error())
		return
	}

	fmt.Printf("Request received - API Key: %d, Version: %d, CorrelationID: %d\n", req.RequestAPIKey, req.RequestAPIVer, req.CorrelationID)

	apiVersion := binary.BigEndian.Uint16(buff[6:8])
	fmt.Println("request received with API Version: ", apiVersion)

	var errorCode int16 = 0
	// If it's an ApiVersions request (18) and the version is not between 0 and 4
	if req.RequestAPIKey == 18 && (req.RequestAPIVer < 0 || req.RequestAPIVer > 4) {
		errorCode = 35 // UNSUPPORTED_VERSION error code
	}

	// response
	var body bytes.Buffer

	// header
	correlationID := binary.BigEndian.Uint32(buff[8:12])
	binary.Write(&body, binary.BigEndian, correlationID)

	// body
	// error code 0
	binary.Write(&body, binary.BigEndian, errorCode)
	// api_keys array length (Compact Array format: Number of elements + 1)
	binary.Write(&body, binary.BigEndian, int8(2))

	// Array Element 1: API Key 18 (ApiVersions)
	binary.Write(&body, binary.BigEndian, int16(18)) // api_key
	binary.Write(&body, binary.BigEndian, int16(0))  // min_version
	binary.Write(&body, binary.BigEndian, int16(4))  // max_version
	binary.Write(&body, binary.BigEndian, int8(0))   // TAG_BUFFER for element

	// throttle_time_ms
	binary.Write(&body, binary.BigEndian, int32(0))

	// TAG_BUFFER for response body
	binary.Write(&body, binary.BigEndian, int8(0))

	// Prepend the message_size dynamically
	// The message_size is the length of the Header + Body (excluding the 4 size bytes)
	responseBytes := body.Bytes()
	messageSize := uint32(len(responseBytes))

	var finalResponse bytes.Buffer
	binary.Write(&finalResponse, binary.BigEndian, messageSize) // Write 4-byte size
	finalResponse.Write(responseBytes)                          // Write the rest of the payload

	// Send response
	_, err = conn.Write(finalResponse.Bytes())
	if err != nil {
		fmt.Println("Error writing to connection:", err.Error())
	}
}

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	l, err := net.Listen("tcp", "0.0.0.0:9092")
	if err != nil {
		fmt.Println("Failed to bind to port 9092")
		os.Exit(1)
	}

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}

		go handleConnection(conn)
	}
}
