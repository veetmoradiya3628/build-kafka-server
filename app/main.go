package main

import (
	"encoding/binary"
	"fmt"
	"net"
	"os"
)

func handleConnection(conn net.Conn) {
	defer conn.Close()

	buff := make([]byte, 1024)
	n, err := conn.Read(buff)
	if err != nil {
		fmt.Println("Error reading from connection: ", err.Error())
		return
	}

	if n < 12 {
		fmt.Println("Received request is too short")
		return
	}

	// Extract the correction_id from the request
	// 0-3 [message size - 4 bytes]
	// 4-5 [request_api_key - 2 bytes]
	// 6-7 [request_api_version - 2 bytes]
	// 8-11 [correlation_id - 4 bytes]
	correlationID := binary.BigEndian.Uint32(buff[8:12])

	// 8 byte response
	response := make([]byte, 8)
	binary.BigEndian.PutUint32(response[0:4], 0)
	binary.BigEndian.PutUint32(response[4:8], correlationID)

	_, err = conn.Write(response)
	if err != nil {
		fmt.Println("Error writing to connection: ", err.Error())
		return
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
