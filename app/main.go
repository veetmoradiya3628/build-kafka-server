package main

import (
	"fmt"
	"net"
	"os"

	"github.com/codecrafters-io/kafka-starter-go/app/broker"
)

func handleConnection(conn net.Conn) {
	defer conn.Close()

	for {
		buff := make([]byte, 1024)
		n, err := conn.Read(buff)
		if err != nil {
			fmt.Println("Error reading from connection:", err.Error())
			return
		}

		// Pass the raw bytes to the broker for routing and execution
		responsePayload, err := broker.RouteRequest(buff, n)
		if err != nil {
			fmt.Println("Error handling request:", err.Error())
			continue
		}

		// Send response back over TCP
		if responsePayload != nil {
			_, err = conn.Write(responsePayload)
			if err != nil {
				fmt.Println("Error writing to connection:", err.Error())
			}
		}
	}
}

func main() {
	fmt.Println("Logs from your program will appear here!")

	l, err := net.Listen("tcp", "0.0.0.0:9092")
	if err != nil {
		fmt.Println("Failed to bind to port 9092")
		os.Exit(1)
	}
	defer l.Close()

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection:", err.Error())
			os.Exit(1) // Or continue, depending on your preferred failure mode
		}

		go handleConnection(conn)
	}
}
