package main

import (
	"fmt"
	"io"

	// Uncomment this block to pass the first stage
	"net"
	"os"
)

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Redis-go server started listening on port 6379!")

	// Uncomment this block to pass the first stage
	//
	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}

	// Accept connections forever
	conn, err := l.Accept()
	defer conn.Close()
	for {
		buf := make([]byte, 1024)
		_, err := conn.Read(buf)
		if err == io.EOF {
			break
		} else if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}
		fmt.Println("Connection received from: ", conn.RemoteAddr().String())
		// write a response using conn
		conn.Write([]byte("+PONG\r\n"))
	}

}
