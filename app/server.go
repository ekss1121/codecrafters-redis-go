package main

import (
	"fmt"
	"io"

	// Uncomment this block to pass the first stage
	"net"
	"os"
)

var (
	PING_COMMAND = "ping"
	ECHO_COMMAND = "echo"
)

func handleRequest(conn net.Conn) {
	// Handle the connection
	defer conn.Close()
	buf := make([]byte, 1024)
	for {
		_, err := conn.Read(buf)
		if err == io.EOF {
			fmt.Println("Connection closed")
			return
		}
		if err != nil {
			fmt.Println("Error converting command length to int:", err.Error())
		}
		command := string(buf)
		i := 1
		bukLen := 0
		for ; command[i] != '\r'; i++ {
			bukLen = bukLen*10 + int(command[i]-'0')
		}

		commandBuks := parseRedisCommand(command[i+2:])
		if bukLen == 1 && commandBuks[0] == PING_COMMAND { // Ping command
			handlePingCommand(conn)
		} else if bukLen == 2 && commandBuks[0] == ECHO_COMMAND { // Echo command
			handleEchoCommand(commandBuks[1], conn)
		} else {
			conn.Write([]byte("-ERR unknown command\r\n"))
		}
	}

}

func handlePingCommand(conn net.Conn) {
	conn.Write([]byte("+PONG\r\n"))
}

func handleEchoCommand(message string, conn net.Conn) {
	conn.Write([]byte(fmt.Sprintf("+%s\r\n", message)))
}

func parseRedisCommand(command string) []string {
	if len(command) == 0 {
		return nil
	}
	commands := make([]string, 0)
	if command[0] != '$' {
		return nil
	}

	i := 1
	bukLen := 0
	for ; command[i] != '\r'; i++ {
		bukLen = bukLen*10 + int(command[i]-'0')
	}
	curCommand := command[i+2 : i+2+bukLen]
	commands = append(commands, curCommand)
	restCommand := parseRedisCommand(command[i+2+bukLen+2:])
	return append(commands, restCommand...)
}

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
	defer l.Close()
	// Accept connections forever
	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}
		go handleRequest(conn)
	}

}
