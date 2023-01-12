package main

import (
	"fmt"
	"io"
	"strconv"
	"sync"
	"time"

	// Uncomment this block to pass the first stage
	"net"
	"os"
)

var (
	PING_COMMAND = "ping"
	ECHO_COMMAND = "echo"
	SET_COMMAND  = "set"
	GET_COMMAND  = "get"
)

type data struct {
	Value    string
	Px       time.Duration
	ExpireAt time.Time
}

type cache struct {
	Table map[string]*data
	Mu    sync.RWMutex
}

var localCache = &cache{
	Table: make(map[string]*data),
}

func handleRequest(conn net.Conn) {
	// Handle the connection
	defer conn.Close()
	buf := make([]byte, 1024)
	for {
		_, err := conn.Read(buf)
		if err == io.EOF {
			// fmt.Println("Connection closed")
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
		// fmt.Println(commandBuks)
		if bukLen == 1 && commandBuks[0] == PING_COMMAND { // Ping command
			handlePingCommand(conn)
		} else if bukLen == 2 && commandBuks[0] == ECHO_COMMAND { // Echo command
			handleEchoCommand(commandBuks[1], conn)
		} else if bukLen == 3 && commandBuks[0] == SET_COMMAND { // Set command
			handleSetCommand(commandBuks[1], commandBuks[2], -1, conn)
		} else if bukLen == 5 && commandBuks[0] == SET_COMMAND {
			exp, err := strconv.Atoi(commandBuks[4])
			if err != nil || commandBuks[3] != "px" {
				conn.Write([]byte(fmt.Sprintf("-ERR syntax error\r\n")))
				return
			}

			handleSetCommand(commandBuks[1], commandBuks[2], exp, conn)
		} else if bukLen == 2 && commandBuks[0] == GET_COMMAND { // Get command
			handleGetCommand(commandBuks[1], conn)
		} else {
			conn.Write([]byte("-ERR unknown command\r\n"))
		}
	}

}

func handleSetCommand(key string, value string, px int, conn net.Conn) {
	localCache.Mu.Lock()
	if _, found := localCache.Table[key]; found {
		fmt.Println("key already exists, update it")
	}
	newV := &data{
		Value:    value,
		Px:       time.Duration(px),
		ExpireAt: setNewExpireAt(time.Duration(px)),
	}

	localCache.Table[key] = newV
	localCache.Mu.Unlock()
	conn.Write([]byte("+OK\r\n"))
}

func handleGetCommand(key string, conn net.Conn) {
	localCache.Mu.RLock()
	if v, found := localCache.Table[key]; !found {
		localCache.Mu.RUnlock()
		conn.Write([]byte("$-1\r\n"))
		return
	} else {
		if v.ExpireAt.Before(time.Now()) {
			localCache.Mu.RUnlock()
			conn.Write([]byte("$-1\r\n"))
			return
		}
	}
	newV := &data{
		Value:    localCache.Table[key].Value,
		Px:       localCache.Table[key].Px,
		ExpireAt: setNewExpireAt(localCache.Table[key].Px),
	}

	localCache.Table[key] = newV
	localCache.Mu.RUnlock()
	conn.Write([]byte(fmt.Sprintf("+%s\r\n", newV.Value)))
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

func setNewExpireAt(px time.Duration) time.Time {
	newExp := time.Now().Add(px * time.Millisecond)
	if px == -1 {
		return time.Now().Add(time.Hour * 24 * 365 * 100)
	}
	return newExp
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
