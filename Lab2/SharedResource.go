package main

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	_ "strconv"
)

// global variables
var Connection *net.UDPConn

type MessageStruct struct {
	Id int
	LogicalClock int
	Text string
}
var messageReceived MessageStruct

// auxiliary functions
func CheckError(err error) {
	if err != nil {
		fmt.Println("Erro: ", err)
		os.Exit(0)
	}
}

func doServerJob() {
	buf := make([]byte, 1024)

	n, _, err := Connection.ReadFromUDP(buf)
	CheckError(err)

	err = json.Unmarshal(buf[:n], &messageReceived)
	CheckError(err)

	//fmt.Println("Received", messageReceived)
	fmt.Println(messageReceived.Text)
}

func main() {
	Address, err := net.ResolveUDPAddr("udp", ":10001")
	CheckError(err)
	Connection, err = net.ListenUDP("udp", Address)
	CheckError(err)
	defer Connection.Close()

	for {
		doServerJob()
	}
}