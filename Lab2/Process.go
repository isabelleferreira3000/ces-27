package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"strconv"
	_ "strconv"
	"time"
)

// global variables
var logicalClock int
var myID int
var myPort string

var nPorts int

var ClientsConn []*net.UDPConn
var ServerConn *net.UDPConn

var ch = make(chan string)

type MessageStruct struct {
	Id int
	LogicalClock int
	Text string
}
var messageSent MessageStruct

// auxiliary functions
func max(x int, y int) int {
	if x >= y {
		return x
	} else {
		return y
	}
}

func CheckError(err error) {
	if err != nil {
		fmt.Println("Erro: ", err)
		os.Exit(0)
	}
}

func readInput(ch chan string) {
	reader := bufio.NewReader(os.Stdin)
	for {
		text, _, _ := reader.ReadLine()
		ch <- string(text)
	}
}

func doServerJob() {
	buf := make([]byte, 1024)

	n, _, err := ServerConn.ReadFromUDP(buf)
	CheckError(err)

	var messageReceived MessageStruct
	err = json.Unmarshal(buf[:n], &messageReceived)
	CheckError(err)

	fmt.Println("Received", messageReceived)
	//otherProcessID := messageReceived.Id
	otherProcessLogicalClock := messageReceived.LogicalClock
	//otherProcessText := messageReceived.Text

	// updating clocks
	logicalClock = max(otherProcessLogicalClock, logicalClock) + 1
	fmt.Printf("logicalClock atualizado: %d \n", logicalClock)
}

func doClientJob(messageSent MessageStruct) {
	jsonRequest, err := json.Marshal(messageSent)
	CheckError(err)

	for otherProcessID := 1; otherProcessID <= nPorts; otherProcessID++ {
		if otherProcessID != myID {
			_, err = ClientsConn[otherProcessID - 1].Write(jsonRequest)
			CheckError(err)
		}
	}
	time.Sleep(time.Second * 1)
}

func initConnections() {
	nPorts = len(os.Args) - 2

	// my process
	logicalClock = 0
	auxMyID, err := strconv.Atoi(os.Args[1])
	CheckError(err)
	myID = auxMyID
	myPort = os.Args[myID+1]

	// Server
	ServerAddr, err := net.ResolveUDPAddr("udp", myPort)
	CheckError(err)
	aux, err := net.ListenUDP("udp", ServerAddr)
	ServerConn = aux
	CheckError(err)

	// Clients
	for i := 0; i < nPorts; i++ {
		aPort := os.Args[i+2]

		ServerAddr, err := net.ResolveUDPAddr("udp","127.0.0.1" + aPort)
		CheckError(err)

		LocalAddr, err := net.ResolveUDPAddr("udp", "127.0.0.1:0")
		CheckError(err)

		auxConn, err := net.DialUDP("udp", LocalAddr, ServerAddr)
		ClientsConn = append(ClientsConn, auxConn)
		CheckError(err)
	}
}

func main() {
	initConnections()

	defer ServerConn.Close()
	for i := 0; i < nPorts; i++ {
		defer ClientsConn[i].Close()
	}

	myIDString := strconv.Itoa(myID)

	go readInput(ch)

	go doServerJob()

	for {
		select {
		case textReceived, valid := <-ch:
			if valid {
				// updating my clock
				logicalClock++

				// Clients
				if textReceived == myIDString {
					fmt.Println("logicalClock atualizado:", logicalClock)
				} else {
					messageSent.Id = myID
					messageSent.LogicalClock = logicalClock
					messageSent.Text = textReceived
					fmt.Println("Mensagem enviado:", messageSent)
					go doClientJob(messageSent)
				}

			} else {
				fmt.Println("Channel closed!")
			}
		default:
			time.Sleep(time.Second * 1)
		}
	}
}