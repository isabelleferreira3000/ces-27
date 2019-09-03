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
var myState string

var nReplies int

var nPorts int

var ClientsConn []*net.UDPConn
var SharedResourceConn *net.UDPConn
var ServerConn *net.UDPConn

var ch = make(chan string)

type RequestReplyStruct struct {
	Type string
	Id int
	LogicalClock int
}
var request RequestReplyStruct
var reply RequestReplyStruct

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

func setState(newState string) {
	myState = newState
	fmt.Println("Estado:", myState)
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

	var messageReceived RequestReplyStruct
	err = json.Unmarshal(buf[:n], &messageReceived)
	CheckError(err)

	fmt.Println("Received", messageReceived)
	messageType := messageReceived.Type
	messageLogicalClock := messageReceived.LogicalClock
	messageId := messageReceived.Id

	// updating clocks
	logicalClock = max(messageLogicalClock, logicalClock) + 1
	fmt.Printf("logicalClock atualizado: %d \n", logicalClock)

	if messageType == "request" {
		if myState == "HELD" ||
			( myState == "WANTED" && ( messageLogicalClock < logicalClock ||
				( messageLogicalClock == logicalClock && messageId < myID ))) {

		} else {
			reply.Id = myID
			reply.LogicalClock = logicalClock
			reply.Type = "reply"

			jsonReply, err := json.Marshal(reply)
			CheckError(err)
			_, err = ClientsConn[messageId-1].Write(jsonReply)
			CheckError(err)
		}

	} else if messageType == "reply" {
		nReplies++
	} else {
		fmt.Println("Error in message type received: neither request nor reply!")
	}
}

func doClientJob(request RequestReplyStruct) {
	jsonRequest, err := json.Marshal(request)
	CheckError(err)

	for otherProcessID := 1; otherProcessID <= nPorts; otherProcessID++ {
		if otherProcessID != myID {
			_, err = ClientsConn[otherProcessID - 1].Write(jsonRequest)
			CheckError(err)
		}
	}

	fmt.Println("Esperando N-1 respostas")
	for nReplies != nPorts-1 {}
	nReplies = 0
	setState("HELD")

	messageSent.LogicalClock = logicalClock
	messageSent.Id = myID

	jsonMessage, err := json.Marshal(messageSent)
	CheckError(err)
	_, err = SharedResourceConn.Write(jsonMessage)
	CheckError(err)

	time.Sleep(time.Second * 1)

	setState("RELEASED")
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

	ServerAddr, err = net.ResolveUDPAddr("udp","127.0.0.1" + ":10001")
	CheckError(err)

	LocalAddr, err := net.ResolveUDPAddr("udp", "127.0.0.1:0")
	CheckError(err)

	SharedResourceConn, err = net.DialUDP("udp", LocalAddr, ServerAddr)
	CheckError(err)

}

func main() {
	initConnections()
	setState("RELEASED")
	nReplies = 0

	defer ServerConn.Close()
	for i := 0; i < nPorts; i++ {
		defer ClientsConn[i].Close()
	}

	myIDString := strconv.Itoa(myID)

	go readInput(ch)

	for {
		go doServerJob()

		select {
		case textReceived, valid := <-ch:
			if valid {
				// updating my clock
				logicalClock++

				// Clients
				if textReceived == myIDString {
					fmt.Println("logicalClock atualizado:", logicalClock)
				} else {
					messageSent.Text = textReceived

					// updating my clock
					logicalClock++
					fmt.Println("logicalClock atualizado:", logicalClock)

					setState("WANTED")
					fmt.Println("Multicast request to all processes")

					request.Id = myID
					request.LogicalClock = logicalClock
					request.Type = "request"
					fmt.Println("Request enviado:", request)

					go doClientJob(request)
				}

			} else {
				fmt.Println("Channel closed!")
			}
		default:
			time.Sleep(time.Second * 1)
		}
	}
}