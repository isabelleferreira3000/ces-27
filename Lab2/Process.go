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
var myTimestamp int
var myID int
var myIDString string
var myPort string
var myState string

var nReplies int

var nPorts int

var requestsQueue []int
var ClientsConn []*net.UDPConn
var SharedResourceConn *net.UDPConn
var ServerConn *net.UDPConn

var ch = make(chan string)

type RequestReplyStruct struct {
	Type string
	Id int
	Timestamp int
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

func useCS(){
	fmt.Println("Entrei na CS")
	messageSent.LogicalClock = logicalClock

	jsonMessage, err := json.Marshal(messageSent)
	CheckError(err)
	_, err = SharedResourceConn.Write(jsonMessage)
	CheckError(err)

	time.Sleep(time.Second * 10)
	fmt.Println("Sai da CS")
}

func readInput(ch chan string) {
	reader := bufio.NewReader(os.Stdin)
	for {
		text, _, _ := reader.ReadLine()
		ch <- string(text)
	}
}

func waitReplies() {
	fmt.Println("Esperando N-1 respostas")
	for nReplies != nPorts-1 {}
	nReplies = 0

	setState("HELD")
	useCS()
	setState("RELEASED")

	// reply requests
	for _, element := range requestsQueue {
		logicalClock++
		reply.LogicalClock = logicalClock

		fmt.Println("Reply enviado:", reply.LogicalClock, "reply")
		jsonReply, err := json.Marshal(reply)
		CheckError(err)
		_, err = ClientsConn[element-1].Write(jsonReply)
		CheckError(err)
	}
	requestsQueue = make([]int, 0)
}

func doServerJob() {
	buf := make([]byte, 1024)

	n, _, err := ServerConn.ReadFromUDP(buf)
	CheckError(err)

	var messageReceived RequestReplyStruct
	err = json.Unmarshal(buf[:n], &messageReceived)
	CheckError(err)

	messageType := messageReceived.Type
	messageLogicalClock := messageReceived.LogicalClock
	messageTimestamp := messageReceived.Timestamp

	// updating clocks
	logicalClock = max(messageLogicalClock, logicalClock) + 1

	if messageType == "request" {
		fmt.Println("Request recebido:",
			messageReceived.LogicalClock, ", <", messageReceived.Timestamp, ",", messageReceived.Id, ">")
		fmt.Println("logicalClock atualizado:", logicalClock)
		messageId := messageReceived.Id

		if myState == "HELD" ||
			( myState == "WANTED" && ( messageTimestamp < myTimestamp ||
				( messageTimestamp == myTimestamp && messageId < myID ))) {
			requestsQueue = append(requestsQueue, messageId)

		} else {
			// updating clocks
			logicalClock++
			reply.LogicalClock = logicalClock

			jsonReply, err := json.Marshal(reply)
			CheckError(err)
			_, err = ClientsConn[messageId-1].Write(jsonReply)
			CheckError(err)
			fmt.Println("Reply enviado:", reply.LogicalClock, "reply")
			fmt.Println("logicalClock atualizado:", logicalClock)
		}

	} else if messageType == "reply" {
		fmt.Println("Reply recebido:", messageReceived.LogicalClock, "reply")
		fmt.Println("logicalClock atualizado:", logicalClock)
		nReplies++
	}
}

func doClientJob(request RequestReplyStruct, otherProcessID int) {
	// updating my clock
	logicalClock++
	fmt.Println("logicalClock atualizado:", logicalClock)

	request.LogicalClock = logicalClock

	fmt.Println("Request enviado:", request.LogicalClock, ", <", request.Timestamp, ",", request.Id, ">")
	jsonRequest, err := json.Marshal(request)
	CheckError(err)

	_, err = ClientsConn[otherProcessID - 1].Write(jsonRequest)
	CheckError(err)
}

func initConnections() {
	nPorts = len(os.Args) - 2

	// my process
	nReplies = 0
	auxMyID, err := strconv.Atoi(os.Args[1])
	CheckError(err)
	myID = auxMyID
	myIDString = strconv.Itoa(myID)
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

	// set initial values
	logicalClock = 0
	messageSent.Id = myID
	request.Id = myID
	request.Type = "request"
	reply.Id = myID
	reply.Type = "reply"

	setState("RELEASED")

	defer ServerConn.Close()
	for i := 0; i < nPorts; i++ {
		defer ClientsConn[i].Close()
	}

	go readInput(ch)

	for {
		go doServerJob()

		select {
		case textReceived, valid := <-ch:
			if valid {
				if myState == "WANTED" || myState == "HELD" {
					fmt.Println(textReceived, "invalido")
				} else {

					if textReceived != myIDString {
						messageSent.Text = textReceived

						setState("WANTED")
						myTimestamp = logicalClock
						request.Timestamp = myTimestamp

						// multicast requests
						fmt.Println("Multicast request to all processes")
						for otherProcessID := 1; otherProcessID <= nPorts; otherProcessID++ {
							if otherProcessID != myID {
								go doClientJob(request, otherProcessID)
							}
						}
						go waitReplies()
					} else {
						// updating my clock
						logicalClock++
						fmt.Println("logicalClock atualizado:", logicalClock)
					}
				}

			} else {
				fmt.Println("Channel closed!")
			}
		default:
			time.Sleep(time.Second * 1)
		}
	}
}