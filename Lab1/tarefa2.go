package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	_ "strconv"
	"time"
)

// Variáveis globais
var myPort string

var nPorts int
var AllConn []*net.UDPConn

var ServConn *net.UDPConn

var ch = make(chan int)

type ClockStruct struct {
	Id int
	Clocks []int
}
var logicalClock ClockStruct

func max(x int, y int) int {
	if x >= y {
		return x
	} else {
		return y
	}
}

func readInput(ch chan int) {
	reader := bufio.NewReader(os.Stdin)
	for {
		text, _, _ := reader.ReadLine()
		aux, err := strconv.Atoi(string(text))
		PrintError(err)
		ch <- aux
	}
}

func CheckError(err error) {
	if err != nil {
		fmt.Println("Erro: ", err)
		os.Exit(0)
	}
}

func PrintError(err error) {
	if err != nil {
		fmt.Println("Erro: ", err)
	}
}

func doServerJob() {
	buf := make([]byte, 1024)

	n, _, err := ServConn.ReadFromUDP(buf)
	PrintError(err)

	var otherLogicalClock ClockStruct
	err = json.Unmarshal(buf[:n], &otherLogicalClock)
	PrintError(err)

	fmt.Println("Received", otherLogicalClock)
	myId := logicalClock.Id
	myClocks := logicalClock.Clocks
	//otherProcessId := otherLogicalClock.Id
	otherProcessClocks := otherLogicalClock.Clocks

	// updating clocks
	logicalClock.Clocks[myId-1]++
	for i := 0; i < nPorts; i++ {
		logicalClock.Clocks[i] = max(otherProcessClocks[i], myClocks[i])
	}
	fmt.Println("logicalClock atualizado:", logicalClock)
}

func doClientJob(otherProcessID int) {
	otherProcess := otherProcessID - 1

	//msg := strconv.Itoa(myLogicalClock)
	//buf := []byte(msg)

	jsonRequest, err := json.Marshal(logicalClock)
	if err != nil {
		log.Print("Marshal Register information failed.")
		log.Fatal(err)
	}

	_, err = AllConn[otherProcess].Write(jsonRequest)
	if err != nil {
		fmt.Println(err)
	}
	time.Sleep(time.Second * 1)

}

func initConnections() {
	nPorts = len(os.Args) - 2

	// getting my Id
	auxMyId, err := strconv.Atoi(os.Args[1])
	PrintError(err)
	myId := auxMyId

	// getting my port
	myPort = os.Args[myId + 1]

	// creating logicalClock
	var clocks []int
	for i := 0; i < nPorts; i++ {
		clocks = append(clocks, 0)
	}
	logicalClock = ClockStruct{
		myId,
		clocks,
	}

	// Server
	ServerAddr, err := net.ResolveUDPAddr("udp", myPort)
	CheckError(err)
	aux, err := net.ListenUDP("udp", ServerAddr)
	ServConn = aux
	CheckError(err)

	// Clients
	for i := 0; i < nPorts; i++ {
		// getting each port
		aPort := os.Args[i+2]

		ServerAddr, err := net.ResolveUDPAddr("udp","127.0.0.1" + aPort)
		CheckError(err)

		LocalAddr, err := net.ResolveUDPAddr("udp", "127.0.0.1:0")
		CheckError(err)

		auxConn, err := net.DialUDP("udp", LocalAddr, ServerAddr)
		AllConn = append(AllConn, auxConn)
		CheckError(err)
	}
}

func main() {
	initConnections()

	defer ServConn.Close()
	for i := 0; i < nPorts; i++ {
		defer AllConn[i].Close()
	}

	go readInput(ch)

	for {
		//Server
		go doServerJob()
		// When there is a request (from stdin). Do it!
		select {
		case processID, valid := <-ch:
			if valid {
				// updating my clock
				myId := logicalClock.Id
				logicalClock.Clocks[myId-1]++
				//Client
				if processID == myId {
					fmt.Printf("logicalClock atualizado: %d \n", logicalClock)
				} else {
					fmt.Printf("logicalClock enviado: %d \n", logicalClock)
					go doClientJob(processID)
				}

			} else {
				fmt.Println("Channel closed!")
			}
		default:
			time.Sleep(time.Second * 1)
		}
	}
}