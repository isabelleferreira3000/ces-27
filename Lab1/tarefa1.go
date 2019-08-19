package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strconv"
	_ "strconv"
	"time"
)

// Variáveis globais
var logicalClock int
var myID int
var myPort string

var nPorts int
var AllConn []*net.UDPConn

var ServConn *net.UDPConn

var ch = make(chan int)

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

	aux := string(buf[0:n])
	otherLogicalClock, err := strconv.Atoi(aux)
	PrintError(err)

	fmt.Println("Received", otherLogicalClock)
	logicalClock = max(otherLogicalClock, logicalClock) + 1
	fmt.Printf("logicalClock atualizado: %d \n", logicalClock)
}

func doClientJob(otherProcessID int, logicalClock int) {
	otherProcess := otherProcessID - 1

	msg := strconv.Itoa(logicalClock)
	buf := []byte(msg)

	_,err := AllConn[otherProcess].Write(buf)
	if err != nil {
		fmt.Println(msg, err)
	}
	time.Sleep(time.Second * 1)

}

func initConnections() {
	nPorts = len(os.Args) - 2

	// my process
	logicalClock = 0
	auxMyID, err := strconv.Atoi(os.Args[1])
	PrintError(err)
	myID = auxMyID
	myPort = os.Args[myID+1]

	// Server
	ServerAddr, err := net.ResolveUDPAddr("udp", myPort)
	CheckError(err)
	aux, err := net.ListenUDP("udp", ServerAddr)
	ServConn = aux
	CheckError(err)

	// Clients
	for i := 0; i < nPorts; i++ {
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
				//Client
				if processID == myID {
					logicalClock = logicalClock + 1
					fmt.Printf("logicalClock atualizado: %d \n", logicalClock)
				} else {
					logicalClock = logicalClock + 1
					fmt.Printf("logicalClock enviado: %d \n", logicalClock)
					go doClientJob(processID, logicalClock)
				}

			} else {
				fmt.Println("Channel closed!")
			}
		default:
			time.Sleep(time.Second * 1)
		}
	}
}