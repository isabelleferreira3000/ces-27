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

//Variáveis globais interessantes para o processo
//var err string
var logicalClock int
var myID int               // id do meu processo
var otherProcessID int     // id do outro processo
var myPort string          //porta do meu servidor
var nPorts int             //qtde de outros processo
var AllConn []*net.UDPConn //vetor com conexões para os servidores
//dos outros processos
var ServConn *net.UDPConn 	//conexão do meu servidor (onde recebo
//mensagens dos outros processos)
var ch = make(chan int)

func readInput(ch chan int) {
	// Non-blocking async routine to listen for terminal input
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
	//Ler (uma vez somente) da conexão UDP a mensagem
	//Escrever na tela a msg recebida (indicando o endereço de quem enviou)

	buf := make([]byte, 1024)

	n,addr,err := ServConn.ReadFromUDP(buf)
	fmt.Println("Received ",string(buf[0:n]), " from ",addr)

	PrintError(err)
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
	auxServConn, err := net.ListenUDP("udp", ServerAddr)
	ServConn = auxServConn
	CheckError(err)

	// Clients
	for i := 0; i < nPorts; i++ {
		aPort := os.Args[i+2]

		ServerAddr,err := net.ResolveUDPAddr("udp","127.0.0.1" + aPort)
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
				fmt.Printf("Recebi do teclado: %d \n", processID)
				fmt.Printf("Meu ID: %d \n", myID)

				//Client
				if processID == myID {
					fmt.Println("Meu ID!")
				} else {
					fmt.Println("Outro ID!")
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