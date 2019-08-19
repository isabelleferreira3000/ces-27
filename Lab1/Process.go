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
var myPort string //porta do meu servidor
var nServers int //qtde de outros processo
var CliConn []*net.UDPConn 	//vetor com conexões para os servidores
							//dos outros processos
var ServConn *net.UDPConn 	//conexão do meu servidor (onde recebo
							//mensagens dos outros processos)
var ch = make(chan string)

func readInput(ch chan string) {
	// Non-blocking async routine to listen for terminal input
	reader := bufio.NewReader(os.Stdin)
	for {
		text, _, _ := reader.ReadLine()
		ch <- string(text)
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

func doClientJob(otherProcess int, i int) {
	//Enviar uma mensagem (com valor i) para o servidor do processo
	//otherServer
	msg := strconv.Itoa(i)
	buf := []byte(msg)

	_,err := CliConn[otherProcess].Write(buf)
	if err != nil {
		fmt.Println(msg, err)
	}
	time.Sleep(time.Second * 1)

}
func initConnections() {
	myPort = os.Args[1]
	nServers = len(os.Args) - 2		//Esse 2 tira o nome (no caso Process) e tira a primeira porta
	  								//(que é a minha). As demais portas são dos outros processos

	// Server
	ServerAddr, err := net.ResolveUDPAddr("udp", myPort)
	CheckError(err)
	auxCliConn, err := net.ListenUDP("udp", ServerAddr)
	ServConn = auxCliConn
	CheckError(err)

	// Clients
	for i := 0; i < nServers; i++ {
		//Outros códigos para deixar ok a conexão do meu servidor (onde recebo msgs).
		// O processo já deve ficar habilitado a receber msgs.
		cliPort := os.Args[i+2]
		ServerAddr,err := net.ResolveUDPAddr("udp","127.0.0.1" + cliPort)
		CheckError(err)

		LocalAddr, err := net.ResolveUDPAddr("udp", "127.0.0.1:0")
		CheckError(err)

		//Outros códigos para deixar ok as conexões com os servidores dos outros processos.
		// Colocar tais conexões no vetor CliConn.
		auxCliConn, err := net.DialUDP("udp", LocalAddr, ServerAddr)
		CliConn = append(CliConn, auxCliConn)
		CheckError(err)
	}
}

func main() {
	initConnections()

	// O fechamento de conexões deve ficar aqui, assim só fecha
	// conexão quando a main morrer
	defer ServConn.Close()
	for i := 0; i < nServers; i++ {
		defer CliConn[i].Close()
	}

	go readInput(ch)

	// Todos Process fará a mesma coisa: ouvir msg e mandar infinitos i’s para os outros processos
	for {
		//Server
		go doServerJob()
		// When there is a request (from stdin). Do it!
		select {
		case x, valid := <-ch:
			if valid {
				fmt.Printf("Recebi do teclado: %s \n", x)
				//Client
				for j := 0; j < nServers; j++ {
					go doClientJob(j, 100)
				}
			} else {
				fmt.Println("Channel closed!")
			}
		default:
			// Do nothing in the non-blocking approach.
			time.Sleep(time.Second * 1)
		}
	}
}