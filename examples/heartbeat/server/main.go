package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/cloudwebrtc/nats-grpc/examples/protos/echo"
	"github.com/cloudwebrtc/nats-grpc/pkg/rpc"
	"github.com/nats-io/nats.go"
)

type server struct {
	echo.UnimplementedEchoServer
	messageCount int
}

func (s *server) Echo(stream echo.Echo_EchoServer) error {
	log.Println("Client connected, starting to stream data...")
	
	for {
		req, err := stream.Recv()
		if err != nil {
			log.Printf("Error receiving: %v", err)
			return err
		}
		
		s.messageCount++
		msg := fmt.Sprintf("Message %d at %s (received: %s)", s.messageCount, time.Now().Format("15:04:05"), req.Msg)
		log.Printf("Sending: %s", msg)
		
		if err := stream.Send(&echo.EchoReply{Msg: msg}); err != nil {
			log.Printf("Error sending message: %v", err)
			return err
		}
		
		// Send a message every 1 second
		time.Sleep(1 * time.Second)
		
		// Simulate server crash after 10 messages
		if s.messageCount == 10 {
			log.Println("!!! SIMULATING SERVER CRASH !!!")
			log.Println("Server will exit in 1 second...")
			time.Sleep(1 * time.Second)
			os.Exit(1)
		}
	}
}

func main() {
	// Connect to NATS
	nc, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		log.Fatalf("Failed to connect to NATS: %v", err)
	}
	defer nc.Close()

	log.Println("Connected to NATS")

	// Create gRPC-over-NATS server
	srv := rpc.NewServer(nc, "heartbeat-demo")
	defer srv.Stop()

	// Register the Echo service
	echo.RegisterEchoServer(srv, &server{})

	log.Println("Server is ready and listening for requests on service 'heartbeat-demo'")
	log.Println("Server will crash after sending 10 messages to demonstrate heartbeat detection")
	log.Println("Press Ctrl+C to exit, or wait for crash demo")

	// Keep running - server will crash after 10 messages
	select {}
}
