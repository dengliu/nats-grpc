package main

import (
	"context"
	"io"
	"log"
	"time"

	"github.com/cloudwebrtc/nats-grpc/examples/protos/echo"
	"github.com/cloudwebrtc/nats-grpc/pkg/rpc"
	"github.com/nats-io/nats.go"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func main() {
	// Connect to NATS
	nc, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		log.Fatalf("Failed to connect to NATS: %v", err)
	}
	defer nc.Close()

	log.Println("Connected to NATS")

	// Create gRPC-over-NATS client
	client := rpc.NewClient(nc, "heartbeat-demo", "client-1")
	defer client.Close()

	// Create Echo client
	echoClient := echo.NewEchoClient(client)

	// Start a streaming RPC call
	ctx := context.Background()
	stream, err := echoClient.Echo(ctx)
	if err != nil {
		log.Fatalf("Failed to create stream: %v", err)
	}

	log.Println("Streaming started, sending messages...")
	log.Println("Heartbeat: Client sends Ping every 2 seconds, expects Pong within 5 seconds")
	log.Println("If no Pong received within 5 seconds, client will detect server death")
	log.Println()

	startTime := time.Now()
	messageCount := 0

	// Start goroutine to send messages
	go func() {
		for i := 0; i < 50; i++ {
			if err := stream.Send(&echo.EchoRequest{Msg: "ping"}); err != nil {
				log.Printf("Error sending: %v", err)
				return
			}
			time.Sleep(200 * time.Millisecond)
		}
	}()

	// Receive messages
	for {
		reply, err := stream.Recv()
		
		if err == io.EOF {
			log.Println("Stream ended normally")
			break
		}
		
		if err != nil {
			elapsed := time.Since(startTime)
			log.Printf("\n!!! ERROR DETECTED after %v !!!", elapsed)
			log.Printf("Error: %v", err)
			
			// Check if it's a heartbeat timeout
			if st, ok := status.FromError(err); ok {
				log.Printf("gRPC Status Code: %v (%s)", st.Code(), st.Code().String())
				log.Printf("gRPC Status Message: %s", st.Message())
				
				if st.Code() == codes.Unavailable && messageCount < 20 {
					log.Println("\n✓ SUCCESS: Client detected server death via heartbeat timeout!")
					log.Printf("✓ Detection time: %v (expected 3-6 seconds after last message)", elapsed)
					log.Printf("✓ Messages received before detection: %d", messageCount)
					log.Println("\nThis demonstrates that the heartbeat mechanism successfully detected")
					log.Println("that the server died, even though no explicit End message was sent.")
				}
			} else {
				log.Println("Non-gRPC error:", err)
			}
			break
		}
		
		messageCount++
		elapsedTime := time.Since(startTime).Truncate(time.Millisecond)
		log.Printf("[%v] Received message %d: %s", elapsedTime, messageCount, reply.Msg)
	}

	if messageCount >= 20 {
		log.Println("\nReceived all messages - server completed normally")
	}

	log.Println("\nClient finished")
}
