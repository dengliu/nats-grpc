package main

import (
	"context"
	"os"
	"time"

	"github.com/cloudwebrtc/nats-grpc/examples/protos/echo"
	"github.com/cloudwebrtc/nats-grpc/pkg/rpc"
	"github.com/nats-io/nats.go"
	log "github.com/pion/ion-log"
	grpc "google.golang.org/grpc"
)

// timeoutInterceptor adds a timeout to all RPC calls
func timeoutInterceptor(timeout time.Duration) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		// Add timeout to context
		ctx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()

		log.Infof("Timeout interceptor: calling %s with %v timeout", method, timeout)
		start := time.Now()

		// Call the actual RPC
		err := invoker(ctx, method, req, reply, cc, opts...)

		duration := time.Since(start)
		if err != nil {
			log.Errorf("Timeout interceptor: %s failed after %v: %v", method, duration, err)
		} else {
			log.Infof("Timeout interceptor: %s succeeded in %v", method, duration)
		}

		return err
	}
}

// loggingInterceptor logs all RPC calls
func loggingInterceptor(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	log.Infof("Logging interceptor: calling %s with request: %v", method, req)

	err := invoker(ctx, method, req, reply, cc, opts...)

	if err != nil {
		log.Errorf("Logging interceptor: %s returned error: %v", method, err)
	} else {
		log.Infof("Logging interceptor: %s returned reply: %v", method, reply)
	}

	return err
}

// chainInterceptors chains multiple interceptors together
func chainInterceptors(interceptors ...grpc.UnaryClientInterceptor) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		// Build a chain by wrapping each interceptor
		chain := invoker
		for i := len(interceptors) - 1; i >= 0; i-- {
			interceptor := interceptors[i]
			next := chain
			chain = func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, opts ...grpc.CallOption) error {
				return interceptor(ctx, method, req, reply, cc, next, opts...)
			}
		}
		return chain(ctx, method, req, reply, cc, opts...)
	}
}

func main() {
	var natsURL = nats.DefaultURL
	if len(os.Args) == 2 {
		natsURL = os.Args[1]
	}

	opts := []nats.Option{nats.Name("nats-grpc interceptor example client")}
	opts = rpc.SetupConnOptions(opts)

	// Connect to the NATS server.
	nc, err := nats.Connect(natsURL, opts...)
	if err != nil {
		log.Errorf("%v", err)
		return
	}
	defer nc.Close()

	// Example 1: Client with timeout interceptor (5 second timeout)
	log.Infof("=== Example 1: Timeout Interceptor ===")
	ncli1 := rpc.NewClientWithOptions(nc, "someid", "client1",
		rpc.WithUnaryInterceptor(timeoutInterceptor(5*time.Second)),
	)
	defer ncli1.Close()

	cli1 := echo.NewEchoClient(ncli1)
	reply1, err := cli1.SayHello(context.Background(), &echo.HelloRequest{Msg: "hello with timeout"})
	if err != nil {
		log.Infof("Example 1 error: %v\n", err)
	} else {
		log.Infof("Example 1 reply: %s\n", reply1.GetMsg())
	}

	time.Sleep(1 * time.Second)

	// Example 2: Client with logging interceptor
	log.Infof("\n=== Example 2: Logging Interceptor ===")
	ncli2 := rpc.NewClientWithOptions(nc, "someid", "client2",
		rpc.WithUnaryInterceptor(loggingInterceptor),
	)
	defer ncli2.Close()

	cli2 := echo.NewEchoClient(ncli2)
	reply2, err := cli2.SayHello(context.Background(), &echo.HelloRequest{Msg: "hello with logging"})
	if err != nil {
		log.Infof("Example 2 error: %v\n", err)
	} else {
		log.Infof("Example 2 reply: %s\n", reply2.GetMsg())
	}

	time.Sleep(1 * time.Second)

	// Example 3: Client with chained interceptors (logging + timeout)
	log.Infof("\n=== Example 3: Chained Interceptors ===")
	ncli3 := rpc.NewClientWithOptions(nc, "someid", "client3",
		rpc.WithUnaryInterceptor(chainInterceptors(
			loggingInterceptor,
			timeoutInterceptor(3*time.Second),
		)),
	)
	defer ncli3.Close()

	cli3 := echo.NewEchoClient(ncli3)
	reply3, err := cli3.SayHello(context.Background(), &echo.HelloRequest{Msg: "hello with chain"})
	if err != nil {
		log.Infof("Example 3 error: %v\n", err)
	} else {
		log.Infof("Example 3 reply: %s\n", reply3.GetMsg())
	}

	log.Infof("\n=== All examples completed ===")
}
