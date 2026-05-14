# Interceptor Example

This example demonstrates how to use gRPC interceptors with nats-grpc.

## Overview

Starting with this version, nats-grpc supports client-side interceptors, allowing you to add cross-cutting concerns like:
- Timeouts
- Logging
- Metrics collection
- Authentication
- Retry logic
- Error handling

## Features

- **Timeout Interceptor**: Automatically adds timeouts to all RPC calls
- **Logging Interceptor**: Logs all RPC calls and responses
- **Chain Interceptors**: Combine multiple interceptors together
- **Standard gRPC API**: Uses the standard `grpc.UnaryClientInterceptor` interface

## Usage

### Basic Interceptor

```go
// Create a client with a timeout interceptor
ncli := rpc.NewClientWithOptions(nc, "service-id", "client-id",
    rpc.WithUnaryInterceptor(timeoutInterceptor(5*time.Second)),
)

cli := echo.NewEchoClient(ncli)
reply, err := cli.SayHello(ctx, &echo.HelloRequest{Msg: "hello"})
```

### Chained Interceptors

```go
// Chain multiple interceptors (executed in order)
ncli := rpc.NewClientWithOptions(nc, "service-id", "client-id",
    rpc.WithUnaryInterceptor(chainInterceptors(
        loggingInterceptor,
        timeoutInterceptor(3*time.Second),
        metricsInterceptor,
    )),
)
```

### Custom Interceptor

```go
func myInterceptor(ctx context.Context, method string, req, reply interface{}, 
    cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
    
    // Pre-processing
    fmt.Printf("Calling %s\n", method)
    
    // Call the actual RPC
    err := invoker(ctx, method, req, reply, cc, opts...)
    
    // Post-processing
    if err != nil {
        fmt.Printf("RPC failed: %v\n", err)
    }
    
    return err
}
```

## Running the Example

### Start NATS server
```bash
nats-server
```

### Start the echo server
```bash
go run examples/echo/server/main.go
```

### Run the interceptor client example
```bash
go run examples/interceptor/client/main.go
```

## Expected Output

You should see output showing:

1. **Example 1**: Timeout interceptor adding a 5-second timeout
2. **Example 2**: Logging interceptor logging requests and responses
3. **Example 3**: Chained interceptors (logging + timeout) working together

## Interceptor Patterns

### Timeout Pattern

```go
func timeoutInterceptor(timeout time.Duration) grpc.UnaryClientInterceptor {
    return func(ctx context.Context, method string, req, reply interface{}, 
        cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
        
        ctx, cancel := context.WithTimeout(ctx, timeout)
        defer cancel()
        
        return invoker(ctx, method, req, reply, cc, opts...)
    }
}
```

### Retry Pattern

```go
func retryInterceptor(maxRetries int) grpc.UnaryClientInterceptor {
    return func(ctx context.Context, method string, req, reply interface{}, 
        cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
        
        var err error
        for i := 0; i < maxRetries; i++ {
            err = invoker(ctx, method, req, reply, cc, opts...)
            if err == nil {
                return nil
            }
            time.Sleep(time.Second * time.Duration(i+1))
        }
        return err
    }
}
```

### Metrics Pattern

```go
func metricsInterceptor(ctx context.Context, method string, req, reply interface{}, 
    cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
    
    start := time.Now()
    err := invoker(ctx, method, req, reply, cc, opts...)
    duration := time.Since(start)
    
    // Record metrics
    requestDuration.WithLabelValues(method).Observe(duration.Seconds())
    if err != nil {
        requestErrors.WithLabelValues(method).Inc()
    }
    
    return err
}
```

## Backward Compatibility

The existing `NewClient()` function continues to work without any changes. Interceptors are opt-in via the new `NewClientWithOptions()` function.

```go
// Old way - still works
ncli := rpc.NewClient(nc, "service-id", "client-id")

// New way - with interceptors
ncli := rpc.NewClientWithOptions(nc, "service-id", "client-id",
    rpc.WithUnaryInterceptor(myInterceptor),
)
```

## Integration with Existing Libraries

You can now use standard gRPC interceptor libraries:

```go
import (
    grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
    grpc_retry "github.com/grpc-ecosystem/go-grpc-middleware/retry"
    grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
)

ncli := rpc.NewClientWithOptions(nc, "service-id", "client-id",
    rpc.WithUnaryInterceptor(grpc_middleware.ChainUnaryClient(
        grpc_retry.UnaryClientInterceptor(),
        grpc_prometheus.UnaryClientInterceptor,
    )),
)
```

## Limitations

- Currently only unary and stream interceptors are supported on the client side
- Server-side interceptor support is planned for a future release
- Interceptor chaining must be done manually (see `chainInterceptors` function in the example)

## See Also

- [gRPC Interceptors Documentation](https://github.com/grpc/grpc-go/blob/master/examples/features/interceptor/README.md)
- [grpc-ecosystem/go-grpc-middleware](https://github.com/grpc-ecosystem/go-grpc-middleware)
- [Echo Example](../echo/) - Basic usage without interceptors
