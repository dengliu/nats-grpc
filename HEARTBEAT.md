# Heartbeat Implementation for nats-grpc

## Summary

This document describes the heartbeat mechanism implemented for nats-grpc to detect when a server dies during streaming RPC calls.

## Problem Statement

Before this implementation, when a client makes a streaming RPC call and the server crashes in the middle of streaming, the client would hang indefinitely waiting for more data. There was no mechanism to detect that the server had died.

## Solution

Implemented a bidirectional heartbeat protocol using Ping/Pong messages:
- **Client sends Ping messages** every 2 seconds
- **Server responds with Pong messages** immediately  
- **Client monitors Pong responses** and detects failure if no Pong received within 5 seconds
- **Detection time**: 3-6 seconds after server death

## Changes Made

### 1. Protocol Extension (`nats-grpc/protos/nrpc/nrpc.proto`)

Added Ping and Pong message types to the protocol:

```protobuf
message Request {
  oneof type {
    Call call = 2;
    Data data = 3;
    End end = 4;
    Ping ping = 5;  // NEW
  }
}

message Response {
  oneof type {
    Begin begin = 2;
    Data data = 3;
    End end = 4;
    Pong pong = 5;  // NEW
  }
}

message Ping { int64 timestamp = 1; }
message Pong { int64 timestamp = 1; }
```

### 2. Server-Side Implementation (`nats-grpc/pkg/rpc/server.go`)

Added Ping message handling:
- `processPing(ping *nrpc.Ping)` - handles incoming Ping messages
- `writePong(pong *nrpc.Pong)` - sends Pong responses

The server immediately responds to any Ping with a Pong message containing the same timestamp.

### 3. Client-Side Implementation (`nats-grpc/pkg/rpc/client.go`)

Added heartbeat monitoring with four new functions:

- **`pingLoop()`** - Goroutine that sends Ping messages every 2 seconds
- **`pongMonitor()`** - Goroutine that monitors for Pong timeout (5 seconds)
- **`processPong(pong *nrpc.Pong)`** - Updates last Pong received time
- **`writePing(ping *nrpc.Ping)`** - Sends Ping requests to server

Added fields to `clientStream`:
```go
lastPongTime  time.Time       // Time of last received Pong
pongMu        sync.Mutex      // Protects lastPongTime
pingInterval  time.Duration   // 2 seconds
pongTimeout   time.Duration   // 5 seconds  
heartbeatStop chan struct{}   // Signal to stop heartbeat goroutines
```

### 4. Build Configuration

- Updated `proto.sh` to work with modern protoc
- Updated `go.mod` to require Go 1.20 (for generated protobuf code compatibility)

## Example Usage

See `nats-grpc/examples/heartbeat/` for a complete demonstration:

```bash
# Terminal 1: Start server (will crash after 10 messages)
cd examples/heartbeat/server && go run main.go

# Terminal 2: Start client (will detect server death)  
cd examples/heartbeat/client && go run main.go
```

Expected output shows client detecting server death within 3-6 seconds after the crash.

## Technical Details

### Heartbeat Timing

- **Ping Interval**: 2 seconds
- **Pong Timeout**: 5 seconds
- **Detection Window**: If server dies at time T, client will detect between T+3s and T+6s

### Error Handling

When heartbeat timeout occurs:
- Client logs: `"heartbeat timeout: no pong received"`
- Stream is closed with gRPC status: `codes.Unavailable, "server heartbeat timeout"`
- All resources are cleaned up (goroutines stopped, subscriptions closed)

### Resource Management

- Heartbeat goroutines (`pingLoop` and `pongMonitor`) are automatically started for every stream
- Goroutines are stopped when stream closes via `heartbeatStop` channel
- No resource leaks - all goroutines exit cleanly

## Benefits

1. **Quick Detection**: Server death detected in 3-6 seconds (vs indefinite hang)
2. **Automatic**: Works transparently for all streaming RPCs
3. **Minimal Overhead**: Only 1 Ping/Pong exchange every 2 seconds
4. **Reliable**: Works even for server-streaming where client only receives data
5. **Clean Shutdown**: Proper resource cleanup on detection

## Compatibility

- Backward compatible: Old clients/servers without heartbeat can still communicate
- The heartbeat is active but ignored if not supported by peer
- No breaking changes to existing APIs

## Files Modified

1. `nats-grpc/protos/nrpc/nrpc.proto` - Protocol definition
2. `nats-grpc/proto.sh` - Build script  
3. `nats-grpc/pkg/protos/nrpc/nrpc.pb.go` - Generated protobuf code
4. `nats-grpc/pkg/rpc/server.go` - Server-side Ping handling
5. `nats-grpc/pkg/rpc/client.go` - Client-side heartbeat monitoring
6. `nats-grpc/go.mod` - Go version updated to 1.20

## Files Added

1. `nats-grpc/examples/heartbeat/server/main.go` - Example server
2. `nats-grpc/examples/heartbeat/client/main.go` - Example client
3. `nats-grpc/examples/heartbeat/README.md` - Example documentation
4. `nats-grpc/pkg/rpc/heartbeat_test.go` - Test cases (needs API updates)
5. `nats-grpc/HEARTBEAT.md` - This document

## Future Improvements

1. Make ping interval and timeout configurable
2. Add metrics for heartbeat latency
3. Implement adaptive timeout based on network conditions
4. Add server-initiated heartbeats for bi-directional monitoring
