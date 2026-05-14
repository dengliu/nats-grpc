# Heartbeat Example

This example demonstrates the heartbeat mechanism in nats-grpc that allows clients to detect when a server dies during a streaming RPC call.

## Overview

The heartbeat mechanism works as follows:
- **Client sends Ping messages** every 2 seconds to the server
- **Server responds with Pong messages** immediately upon receiving Ping
- **Client monitors Pong responses** and if no Pong is received within 5 seconds, it detects server failure
- **Detection time**: The client will detect server death within 3-6 seconds

## Architecture

```
Client                          Server
  |                               |
  |--- Ping (every 2s) --------->|
  |<-- Pong (immediate) ---------|
  |                               |
  |--- Ping ----------------------|
  |<-- Pong ----------------------|
  |                               |
  |--- Ping ----------------------|
  |                               X (server dies)
  |                               
  |--- Ping ----------------------X (no response)
  |                               
  | (after 5s timeout)            
  | Detect failure!               
  | Close stream with error       
```

## Running the Example

### Prerequisites

1. Start a NATS server:
```bash
nats-server
```

### Run the Demo

First, make sure you have a NATS server running:
```bash
nats-server
```

Then in separate terminals:

1. Start the server:
```bash
cd examples/heartbeat/server
go run main.go
```

2. Start the client:
```bash
cd examples/heartbeat/client
go run main.go
```

### Expected Behavior

1. The server will start streaming messages to the client (one per second)
2. After sending 10 messages, the server will **simulate a crash** by calling `os.Exit(1)`
3. The client will continue trying to receive messages
4. Within 3-6 seconds after the server crash, the client will detect the failure via heartbeat timeout
5. The client will log: `✓ SUCCESS: Client detected server death via heartbeat timeout!`

### Sample Output

**Server output:**
```
Connected to NATS
Server is ready and listening for requests
Server will crash after sending 10 messages to demonstrate heartbeat detection
Client connected, starting to stream data...
Sending: Message 1 at 17:08:30
Sending: Message 2 at 17:08:31
...
Sending: Message 10 at 17:08:39
!!! SIMULATING SERVER CRASH !!!
Server will exit in 1 second...
```

**Client output:**
```
Connected to NATS
Streaming started, receiving messages...
Heartbeat: Client sends Ping every 2 seconds, expects Pong within 5 seconds
If no Pong received within 5 seconds, client will detect server death

[1.234s] Received message 1: Message 1 at 17:08:30
[2.345s] Received message 2: Message 2 at 17:08:31
...
[10.456s] Received message 10: Message 10 at 17:08:39

!!! ERROR DETECTED after 15.789s !!!
Error: rpc error: code = Unavailable desc = server heartbeat timeout
gRPC Status Code: 14 (Unavailable)
gRPC Status Message: server heartbeat timeout

✓ SUCCESS: Client detected server death via heartbeat timeout!
✓ Detection time: 15.789s (expected 3-6 seconds from last message)
✓ Messages received before detection: 10

This demonstrates that the heartbeat mechanism successfully detected
that the server died, even though no explicit End message was sent.

Client finished
```

## Implementation Details

### Protocol Extension

The nrpc protocol was extended with Ping/Pong messages:

```protobuf
message Request {
  oneof type {
    Call call = 2;
    Data data = 3;
    End end = 4;
    Ping ping = 5;  // NEW: Client sends periodic pings
  }
}

message Response {
  oneof type {
    Begin begin = 2;
    Data data = 3;
    End end = 4;
    Pong pong = 5;  // NEW: Server responds with pongs
  }
}

message Ping { int64 timestamp = 1; }
message Pong { int64 timestamp = 1; }
```

### Client-Side Implementation

The client maintains:
- `pingLoop()`: Goroutine that sends Ping every 2 seconds
- `pongMonitor()`: Goroutine that checks for Pong timeout every 2 seconds
- `processPong()`: Updates last Pong received time
- If no Pong received within 5 seconds, the stream is closed with `codes.Unavailable`

### Server-Side Implementation

The server:
- Listens for Ping messages in the request stream
- Immediately responds with Pong messages containing the same timestamp
- No additional goroutines or timers needed on server side

## Why This Matters

Without heartbeats, if a server dies during streaming:
- The client would **hang indefinitely** waiting for more data
- There would be no way to detect the failure until trying to send data
- For server-streaming RPCs where client only receives, detection is impossible

With heartbeats:
- **Quick detection** of server failures (3-6 seconds)
- **Automatic cleanup** of resources
- **Improved reliability** of streaming applications
- Works even when client is only receiving data
