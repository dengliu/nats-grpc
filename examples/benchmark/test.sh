#!/bin/bash

# Simple test script for the benchmark
# This script tests with a small number of clients/servers (default: 5)

set -e

CLIENT_COUNT=${1:-5}
PAYLOAD_SIZE=${2:-4096}

echo "=========================================="
echo "Benchmark Test Script"
echo "=========================================="
echo "Client/Server Count: $CLIENT_COUNT"
echo "Payload Size: $PAYLOAD_SIZE bytes"
echo "=========================================="
echo ""

# Check if NATS server is already running on default port
echo "Step 1: Checking NATS server..."
if nc -z localhost 4222 2>/dev/null; then
    echo "NATS server is already running on localhost:4222"
    NATS_PID=""
else
    echo "Starting NATS server..."
    if command -v nats-server &> /dev/null; then
        nats-server &
        NATS_PID=$!
        sleep 2
        echo "NATS server started (PID: $NATS_PID)"
    else
        echo "ERROR: NATS server not running and nats-server command not found"
        echo "Please start NATS server manually or install it"
        exit 1
    fi
fi

echo "Step 2: Building server..."
go build -o /tmp/benchmark-server examples/benchmark/server/main.go

echo "Step 3: Building client..."
go build -o /tmp/benchmark-client examples/benchmark/client/main.go

echo "Step 4: Starting benchmark server..."
/tmp/benchmark-server --server-count=$CLIENT_COUNT --payload-size=$PAYLOAD_SIZE &
SERVER_PID=$!
sleep 3

echo "Step 5: Starting benchmark client..."
/tmp/benchmark-client --client-count=$CLIENT_COUNT --payload-size=$PAYLOAD_SIZE &
CLIENT_PID=$!

echo ""
echo "=========================================="
echo "Benchmark is running!"
echo "=========================================="
echo "Server PID: $SERVER_PID"
echo "Client PID: $CLIENT_PID"
echo "NATS PID: $NATS_PID"
echo ""
echo "Metrics:"
echo "  Server: http://localhost:9090/metrics"
echo "  Client: http://localhost:9091/metrics"
echo ""
echo "Press Ctrl+C to stop..."
echo "=========================================="

# Cleanup function
cleanup() {
    echo ""
    echo "Shutting down..."
    kill $CLIENT_PID 2>/dev/null || true
    kill $SERVER_PID 2>/dev/null || true
    if [ -n "$NATS_PID" ]; then
        kill $NATS_PID 2>/dev/null || true
        echo "NATS server stopped"
    fi
    echo "Cleanup complete"
    exit 0
}

trap cleanup SIGINT SIGTERM

# Wait for user to stop
wait
