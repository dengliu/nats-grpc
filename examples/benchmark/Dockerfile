# Multi-stage build for nats-grpc benchmark client and server
FROM golang:1.26-alpine AS builder

# Install build dependencies
RUN apk add --no-cache git

WORKDIR /build

# Copy go mod files
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build both binaries
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o benchmark-server ./examples/benchmark/server/main.go
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o benchmark-client ./examples/benchmark/client/main.go

# Final stage - minimal image with both binaries
FROM alpine:latest

RUN apk --no-cache add ca-certificates

WORKDIR /app

# Copy both binaries from builder
COPY --from=builder /build/benchmark-server .
COPY --from=builder /build/benchmark-client .

# Expose metrics ports
# 9090 for server metrics
# 9091 for client metrics
EXPOSE 9090 9091

# Default to server, but can be overridden
ENTRYPOINT ["/app/benchmark-server"]
