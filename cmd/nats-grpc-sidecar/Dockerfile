# Multi-stage build for the nats-grpc sidecar.
#
# The image is intentionally minimal — the sidecar is a one-binary
# product that connects to NATS over the network and listens on two
# loopback gRPC ports. Build it once, deploy it as a Kubernetes
# sidecar container next to any gRPC application.
FROM golang:1.26-alpine AS builder

RUN apk add --no-cache git

WORKDIR /build

# Copy go module metadata first so the dependency download caches
# independently of source changes.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# Static, stripped, no cgo so the binary runs on plain alpine.
RUN CGO_ENABLED=0 GOOS=linux go build \
    -trimpath -ldflags="-s -w" \
    -o /out/nats-grpc-sidecar \
    ./cmd/nats-grpc-sidecar

FROM alpine:latest

RUN apk --no-cache add ca-certificates

COPY --from=builder /out/nats-grpc-sidecar /usr/local/bin/nats-grpc-sidecar

# Defaults from main.go bind to 127.0.0.1, which is the correct
# behaviour for a Kubernetes sidecar container (the app container
# shares the pod's network namespace, so localhost reaches us).
# For non-Kubernetes deployments, override with the -egress / -admin
# flags as needed.
EXPOSE 50051 50100 50101

ENTRYPOINT ["/usr/local/bin/nats-grpc-sidecar"]
