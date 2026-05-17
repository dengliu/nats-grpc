FROM golang:1.26-alpine AS builder
WORKDIR /build
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" \
    -o /out/go-client ./examples/sidecar/docker-compose/go-client

FROM alpine:latest
RUN apk --no-cache add ca-certificates
COPY --from=builder /out/go-client /usr/local/bin/go-client
ENTRYPOINT ["/usr/local/bin/go-client"]
