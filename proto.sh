#!/bin/sh

# note: need protoc-gen-go-grpc@v1.1.0
# go get google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.1.0

OUT_DIR=./pkg/protos
SRC_DIR=./protos

rm -fr ${OUT_DIR}
mkdir -p ${OUT_DIR}
touch ${OUT_DIR}/.generated

protoc -I ${SRC_DIR} \
	--go_out=${OUT_DIR} \
	--go_opt=paths=source_relative \
	${SRC_DIR}/nrpc/nrpc.proto

protoc -I ${SRC_DIR} \
	--go_out=${OUT_DIR} \
	--go_opt=paths=source_relative \
	--go-grpc_out=${OUT_DIR} \
	--go-grpc_opt=paths=source_relative \
	${SRC_DIR}/sidecar/sidecar.proto
