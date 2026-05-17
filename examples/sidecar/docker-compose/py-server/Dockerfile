FROM python:3.11-slim

WORKDIR /app

# Python deps including grpcio-tools for proto codegen.
COPY examples/sidecar/docker-compose/py-server/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Generate echo proto stubs from the shared .proto.
COPY examples/protos/echo.proto /protos/echo.proto
RUN python -m grpc_tools.protoc \
    -I /protos \
    --python_out=. \
    --grpc_python_out=. \
    /protos/echo.proto

COPY examples/sidecar/docker-compose/py-server/server.py .

CMD ["python", "-u", "server.py"]
