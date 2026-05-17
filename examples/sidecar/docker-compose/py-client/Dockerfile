FROM python:3.11-slim

WORKDIR /app

COPY examples/sidecar/docker-compose/py-client/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY examples/protos/echo.proto /protos/echo.proto
RUN python -m grpc_tools.protoc \
    -I /protos \
    --python_out=. \
    --grpc_python_out=. \
    /protos/echo.proto

COPY examples/sidecar/docker-compose/py-client/client.py .

CMD ["python", "-u", "client.py"]
