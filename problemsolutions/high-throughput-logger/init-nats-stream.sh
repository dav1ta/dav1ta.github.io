#!/bin/bash
# Create JetStream stream for logs

echo "Creating LOGS stream in NATS JetStream..."

docker run --rm --network high-throughput-logger_default natsio/nats-box:latest \
  nats stream add LOGS \
    --server nats://nats:4222 \
    --subjects "logs.>" \
    --storage file \
    --retention limits \
    --max-age 1h \
    --max-bytes 1073741824 \
    --replicas 1 \
    --defaults

echo "✓ Stream created successfully"
