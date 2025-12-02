#!/usr/bin/env python3
"""
Log Simulator for NATS JetStream
Simulates a microservice generating logs and publishing them to NATS.

Run: python log_simulator.py
"""

import asyncio
import json
import random
import time
from datetime import datetime
from nats.aio.client import Client as NATS
from nats.js.api import StreamConfig


# Configuration
NATS_URL = "nats://localhost:4222"
SUBJECT = "logs.app"  # Vector subscribes to "logs.>"
STREAM_NAME = "LOGS"
LOGS_PER_SECOND = 1000  # Adjust this to simulate load


# Sample log data
SERVICE_IDS = ["auth-service", "payment-service", "user-service", "notification-service"]
LOG_LEVELS = ["INFO", "WARN", "ERROR", "DEBUG"]
MESSAGES = [
    "User logged in successfully",
    "Payment processed",
    "Database connection established",
    "Cache miss for key",
    "API request received",
    "Request timeout",
    "Invalid token provided",
    "Rate limit exceeded",
]


def generate_log():
    """Generate a realistic log entry."""
    return {
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "service_id": random.choice(SERVICE_IDS),
        "level": random.choice(LOG_LEVELS),
        "message": random.choice(MESSAGES),
        "trace_id": f"trace-{random.randint(1000, 9999)}",
        "user_id": f"user-{random.randint(1, 1000)}",
    }


async def setup_stream(js):
    """
    Create the LOGS stream if it doesn't exist.
    This is the "durable buffer" that prevents data loss.
    """
    try:
        await js.stream_info(STREAM_NAME)
        print(f"✓ Stream '{STREAM_NAME}' already exists")
    except Exception:
        # Stream doesn't exist, create it
        stream_config = StreamConfig(
            name=STREAM_NAME,
            subjects=["logs.>"],  # Capture all logs.* subjects
            retention="limits",  # Delete old messages when limits are hit
            max_age=3600 * 1_000_000_000,  # 1 hour (in nanoseconds)
            max_bytes=1024 * 1024 * 1024,  # 1 GB
            storage="file",  # Persist to disk (critical for durability)
        )
        await js.add_stream(stream_config)
        print(f"✓ Created stream '{STREAM_NAME}'")


async def publish_logs():
    """Main loop: Publish logs to NATS JetStream."""
    nc = NATS()
    
    try:
        # Connect to NATS
        await nc.connect(NATS_URL)
        print(f"✓ Connected to NATS at {NATS_URL}")
        
        print(f"\n🚀 Publishing {LOGS_PER_SECOND} logs/sec to subject '{SUBJECT}'")
        print("Press Ctrl+C to stop\n")

        
        total_sent = 0
        start_time = time.time()
        
        while True:
            batch_start = time.time()
            
            # Send logs in batches
            for _ in range(LOGS_PER_SECOND):
                log = generate_log()
                log_json = json.dumps(log)
                
                # Publish to NATS (will be captured by JetStream stream)
                # Use nc.publish instead of js.publish to avoid metadata validation
                await nc.publish(SUBJECT, log_json.encode())
                total_sent += 1
            
            # Calculate sleep time to maintain rate
            elapsed = time.time() - batch_start
            sleep_time = max(0, 1.0 - elapsed)
            
            # Print stats every second
            runtime = time.time() - start_time
            rate = total_sent / runtime if runtime > 0 else 0
            print(f"📊 Sent: {total_sent:,} logs | Rate: {rate:.1f} logs/sec | Latency: {elapsed*1000:.1f}ms")
            
            await asyncio.sleep(sleep_time)
    
    except KeyboardInterrupt:
        print("\n\n✓ Stopped by user")
    except Exception as e:
        print(f"\n❌ Error: {e}")
    finally:
        await nc.close()
        print("✓ Connection closed")


if __name__ == "__main__":
    asyncio.run(publish_logs())
