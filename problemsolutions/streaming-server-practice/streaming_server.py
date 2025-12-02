import argparse
import asyncio
from collections import deque
from dataclasses import dataclass
from typing import Deque, Dict, List, Optional


@dataclass
class Packet:
    is_video: bool
    is_keyframe: bool
    timestamp_ms: int
    payload: bytes


class ReadBuffer:
    def __init__(self, reader: asyncio.StreamReader, chunk_size: int = 16 * 1024) -> None:
        self._reader = reader
        self._chunk_size = chunk_size
        self._buffer = bytearray()
        self._view = memoryview(self._buffer)
        self._pos = 0
        self._limit = 0

    async def _fill(self, need: int) -> None:
        while self._limit - self._pos < need:
            data = await self._reader.read(self._chunk_size)
            if not data:
                break
            if self._pos == self._limit:
                self._buffer = bytearray(data)
                self._view = memoryview(self._buffer)
                self._pos = 0
                self._limit = len(self._buffer)
            else:
                remaining = self._buffer[self._pos : self._limit]
                self._buffer = bytearray(remaining + data)
                self._view = memoryview(self._buffer)
                self._pos = 0
                self._limit = len(self._buffer)

    async def read_exactly(self, size: int) -> bytes:
        if size < 0:
            raise ValueError("size must be non-negative")
        await self._fill(size)
        if self._limit - self._pos < size:
            raise EOFError("not enough data")
        start = self._pos
        self._pos += size
        return bytes(self._view[start : start + size])


class GopCache:
    def __init__(self, max_packets: int = 512) -> None:
        self._packets: Deque[Packet] = deque()
        self._max_packets = max_packets

    def add(self, packet: Packet) -> None:
        if packet.is_video and packet.is_keyframe:
            self._packets.clear()
        self._packets.append(packet)
        while len(self._packets) > self._max_packets:
            self._packets.popleft()

    def snapshot(self) -> List[Packet]:
        return list(self._packets)


class StreamTopic:
    def __init__(self, queue_size: int = 256) -> None:
        self._subscribers: List[asyncio.Queue[Packet]] = []
        self._queue_size = queue_size
        self._gop_cache = GopCache()

    def subscribe(self) -> asyncio.Queue:
        queue: asyncio.Queue[Packet] = asyncio.Queue(maxsize=self._queue_size)
        for packet in self._gop_cache.snapshot():
            self._offer(queue, packet)
        self._subscribers.append(queue)
        return queue

    def unsubscribe(self, queue: asyncio.Queue) -> None:
        if queue in self._subscribers:
            self._subscribers.remove(queue)

    def publish(self, packet: Packet) -> None:
        self._gop_cache.add(packet)
        for queue in list(self._subscribers):
            self._offer(queue, packet)

    def _offer(self, queue: asyncio.Queue, packet: Packet) -> None:
        if queue.full():
            try:
                queue.get_nowait()
            except asyncio.QueueEmpty:
                return
        try:
            queue.put_nowait(packet)
        except asyncio.QueueFull:
            return


class StreamHub:
    def __init__(self) -> None:
        self._topics: Dict[str, StreamTopic] = {}

    def topic_for(self, stream_id: str) -> StreamTopic:
        topic = self._topics.get(stream_id)
        if topic is None:
            topic = StreamTopic()
            self._topics[stream_id] = topic
        return topic


class IngestServer:
    def __init__(self, host: str, port: int, hub: StreamHub) -> None:
        self._host = host
        self._port = port
        self._hub = hub
        self._server: Optional[asyncio.base_events.Server] = None
        self._bound_port: Optional[int] = None

    async def start(self) -> None:
        self._server = await asyncio.start_server(self._handle_client, self._host, self._port)
        sockets = getattr(self._server, "sockets", None)
        if sockets:
            sock = sockets[0]
            self._bound_port = sock.getsockname()[1]

    async def serve_forever(self) -> None:
        if self._server is None:
            await self.start()
        assert self._server is not None
        async with self._server:
            await self._server.serve_forever()

    async def _handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            line = await reader.readline()
            if not line:
                writer.close()
                await writer.wait_closed()
                return
            try:
                decoded = line.decode("utf-8").strip()
            except UnicodeDecodeError:
                writer.close()
                await writer.wait_closed()
                return
            parts = decoded.split()
            if len(parts) != 2 or parts[0] != "STREAM":
                writer.close()
                await writer.wait_closed()
                return
            stream_id = parts[1]
            topic = self._hub.topic_for(stream_id)
            buffer = ReadBuffer(reader)
            await self._ingest_loop(buffer, topic)
        except Exception:
            writer.close()
            await writer.wait_closed()

    async def _ingest_loop(self, buffer: ReadBuffer, topic: StreamTopic) -> None:
        while True:
            try:
                header = await buffer.read_exactly(9)
            except EOFError:
                break
            if len(header) < 9:
                break
            flags = header[0]
            timestamp_ms = int.from_bytes(header[1:5], "big")
            length = int.from_bytes(header[5:9], "big")
            if length < 0 or length > 10 * 1024 * 1024:
                break
            try:
                payload = await buffer.read_exactly(length)
            except EOFError:
                break
            is_video = bool(flags & 0x01)
            is_keyframe = bool(flags & 0x02)
            packet = Packet(
                is_video=is_video,
                is_keyframe=is_keyframe,
                timestamp_ms=timestamp_ms,
                payload=payload,
            )
            topic.publish(packet)


async def _run_dummy_ingest(host: str, port: int, stream_id: str, duration_seconds: int = 5) -> None:
    reader, writer = await asyncio.open_connection(host, port)
    writer.write(f"STREAM {stream_id}\n".encode("utf-8"))
    await writer.drain()
    start = asyncio.get_event_loop().time()
    frame_index = 0
    while True:
        now = asyncio.get_event_loop().time()
        if now - start > duration_seconds:
            break
        timestamp_ms = int((now - start) * 1000)
        is_video = True
        is_keyframe = frame_index % 30 == 0
        flags = 0
        if is_video:
            flags |= 0x01
        if is_keyframe:
            flags |= 0x02
        payload = f"frame-{frame_index}-key={is_keyframe}".encode("utf-8")
        length = len(payload)
        header = bytes(
            (
                flags,
                *timestamp_ms.to_bytes(4, "big"),
                *length.to_bytes(4, "big"),
            )
        )
        writer.write(header + payload)
        await writer.drain()
        frame_index += 1
        await asyncio.sleep(1 / 30)
    writer.close()
    await writer.wait_closed()


async def _run_consumer(hub: StreamHub, stream_id: str, name: str, run_seconds: int = 5) -> None:
    topic = hub.topic_for(stream_id)
    queue = topic.subscribe()
    try:
        start = asyncio.get_event_loop().time()
        while True:
            now = asyncio.get_event_loop().time()
            if now - start > run_seconds:
                break
            try:
                packet = await asyncio.wait_for(queue.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            label = "K" if packet.is_keyframe else "P"
            print(f"[{name}] {label} ts={packet.timestamp_ms}ms size={len(packet.payload)}")
    finally:
        topic.unsubscribe(queue)


async def _run_demo() -> None:
    hub = StreamHub()
    server = IngestServer("0.0.0.0", 0, hub)
    await server.start()
    port = server._bound_port or 0
    if port == 0:
        raise RuntimeError("server did not bind to a port")
    server_task = asyncio.create_task(server.serve_forever())
    await asyncio.sleep(0.1)
    stream_id = "demo"
    ingest_task = asyncio.create_task(_run_dummy_ingest("127.0.0.1", port, stream_id, duration_seconds=5))
    consumer_tasks = [
        asyncio.create_task(_run_consumer(hub, stream_id, "segmenter", run_seconds=6)),
        asyncio.create_task(_run_consumer(hub, stream_id, "archive", run_seconds=6)),
        asyncio.create_task(_run_consumer(hub, stream_id, "relay", run_seconds=6)),
    ]
    await ingest_task
    await asyncio.sleep(1.0)
    server_task.cancel()
    try:
        await server_task
    except asyncio.CancelledError:
        pass
    for task in consumer_tasks:
        await task


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=["demo"])
    args = parser.parse_args()
    if args.command == "demo":
        asyncio.run(_run_demo())


if __name__ == "__main__":
    main()
