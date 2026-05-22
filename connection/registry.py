import asyncio
import json
from collections import defaultdict

from websockets.server import WebSocketServerProtocol


class ConnectionRegistry:
    def __init__(self):
        self.device_connections: dict[str, set[WebSocketServerProtocol]] = defaultdict(
            set
        )
        self._connection_to_device: dict[WebSocketServerProtocol, str] = {}
        self._queues: dict[WebSocketServerProtocol, asyncio.Queue] = {}
        self._lock = asyncio.Lock()

    async def add(self, websocket: WebSocketServerProtocol, device_uuid: str):
        async with self._lock:
            self.device_connections[device_uuid].add(websocket)
            self._connection_to_device[websocket] = device_uuid
            self._queues[websocket] = asyncio.Queue()

    async def remove(self, websocket: WebSocketServerProtocol):
        async with self._lock:
            if websocket not in self._connection_to_device:
                return
            device_uuid = self._connection_to_device.pop(websocket)
            self.device_connections[device_uuid].discard(websocket)
            self._queues.pop(websocket, None)

    async def remove_all(self):
        for websocket in list(self._connection_to_device.keys()):
            await self.remove(websocket)

    async def broadcast(self, device_uuid: str, message: dict):
        for connection in self.device_connections[device_uuid].copy():
            queue = self._queues.get(connection)
            if not queue:
                continue
            try:
                await queue.put(json.dumps(message))
            except Exception as e:
                print(f"Error queuing message for connection: {e}")
                await self.remove(connection)

    def get_queue(self, websocket: WebSocketServerProtocol) -> asyncio.Queue | None:
        return self._queues.get(websocket)

    def count(self, device_uuid: str) -> int:
        return len(self.device_connections[device_uuid])

    def total(self) -> int:
        return sum(len(c) for c in self.device_connections.values())
