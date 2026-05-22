import asyncio
import json
from collections import defaultdict

from websockets.server import WebSocketServerProtocol


class ConnectionRegistry:
    def __init__(self):
        self.equipment_connections: dict[str, set[WebSocketServerProtocol]] = defaultdict(
            set
        )
        self._connection_to_equipment: dict[WebSocketServerProtocol, str] = {}
        self._queues: dict[WebSocketServerProtocol, asyncio.Queue] = {}
        self._lock = asyncio.Lock()

    async def add(self, websocket: WebSocketServerProtocol, asset_uuid: str):
        async with self._lock:
            self.equipment_connections[asset_uuid].add(websocket)
            self._connection_to_equipment[websocket] = asset_uuid
            self._queues[websocket] = asyncio.Queue()

    async def remove(self, websocket: WebSocketServerProtocol):
        async with self._lock:
            if websocket not in self._connection_to_equipment:
                return
            asset_uuid = self._connection_to_equipment.pop(websocket)
            self.equipment_connections[asset_uuid].discard(websocket)
            self._queues.pop(websocket, None)

    async def remove_all(self):
        for websocket in list(self._connection_to_equipment.keys()):
            await self.remove(websocket)

    async def broadcast(self, asset_uuid: str, message: dict):
        for connection in self.equipment_connections[asset_uuid].copy():
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

    def count(self, asset_uuid: str) -> int:
        return len(self.equipment_connections[asset_uuid])

    def total(self) -> int:
        return sum(len(c) for c in self.equipment_connections.values())
