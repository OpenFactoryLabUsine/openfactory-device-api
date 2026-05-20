import asyncio
import json
from typing import Callable

from websockets.exceptions import ConnectionClosed
from websockets.server import WebSocketServerProtocol

from connection.registry import ConnectionRegistry
from monitoring.device_monitor import DeviceMonitor
from exceptions import DeviceNotFoundException, StreamCreationException
from messages import (
    ConnectionEstablishedMessage,
    DevicesListMessage,
    ErrorMessage,
    PingMessage,
    SimulationModeUpdatedMessage,
    StreamDroppedMessage,
)
from models import ClientMessage
from services.device_service import DeviceService


def _catch_websocket_errors(method: Callable) -> Callable:
    async def wrapper(self, websocket: WebSocketServerProtocol, *args, **kwargs):
        try:
            return await method(self, websocket, *args, **kwargs)
        except DeviceNotFoundException as e:
            await self._send_error(websocket, str(e))
        except StreamCreationException as e:
            await self._send_error(websocket, str(e))
        except Exception as e:
            await self._send_error(websocket, f"Unexpected error: {e}")

    return wrapper


class DeviceSession:
    def __init__(
        self,
        registry: ConnectionRegistry,
        monitor: DeviceMonitor,
        device_service: DeviceService,
        openfactory_app,
    ):
        self._registry = registry
        self._monitor = monitor
        self._device_service = device_service
        self._openfactory_app = openfactory_app
        self._dispatch: dict[str, Callable] = {
            "simulation_mode": self._set_simulation_mode,
            "drop_stream": self._drop_stream,
        }

    async def accept(self, websocket: WebSocketServerProtocol):
        path = websocket.request.path

        if path == "/ws/devices":
            await self._send_devices_list(websocket)
            return

        if not path.startswith("/ws/devices/"):
            await self._send_error(websocket, "Invalid endpoint")
            return

        device_uuid = path.split("/")[3]
        await self._run(websocket, device_uuid)

    @_catch_websocket_errors
    async def _run(self, websocket: WebSocketServerProtocol, device_uuid: str):
        await self._registry.add(websocket, device_uuid)
        try:
            if not self._monitor.is_active(device_uuid):
                self._monitor.start(device_uuid)

            await self._send_initial_data(websocket, device_uuid)

            outgoing = asyncio.create_task(self._pipe_outgoing(websocket))
            incoming = asyncio.create_task(self._pipe_incoming(websocket, device_uuid))

            done, pending = await asyncio.wait(
                [outgoing, incoming],
                return_when=asyncio.FIRST_COMPLETED,
            )
            for task in pending:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
            for task in done:
                if not task.cancelled() and task.exception():
                    print(f"Task error: {task.exception()}")
        finally:
            await self._registry.remove(websocket)
            print(f"Session closed for device: {device_uuid}")

    @_catch_websocket_errors
    async def _send_initial_data(
        self, websocket: WebSocketServerProtocol, device_uuid: str
    ):
        data_items = self._device_service.get_device_dataitems(device_uuid)
        await websocket.send(
            ConnectionEstablishedMessage(
                device_uuid=device_uuid,
                data_items=data_items,
                connection_count=self._registry.count(device_uuid),
            ).to_json()
        )

    async def _pipe_outgoing(self, websocket: WebSocketServerProtocol):
        queue = self._registry.get_queue(websocket)
        if not queue:
            return
        try:
            while True:
                try:
                    message = await asyncio.wait_for(queue.get(), timeout=1.0)
                    await websocket.send(message)
                except asyncio.TimeoutError:
                    await websocket.send(PingMessage().to_json())
                except ConnectionClosed:
                    break
        except Exception as e:
            print(f"Outgoing pipe error: {e}")

    async def _pipe_incoming(
        self, websocket: WebSocketServerProtocol, device_uuid: str
    ):
        try:
            while True:
                try:
                    raw = await asyncio.wait_for(websocket.recv(), timeout=30.0)
                    message = ClientMessage.from_dict(json.loads(raw))
                    await self._route(websocket, device_uuid, message)
                except asyncio.TimeoutError:
                    continue
                except json.JSONDecodeError as e:
                    await self._send_error(websocket, f"Invalid JSON: {e}")
                except ConnectionClosed:
                    break
        except Exception as e:
            print(f"Incoming pipe error for {device_uuid}: {e}")

    async def _route(
        self,
        websocket: WebSocketServerProtocol,
        device_uuid: str,
        message: ClientMessage,
    ):
        handler = self._dispatch.get(message.method)
        if not handler:
            await self._send_error(websocket, f"Unknown method: {message.method}")
            return
        await handler(websocket, device_uuid, message.params)

    async def _set_simulation_mode(
        self,
        websocket: WebSocketServerProtocol,
        device_uuid: str,
        params: dict,
    ):
        name = params.get("name")
        args = params.get("args")

        if not name or args is None:
            await self._send_error(websocket, "Missing name or args")
            return

        try:
            self._openfactory_app.send_method(name, str(args).lower())
            await websocket.send(SimulationModeUpdatedMessage(value=args).to_json())
        except Exception as e:
            await websocket.send(
                SimulationModeUpdatedMessage(
                    value=args, success=False, error=str(e)
                ).to_json()
            )

    async def _drop_stream(
        self,
        websocket: WebSocketServerProtocol,
        device_uuid: str,
        params: dict,
    ):
        self._monitor.stop(device_uuid)
        await websocket.send(StreamDroppedMessage(device_uuid=device_uuid).to_json())

    async def _send_devices_list(self, websocket: WebSocketServerProtocol):
        try:
            devices = self._device_service.get_all_devices()
            device_list = [
                {
                    "device_uuid": uuid,
                    "dataitems": self._device_service.get_device_dataitems(uuid),
                    "durations": self._device_service.get_device_stats(uuid),
                }
                for uuid in devices
            ]
            await websocket.send(DevicesListMessage(devices=device_list).to_json())

            while True:
                try:
                    await asyncio.sleep(30)
                    await websocket.send(
                        PingMessage(active_devices=len(device_list)).to_json()
                    )
                except ConnectionClosed:
                    break
        except Exception as e:
            await self._send_error(websocket, f"Failed to get devices list: {e}")
        finally:
            try:
                await websocket.close()
            except Exception:
                pass

    async def _send_error(self, websocket: WebSocketServerProtocol, message: str):
        try:
            await websocket.send(ErrorMessage(message=message).to_json())
        except ConnectionClosed:
            pass
