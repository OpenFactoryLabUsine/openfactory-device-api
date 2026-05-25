import asyncio
import contextlib
import json
import traceback
from collections.abc import Callable

from websockets.exceptions import ConnectionClosed
from websockets.server import WebSocketServerProtocol

from connection.registry import ConnectionRegistry
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
from monitoring.equipment_monitor import EquipmentMonitor
from services.equipment_service import EquipmentService


def _catch_websocket_errors(method: Callable) -> Callable:
    async def wrapper(self, websocket: WebSocketServerProtocol, *args, **kwargs):
        try:
            return await method(self, websocket, *args, **kwargs)
        except DeviceNotFoundException as e:
            await self._send_error(websocket, str(e))
            traceback.print_exc()
        except StreamCreationException as e:
            await self._send_error(websocket, str(e))
            traceback.print_exc()
        except Exception as e:
            await self._send_error(websocket, f"Unexpected error: {e} in method {method.__name__}")
            traceback.print_exc()

    return wrapper


class DeviceSession:
    def __init__(
        self,
        registry: ConnectionRegistry,
        monitor: EquipmentMonitor,
        equipment_service: EquipmentService,
        openfactory_app,
    ):
        self._registry = registry
        self._monitor = monitor
        self._equipment_service = equipment_service
        self._openfactory_app = openfactory_app
        self._dispatch: dict[str, Callable] = {
            "simulation_mode": self._set_simulation_mode,
            "drop_connection": self._drop_connection,
        }

    async def accept(self, websocket: WebSocketServerProtocol):
        path = websocket.request.path

        if path == "/ws/equipments":
            await self._send_equipments_list(websocket)
            return

        if not path.startswith("/ws/equipments/"):
            await self._send_error(websocket, "Invalid endpoint")
            return

        asset_uuid = path.split("/")[3].upper()

        await self._run(websocket, asset_uuid)

    @_catch_websocket_errors
    async def _run(self, websocket: WebSocketServerProtocol, asset_uuid: str):
        await self._registry.add(websocket, asset_uuid)
        try:
            if not self._monitor.is_active(asset_uuid):
                self._monitor.start(asset_uuid)

            await self._send_initial_data(websocket, asset_uuid)

            outgoing = asyncio.create_task(self._pipe_outgoing(websocket))
            incoming = asyncio.create_task(self._pipe_incoming(websocket, asset_uuid))

            done, pending = await asyncio.wait(
                [outgoing, incoming],
                return_when=asyncio.FIRST_COMPLETED,
            )
            for task in pending:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    contextlib.suppress(asyncio.CancelledError)
            for task in done:
                if not task.cancelled() and task.exception():
                    print(f"Task error: {task.exception()}")
        finally:
            await self._registry.remove(websocket)
            print(f"Session closed for equipment: {asset_uuid}")

    @_catch_websocket_errors
    async def _send_initial_data(
        self, websocket: WebSocketServerProtocol, asset_uuid: str
    ):
        variables = self._equipment_service.get_equipment_variables(asset_uuid)
        await websocket.send(
            ConnectionEstablishedMessage(
                asset_uuid=asset_uuid,
                variables=variables,
                connection_count=self._registry.count(asset_uuid),
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
                except (TimeoutError, asyncio.TimeoutError):
                    await websocket.send(
                        PingMessage(
                            active_equipments=self._registry.active_equipment_count()
                        ).to_json()
                    )
                except ConnectionClosed:
                    break
        except BaseException as e:
            if isinstance(e, (KeyboardInterrupt, SystemExit, GeneratorExit)) or type(e).__name__ == "CancelledError":
                raise
            print(f"Outgoing pipe error: {e}")

    async def _pipe_incoming(
        self, websocket: WebSocketServerProtocol, asset_uuid: str
    ):
        try:
            while True:
                try:
                    raw = await asyncio.wait_for(websocket.recv(), timeout=30.0)
                    message = ClientMessage.from_dict(json.loads(raw))
                    await self._route(websocket, asset_uuid, message)
                except TimeoutError:
                    continue
                except json.JSONDecodeError as e:
                    await self._send_error(websocket, f"Invalid JSON: {e}")
                except ConnectionClosed:
                    break
        except BaseException as e:
            if isinstance(e, (KeyboardInterrupt, SystemExit, GeneratorExit)) or type(e).__name__ == "CancelledError":
                raise
            print(f"Incoming pipe error for {asset_uuid}: {e}")

    async def _route(
        self,
        websocket: WebSocketServerProtocol,
        asset_uuid: str,
        message: ClientMessage,
    ):
        handler = self._dispatch.get(message.method)
        if not handler:
            await self._send_error(websocket, f"Unknown method: {message.method}")
            return
        await handler(websocket, asset_uuid, message.params)

    async def _set_simulation_mode(
        self,
        websocket: WebSocketServerProtocol,
        asset_uuid: str,
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

    async def _drop_connection(
        self,
        websocket: WebSocketServerProtocol,
        asset_uuid: str,
        params: dict,
    ):
        self._monitor.stop(asset_uuid)
        await websocket.send(StreamDroppedMessage(asset_uuid=asset_uuid).to_json())

    async def _send_equipments_list(self, websocket: WebSocketServerProtocol):
        try:
            equipments = self._equipment_service.get_all_equipments()
            equipment_list = [
                {
                    "asset_uuid": uuid,
                    "variables": self._equipment_service.get_equipment_variables(uuid),
                }
                for uuid in equipments
            ]
            await websocket.send(DevicesListMessage(equipments=equipment_list).to_json())

            while True:
                try:
                    await asyncio.sleep(30)
                    await websocket.send(
                        PingMessage(
                            active_equipments=self._registry.active_equipment_count()
                        ).to_json()
                    )
                except ConnectionClosed:
                    break
        except Exception as e:
            await self._send_error(websocket, f"Failed to get equipments list: {e}")
        finally:
            try:
                await websocket.close()
            except Exception:
                contextlib.suppress(Exception)

    async def _send_error(self, websocket: WebSocketServerProtocol, message: str):
        try:
            await websocket.send(ErrorMessage(message=message).to_json())
        except ConnectionClosed:
            contextlib.suppress(ConnectionClosed)
