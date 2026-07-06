import asyncio

from openfactory.assets import Asset

from connection.registry import ConnectionRegistry
from exceptions import StreamCreationException
from messages import DeviceUpdateMessage
from monitoring.asset_subscription import AssetSubscriber
from services.equipment_service import EquipmentService
from services.stream_service import StreamService


class EquipmentMonitor:
    def __init__(
        self,
        stream_service: StreamService,
        topic_subscriber: AssetSubscriber,
        openfactory_app,
        equipment_service: EquipmentService,
        registry: ConnectionRegistry,
    ):
        self._stream_service = stream_service
        self._asset_subscriber = topic_subscriber
        self._openfactory_app = openfactory_app
        self._equipment_service = equipment_service
        self._registry = registry
        self._active: dict[str, str] = {}
        self._loop: asyncio.AbstractEventLoop | None = None
        self.asset = None

    def set_event_loop(self, loop: asyncio.AbstractEventLoop):
        self._loop = loop

    def is_active(self, asset_uuid: str) -> bool:
        return asset_uuid in self._active

    def start(self, asset_uuid: str):
        if self.is_active(asset_uuid):
            return

        try:
            self._initialize_asset(asset_uuid)
            subject = f"{asset_uuid.upper()}.>"
            self._asset_subscriber.subscribe(
                subject=subject,
                on_message=self._on_message,
                message_filter=lambda key: key == asset_uuid.upper(),
            )
            self._active[asset_uuid] = subject
            print(f"Started monitoring for equipment {asset_uuid}")

        except StreamCreationException as e:
            print(f"Failed to start monitoring for {asset_uuid}: {e}")
            raise

    def stop(self, asset_uuid: str):
        if not self.is_active(asset_uuid):
            return
        subject = self._active.pop(asset_uuid)
        self._stream_service.drop_equipment_stream(asset_uuid)
        self._asset_subscriber.unsubscribe(subject)
        print(f"Stopped monitoring for equipment {asset_uuid}")

    def _initialize_asset(self, asset_uuid: str):
        try:
            self.asset = Asset(
                asset_uuid=asset_uuid,
                ksqlClient=self._openfactory_app.ksqlClient,
                bootstrap_servers=self._openfactory_app.bootstrap_servers,
            )
        except Exception as e:
            print(f"Error initializing asset {asset_uuid}: {e}")
            raise StreamCreationException(f"Could not initialize asset {asset_uuid}") from e

    def _on_message(self, asset_uuid: str, msg_value: dict):
        try:
            variables = self._equipment_service.enrich_update(asset_uuid, msg_value)
            message = DeviceUpdateMessage(asset_uuid=asset_uuid, variables=variables).to_dict()
            loop = self._loop or asyncio.get_event_loop()
            if loop.is_running():
                asyncio.run_coroutine_threadsafe(
                    self._registry.broadcast(asset_uuid, message), loop
                )
            else:
                print(f"Event loop not available, dropping message for {asset_uuid}")
        except Exception as e:
            print(f"Error handling message for {asset_uuid}: {e}")