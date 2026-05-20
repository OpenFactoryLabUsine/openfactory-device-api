import asyncio
import time

from connection.registry import ConnectionRegistry
from exceptions import StreamCreationException
from services.device_service import DeviceService
from services.stream_service import StreamService
from monitoring.topic_subscription import TopicSubscriber


class DeviceMonitor:
    def __init__(
        self,
        stream_service: StreamService,
        topic_subscriber: TopicSubscriber,
        openfactory_app,
        device_service: DeviceService,
        registry: ConnectionRegistry,
    ):
        self._stream_service = stream_service
        self._topic_subscriber = topic_subscriber
        self._openfactory_app = openfactory_app
        self._device_service = device_service
        self._registry = registry
        self._active: dict[str, str] = {}

    def is_active(self, device_uuid: str) -> bool:
        return device_uuid in self._active

    def start(self, device_uuid: str):
        if self.is_active(device_uuid):
            return

        try:
            self._openfactory_app.initialize_asset(device_uuid)
            topic = self._stream_service.create_device_stream(device_uuid)
            self._topic_subscriber.subscribe(
                topic=topic,
                group_id=f"api_device_stream_group_{device_uuid}",
                on_message=self._on_message,
                message_filter=lambda key: key == device_uuid,
            )
            self._active[device_uuid] = topic
            print(f"Started monitoring for device {device_uuid}")

        except StreamCreationException as e:
            print(f"Failed to start monitoring for {device_uuid}: {e}")
            raise

    def stop(self, device_uuid: str):
        if not self.is_active(device_uuid):
            return
        topic = self._active.pop(device_uuid)
        self._stream_service.drop_device_stream(device_uuid)
        self._topic_subscriber.unsubscribe(topic)
        print(f"Stopped monitoring for device {device_uuid}")

    def _on_message(self, device_uuid: str, msg_value: dict):
        try:
            self._device_service.process_update(device_uuid, msg_value)

            message = {
                "asset_uuid": device_uuid,
                "data": dict(msg_value),
                "timestamp": time.time(),
            }

            loop = asyncio.get_event_loop()
            if loop.is_running():
                asyncio.run_coroutine_threadsafe(
                    self._registry.broadcast(device_uuid, message),
                    loop,
                )
            else:
                print(f"No running event loop to broadcast message for {device_uuid}")

        except Exception as e:
            print(f"Error handling message for {device_uuid}: {e}")
