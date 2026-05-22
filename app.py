import asyncio
import os
import threading
import time

import websockets
from openfactory.apps import OpenFactoryApp
from openfactory.assets import Asset
from openfactory.kafka import KSQLDBClient

from config import Config
from connection.registry import ConnectionRegistry
from connection.session import DeviceSession
from monitoring.device_monitor import DeviceMonitor
from monitoring.topic_subscription import TopicSubscriber
from services.device_service import DeviceService
from services.stream_service import StreamService


class OpenFactoryAPI(OpenFactoryApp):
    def __init__(
        self,
        config: Config,
        app_uuid: str,
        ksql_client: KSQLDBClient,
        bootstrap_servers: str,
        loglevel: str = "INFO",
    ):
        print(f"OpenFactory version: {os.getenv('OPENFACTORY_VERSION', 'unknown')}")
        super().__init__(
            ksqlClient=ksql_client,
            bootstrap_servers=bootstrap_servers,
            loglevel=loglevel,
        )

        self.api = Asset(
            app_uuid, ksqlClient=ksql_client, bootstrap_servers=bootstrap_servers
        )
        self.config = config
        self.running = True

        device_service = DeviceService(ksql_client)
        stream_service = StreamService(ksql_client)
        topic_subscriber = TopicSubscriber(bootstrap_servers)
        registry = ConnectionRegistry()

        monitor = DeviceMonitor(
            stream_service=stream_service,
            topic_subscriber=topic_subscriber,
            openfactory_app=self,
            device_service=device_service,
            registry=registry,
        )

        self._registry = registry
        self._session = DeviceSession(
            registry=registry,
            monitor=monitor,
            device_service=device_service,
            openfactory_app=self,
        )

        self._websocket_server = None
        self._websocket_thread: threading.Thread | None = None

    def send_method(self, name: str, args: str):
        self.method(name, args)

    def main_loop(self):
        print("Starting OpenFactory main loop...")
        self._websocket_thread = threading.Thread(
            target=self._run_server_thread, daemon=True
        )
        self._websocket_thread.start()

        while self.running:
            try:
                total = self._registry.total()
                if total > 0:
                    print(
                        f"Active connections: {total} "
                        f"across {len(self._registry.device_connections)} devices"
                    )
                time.sleep(30)
            except KeyboardInterrupt:
                self.running = False

    def _run_server_thread(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(self._serve())
        except Exception as e:
            print(f"Server thread error: {e}")
            self.running = False
        finally:
            loop.close()

    async def _serve(self):
        try:
            self._websocket_server = await websockets.serve(
                self._session.accept,
                self.config.websocket_host,
                self.config.websocket_port,
                ping_interval=self.config.ping_interval,
                ping_timeout=self.config.ping_timeout,
            )
            print(
                f"WebSocket server started on "
                f"{self.config.websocket_host}:{self.config.websocket_port}"
            )
            while self.running:
                await asyncio.sleep(1)
        except Exception as e:
            print(f"Server error: {e}")
            self.running = False
        finally:
            if self._websocket_server:
                self._websocket_server.close()
                await self._websocket_server.wait_closed()

    def app_event_loop_stopped(self):
        print("Event loop stopped.")
        self.running = False


def main():
    config = Config()
    api = OpenFactoryAPI(
        app_uuid="OFA-API",
        config=config,
        ksql_client=KSQLDBClient(os.getenv("KSQLDB_URL", "http://ksqldb-server:8088")),
        bootstrap_servers=os.getenv("KAFKA_BROKER", "broker:29092"),
    )
    try:
        api.run()
    except KeyboardInterrupt:
        print("Received shutdown signal")
    finally:
        api.running = False


if __name__ == "__main__":
    main()
