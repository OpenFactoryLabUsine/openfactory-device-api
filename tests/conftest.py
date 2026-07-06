import sys
from unittest.mock import AsyncMock, MagicMock

import pytest

_MISSING_MODULES = [
    "kafka",
    "kafka.errors",
    "openfactory",
    "openfactory.apps",
    "openfactory.assets",
    "openfactory.kafka",
]
for _mod in _MISSING_MODULES:
    sys.modules.setdefault(_mod, MagicMock())

from config import Config  # noqa: E402
from connection.registry import ConnectionRegistry  # noqa: E402
from connection.session import DeviceSession  # noqa: E402
from monitoring.equipment_monitor import EquipmentMonitor  # noqa: E402
from services.equipment_service import EquipmentService  # noqa: E402
from services.stream_service import StreamService  # noqa: E402


@pytest.fixture
def mock_ksql():
    mock_ksql_client = MagicMock()
    
    mock_ksql_client.query.return_value = [{"row": {"columns": ["data1", "data2"]}}]
    
    return mock_ksql_client

@pytest.fixture
def config():
    cfg = Config()
    cfg.websocket_host = "localhost"
    cfg.websocket_port = 9999
    cfg.ping_interval = 20
    cfg.ping_timeout = 10
    return cfg



@pytest.fixture
def equipment_service(mock_ksql):
    return EquipmentService(mock_ksql)

@pytest.fixture
def stream_service(mock_ksql):
    return StreamService(mock_ksql)

@pytest.fixture
def registry():
    return ConnectionRegistry()

@pytest.fixture
def mock_openfactory_app():
    return MagicMock()

@pytest.fixture
def monitor(stream_service, equipment_service, registry, mock_openfactory_app):
    topic_subscriber = MagicMock()
    return EquipmentMonitor(
        stream_service=stream_service,
        topic_subscriber=topic_subscriber,
        openfactory_app=mock_openfactory_app,
        equipment_service=equipment_service,
        registry=registry,
    )

@pytest.fixture
def session(registry, monitor, equipment_service, mock_openfactory_app):
    return DeviceSession(
        registry=registry,
        monitor=monitor,
        equipment_service=equipment_service,
        openfactory_app=mock_openfactory_app,
    )

@pytest.fixture
def mock_websocket():
    ws = AsyncMock()
    ws.remote_address = ("127.0.0.1", 12345)

    ws.request = MagicMock()
    ws.request.path = "/ws/equipments" 
    ws.send = AsyncMock()
    ws.recv = AsyncMock()
    ws.close = AsyncMock()
    return ws