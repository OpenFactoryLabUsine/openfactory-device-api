import asyncio
import json
from unittest.mock import AsyncMock, MagicMock

import pytest
from websockets import ConnectionClosed


@pytest.fixture
def patch_sleep(monkeypatch):
    mock = AsyncMock()
    monkeypatch.setattr("asyncio.sleep", mock)
    return mock

@pytest.mark.asyncio
async def test_accept_wrong_path_sends_error(session, mock_websocket):
    mock_websocket.request.path = "/invalid"

    await session.accept(mock_websocket)

    sent = json.loads(mock_websocket.send.call_args[0][0])
    print(sent)
    assert sent["event"] == "error"

@pytest.mark.asyncio
async def test_accept_equipments_list_path(session, mock_websocket):
    mock_websocket.request.path = "/equipments"
    mock_websocket.send.side_effect = [None, ConnectionClosed(None, None)]

    await session.accept(mock_websocket)

    sent_data = json.loads(mock_websocket.send.call_args_list[0][0][0])
    assert "equipments" in sent_data
    assert isinstance(sent_data["equipments"], list)

@pytest.mark.asyncio
async def test_send_next_update_sends_ping(session, mock_websocket):
    session._equipment_service.equipment_list_has_changed = MagicMock(return_value=False)
    
    await session._send_next_equipment_update(mock_websocket)
    
    assert "ping" in mock_websocket.send.call_args[0][0]

@pytest.mark.asyncio
async def test_send_next_update_sends_list(session, mock_websocket):
    session._equipment_service.equipment_list_has_changed = MagicMock(return_value=True)
    session._equipment_service.get_all_equipments = MagicMock(return_value=["DEV1"])
    
    await session._send_next_equipment_update(mock_websocket)
    
    assert "equipments" in mock_websocket.send.call_args[0][0]

@pytest.mark.asyncio
async def test_pipe_outgoing_sends_ping_with_active_equipment_count(
    session, registry, mock_websocket
):
    mock_websocket.send = AsyncMock(side_effect=ConnectionClosed(None, None))

    await registry.add(websocket=mock_websocket, asset_uuid="CNC")

    queue = registry.get_queue(mock_websocket)
    assert queue is not None
    queue.get = AsyncMock(side_effect=asyncio.TimeoutError)

    await session._pipe_outgoing(mock_websocket)

    assert mock_websocket.send.await_count == 1
    assert mock_websocket.send.await_args is not None
    ping_payload = json.loads(mock_websocket.send.await_args[0][0])
    assert ping_payload["event"] == "ping"
    assert ping_payload["active_equipments"] == 1
