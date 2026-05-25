import json
from unittest.mock import AsyncMock

import pytest
from websockets import ConnectionClosed


@pytest.mark.asyncio
async def test_accept_wrong_path_sends_error(session, mock_websocket):
    mock_websocket.request.path = "/ws/invalid"

    await session.accept(mock_websocket)

    sent = json.loads(mock_websocket.send.call_args[0][0])
    print(sent)
    assert sent["event"] == "error"

@pytest.mark.asyncio
async def test_accept_equipments_list_path(session, mock_websocket):
    mock_websocket.request.path = "/ws/equipments"
    mock_websocket.send.side_effect = [None, ConnectionClosed(None, None)]

    await session.accept(mock_websocket)

    sent = json.loads(mock_websocket.send.call_args_list[0][0][0])
    print(sent)
    assert sent["event"] == "equipments_list"

@pytest.mark.asyncio
async def test_pipe_outgoing_sends_ping_with_active_equipment_count(
    session, registry, mock_websocket
):
    mock_websocket.request.path = "/ws/equipments/CNC"
    mock_websocket.send = AsyncMock(side_effect=ConnectionClosed(1000, 1000, False))

    await registry.add(asset_uuid="CNC", websocket=mock_websocket)

    await session._pipe_outgoing(mock_websocket)

    assert mock_websocket.send.await_count == 1
    ping_payload = json.loads(mock_websocket.send.await_args[0][0])
    assert ping_payload["event"] == "ping"
    assert ping_payload["active_equipments"] == 1
