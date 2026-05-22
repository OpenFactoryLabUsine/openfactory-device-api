import json

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
