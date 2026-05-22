import json

import pytest


@pytest.fixture
def conn():
    return object()

def get_connected_devices_count(registry) -> int:
    return sum(len(c) for c in registry.device_connections.values())

def test_registry_starts_empty(registry):
    assert registry.total() == 0
    assert registry.device_connections == {}

@pytest.mark.asyncio
async def test_registry_tracks_connections(registry):
    await registry.add(device_uuid="device-1", websocket=conn)

    assert get_connected_devices_count(registry) == 1

@pytest.mark.asyncio
async def test_registry_removes_connection(registry):
    await registry.add(device_uuid="device-1", websocket=conn)
    await registry.remove(websocket=conn)

    assert get_connected_devices_count(registry) == 0

@pytest.mark.asyncio
async def test_registry_removes_all_connections_to_same_device(registry):
    other_conn = object()
    await registry.add(device_uuid="device-1", websocket=conn)
    await registry.add(device_uuid="device-1", websocket=other_conn)

    await registry.remove_all()

    assert get_connected_devices_count(registry) == 0

@pytest.mark.asyncio
async def test_broadcast_adds_msg_to_subscribed_queue(registry):
    await registry.add(device_uuid="device-1", websocket=conn)
    expected_msg = {'event':'hello'}

    await registry.broadcast(device_uuid="device-1", message=expected_msg)

    queue = registry.get_queue(websocket=conn)
    actual_msg = await queue.get()
    assert expected_msg == json.loads(actual_msg)

@pytest.mark.asyncio
async def test_broadcast_does_not_add_msg_to_other_queue(registry):
    await registry.add(device_uuid="device-2", websocket=conn)
    expected_msg = "hello"

    await registry.broadcast(device_uuid="device-1", message=expected_msg)

    queue = registry.get_queue(websocket=conn)
    assert queue.empty()