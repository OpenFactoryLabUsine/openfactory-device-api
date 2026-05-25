import json

import pytest


@pytest.fixture
def conn():
    return object()

def get_connected_equipments_count(registry) -> int:
    return sum(len(c) for c in registry.equipment_connections.values())

def test_registry_starts_empty(registry):
    assert registry.total() == 0
    assert registry.equipment_connections == {}

@pytest.mark.asyncio
async def test_registry_tracks_connections(registry):
    await registry.add(asset_uuid="equipment-1", websocket=conn)

    assert get_connected_equipments_count(registry) == 1

@pytest.mark.asyncio
async def test_registry_removes_connection(registry):
    await registry.add(asset_uuid="equipment-1", websocket=conn)
    await registry.remove(websocket=conn)

    assert get_connected_equipments_count(registry) == 0
    assert registry.equipment_connections == {}
    assert registry.active_equipment_count() == 0

@pytest.mark.asyncio
async def test_registry_active_equipment_count(registry):
    await registry.add(asset_uuid="equipment-1", websocket=conn)
    other_conn = object()
    await registry.add(asset_uuid="equipment-2", websocket=other_conn)

    assert registry.active_equipment_count() == 2

@pytest.mark.asyncio
async def test_registry_removes_all_connections_to_same_equipment(registry):
    other_conn = object()
    await registry.add(asset_uuid="equipment-1", websocket=conn)
    await registry.add(asset_uuid="equipment-1", websocket=other_conn)

    await registry.remove_all()

    assert get_connected_equipments_count(registry) == 0

@pytest.mark.asyncio
async def test_broadcast_adds_msg_to_subscribed_queue(registry):
    await registry.add(asset_uuid="equipment-1", websocket=conn)
    expected_msg = {'event':'hello'}

    await registry.broadcast(asset_uuid="equipment-1", message=expected_msg)

    queue = registry.get_queue(websocket=conn)
    actual_msg = await queue.get()
    assert expected_msg == json.loads(actual_msg)

@pytest.mark.asyncio
async def test_broadcast_does_not_add_msg_to_other_queue(registry):
    await registry.add(asset_uuid="equipment-2", websocket=conn)
    expected_msg = "hello"

    await registry.broadcast(asset_uuid="equipment-1", message=expected_msg)

    queue = registry.get_queue(websocket=conn)
    assert queue.empty()