import asyncio
import threading
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from monitoring.asset_subscription import AssetSubscriber


@pytest.fixture
def subscriber():
    return AssetSubscriber()


class FakeSubscription:
    def __init__(self, messages):
        self._messages = list(messages)
        self.unsubscribed = False

    async def next_msg(self):
        if not self._messages:
            raise StopAsyncIteration
        return self._messages.pop(0)

    async def unsubscribe(self):
        self.unsubscribed = True


class TestSubscriptionLifecycle:
    def test_subscribe_registers_the_handler_and_schedules_registration(self, subscriber):
        handler = MagicMock()
        scheduled = []

        def fake_schedule(callback, *args):
            scheduled.append((callback, args))

        subscriber._schedule = fake_schedule

        subscriber.subscribe("subject-1", on_message=handler)

        assert subscriber._message_handlers["subject-1"] is handler
        assert "subject-1" in subscriber._stop_flags
        assert getattr(scheduled[0][0], "__name__", None) == "_register_subscription"
        assert scheduled[0][1] == ("subject-1",)

    def test_subscribe_rejects_missing_inputs(self, subscriber):
        with pytest.raises(ValueError):
            subscriber.subscribe(None, on_message=MagicMock())

        with pytest.raises(ValueError):
            subscriber.subscribe("subject-1", on_message=None)

    def test_subscribe_does_not_duplicate_an_existing_subscription(self, subscriber):
        subscriber._subscriptions["subject-1"] = MagicMock()
        scheduled = []
        subscriber._schedule = lambda callback, *args: scheduled.append((callback, args))

        subscriber.subscribe("subject-1", on_message=MagicMock())

        assert scheduled == []


class TestUnsubscribe:
    def test_unsubscribe_marks_the_stop_flag_and_clears_handler_state(self, subscriber):
        stop_flag = threading.Event()
        subscriber._stop_flags["subject-1"] = stop_flag
        subscriber._message_handlers["subject-1"] = MagicMock()
        subscriber._message_filters["subject-1"] = lambda key: True
        subscriber._subscriptions["subject-1"] = MagicMock()

        def run_schedule(callback, *args):
            asyncio.run(callback(*args))

        subscriber._schedule = run_schedule

        subscriber.unsubscribe("subject-1")

        assert stop_flag.is_set()
        assert "subject-1" not in subscriber._message_handlers
        assert "subject-1" not in subscriber._message_filters
        assert "subject-1" not in subscriber._stop_flags

    def test_unsubscribe_all_stops_every_subject(self, subscriber):
        for subject in ["subject-1", "subject-2"]:
            subscriber._stop_flags[subject] = threading.Event()
            subscriber._message_handlers[subject] = MagicMock()

        subscriber._schedule = lambda callback, *args: None

        subscriber.unsubscribe_all()

        assert subscriber._stop_flags == {}
        assert subscriber._message_handlers == {}


class TestActiveSubjects:
    def test_reports_the_current_subscriptions(self, subscriber):
        subscriber._subscriptions["subject-1"] = object()
        subscriber._subscriptions["subject-2"] = object()

        assert set(subscriber.active_subjects()) == {"subject-1", "subject-2"}
        assert subscriber.active_subjects() == ["subject-1", "subject-2"]


class TestMessageHandling:
    def test_delivers_normalized_payloads_to_the_registered_handler(self, subscriber):
        handler = MagicMock()
        subscriber._message_handlers["subject-1"] = handler
        subscriber._message_filters["subject-1"] = lambda key: key == "DUSTTRAK"
        subscriber._stop_flags["subject-1"] = threading.Event()

        message = SimpleNamespace(
            subject="DUSTTRAK.pm2_5_concentration",
            data=b'{"VALUE": 1.23, "TAG": "DustTrak.pm2_5_concentration", "attributes": {"timestamp": "2026-07-06T14:42:07.908Z"}}',
        )
        subscription = FakeSubscription([message])

        asyncio.run(subscriber._listen_for_messages("subject-1", subscription))

        handler.assert_called_once()
        key, payload = handler.call_args.args
        assert key == "DUSTTRAK"
        assert payload["ID"] == "DustTrak.pm2_5_concentration"
        assert payload["VALUE"] == 1.23
        assert payload["TIMESTAMP"] == "2026-07-06T14:42:07.908Z"
        assert subscription.unsubscribed is True

    def test_skips_messages_that_do_not_match_the_filter(self, subscriber):
        handler = MagicMock()
        subscriber._message_handlers["subject-1"] = handler
        subscriber._message_filters["subject-1"] = lambda key: key == "OTHER"
        subscriber._stop_flags["subject-1"] = threading.Event()

        message = SimpleNamespace(subject="DUSTTRAK.pm2_5_concentration", data=b'{"VALUE": 1.23}')
        subscription = FakeSubscription([message])

        asyncio.run(subscriber._listen_for_messages("subject-1", subscription))

        handler.assert_not_called()


@pytest.mark.parametrize(
    ("payload", "expected"),
    [
        (
            {"VALUE": 1.23, "TAG": "DustTrak.pm2_5_concentration", "attributes": {"timestamp": "2026-07-06T14:42:07.908Z"}},
            {"ID": "DustTrak.pm2_5_concentration", "VALUE": 1.23, "TIMESTAMP": "2026-07-06T14:42:07.908Z"},
        ),
        ({"ID": "foo", "VALUE": 1.23, "TIMESTAMP": "2026-07-06T14:42:07.908Z"}, {"ID": "foo", "VALUE": 1.23, "TIMESTAMP": "2026-07-06T14:42:07.908Z"}),
        ({"VALUE": 1.23}, {"VALUE": 1.23}),
    ],
)
def test_normalize_payload(payload, expected):
    subscriber = AssetSubscriber()
    assert subscriber._normalize_payload(payload) == expected
