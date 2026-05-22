import threading
from unittest.mock import MagicMock, patch


class TestSubscribe:
    def test_starts_consumer_thread(self, topic_subscriber):
        with patch.object(topic_subscriber, "_consume"):
            topic_subscriber.subscribe("topic-1", "group-1", MagicMock())
            assert "topic-1" in topic_subscriber._threads

    def test_does_not_subscribe_twice(self, topic_subscriber):
        topic_subscriber._consumers["topic-1"] = MagicMock()
        with patch.object(topic_subscriber, "_consume") as mock_consume:
            topic_subscriber.subscribe("topic-1", "group-1", MagicMock())
            mock_consume.assert_not_called()

    def test_creates_stop_flag(self, topic_subscriber):
        with patch.object(topic_subscriber, "_consume"):
            topic_subscriber.subscribe("topic-1", "group-1", MagicMock())
            assert "topic-1" in topic_subscriber._stop_flags


class TestUnsubscribe:
    def test_sets_stop_flag(self, topic_subscriber):
        flag = threading.Event()
        topic_subscriber._stop_flags["topic-1"] = flag
        topic_subscriber._threads["topic-1"] = MagicMock()
        topic_subscriber.unsubscribe("topic-1")
        assert flag.is_set()

    def test_removes_thread(self, topic_subscriber):
        topic_subscriber._stop_flags["topic-1"] = threading.Event()
        topic_subscriber._threads["topic-1"] = MagicMock()
        topic_subscriber.unsubscribe("topic-1")
        assert "topic-1" not in topic_subscriber._threads

    def test_does_nothing_for_unknown_topic(self, topic_subscriber):
        topic_subscriber.unsubscribe("nonexistent")  # should not raise


class TestUnsubscribeAll:
    def test_unsubscribes_all_topics(self, topic_subscriber):
        for topic in ["topic-1", "topic-2"]:
            topic_subscriber._stop_flags[topic] = threading.Event()
            topic_subscriber._threads[topic] = MagicMock()
        topic_subscriber.unsubscribe_all()
        assert topic_subscriber._stop_flags == {}
        assert topic_subscriber._threads == {}


class TestActiveTopics:
    def test_returns_active_topics(self, topic_subscriber):
        topic_subscriber._consumers["topic-1"] = MagicMock()
        topic_subscriber._consumers["topic-2"] = MagicMock()
        assert set(topic_subscriber.active_topics()) == {"topic-1", "topic-2"}

    def test_returns_empty_when_no_consumers(self, topic_subscriber):
        assert topic_subscriber.active_topics() == []


class TestConsume:
    def _make_message(self, key: str, value: dict):
        msg = MagicMock()
        msg.key = key
        msg.value = value
        return msg

    def _make_consumer(self, messages: list):
        mock_consumer = MagicMock()
        mock_consumer.__iter__ = MagicMock(return_value=iter(messages))
        return mock_consumer

    def test_calls_on_message_for_matching_key(self, topic_subscriber):
        on_message = MagicMock()
        msg = self._make_message("EQUIPMENT-1", {"ID": "var1", "VALUE": "1.0"})
        topic_subscriber._stop_flags["topic-1"] = threading.Event()

        with patch("monitoring.topic_subscription.KafkaConsumer", return_value=self._make_consumer([msg])):
            topic_subscriber._consume("topic-1", "group-1", on_message, lambda key: key == "EQUIPMENT-1")

        on_message.assert_called_once_with("EQUIPMENT-1", {"ID": "var1", "VALUE": "1.0"})

    def test_skips_message_not_matching_filter(self, topic_subscriber):
        on_message = MagicMock()
        msg = self._make_message("EQUIPMENT-2", {"ID": "var1", "VALUE": "1.0"})
        topic_subscriber._stop_flags["topic-1"] = threading.Event()

        with patch("monitoring.topic_subscription.KafkaConsumer", return_value=self._make_consumer([msg])):
            topic_subscriber._consume("topic-1", "group-1", on_message, lambda key: key == "EQUIPMENT-1")

        on_message.assert_not_called()

    def test_skips_message_with_none_value(self, topic_subscriber):
        on_message = MagicMock()
        msg = self._make_message("EQUIPMENT-1", None)
        topic_subscriber._stop_flags["topic-1"] = threading.Event()

        with patch("monitoring.topic_subscription.KafkaConsumer", return_value=self._make_consumer([msg])):
            topic_subscriber._consume("topic-1", "group-1", on_message, None)

        on_message.assert_not_called()

    def test_calls_on_message_without_filter(self, topic_subscriber):
        on_message = MagicMock()
        msg = self._make_message("EQUIPMENT-1", {"ID": "var1", "VALUE": "1.0"})
        topic_subscriber._stop_flags["topic-1"] = threading.Event()

        with patch("monitoring.topic_subscription.KafkaConsumer", return_value=self._make_consumer([msg])):
            topic_subscriber._consume("topic-1", "group-1", on_message, None)

        on_message.assert_called_once()

    def test_stops_consuming_when_flag_set(self, topic_subscriber):
        on_message = MagicMock()
        stop_flag = threading.Event()
        stop_flag.set()
        topic_subscriber._stop_flags["topic-1"] = stop_flag
        msg = self._make_message("EQUIPMENT-1", {"ID": "var1", "VALUE": "1.0"})

        with patch("monitoring.topic_subscription.KafkaConsumer", return_value=self._make_consumer([msg])):
            topic_subscriber._consume("topic-1", "group-1", on_message, None)

        on_message.assert_not_called()

    def test_closes_consumer_on_completion(self, topic_subscriber):
        topic_subscriber._stop_flags["topic-1"] = threading.Event()
        mock_consumer = self._make_consumer([])

        with patch("monitoring.topic_subscription.KafkaConsumer", return_value=mock_consumer):
            topic_subscriber._consume("topic-1", "group-1", MagicMock(), None)

        mock_consumer.close.assert_called_once()

    def test_closes_consumer_on_exception(self, topic_subscriber):
        topic_subscriber._stop_flags["topic-1"] = threading.Event()
        mock_consumer = MagicMock()
        mock_consumer.__iter__ = MagicMock(side_effect=Exception("kafka error"))

        with patch("monitoring.topic_subscription.KafkaConsumer", return_value=mock_consumer):
            topic_subscriber._consume("topic-1", "group-1", MagicMock(), None)

        mock_consumer.close.assert_called_once()