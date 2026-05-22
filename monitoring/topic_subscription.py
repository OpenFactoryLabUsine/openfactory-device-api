import json
import threading
from collections.abc import Callable

from kafka import KafkaConsumer


class TopicSubscriber:
    def __init__(self, bootstrap_servers: str):
        self._bootstrap_servers = bootstrap_servers
        self._consumers: dict[str, KafkaConsumer] = {}
        self._threads: dict[str, threading.Thread] = {}
        self._stop_flags: dict[str, threading.Event] = {}

    def subscribe(
        self,
        topic: str,
        group_id: str,
        on_message: Callable[[str, dict], None],
        message_filter: Callable[[str], bool] | None = None,
    ):
        if topic in self._consumers:
            print(f"Already subscribed to {topic}")
            return

        self._stop_flags[topic] = threading.Event()
        thread = threading.Thread(
            target=self._consume,
            args=(topic, group_id, on_message, message_filter),
            daemon=True,
        )
        thread.start()
        self._threads[topic] = thread

    def unsubscribe(self, topic: str):
        self._stop_flags.get(topic, threading.Event()).set()
        if thread := self._threads.pop(topic, None):
            thread.join(timeout=5)
        self._stop_flags.pop(topic, None)

    def unsubscribe_all(self):
        for topic in list(self._stop_flags):
            self.unsubscribe(topic)

    def active_topics(self) -> list[str]:
        return list(self._consumers)

    def _consume(
        self,
        topic: str,
        group_id: str,
        on_message: Callable[[str, dict], None],
        message_filter: Callable[[str], bool] | None,
    ):
        try:
            consumer = KafkaConsumer(
                topic,
                bootstrap_servers=self._bootstrap_servers,
                group_id=group_id,
                value_deserializer=lambda m: (
                    json.loads(m.decode("utf-8")) if m else None
                ),
                key_deserializer=lambda m: m.decode("utf-8") if m else None,
                auto_offset_reset="latest",
                enable_auto_commit=True,
            )
            self._consumers[topic] = consumer

            for msg in consumer:
                if self._stop_flags[topic].is_set():
                    break
                if msg.value and (not message_filter or message_filter(msg.key)):
                    on_message(msg.key, msg.value)

        except Exception as e:
            print(f"Consumer error for {topic}: {e}")
        finally:
            if topic in self._consumers:
                self._consumers.pop(topic).close()
