import asyncio
import json
import os
import threading
from collections.abc import Callable
from typing import Any

import nats
from nats.aio.client import Client as NatsClient


class AssetSubscriber:
    def __init__(self):
        self._nats_server = os.getenv("NATS_SERVER", "nats://localhost:4222")
        self._loop: asyncio.AbstractEventLoop | None = None
        self._loop_thread: threading.Thread | None = None
        self._event_loop_ready = threading.Event()
        self._connection: NatsClient | None = None
        self._subscriptions: dict[str, Any] = {}
        self._listener_tasks: dict[str, asyncio.Task] = {}
        self._stop_flags: dict[str, threading.Event] = {}
        self._message_handlers: dict[str, Callable[[str, dict], None]] = {}
        self._message_filters: dict[str, Callable[[str], bool] | None] = {}

    def subscribe(
        self,
        subject: str | None = None,
        on_message: Callable[[str, dict], None] | None = None,
        message_filter: Callable[[str], bool] | None = None,
        **_: Any,
    ):
        if not subject or on_message is None:
            raise ValueError("subject/topic and on_message are required")

        if subject in self._subscriptions or subject in self._listener_tasks:
            print(f"Already subscribed to {subject}")
            return

        self._stop_flags[subject] = threading.Event()
        self._message_handlers[subject] = on_message
        self._message_filters[subject] = message_filter
        self._ensure_loop_started()
        self._schedule(self._register_subscription, subject)

    def unsubscribe(self, subject: str):
        stop_flag = self._stop_flags.get(subject)
        if stop_flag is not None:
            stop_flag.set()

        self._message_handlers.pop(subject, None)
        self._message_filters.pop(subject, None)
        self._stop_flags.pop(subject, None)
        self._schedule(self._remove_subscription, subject)

    def unsubscribe_all(self):
        for subject in list(self._stop_flags):
            self.unsubscribe(subject)

    def active_subjects(self) -> list[str]:
        return list(self._subscriptions)

    def _ensure_loop_started(self):
        if self._loop_thread is not None and self._loop_thread.is_alive():
            return

        self._event_loop_ready.clear()
        self._loop_thread = threading.Thread(target=self._run_event_loop, daemon=True)
        self._loop_thread.start()
        self._event_loop_ready.wait(timeout=5)

    def _run_event_loop(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        self._loop = loop
        self._event_loop_ready.set()
        try:
            loop.run_forever()
        finally:
            loop.run_until_complete(self._close_connection())
            loop.close()
            self._loop = None

    def _schedule(self, coro_factory: Callable[..., Any], *args: Any):
        if self._loop is None:
            self._ensure_loop_started()
        if self._loop is not None:
            self._loop.call_soon_threadsafe(self._spawn, coro_factory, *args)

    def _spawn(self, coro_factory: Callable[..., Any], *args: Any):
        coroutine = coro_factory(*args)
        if asyncio.iscoroutine(coroutine):
            asyncio.create_task(coroutine)

    async def _register_subscription(self, subject: str):
        try:
            if self._connection is None:
                await self._connect_once()

            if self._connection is None:
                raise RuntimeError("NATS connection is not available")

            subscription = await self._connection.subscribe(subject)
            await self._connection.flush()
            self._subscriptions[subject] = subscription
            self._listener_tasks[subject] = asyncio.create_task(
                self._listen_for_messages(subject, subscription)
            )
        except Exception as exc:
            self._stop_flags.pop(subject, None)
            self._message_handlers.pop(subject, None)
            self._message_filters.pop(subject, None)
            print(f"Failed to register NATS subscription for {subject}: {exc}")

    async def _listen_for_messages(self, subject: str, subscription: Any):
        print(f"Connected to NATS subject {subject}")
        try:
            while True:
                stop_flag = self._stop_flags.get(subject)
                if stop_flag is None or stop_flag.is_set():
                    break
                try:
                    message = await asyncio.wait_for(subscription.next_msg(), timeout=0.5)
                except TimeoutError:
                    continue
                except Exception as exc:
                    print(f"Error reading NATS message for {subject}: {exc}")
                    break

                if message is None:
                    continue

                payload = self._decode_payload(message.data)
                if payload is None:
                    continue

                normalized_payload = self._normalize_payload(payload)
                if normalized_payload is None:
                    continue

                key = self._extract_key(message.subject)
                handler = self._message_handlers.get(subject)
                message_filter = self._message_filters.get(subject)
                if handler is not None and (message_filter is None or message_filter(key)):
                    try:
                        handler(key, normalized_payload)
                    except Exception as e:
                        print(f"Error in NATS message handler for {subject}: {e}")
        except Exception as e:
            print(f"Listener error for {subject}: {e}")
        finally:
            await self._remove_subscription(subject, unsubscribe=True, subscription=subscription)

    async def _remove_subscription(self, subject: str, unsubscribe: bool = False, subscription: Any = None):
        task = self._listener_tasks.pop(subject, None)
        if task is not None:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

        if subscription is None:
            subscription = self._subscriptions.pop(subject, None)
        else:
            self._subscriptions.pop(subject, None)

        if unsubscribe and subscription is not None:
            try:
                await subscription.unsubscribe()
            except Exception as e:
                print(f"Failed to unsubscribe from {subject}: {e}")

        self._stop_flags.pop(subject, None)

    async def _connect_once(self):
        if self._connection is not None:
            return
        try:
            self._connection = await nats.connect(self._nats_server)
        except Exception as e:
            raise RuntimeError(f"Unable to connect to NATS server {self._nats_server}: {e}") from e

    async def _close_connection(self):
        if self._connection is None:
            return
        connection = self._connection
        self._connection = None
        try:
            await connection.close()
        except Exception as e:
            print(f"Failed to close NATS connection: {e}")

    @staticmethod
    def _extract_key(subject: str) -> str:
        if not subject:
            return ""
        return subject.split(".", 1)[0]

    @staticmethod
    def _decode_payload(raw_payload: Any) -> Any:
        if raw_payload is None:
            return None
        if isinstance(raw_payload, (bytes, bytearray)):
            raw_payload = raw_payload.decode("utf-8")
        if isinstance(raw_payload, str):
            raw_payload = raw_payload.strip()
            if not raw_payload:
                return None
            try:
                return json.loads(raw_payload)
            except json.JSONDecodeError:
                return raw_payload
        return raw_payload

    @staticmethod
    def _normalize_payload(payload: Any) -> Any:
        if not isinstance(payload, dict):
            return payload

        if "ID" in payload and "VALUE" in payload:
            return payload

        if "VALUE" in payload and "TAG" in payload:
            attributes = payload.get("attributes") or {}
            return {
                "ID": payload.get("TAG"),
                "VALUE": payload.get("VALUE"),
                "TIMESTAMP": attributes.get("timestamp"),
            }

        return payload