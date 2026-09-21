# src/core/event_bus.py

import asyncio
import time
import uuid
from dataclasses import dataclass, field
from typing import Any


@dataclass(slots=True)
class Event:
    type: str
    data: dict[str, Any] = field(default_factory=dict)
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    timestamp: float = field(default_factory=time.time)


class EventBus:
    def __init__(self, queue_size: int = 5000):
        self.queue_size = queue_size
        self.subscribers: dict[str, set[asyncio.Queue[Event]]] = {}
        self.lock = asyncio.Lock()
        self.published = 0
        self.dropped = 0

    async def subscribe(self, event_type: str = "*") -> asyncio.Queue[Event]:
        queue: asyncio.Queue[Event] = asyncio.Queue(maxsize=self.queue_size)

        async with self.lock:
            self.subscribers.setdefault(event_type, set()).add(queue)

        return queue

    async def unsubscribe(self, event_type: str, queue: asyncio.Queue[Event]) -> None:
        async with self.lock:
            queues = self.subscribers.get(event_type)
            if queues:
                queues.discard(queue)

                if not queues:
                    del self.subscribers[event_type]

    async def publish(self, event_type: str, data: dict[str, Any] | None = None) -> None:
        event = Event(
            type=event_type,
            data=data or {},
        )

        async with self.lock:
            self.published += 1
            targets = set()

            if event_type in self.subscribers:
                targets.update(self.subscribers[event_type])

            if "*" in self.subscribers:
                targets.update(self.subscribers["*"])

        for queue in targets:
            try:
                queue.put_nowait(event)
            except asyncio.QueueFull:
                # A slow websocket must not be silently unsubscribed forever. Keep
                # the newest state by evicting one old notification instead.
                try:
                    queue.get_nowait()
                except asyncio.QueueEmpty:
                    pass
                else:
                    self.dropped += 1
                try:
                    queue.put_nowait(event)
                except asyncio.QueueFull:
                    self.dropped += 1

    def get_stats(self) -> dict[str, int]:
        return {
            "published": self.published,
            "dropped": self.dropped,
            "subscribers": sum(len(queues) for queues in self.subscribers.values()),
        }
