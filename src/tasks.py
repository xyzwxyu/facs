from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any

from src.utils import generate_id


def _values(row: Any) -> dict[str, Any]:
    if isinstance(row, Mapping):
        return dict(row)
    if hasattr(row, "_mapping"):
        return dict(row._mapping)
    return {key: value for key, value in vars(row).items() if not key.startswith("_")}


class Task:
    def __init__(
        self,
        id=None,
        task_id=None,
        device_id: str | None = None,
        command_type: str | None = None,
        command: Any = None,
        command_key=None,
        status: str = "pending",
        retry_count: int = 0,
        created_at=None,
        sent_at=None,
        completed_at=None,
        **_extra,
    ):
        self.id = id
        self.task_id = task_id or generate_id(device_id)
        self.device_id = device_id
        self.command_type = command_type
        self.command = command
        self.command_key = command_key
        self.status = status or "pending"
        self.retry_count = retry_count
        self.created_at = created_at or datetime.now(UTC)
        self.sent_at = sent_at
        self.completed_at = completed_at

    def __repr__(self) -> str:
        return (
            f"<Task task_id={self.task_id!r} device_id={self.device_id!r} "
            f"command_type={self.command_type!r} status={self.status!r}>"
        )


class Tasks:
    def __init__(self, db):
        self.db = db

    async def add_task(self, task: Task) -> Task:
        row = await self.db.add_task(task)
        return Task(**_values(row))

    async def get_task(self, task_id: str) -> Task | None:
        row = await self.db.get_task(task_id)
        return Task(**_values(row)) if row else None

    async def add_task_raw(
        self,
        device_id: str,
        command_type: str,
        payload=None,
        command_key=None,
    ) -> Task:
        return await self.add_task(
            Task(
                device_id=device_id,
                command_type=command_type,
                command=payload,
                command_key=command_key,
            )
        )

    async def claim_next_task(self, device_id: str) -> Task | None:
        row = await self.db.claim_pending_task(device_id)
        return Task(**_values(row)) if row else None

    async def get_all_tasks(
        self,
        device_id: str,
        *,
        offset: int = 0,
        limit: int = 1000,
    ) -> list[Task]:
        rows = await self.db.get_all_tasks(device_id, offset=offset, limit=limit)
        return [Task(**_values(row)) for row in rows]

    async def complete_task(self, task_id: str, status: str) -> Task | None:
        row = await self.db.update_task_status(task_id, status)
        return Task(**_values(row)) if row else None
