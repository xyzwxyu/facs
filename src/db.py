"""PostgreSQL persistence and atomic RPC task claiming."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import (
    JSON,
    Boolean,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    delete,
    or_,
    select,
    update,
)
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from src.config import DatabaseSettings, require_asyncpg_url

PARAMETER_UPSERT_BATCH_SIZE = 1_000


def utc_now() -> datetime:
    return datetime.now(UTC)


class Base(DeclarativeBase):
    pass


class Device(Base):
    __tablename__ = "devices"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    device_id: Mapped[str] = mapped_column(String(255), unique=True, nullable=False, index=True)
    serial_number: Mapped[str] = mapped_column(String(128), unique=True, nullable=False, index=True)
    manufacturer: Mapped[str | None] = mapped_column(String(128))
    oui: Mapped[str] = mapped_column(String(6), nullable=False)
    product_class: Mapped[str | None] = mapped_column(String(128))
    software_version: Mapped[str | None] = mapped_column(String(128))
    hardware_version: Mapped[str | None] = mapped_column(String(128))
    ip_address: Mapped[str | None] = mapped_column(String(45))
    connection_request_url: Mapped[str | None] = mapped_column(Text)
    last_inform: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)


class DeviceParameter(Base):
    __tablename__ = "device_parameters"
    __table_args__ = (
        UniqueConstraint("device_id", "path", name="uq_device_parameter_path"),
        Index("idx_device_parameters_device_path", "device_id", "path"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    device_id: Mapped[str] = mapped_column(
        String(255),
        ForeignKey("devices.device_id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    path: Mapped[str] = mapped_column(String(1024), nullable=False)
    value: Mapped[str | None] = mapped_column(Text)
    value_type: Mapped[str] = mapped_column(String(64), default="xsd:string")
    writable: Mapped[bool] = mapped_column(Boolean, default=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now, onupdate=utc_now)


class TaskQueue(Base):
    __tablename__ = "task_queue"
    __table_args__ = (Index("idx_task_queue_device_status_created", "device_id", "status", "created_at"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    task_id: Mapped[str] = mapped_column(String(64), unique=True, nullable=False, index=True)
    device_id: Mapped[str] = mapped_column(
        String(255),
        ForeignKey("devices.device_id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    command_type: Mapped[str] = mapped_column(String(64), nullable=False)
    command: Mapped[Any] = mapped_column(JSON, nullable=True)
    command_key: Mapped[str | None] = mapped_column(String(64))
    status: Mapped[str] = mapped_column(String(32), default="pending", nullable=False, index=True)
    retry_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)
    sent_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class Files(Base):
    __tablename__ = "files"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    filename: Mapped[str] = mapped_column(String(255), nullable=False, unique=True)
    file_type: Mapped[str] = mapped_column(String(128), nullable=False)
    content: Mapped[bytes] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)


def _mapping(result) -> Mapping[str, Any] | None:
    row = result.mappings().first()
    return dict(row) if row is not None else None


class Database:
    """Async database interface. One instance owns one engine and pool."""

    def __init__(self, database_url: str | None = None):
        settings = DatabaseSettings.from_env()
        self.task_lease_seconds = settings.task_lease_seconds
        self.task_max_retries = settings.task_max_retries
        self.engine: AsyncEngine = create_async_engine(
            require_asyncpg_url(database_url or settings.url),
            pool_pre_ping=True,
            pool_size=settings.pool_size,
            max_overflow=settings.max_overflow,
            pool_recycle=1800,
        )
        self.SessionLocal = async_sessionmaker(bind=self.engine, expire_on_commit=False)

    async def init_db(self) -> None:
        async with self.engine.begin() as connection:
            await connection.run_sync(Base.metadata.create_all)

    async def add_device(self, cpe) -> Mapping[str, Any]:
        values = {
            "device_id": cpe.device_id,
            "serial_number": cpe.serial_number,
            "manufacturer": cpe.manufacturer,
            "oui": cpe.oui,
            "product_class": cpe.product_class,
            "software_version": cpe.software_version,
            "hardware_version": cpe.hardware_version,
            "ip_address": cpe.ip_address,
            "connection_request_url": cpe.connection_request_url,
            "last_inform": cpe.last_inform,
            "created_at": cpe.created_at,
        }
        insert_statement = pg_insert(Device).values(**values)
        statement = insert_statement.on_conflict_do_update(
            index_elements=[Device.device_id],
            set_={key: value for key, value in values.items() if key not in {"device_id", "created_at"}},
        ).returning(*Device.__table__.c)
        async with self.SessionLocal.begin() as session:
            result = await session.execute(statement)
            return dict(result.mappings().one())

    async def get_device(self, cpe_or_device_id) -> Mapping[str, Any] | None:
        identifier = (
            cpe_or_device_id
            if isinstance(cpe_or_device_id, str)
            else getattr(cpe_or_device_id, "device_id", None)
        )
        if not identifier:
            return None
        statement = select(Device.__table__).where(
            or_(Device.device_id == identifier, Device.serial_number == identifier)
        ).limit(1)
        async with self.SessionLocal() as session:
            return _mapping(await session.execute(statement))

    async def get_devices(self, *, offset: int = 0, limit: int = 1000) -> Sequence[Mapping[str, Any]]:
        statement = select(Device.__table__).order_by(Device.id).offset(offset).limit(limit)
        async with self.SessionLocal() as session:
            result = await session.execute(statement)
            return [dict(row) for row in result.mappings().all()]

    async def update_device(self, identifier: str, **kwargs) -> Mapping[str, Any] | None:
        allowed = {
            "manufacturer",
            "oui",
            "product_class",
            "software_version",
            "hardware_version",
            "ip_address",
            "connection_request_url",
            "last_inform",
        }
        values = {key: value for key, value in kwargs.items() if key in allowed}
        if not values:
            return await self.get_device(identifier)
        statement = (
            update(Device)
            .where(or_(Device.device_id == identifier, Device.serial_number == identifier))
            .values(**values)
            .returning(*Device.__table__.c)
        )
        async with self.SessionLocal.begin() as session:
            return _mapping(await session.execute(statement))

    async def upsert_device_parameters(self, device_id: str, parameters: list[Mapping[str, str]]) -> None:
        if not parameters:
            return
        timestamp = utc_now()
        values = [
            {
                "device_id": device_id,
                "path": parameter["Name"],
                "value": parameter.get("Value"),
                "value_type": parameter.get("Type", "xsd:string"),
                "updated_at": timestamp,
            }
            for parameter in parameters
            if parameter.get("Name")
        ]
        if not values:
            return
        async with self.SessionLocal.begin() as session:
            for start in range(0, len(values), PARAMETER_UPSERT_BATCH_SIZE):
                statement = pg_insert(DeviceParameter).values(values[start : start + PARAMETER_UPSERT_BATCH_SIZE])
                statement = statement.on_conflict_do_update(
                    index_elements=[DeviceParameter.device_id, DeviceParameter.path],
                    set_={
                        "value": statement.excluded.value,
                        "value_type": statement.excluded.value_type,
                        "updated_at": statement.excluded.updated_at,
                    },
                )
                await session.execute(statement)

    async def delete_device(self, identifier: str) -> bool:
        statement = delete(Device).where(
            or_(Device.device_id == identifier, Device.serial_number == identifier)
        ).returning(Device.id)
        async with self.SessionLocal.begin() as session:
            result = await session.execute(statement)
            return result.scalar_one_or_none() is not None

    async def delete_all_devices(self) -> int:
        async with self.SessionLocal.begin() as session:
            result = await session.execute(delete(Device).returning(Device.id))
            return len(result.scalars().all())

    async def add_task(self, task) -> Mapping[str, Any]:
        statement = (
            pg_insert(TaskQueue)
            .values(
                task_id=task.task_id,
                device_id=task.device_id,
                command_type=task.command_type,
                command=task.command,
                command_key=task.command_key,
                status="pending",
                retry_count=task.retry_count,
                created_at=task.created_at,
            )
            .returning(*TaskQueue.__table__.c)
        )
        async with self.SessionLocal.begin() as session:
            result = await session.execute(statement)
            return dict(result.mappings().one())

    async def get_task(self, task_id: str) -> Mapping[str, Any] | None:
        statement = select(TaskQueue.__table__).where(TaskQueue.task_id == task_id)
        async with self.SessionLocal() as session:
            return _mapping(await session.execute(statement))

    async def claim_pending_task(self, device_id: str) -> Mapping[str, Any] | None:
        """Atomically claim one FIFO task so concurrent sessions cannot duplicate it."""
        async with self.SessionLocal.begin() as session:
            lease_expired = utc_now() - timedelta(seconds=self.task_lease_seconds)
            # A process or HTTP session can disappear after claiming a task. A
            # bounded lease prevents that RPC from remaining `sent` forever.
            await session.execute(
                update(TaskQueue)
                .where(
                    TaskQueue.device_id == device_id,
                    TaskQueue.status == "sent",
                    or_(TaskQueue.sent_at.is_(None), TaskQueue.sent_at < lease_expired),
                    TaskQueue.retry_count < self.task_max_retries,
                )
                .values(
                    status="pending",
                    sent_at=None,
                    retry_count=TaskQueue.retry_count + 1,
                )
            )
            await session.execute(
                update(TaskQueue)
                .where(
                    TaskQueue.device_id == device_id,
                    TaskQueue.status == "sent",
                    or_(TaskQueue.sent_at.is_(None), TaskQueue.sent_at < lease_expired),
                    TaskQueue.retry_count >= self.task_max_retries,
                )
                .values(status="failed", completed_at=utc_now())
            )
            next_id = await session.scalar(
                select(TaskQueue.task_id)
                .where(TaskQueue.device_id == device_id, TaskQueue.status == "pending")
                .order_by(TaskQueue.created_at, TaskQueue.id)
                .with_for_update(skip_locked=True)
                .limit(1)
            )
            if next_id is None:
                return None
            result = await session.execute(
                update(TaskQueue)
                .where(TaskQueue.task_id == next_id, TaskQueue.status == "pending")
                .values(status="sent", sent_at=utc_now())
                .returning(*TaskQueue.__table__.c)
            )
            return _mapping(result)

    async def get_all_tasks(
        self,
        device_id: str,
        *,
        offset: int = 0,
        limit: int = 1000,
    ) -> Sequence[Mapping[str, Any]]:
        statement = (
            select(TaskQueue.__table__)
            .where(TaskQueue.device_id == device_id)
            .order_by(TaskQueue.created_at.desc(), TaskQueue.id.desc())
            .offset(offset)
            .limit(limit)
        )
        async with self.SessionLocal() as session:
            result = await session.execute(statement)
            return [dict(row) for row in result.mappings().all()]

    async def update_task_status(self, task_id: str, status: str) -> Mapping[str, Any] | None:
        if status not in {"completed", "failed", "pending"}:
            raise ValueError(f"invalid task status: {status}")
        values: dict[str, Any] = {"status": status}
        if status in {"completed", "failed"}:
            values["completed_at"] = utc_now()
        statement = (
            update(TaskQueue)
            .where(TaskQueue.task_id == task_id)
            .values(**values)
            .returning(*TaskQueue.__table__.c)
        )
        async with self.SessionLocal.begin() as session:
            return _mapping(await session.execute(statement))

    async def close(self) -> None:
        await self.engine.dispose()
