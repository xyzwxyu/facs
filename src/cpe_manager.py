from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import UTC, datetime, timedelta
from typing import Any

from src.db import Database


def _values(row: Any) -> dict[str, Any]:
    if isinstance(row, Mapping):
        return dict(row)
    if hasattr(row, "_mapping"):
        return dict(row._mapping)
    return {key: value for key, value in vars(row).items() if not key.startswith("_")}


class CPE:
    def __init__(
        self,
        id=None,
        device_id=None,
        serial_number=None,
        manufacturer=None,
        oui=None,
        product_class=None,
        software_version=None,
        hardware_version=None,
        ip_address=None,
        connection_request_url=None,
        **kwargs,
    ):
        self.id = id
        self.serial_number = serial_number
        self.manufacturer = manufacturer
        self.oui = oui
        self.device_id = device_id or (f"{oui}-{serial_number}" if oui and serial_number else None)
        self.product_class = product_class
        self.software_version = software_version
        self.hardware_version = hardware_version
        self.ip_address = ip_address
        self.connection_request_url = connection_request_url
        self.last_inform = kwargs.get("last_inform")
        self.created_at = kwargs.get("created_at") or datetime.now(UTC)
        self.session_id = kwargs.get("session_id")
        explicit_online = kwargs.get("is_online")
        self.is_online = explicit_online if explicit_online is not None else self._recently_informed()

    def _recently_informed(self) -> bool:
        if not isinstance(self.last_inform, datetime):
            return False
        value = self.last_inform
        if value.tzinfo is None:
            value = value.replace(tzinfo=UTC)
        return datetime.now(UTC) - value <= timedelta(minutes=10)

    def __repr__(self) -> str:
        return f"<CPE device_id={self.device_id!r}>"


class CPEManager:
    def __init__(self, db: Database):
        self.db = db

    async def add_cpe(self, cpe: CPE) -> CPE:
        row = await self.db.add_device(cpe)
        return CPE(**_values(row))

    async def get_cpe(self, cpe_or_device_id) -> CPE | None:
        row = await self.db.get_device(cpe_or_device_id)
        return CPE(**_values(row)) if row else None

    async def update_cpe(self, cpe: CPE) -> CPE | None:
        if not cpe.device_id:
            return None
        row = await self.db.update_device(
            cpe.device_id,
            manufacturer=cpe.manufacturer,
            oui=cpe.oui,
            product_class=cpe.product_class,
            software_version=cpe.software_version,
            hardware_version=cpe.hardware_version,
            ip_address=cpe.ip_address,
            connection_request_url=cpe.connection_request_url,
            last_inform=cpe.last_inform,
        )
        return CPE(**_values(row)) if row else None

    async def update_parameters(self, device_id: str, parameters: Sequence[Mapping[str, str]]) -> None:
        await self.db.upsert_device_parameters(device_id, list(parameters))

    async def remove_cpe(self, cpe_or_device_id) -> bool:
        identifier = (
            cpe_or_device_id
            if isinstance(cpe_or_device_id, str)
            else getattr(cpe_or_device_id, "device_id", None)
        )
        return bool(identifier and await self.db.delete_device(identifier))

    async def remove_all_cpe(self) -> int:
        return await self.db.delete_all_devices()

    async def get_all_cpe(self, *, offset: int = 0, limit: int = 1000) -> list[CPE]:
        rows = await self.db.get_devices(offset=offset, limit=limit)
        return [CPE(**_values(row)) for row in rows]
