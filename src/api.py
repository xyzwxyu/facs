"""Local management API consumed by the interactive CLI."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any

from aiohttp import web

from src.cwmp import SUPPORTED_TASK_METHODS
from src.tasks import Task

MAX_PAGE_SIZE = 1000


def _timestamp(value: Any) -> str | None:
    if value is None:
        return None
    if hasattr(value, "isoformat"):
        return value.isoformat()
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(value, tz=UTC).isoformat()
    return str(value)


def _page(request: web.Request) -> tuple[int, int]:
    try:
        offset = int(request.query.get("offset", "0"))
        limit = int(request.query.get("limit", "1000"))
    except ValueError as exc:
        raise web.HTTPBadRequest(text="offset and limit must be integers") from exc
    if offset < 0 or limit < 1 or limit > MAX_PAGE_SIZE:
        raise web.HTTPBadRequest(text=f"offset must be >= 0 and limit must be 1..{MAX_PAGE_SIZE}")
    return offset, limit


class ACSApi:
    def __init__(self, cpe_manager, tasks, event_bus):
        self.cpe_manager = cpe_manager
        self.tasks = tasks
        self.event_bus = event_bus

    def setup_routes(self, app: web.Application) -> None:
        app.router.add_get("/api/devices", self.list_devices)
        app.router.add_get("/api/devices/{device_id}", self.get_device)
        app.router.add_delete("/api/devices", self.delete_all_devices)
        app.router.add_delete("/api/devices/{device_id}", self.delete_device)
        app.router.add_post("/api/devices/{device_id}/rpc", self.create_rpc_task)
        app.router.add_get("/api/devices/{device_id}/tasks", self.get_device_tasks)

    @staticmethod
    def _device_to_dict(device) -> dict[str, Any]:
        return {
            "id": device.id,
            "device_id": device.device_id,
            "serial_number": device.serial_number,
            "manufacturer": device.manufacturer,
            "oui": device.oui,
            "product_class": device.product_class,
            "software_version": device.software_version,
            "hardware_version": device.hardware_version,
            "ip_address": device.ip_address,
            "connection_request_url": device.connection_request_url,
            "last_inform": _timestamp(device.last_inform),
            "created_at": _timestamp(device.created_at),
            "is_online": device.is_online,
        }

    @staticmethod
    def _task_to_dict(task: Task) -> dict[str, Any]:
        return {
            "id": task.id,
            "task_id": task.task_id,
            "device_id": task.device_id,
            "command_type": task.command_type,
            "command": task.command,
            "command_key": task.command_key,
            "status": task.status,
            "retry_count": task.retry_count,
            "created_at": _timestamp(task.created_at),
            "sent_at": _timestamp(task.sent_at),
            "completed_at": _timestamp(task.completed_at),
        }

    async def list_devices(self, request: web.Request) -> web.Response:
        offset, limit = _page(request)
        devices = await self.cpe_manager.get_all_cpe(offset=offset, limit=limit)
        return web.json_response(
            {"devices": [self._device_to_dict(device) for device in devices], "offset": offset, "limit": limit}
        )

    async def get_device(self, request: web.Request) -> web.Response:
        device_id = request.match_info["device_id"]
        device = await self.cpe_manager.get_cpe(device_id)
        if not device:
            raise web.HTTPNotFound(text="device not found")
        return web.json_response({"device": self._device_to_dict(device)})

    async def delete_device(self, request: web.Request) -> web.Response:
        device_id = request.match_info["device_id"]
        if not await self.cpe_manager.remove_cpe(device_id):
            raise web.HTTPNotFound(text="device not found")
        await self.event_bus.publish("cpe.unprovisioned", {"device_id": device_id})
        return web.json_response({"status": "deleted", "device_id": device_id})

    async def delete_all_devices(self, request: web.Request) -> web.Response:
        deleted_count = await self.cpe_manager.remove_all_cpe()
        await self.event_bus.publish("cpe.unprovisioned_all", {"deleted_count": deleted_count})
        return web.json_response({"status": "deleted_all", "deleted_count": deleted_count})

    async def create_rpc_task(self, request: web.Request) -> web.Response:
        device_id = request.match_info["device_id"]
        if await self.cpe_manager.get_cpe(device_id) is None:
            raise web.HTTPNotFound(text="device not found")
        try:
            data = await request.json()
        except (ValueError, TypeError) as exc:
            raise web.HTTPBadRequest(text="request body must be valid JSON") from exc
        if not isinstance(data, Mapping):
            raise web.HTTPBadRequest(text="request body must be a JSON object")

        method = data.get("method")
        if method not in SUPPORTED_TASK_METHODS:
            allowed = ", ".join(sorted(SUPPORTED_TASK_METHODS))
            raise web.HTTPBadRequest(text=f"unsupported method; allowed: {allowed}")
        payload = self._validate_payload(method, data.get("payload"))

        task = await self.tasks.add_task(
            Task(
                device_id=device_id,
                command_type=method,
                command=payload,
                command_key=data.get("command_key"),
            )
        )
        task_data = self._task_to_dict(task)
        await self.event_bus.publish("task.queued", task_data)
        return web.json_response({"status": "queued", "task": task_data}, status=201)

    @staticmethod
    def _validate_payload(method: str, payload: Any) -> Any:
        if method == "SetParameterValues":
            # Preserve the existing CLI's `setparam NAME VALUE [TYPE]` form.
            if isinstance(payload, list) and 2 <= len(payload) <= 3 and all(
                isinstance(value, str) for value in payload
            ):
                payload = [
                    {
                        "name": payload[0],
                        "value": payload[1],
                        "type": payload[2] if len(payload) == 3 else "xsd:string",
                    }
                ]
            if not isinstance(payload, list) or not payload or not all(isinstance(item, Mapping) for item in payload):
                raise web.HTTPBadRequest(text="SetParameterValues payload must contain parameter objects")
            for item in payload:
                name = item.get("name", item.get("Name"))
                if not isinstance(name, str) or not name:
                    raise web.HTTPBadRequest(text="each parameter requires a non-empty name")
            return payload
        if method == "GetParameterValues":
            if not isinstance(payload, list) or not payload or not all(isinstance(item, str) and item for item in payload):
                raise web.HTTPBadRequest(text="GetParameterValues payload must be a non-empty string list")
            return payload
        if method == "GetParameterNames":
            if payload is None:
                return {"path": "Device.", "next_level": False}
            if isinstance(payload, str):
                return {"path": payload, "next_level": False}
            if not isinstance(payload, Mapping) or not isinstance(payload.get("path", ""), str):
                raise web.HTTPBadRequest(text="GetParameterNames payload must contain path and optional next_level")
            return dict(payload)
        if payload not in (None, {}, []):
            raise web.HTTPBadRequest(text=f"{method} does not accept a payload")
        return None

    async def get_device_tasks(self, request: web.Request) -> web.Response:
        device_id = request.match_info["device_id"]
        if await self.cpe_manager.get_cpe(device_id) is None:
            raise web.HTTPNotFound(text="device not found")
        offset, limit = _page(request)
        tasks = await self.tasks.get_all_tasks(device_id, offset=offset, limit=limit)
        return web.json_response(
            {"tasks": [self._task_to_dict(task) for task in tasks], "offset": offset, "limit": limit}
        )
