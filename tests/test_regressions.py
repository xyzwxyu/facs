from __future__ import annotations

import asyncio

from src.cwmp import CWMP
from src.db import Device
from src.event_bus import EventBus
from src.session_manager import HttpSession
from src.soap import SoapBuilder, SoapParser


class _TaskManager:
    def __init__(self) -> None:
        self.completed: list[tuple[str, str]] = []

    async def complete_task(self, task_id: str, status: str) -> None:
        self.completed.append((task_id, status))

    async def claim_next_task(self, _device_id: str):
        return None


class _CPEManager:
    async def update_parameters(self, _device_id: str, _parameters) -> None:
        return None


def _rpc_response(method: str, task_id: str) -> SoapParser:
    xml = f'<cwmp:{method} xmlns:cwmp="urn:dslforum-org:cwmp-1-0" />'
    return SoapParser(SoapBuilder().soapify(xml, id=task_id))


def test_mismatched_rpc_response_does_not_complete_pending_task() -> None:
    async def run() -> None:
        tasks = _TaskManager()
        cwmp = CWMP(_CPEManager(), tasks, EventBus())  # type: ignore[arg-type]
        session = HttpSession("session-1")
        session.pending_task_id = "task-1"
        session.pending_method = "Reboot"
        session.device_id = "A1B2C3-serial-1"
        session.informed = True

        response = await cwmp._handle_rpc_result(
            session,
            _rpc_response("GetParameterValuesResponse", "task-1"),
            "GetParameterValuesResponse",
        )

        assert "Fault" in response
        assert tasks.completed == []
        assert session.pending_task_id == "task-1"

    asyncio.run(run())


def test_matching_rpc_response_completes_pending_task() -> None:
    async def run() -> None:
        tasks = _TaskManager()
        events = EventBus()
        cwmp = CWMP(_CPEManager(), tasks, events)  # type: ignore[arg-type]
        session = HttpSession("session-1")
        session.pending_task_id = "task-1"
        session.pending_method = "Reboot"
        session.device_id = "A1B2C3-serial-1"
        session.informed = True

        await cwmp._handle_rpc_result(
            session,
            _rpc_response("RebootResponse", "task-1"),
            "RebootResponse",
        )

        assert tasks.completed == [("task-1", "completed")]
        assert session.pending_task_id is None
        assert events.published == 1

    asyncio.run(run())


def test_serial_number_is_not_a_unique_device_key() -> None:
    assert Device.__table__.c.device_id.unique
    assert not Device.__table__.c.serial_number.unique
