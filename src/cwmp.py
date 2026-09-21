"""CWMP conversation coordinator.

State belongs to ``HttpSession``; this service is intentionally stateless so
many CPE conversations can be processed concurrently without corrupting each
other.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping
from datetime import UTC, datetime

from src.cpe_manager import CPE, CPEManager
from src.config import DEFAULT_MAX_SOAP_BYTES
from src.event_bus import EventBus
from src.session_manager import HttpSession
from src.soap import SoapBuilder, SoapParser, SoapProtocolError
from src.tasks import Task, Tasks

logger = logging.getLogger(__name__)

SERVER_METHODS = ["GetRPCMethods", "Inform", "TransferComplete"]
SERVER_FAULTS = {
    8000: ["Method Not Supported", "The requested method is not supported by the server."],
    8001: ["Request Denied", "No reason specified by the server."],
    8002: ["Internal Error", "An internal error occurred while processing the request."],
    8003: ["Invalid Arguments", "Invalid arguments were provided for the operation."],
    8004: ["Resources Exceeded", "The server resources have been exceeded."],
    8005: ["Retry Request", "The server requests the operation to be retried."],
}

SUPPORTED_TASK_METHODS = frozenset(
    {
        "SetParameterValues",
        "GetParameterValues",
        "GetParameterNames",
        "GetRPCMethods",
        "Reboot",
        "FactoryReset",
    }
)


def _parameter_value(parameters: list[dict[str, str]], *suffixes: str) -> str | None:
    for parameter in parameters:
        name = parameter.get("Name", "")
        if any(name.endswith(suffix) for suffix in suffixes):
            return parameter.get("Value")
    return None


class CWMP:
    def __init__(
        self,
        cpe_manager: CPEManager,
        task_manager: Tasks,
        event_bus: EventBus,
        max_xml_bytes: int = DEFAULT_MAX_SOAP_BYTES,
    ):
        self.cpe_manager = cpe_manager
        self.task_manager = task_manager
        self.event_bus = event_bus
        self.max_xml_bytes = max_xml_bytes

    async def handle_request(self, session: HttpSession, body: bytes | str) -> str:
        try:
            soap = SoapParser(body, max_xml_bytes=self.max_xml_bytes)
            method = soap.parse_type_request()
            if method is None:
                builder = SoapBuilder(session.cwmp_namespace) if session.cwmp_namespace else soap
                return await self._dispatch_next_task(session, builder)
            if method == "Inform":
                return await self._handle_inform(session, soap)
            if method == "GetRPCMethods":
                return soap.soapify(
                    soap.response_get_rpc_methods(SERVER_METHODS),
                    id=soap.parse_cwmp_id() or 1,
                )
            if method == "TransferComplete":
                await self.event_bus.publish(
                    "cpe.transfer_complete",
                    {"device_id": session.device_id},
                )
                return soap.soapify(
                    soap.response_transfer_complete(),
                    id=soap.parse_cwmp_id() or 1,
                )
            if method == "Fault" or method.endswith("Response"):
                return await self._handle_rpc_result(session, soap, method)
            return self._fault(soap, 8000)
        except SoapProtocolError as exc:
            logger.info("rejected invalid SOAP message: %s", exc)
            return self._fault(SoapBuilder(), 8003)
        except Exception:
            logger.exception("CWMP request failed")
            return self._fault(SoapBuilder(), 8002)

    async def _handle_inform(self, session: HttpSession, soap: SoapParser) -> str:
        serial_number = soap.parse_serial_number()
        oui = soap.parse_oui()
        if not serial_number or not oui:
            raise SoapProtocolError("Inform DeviceId requires OUI and SerialNumber")

        parameters = soap.parse_parameters()
        cpe = CPE(
            serial_number=serial_number,
            manufacturer=soap.parse_manufacturer(),
            oui=oui,
            product_class=soap.parse_product_class(),
            software_version=_parameter_value(parameters, ".SoftwareVersion"),
            hardware_version=_parameter_value(parameters, ".HardwareVersion"),
            ip_address=_parameter_value(parameters, ".IPAddress", ".ExternalIPAddress"),
            connection_request_url=_parameter_value(parameters, ".ConnectionRequestURL"),
            last_inform=datetime.now(UTC),
            session_id=session.session_id,
        )
        existing = await self.cpe_manager.get_cpe(cpe)
        saved = await self.cpe_manager.add_cpe(cpe)
        if not saved.device_id:
            raise RuntimeError("database returned a device without an identifier")

        session.device_id = saved.device_id
        session.informed = True
        session.pending_task_id = None
        session.pending_method = None
        session.cwmp_namespace = soap.cwmp_namespace

        await self.cpe_manager.update_parameters(saved.device_id, parameters)
        events = soap.parse_events()
        await self.event_bus.publish(
            "cpe.inform",
            {"device_id": saved.device_id, "events": events},
        )

        if existing is None:
            await self.task_manager.add_task(
                Task(
                    device_id=saved.device_id,
                    command_type="GetParameterValues",
                    command=["Device."],
                )
            )
            await self.event_bus.publish("cpe.provisioned", {"device_id": saved.device_id})

        return soap.soapify(
            soap.response_inform(1),
            id=soap.parse_cwmp_id() or 1,
        )

    async def _dispatch_next_task(self, session: HttpSession, soap: SoapBuilder) -> str:
        if not session.informed or not session.device_id:
            return self._fault(soap, 8001)
        if session.pending_task_id:
            # A second empty POST while a response is outstanding must not claim
            # another task or replay the first one.
            return ""

        task = await self.task_manager.claim_next_task(session.device_id)
        if task is None:
            return ""

        try:
            body = self._build_task_request(soap, task)
        except (SoapProtocolError, TypeError, ValueError):
            await self.task_manager.complete_task(task.task_id, "failed")
            logger.exception("invalid payload for task %s", task.task_id)
            return self._fault(soap, 8003, task.task_id)

        session.pending_task_id = task.task_id
        session.pending_method = task.command_type
        await self.event_bus.publish(
            "task.sent",
            {
                "device_id": session.device_id,
                "task_id": task.task_id,
                "method": task.command_type,
            },
        )
        return soap.soapify(body, id=task.task_id)

    @staticmethod
    def _build_task_request(soap: SoapBuilder, task: Task) -> str:
        method = task.command_type
        if method not in SUPPORTED_TASK_METHODS:
            raise SoapProtocolError(f"unsupported queued RPC method: {method}")
        if method == "SetParameterValues":
            return soap.request_set_parameter_values(task.command, task.command_key or "")
        if method == "GetParameterValues":
            return soap.request_get_parameter_values(task.command)
        if method == "GetParameterNames":
            payload = task.command or {}
            if isinstance(payload, Mapping):
                path = payload.get("path", payload.get("parameter_path", ""))
                next_level = payload.get("next_level", False)
            elif isinstance(payload, str):
                path, next_level = payload, False
            else:
                raise SoapProtocolError("GetParameterNames payload must be an object or string")
            return soap.request_get_parameter_names(str(path), bool(next_level))
        if method == "GetRPCMethods":
            return soap.request_get_rpc_methods()
        if method == "Reboot":
            return soap.request_reboot(task.command_key or "")
        return soap.request_reset()

    async def _handle_rpc_result(self, session: HttpSession, soap: SoapParser, method: str) -> str:
        task_id = session.pending_task_id
        if not task_id:
            return self._fault(soap, 8001)
        response_id = soap.parse_cwmp_id()
        if response_id and response_id != task_id:
            return self._fault(soap, 8003, response_id)

        failed = method == "Fault"
        await self.task_manager.complete_task(task_id, "failed" if failed else "completed")

        if not failed and method == "GetParameterValuesResponse" and session.device_id:
            await self.cpe_manager.update_parameters(session.device_id, soap.parse_parameters())

        await self.event_bus.publish(
            "task.failed" if failed else "task.completed",
            {
                "device_id": session.device_id,
                "task_id": task_id,
                "method": session.pending_method,
                "faults": soap.parse_faults() if failed else {},
            },
        )
        session.pending_task_id = None
        session.pending_method = None
        return await self._dispatch_next_task(session, soap)

    @staticmethod
    def _fault(soap: SoapBuilder, code: int, message_id: str | int = 1) -> str:
        return soap.soapify(soap.fault_response(SERVER_FAULTS, code), id=message_id)
