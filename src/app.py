"""Public CWMP transport and private Unix-socket management server."""

from __future__ import annotations

import asyncio
import errno
import json
import logging
import os
import secrets
import socket
import ssl
import stat
from pathlib import Path
from urllib.parse import urlsplit

from aiohttp import ClientError, ClientSession, UnixConnector, web

from src.api import ACSApi
from src.config import DEFAULT_MAX_SESSIONS, DEFAULT_MAX_SOAP_BYTES, socket_path_from_env
from src.cpe_manager import CPEManager
from src.cwmp import CWMP
from src.event_bus import EventBus
from src.session_manager import HttpSession, SessionCapacityError, SessionManager
from src.tasks import Tasks

logger = logging.getLogger(__name__)
MANAGEMENT_SOCKET_KEY = web.AppKey("management_socket_path", str)


def admin_auth_middleware(token: str):
    """Require a bearer token for the remote management application."""
    if len(token) < 32:
        raise ValueError("remote management token must contain at least 32 characters")

    @web.middleware
    async def authenticate(request: web.Request, handler):
        scheme, separator, credentials = request.headers.get("Authorization", "").partition(" ")
        if separator != " " or scheme.lower() != "bearer" or not secrets.compare_digest(credentials, token):
            raise web.HTTPUnauthorized(
                text="valid bearer token required",
                headers={"WWW-Authenticate": "Bearer"},
            )
        return await handler(request)

    return authenticate


class CWMPHandler:
    def __init__(
        self,
        cwmp: CWMP,
        event_bus: EventBus,
        *,
        session_ttl_seconds: int = 3600,
        max_sessions: int = DEFAULT_MAX_SESSIONS,
    ):
        self.cwmp = cwmp
        self.event_bus = event_bus
        self.session_manager = SessionManager(
            ttl_seconds=session_ttl_seconds,
            max_sessions=max_sessions,
        )

    async def handle_cwmp(self, request: web.Request) -> web.Response:
        candidate_id = HttpSession.extract_session_id(request.headers)
        try:
            session = await self.session_manager.get_or_create_session(candidate_id)
        except SessionCapacityError as exc:
            raise web.HTTPServiceUnavailable(
                text=str(exc),
                headers={"Retry-After": "30"},
            ) from exc

        body = await request.read()
        async with session.lock:
            session.session_requests += 1
            session.mark_activity()
            cwmp_response = await self.cwmp.handle_request(session, body)

        response = (
            web.Response(text=cwmp_response, content_type="text/xml", charset="utf-8")
            if cwmp_response
            else web.Response(status=204)
        )
        response.set_cookie(
            "session",
            session.session_id,
            httponly=True,
            max_age=session.ttl_seconds,
            samesite="Lax",
            path="/",
        )
        return response

    async def startup(self) -> None:
        await self.session_manager.start_cleanup_task()

    async def shutdown(self) -> None:
        await self.session_manager.stop_cleanup_task()
        await self.session_manager.clear_all()

    async def websocket_handler(self, request: web.Request) -> web.WebSocketResponse:
        ws = web.WebSocketResponse(heartbeat=30)
        await ws.prepare(request)
        queue = await self.event_bus.subscribe("*")
        socket_path = request.app.get(MANAGEMENT_SOCKET_KEY)
        connector = UnixConnector(path=socket_path) if socket_path else None
        try:
            await ws.send_json({"type": "ws.connected", "data": {}})
            async with ClientSession(connector=connector) if connector is not None else _no_proxy() as proxy:
                incoming = asyncio.create_task(ws.receive())
                next_event = asyncio.create_task(queue.get())
                try:
                    while not ws.closed:
                        done, _ = await asyncio.wait(
                            {incoming, next_event},
                            return_when=asyncio.FIRST_COMPLETED,
                        )
                        if incoming in done:
                            message = incoming.result()
                            if message.type == web.WSMsgType.TEXT:
                                if proxy is not None:
                                    await self._handle_ws_command(ws, proxy, message.data)
                                incoming = asyncio.create_task(ws.receive())
                            else:
                                break
                        if next_event in done:
                            event = next_event.result()
                            await ws.send_json(
                                {
                                    "id": event.id,
                                    "type": event.type,
                                    "timestamp": event.timestamp,
                                    "data": event.data,
                                }
                            )
                            next_event = asyncio.create_task(queue.get())
                finally:
                    incoming.cancel()
                    next_event.cancel()
                    await asyncio.gather(incoming, next_event, return_exceptions=True)
        except (ConnectionError, RuntimeError):
            logger.debug("management websocket disconnected")
        finally:
            await self.event_bus.unsubscribe("*", queue)
        return ws

    @staticmethod
    async def _handle_ws_command(ws: web.WebSocketResponse, proxy: ClientSession, raw: str) -> None:
        """Forward bounded WSS commands only to the local management API."""
        command = None
        try:
            command = json.loads(raw)
            if not isinstance(command, dict):
                raise ValueError("command must be an object")
            command_id = command.get("id")
            method = command.get("method")
            path = command.get("path")
            parsed_path = urlsplit(path) if isinstance(path, str) else None
            if (
                not isinstance(command_id, str)
                or len(command_id) > 64
                or not isinstance(path, str)
                or parsed_path is None
                or parsed_path.scheme
                or parsed_path.netloc
                or not parsed_path.path.startswith("/api/")
                or parsed_path.fragment
                or len(path) > 2048
                or method not in {"GET", "POST", "DELETE"}
            ):
                raise ValueError("unsupported management command")
            if len(raw.encode("utf-8")) > 64 * 1024:
                raise ValueError("command exceeds 64 KiB")
            async with proxy.request(method, f"http://localhost{path}", json=command.get("data")) as response:
                body = await response.text()
                result = json.loads(body) if "application/json" in response.headers.get("Content-Type", "") else body
                await ws.send_json(
                    {"type": "ws.reply", "id": command_id, "status": response.status, "data": result}
                )
        except (ValueError, TypeError) as exc:
            await ws.send_json(
                {
                    "type": "ws.reply",
                    "id": command.get("id") if isinstance(command, dict) else None,
                    "status": 400,
                    "data": str(exc),
                }
            )
        except (ClientError, OSError):
            await ws.send_json(
                {
                    "type": "ws.reply",
                    "id": command.get("id") if isinstance(command, dict) else None,
                    "status": 503,
                    "data": "local management API unavailable",
                }
            )


class _no_proxy:
    async def __aenter__(self):
        return None

    async def __aexit__(self, *_args):
        return None


class CWMPServer:
    def __init__(
        self,
        server_address: tuple[str, int],
        cwmp: CWMP,
        cpe_manager: CPEManager,
        task_manager: Tasks,
        event_bus: EventBus,
        *,
        sock_path: str | None = None,
        max_sessions: int = DEFAULT_MAX_SESSIONS,
        max_soap_bytes: int = DEFAULT_MAX_SOAP_BYTES,
        management_address: tuple[str, int] | None = None,
        management_ssl_context: ssl.SSLContext | None = None,
        admin_token: str | None = None,
    ):
        self.server_address = server_address
        self.cwmp = cwmp
        self.handler = CWMPHandler(cwmp, event_bus, max_sessions=max_sessions)
        self.cpe_manager = cpe_manager
        self.task_manager = task_manager
        self.event_bus = event_bus
        self.sock_path = sock_path or socket_path_from_env()
        self.max_soap_bytes = max_soap_bytes
        self.management_address = management_address
        self.management_ssl_context = management_ssl_context
        self.admin_token = admin_token
        if management_address is not None:
            if management_ssl_context is None:
                raise ValueError("remote management requires a TLS certificate and key")
            if admin_token is None or len(admin_token) < 32:
                raise ValueError("remote management requires an admin token of at least 32 characters")
        self.app_runner: web.AppRunner | None = None
        self.cli_runner: web.AppRunner | None = None
        self.remote_management_runner: web.AppRunner | None = None
        self._started = False
        self._shutdown_lock = asyncio.Lock()

    def create_public_app(self) -> web.Application:
        app = web.Application(client_max_size=self.max_soap_bytes)
        app.router.add_post("/", self.handler.handle_cwmp)
        app.router.add_post("/cwmp", self.handler.handle_cwmp)
        app.router.add_get("/health", self.health)
        return app

    def create_management_app(self, admin_token: str | None = None) -> web.Application:
        middlewares = [admin_auth_middleware(admin_token)] if admin_token else []
        app = web.Application(client_max_size=64 * 1024, middlewares=middlewares)
        if admin_token:
            app[MANAGEMENT_SOCKET_KEY] = self.sock_path
        app.router.add_get("/manage/ws", self.handler.websocket_handler)
        ACSApi(
            cpe_manager=self.cpe_manager,
            tasks=self.task_manager,
            event_bus=self.event_bus,
        ).setup_routes(app)
        return app

    async def health(self, request: web.Request) -> web.Response:
        return web.json_response({"status": "ok"})

    @staticmethod
    def _prepare_socket(path: Path) -> None:
        if not path.exists() and not path.is_symlink():
            return
        mode = path.lstat().st_mode
        if not stat.S_ISSOCK(mode):
            raise RuntimeError(f"refusing to replace non-socket path: {path}")
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as probe:
            state = probe.connect_ex(str(path))
        if state == 0:
            raise RuntimeError(f"management socket is already in use: {path}")
        if state not in {errno.ECONNREFUSED, errno.ENOENT}:
            raise OSError(state, f"cannot safely check existing management socket: {path}")
        path.unlink()

    async def start(self) -> None:
        if self._started:
            return
        socket_path = Path(self.sock_path)
        await asyncio.to_thread(self._prepare_socket, socket_path)
        await self.handler.startup()
        try:
            self.app_runner = web.AppRunner(self.create_public_app(), access_log=logger)
            await self.app_runner.setup()
            await web.TCPSite(
                self.app_runner,
                self.server_address[0],
                self.server_address[1],
            ).start()

            self.cli_runner = web.AppRunner(self.create_management_app(), access_log=None)
            await self.cli_runner.setup()
            await web.UnixSite(self.cli_runner, self.sock_path).start()
            await asyncio.to_thread(os.chmod, self.sock_path, 0o600)

            if self.management_address is not None:
                self.remote_management_runner = web.AppRunner(
                    self.create_management_app(self.admin_token),
                    access_log=logger,
                )
                await self.remote_management_runner.setup()
                await web.TCPSite(
                    self.remote_management_runner,
                    self.management_address[0],
                    self.management_address[1],
                    ssl_context=self.management_ssl_context,
                ).start()
        except Exception:
            await self._cleanup_runners()
            await self.handler.shutdown()
            raise

        self._started = True
        await self.event_bus.publish("server_started", {"address": self.server_address})
        logger.info("CWMP listening on %s:%s", *self.server_address)
        logger.info("management API listening on unix://%s", self.sock_path)
        if self.management_address is not None:
            logger.info("remote management listening on https://%s:%s", *self.management_address)

    async def _cleanup_runners(self) -> None:
        if self.remote_management_runner is not None:
            await self.remote_management_runner.cleanup()
            self.remote_management_runner = None
        if self.cli_runner is not None:
            await self.cli_runner.cleanup()
            self.cli_runner = None
        if self.app_runner is not None:
            await self.app_runner.cleanup()
            self.app_runner = None

    async def shutdown(self) -> None:
        async with self._shutdown_lock:
            if (
                not self._started
                and self.app_runner is None
                and self.cli_runner is None
                and self.remote_management_runner is None
            ):
                return
            await self._cleanup_runners()
            await self.handler.shutdown()
            self._started = False
            logger.info("server shutdown complete")
