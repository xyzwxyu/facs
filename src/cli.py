"""Standalone and optionally attached ACS management shell.

API traffic uses either the owner-only Unix socket or authenticated HTTPS.
Notifications use the corresponding WebSocket endpoint. Neither mode needs
an in-process reference to the ACS or its event bus.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import logging
import platform
import ssl
import subprocess
import threading
import uuid
from concurrent.futures import CancelledError
from typing import Any
from urllib.parse import quote, urlsplit

import aiohttp
import cmd2
from cmd2 import CommandSet, with_argparser, with_category
from cmd2_ansi import Fg, ansi
from rich.table import Table

from src.config import CliSettings, socket_path_from_env

logger = logging.getLogger(__name__)


class AsyncRunner:
    """One event loop for the shell's persistent HTTP and WebSocket client."""

    def __init__(self):
        self.loop = asyncio.new_event_loop()
        self._ready = threading.Event()
        self.thread = threading.Thread(target=self._run_loop, name="facs-cli-io", daemon=True)

    def _run_loop(self) -> None:
        asyncio.set_event_loop(self.loop)
        self._ready.set()
        self.loop.run_forever()

    def start(self) -> None:
        self.thread.start()
        if not self._ready.wait(timeout=5):
            raise RuntimeError("CLI event loop did not start")

    def run(self, coro):
        if not self.thread.is_alive():
            coro.close()
            raise RuntimeError("CLI event loop is not running")
        return asyncio.run_coroutine_threadsafe(coro, self.loop)

    def stop(self) -> None:
        if not self.thread.is_alive():
            return

        async def cancel_pending() -> None:
            current = asyncio.current_task()
            pending = [task for task in asyncio.all_tasks() if task is not current]
            for task in pending:
                task.cancel()
            if pending:
                await asyncio.gather(*pending, return_exceptions=True)
            await self.loop.shutdown_asyncgens()

        self.run(cancel_pending()).result(timeout=10)
        self.loop.call_soon_threadsafe(self.loop.stop)
        self.thread.join(timeout=10)
        self.loop.close()


class ApiError(RuntimeError):
    def __init__(self, status: int, message: str):
        super().__init__(f"management API returned {status}: {message}")
        self.status = status


class ApiClient:
    """Persistent local or TLS-verified remote management client."""

    def __init__(
        self,
        async_runner: AsyncRunner | None,
        *,
        base_url: str = "http://localhost",
        sock_path: str | None = "/tmp/facs.sock",
        token: str | None = None,
        ca_file: str | None = None,
    ):
        self.async_runner = async_runner
        self.base_url = base_url.rstrip("/")
        self.sock_path = sock_path
        self.token = token
        self._session: aiohttp.ClientSession | None = None
        self._ws: aiohttp.ClientWebSocketResponse | None = None
        self._ws_reader_task: asyncio.Task[None] | None = None
        self._ws_lock = asyncio.Lock()
        self._pending_replies: dict[str, asyncio.Future[Any]] = {}
        self._event_queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=256)
        self.ssl_context: ssl.SSLContext | None = None
        if sock_path is None:
            parsed = urlsplit(self.base_url)
            if (
                parsed.scheme != "https"
                or not parsed.hostname
                or parsed.username
                or parsed.password
                or parsed.path not in {"", "/"}
                or parsed.query
                or parsed.fragment
            ):
                raise ValueError("remote management URL must be an https://host:port origin")
            if not token:
                raise ValueError("remote management requires a bearer token")
            self.ssl_context = ssl.create_default_context(cafile=ca_file)
        elif ca_file or token:
            raise ValueError("--token and --ca-file are only used with a remote URL")

    @property
    def ws_url(self) -> str:
        scheme = "wss" if self.sock_path is None else "ws"
        return f"{scheme}://{urlsplit(self.base_url).netloc}/manage/ws"

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            connector: aiohttp.BaseConnector = (
                aiohttp.UnixConnector(path=self.sock_path)
                if self.sock_path is not None
                else aiohttp.TCPConnector(ssl=self.ssl_context or True)
            )
            headers = {"Authorization": f"Bearer {self.token}"} if self.token else None
            self._session = aiohttp.ClientSession(
                connector=connector,
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=10),
            )
        return self._session

    async def request(self, method: str, path: str, json_data: Any = None) -> Any:
        if self.sock_path is None:
            return await self._request_remote(method, path, json_data)
        session = await self._get_session()
        async with session.request(method, f"{self.base_url}{path}", json=json_data) as response:
            text = await response.text()
            if response.status >= 400:
                raise ApiError(response.status, text.strip())
            if "application/json" in response.headers.get("Content-Type", ""):
                return json.loads(text) if text else None
            return text

    async def all_devices(self) -> list[dict[str, Any]]:
        devices: list[dict[str, Any]] = []
        while True:
            response = await self.request("GET", f"/api/devices?offset={len(devices)}&limit=1000")
            page = response.get("devices", [])
            devices.extend(page)
            if len(page) < 1000:
                return devices

    def get_all_devices(self) -> list[dict[str, Any]]:
        if self.async_runner is None:
            raise RuntimeError("CLI event loop is not configured")
        future = self.async_runner.run(self.all_devices())
        try:
            return future.result(timeout=60)
        except TimeoutError:
            future.cancel()
            raise

    async def _get_ws(self) -> aiohttp.ClientWebSocketResponse:
        async with self._ws_lock:
            if self._ws is None or self._ws.closed:
                session = await self._get_session()
                self._ws = await session.ws_connect(
                    self.ws_url,
                    headers={"Authorization": f"Bearer {self.token}"},
                    heartbeat=30,
                )
                self._ws_reader_task = asyncio.create_task(self._read_ws(self._ws))
            return self._ws

    async def _read_ws(self, ws: aiohttp.ClientWebSocketResponse) -> None:
        try:
            async for message in ws:
                if message.type != aiohttp.WSMsgType.TEXT:
                    break
                payload = json.loads(message.data)
                if payload.get("type") == "ws.reply":
                    future = self._pending_replies.pop(payload.get("id"), None)
                    if future is not None and not future.done():
                        future.set_result(payload)
                else:
                    if self._event_queue.full():
                        self._event_queue.get_nowait()
                    self._event_queue.put_nowait(payload)
        finally:
            with contextlib.suppress(Exception):
                await ws.close()
            if self._ws is ws:
                self._ws = None
            for future in self._pending_replies.values():
                if not future.done():
                    future.set_exception(ConnectionError("remote WebSocket disconnected"))
            self._pending_replies.clear()

    async def _request_remote(self, method: str, path: str, data: Any) -> Any:
        ws = await self._get_ws()
        request_id = uuid.uuid4().hex
        reply = asyncio.get_running_loop().create_future()
        self._pending_replies[request_id] = reply
        try:
            await ws.send_json({"id": request_id, "method": method, "path": path, "data": data})
            result = await asyncio.wait_for(reply, timeout=10)
            if result["status"] >= 400:
                raise ApiError(result["status"], str(result.get("data", "")))
            return result.get("data")
        finally:
            self._pending_replies.pop(request_id, None)

    def _run(self, method: str, path: str, json_data: Any = None) -> Any:
        if self.async_runner is None:
            raise RuntimeError("CLI event loop is not configured")
        future = self.async_runner.run(self.request(method, path, json_data))
        try:
            return future.result(timeout=15)
        except TimeoutError:
            future.cancel()
            raise

    def get(self, path: str) -> Any:
        return self._run("GET", path)

    def post(self, path: str, json_data: Any = None) -> Any:
        return self._run("POST", path, json_data)

    def delete(self, path: str) -> Any:
        return self._run("DELETE", path)

    async def events(self):
        if self.sock_path is None:
            ws = await self._get_ws()
            while not ws.closed:
                try:
                    yield await asyncio.wait_for(self._event_queue.get(), timeout=5)
                except TimeoutError:
                    continue
            return
        session = await self._get_session()
        async with session.ws_connect(self.ws_url, heartbeat=30) as ws:
            async for message in ws:
                if message.type == aiohttp.WSMsgType.TEXT:
                    yield json.loads(message.data)
                elif message.type in {aiohttp.WSMsgType.ERROR, aiohttp.WSMsgType.CLOSED}:
                    break

    async def aclose(self) -> None:
        if self._ws is not None and not self._ws.closed:
            await self._ws.close()
        if self._ws_reader_task is not None:
            self._ws_reader_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._ws_reader_task
        self._ws_reader_task = None
        if self._session is not None and not self._session.closed:
            await self._session.close()
        self._session = None


class CPECommandSet(CommandSet):
    def __init__(self, api_client: ApiClient):
        super().__init__()
        self.api_client = api_client
        self.device_id: str | None = None

    def _path(self, suffix: str = "") -> str:
        if self.device_id is None:
            raise RuntimeError("no CPE selected")
        return f"/api/devices/{quote(self.device_id, safe='')}{suffix}"

    def _queue_task(self, method: str, payload: Any = None) -> bool:
        try:
            response = self.api_client.post(
                self._path("/rpc"),
                {"method": method, "payload": payload},
            )
        except Exception as exc:
            self._cmd.poutput(ansi.style(f"Failed to queue {method}: {exc}", fg=Fg.RED))
            return False
        if response.get("status") != "queued":
            self._cmd.poutput(ansi.style(f"Unexpected API response: {response}", fg=Fg.RED))
            return False
        task_id = response.get("task", {}).get("task_id", "")
        self._cmd.poutput(ansi.style(f"Task queued: {method} ({task_id})", fg=Fg.GREEN))
        return True

    set_parser = cmd2.Cmd2ArgumentParser()
    set_parser.add_argument("name", help="TR-069 parameter name")
    set_parser.add_argument("value", help="new parameter value")
    set_parser.add_argument("value_type", nargs="?", default="xsd:string", help="XSD type (default: xsd:string)")

    @with_argparser(set_parser)
    @with_category("CPE commands")
    def do_setparam(self, args) -> None:
        """Set one parameter: setparam NAME VALUE [XSD_TYPE]."""
        self._queue_task("SetParameterValues", [{"name": args.name, "value": args.value, "type": args.value_type}])

    get_parser = cmd2.Cmd2ArgumentParser()
    get_parser.add_argument("kind", choices=["names", "values"])
    get_parser.add_argument("parameter", nargs="+", help="TR-069 path or parameter name")
    get_parser.add_argument("--next-level", action="store_true", help="return only the next hierarchy level")

    @with_argparser(get_parser)
    @with_category("CPE commands")
    def do_getparam(self, args) -> None:
        """Query parameter names or values."""
        if args.kind == "names":
            self._queue_task("GetParameterNames", {"path": args.parameter[0], "next_level": args.next_level})
        else:
            self._queue_task("GetParameterValues", args.parameter)

    @with_category("CPE commands")
    def do_upgrade(self, args) -> None:
        """Queue a Download RPC to upgrade the CPE's firmware or software."""
        self._cmd.poutput(ansi.style("Upgrade RPC is not yet implemented.", fg=Fg.LIGHT_YELLOW))

    @with_category("CPE commands")
    def do_reboot(self, _: cmd2.Statement) -> None:
        """Queue a Reboot RPC."""
        self._queue_task("Reboot")

    restore_parser = cmd2.Cmd2ArgumentParser()
    restore_parser.add_argument("--yes", action="store_true", help="confirm the factory reset")

    @with_argparser(restore_parser)
    @with_category("CPE commands")
    def do_restore(self, args) -> None:
        """Queue a FactoryReset RPC; requires --yes."""
        if not args.yes:
            self._cmd.poutput(ansi.style("Factory reset requires: restore --yes", fg=Fg.LIGHT_YELLOW))
            return
        self._queue_task("FactoryReset")

    @with_category("CPE commands")
    def do_tasks(self, _: cmd2.Statement) -> None:
        """List RPC tasks for the selected CPE."""
        try:
            response = self.api_client.get(self._path("/tasks"))
        except Exception as exc:
            self._cmd.poutput(ansi.style(f"Failed to query tasks: {exc}", fg=Fg.RED))
            return

        table = Table(title=f"Tasks for {self.device_id}")
        for heading in ("Task ID", "Method", "Payload", "Status", "Retries", "Created"):
            table.add_column(heading)
        for task in response.get("tasks", []):
            table.add_row(
                str(task.get("task_id", "")),
                str(task.get("command_type", "")),
                str(task.get("command", "")),
                str(task.get("status", "")),
                str(task.get("retry_count", "")),
                str(task.get("created_at", "")),
            )
        self._cmd.poutput(table if response.get("tasks") else "No tasks for this CPE.")

    @with_category("CPE commands")
    def do_show(self, _: cmd2.Statement) -> None:
        """Display details for the selected CPE."""
        try:
            response = self.api_client.get(self._path())
        except Exception as exc:
            self._cmd.poutput(ansi.style(f"Failed to query CPE: {exc}", fg=Fg.RED))
            return

        device = response.get("device", {})
        table = Table(title=f"Device {self.device_id}")
        table.add_column("Field")
        table.add_column("Value")
        for key, value in sorted(device.items()):
            table.add_row(str(key), str(value))
        self._cmd.poutput(table)


class FacsShell(cmd2.Cmd):
    intro = "Welcome to the FACS CLI. Type help or ? to list commands.\n"
    DEFAULT_CATEGORY = "Main commands"

    def __init__(self, api_client: ApiClient, async_runner: AsyncRunner):
        super().__init__(auto_load_commands=False, allow_cli_args=False)
        self.api_client = api_client
        self.async_runner = async_runner
        self.prompt = "(facs) "
        self.selected_device_id: str | None = None
        self.cpe_choices: list[cmd2.CompletionItem] = []
        self.cpe_commands = CPECommandSet(self.api_client)
        self._cpe_commands_registered = False
        self._closed = False

        # Confirm API access and authentication before presenting a prompt.
        self.refresh_device_choices(required=True)
        self._events_future = self.async_runner.run(self._watch_events())

    def refresh_device_choices(self, *, required: bool = False) -> None:
        try:
            devices = self.api_client.get_all_devices()
        except Exception:
            if required:
                raise
            logger.warning("unable to refresh device completions", exc_info=True)
            return
        device_ids = [device.get("device_id") for device in devices if device.get("device_id")]
        self.cpe_choices = [cmd2.CompletionItem(device_id) for device_id in device_ids]

    async def _watch_events(self) -> None:
        delay = 1
        while True:
            try:
                async for event in self.api_client.events():
                    delay = 1
                    event_type = event.get("type", "")
                    device_id = event.get("data", {}).get("device_id")
                    if event_type == "cpe.provisioned" and device_id:
                        if not any(item.value == device_id for item in self.cpe_choices):
                            self.cpe_choices = [*self.cpe_choices, cmd2.CompletionItem(device_id)]
                    elif event_type == "cpe.unprovisioned" and device_id:
                        self.cpe_choices = [item for item in self.cpe_choices if item.value != device_id]
                    elif event_type == "cpe.unprovisioned_all":
                        self.cpe_choices = []
                    if event_type.startswith(("cpe.", "task.")):
                        details = json.dumps(event.get("data", {}), sort_keys=True, default=str)
                        self.add_alert(
                            msg=ansi.style(f"\n{event_type}: {details}\n", fg=Fg.LIGHT_YELLOW),
                            prompt=self.prompt,
                        )
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.warning("event connection unavailable; retrying in %ss", delay, exc_info=True)
            await asyncio.sleep(delay)
            delay = min(delay * 2, 30)

    def _select_device(self, device_id: str) -> None:
        if self._cpe_commands_registered:
            self.unregister_command_set(self.cpe_commands)  # type: ignore[type-var]
        self.selected_device_id = device_id
        self.cpe_commands.device_id = device_id
        self.register_command_set(self.cpe_commands)  # type: ignore[type-var]
        self._cpe_commands_registered = True
        self.prompt = f"(facs:{device_id}) "

    def _clear_device(self) -> None:
        if self._cpe_commands_registered:
            self.unregister_command_set(self.cpe_commands)  # type: ignore[type-var]
            self._cpe_commands_registered = False
        self.selected_device_id = None
        self.cpe_commands.device_id = None
        self.prompt = "(facs) "

    ping_parser = cmd2.Cmd2ArgumentParser()
    ping_parser.add_argument("host")
    ping_parser.add_argument("-c", "--count", type=int, default=3)

    @with_argparser(ping_parser)
    @with_category("Main commands")
    def do_ping(self, args) -> None:
        """Ping a host without invoking a shell."""
        if args.count < 1 or args.count > 20:
            self.poutput(ansi.style("count must be between 1 and 20", fg=Fg.RED))
            return
        count_flag = "-n" if platform.system().lower() == "windows" else "-c"
        subprocess.run(["ping", count_flag, str(args.count), args.host], check=False)

    use_parser = cmd2.Cmd2ArgumentParser()
    use_parser.add_argument("device_id", choices_provider=lambda self: self.cpe_choices)

    @with_argparser(use_parser)
    @with_category("Main commands")
    def do_use(self, args) -> None:
        """Select a CPE and enable device-specific commands."""
        try:
            response = self.api_client.get(f"/api/devices/{quote(args.device_id, safe='')}")
        except Exception as exc:
            self.poutput(ansi.style(f"Cannot select CPE: {exc}", fg=Fg.RED))
            return
        if not response.get("device"):
            self.poutput(ansi.style("Device not found.", fg=Fg.RED))
            return
        self._select_device(args.device_id)

    list_parser = cmd2.Cmd2ArgumentParser()
    list_parser.add_argument("scope", choices=["all", "active", "inactive"], nargs="?", default="all")

    @with_argparser(list_parser)
    @with_category("Main commands")
    def do_list(self, args) -> None:
        """List all, active, or inactive CPEs."""
        try:
            devices = self.api_client.get_all_devices()
        except Exception as exc:
            self.poutput(ansi.style(f"Failed to query CPEs: {exc}", fg=Fg.RED))
            return
        if args.scope != "all":
            expected = args.scope == "active"
            devices = [device for device in devices if bool(device.get("is_online")) is expected]

        table = Table(title=f"CPE devices ({args.scope})")
        for heading in ("Device ID", "Serial", "Manufacturer", "Software", "IP", "Last inform", "Status"):
            table.add_column(heading)
        for device in devices:
            table.add_row(
                str(device.get("device_id", "")),
                str(device.get("serial_number", "")),
                str(device.get("manufacturer", "")),
                str(device.get("software_version", "")),
                str(device.get("ip_address", "")),
                str(device.get("last_inform", "")),
                "Active" if device.get("is_online") else "Inactive",
            )
        self.poutput(table if devices else f"No {args.scope} devices.")

    unprovision_parser = cmd2.Cmd2ArgumentParser()
    unprovision_parser.add_argument("target", help="device ID or 'all'")
    unprovision_parser.add_argument("--yes", action="store_true", help="confirm deleting all devices")

    @with_argparser(unprovision_parser)
    @with_category("Main commands")
    def do_unprovision(self, args) -> None:
        """Delete one CPE, or all CPEs with `unprovision all`."""
        try:
            if args.target == "all":
                if not args.yes:
                    self.poutput(ansi.style("Deleting all devices requires: unprovision all --yes", fg=Fg.LIGHT_YELLOW))
                    return
                response = self.api_client.delete("/api/devices")
                self._clear_device()
                self.poutput(f"Deleted {response.get('deleted_count', 0)} devices.")
            else:
                self.api_client.delete(f"/api/devices/{quote(args.target, safe='')}")
                if self.selected_device_id == args.target:
                    self._clear_device()
                self.poutput(f"Deleted device {args.target}.")
            self.refresh_device_choices()
        except Exception as exc:
            self.poutput(ansi.style(f"Failed to unprovision: {exc}", fg=Fg.RED))

    @with_category("Main commands")
    def do_exit(self, statement: cmd2.Statement) -> bool | None:
        """Leave the selected CPE or close this CLI session."""
        if self.selected_device_id is not None:
            self._clear_device()
            return None
        return self.do_quit(statement)

    @with_category("Main commands")
    def do_quit(self, _: cmd2.Statement | str) -> bool:
        """Disconnect this CLI; the ACS server continues running."""
        self.poutput(ansi.style("CLI disconnected; ACS continues running.", fg=Fg.LIGHT_YELLOW))
        return True

    def do_eof(self, statement: cmd2.Statement) -> bool:
        """Disconnect this CLI on Ctrl-D."""
        self.poutput("")
        return self.do_quit(statement)

    def start(self) -> None:
        self.cmdloop()

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._events_future.cancel()
        with contextlib.suppress(CancelledError, TimeoutError):
            self._events_future.result(timeout=3)
        self.async_runner.run(self.api_client.aclose()).result(timeout=5)


def cli_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Connect to a running FACS ACS")
    connection = parser.add_mutually_exclusive_group()
    connection.add_argument("--socket", default=None, help="local Unix socket (default: FACS_SOCKET_PATH)")
    connection.add_argument("--url", help="remote HTTPS management origin, e.g. https://acs.example:8443")
    parser.add_argument("--token", help="remote bearer token (defaults to FACS_ADMIN_TOKEN)")
    parser.add_argument("--ca-file", help="PEM CA certificate for a private TLS authority")
    return parser


def run_cli(
    *,
    sock_path: str | None = None,
    url: str | None = None,
    token: str | None = None,
    ca_file: str | None = None,
) -> int:
    if url is not None and sock_path is not None:
        raise ValueError("choose either a local socket or a remote URL")
    if url is None:
        sock_path = sock_path or socket_path_from_env()
    runner = AsyncRunner()
    runner.start()
    shell: FacsShell | None = None
    client: ApiClient | None = None
    try:
        client = ApiClient(
            runner,
            base_url=url or "http://localhost",
            sock_path=sock_path,
            token=token,
            ca_file=ca_file,
        )
        shell = FacsShell(client, runner)
        shell.start()
        return 0
    except KeyboardInterrupt:
        return 130
    finally:
        if shell is not None:
            shell.close()
        elif client is not None:
            runner.run(client.aclose()).result(timeout=5)
        runner.stop()


def main(argv: list[str] | None = None) -> int:
    settings = CliSettings.from_env()
    args = cli_parser().parse_args(argv)
    url = args.url or (settings.url if args.socket is None else None)
    sock_path = args.socket if args.socket is not None else (None if url else settings.socket_path)
    token = args.token or (settings.token if url else None)
    ca_file = args.ca_file or (settings.ca_file if url else None)
    try:
        return run_cli(sock_path=sock_path, url=url, token=token, ca_file=ca_file)
    except (OSError, ValueError, ApiError, aiohttp.ClientError) as exc:
        raise SystemExit(f"cannot connect to FACS: {exc}") from exc


if __name__ == "__main__":
    raise SystemExit(main())
