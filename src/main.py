"""Standalone FACS ACS, with an optional detachable CLI."""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import logging
import signal
import ssl
import threading
from concurrent.futures import Future

from src.app import CWMPServer
from src.cli import run_cli
from src.config import ServerSettings
from src.cpe_manager import CPEManager
from src.cwmp import CWMP
from src.db import Database
from src.event_bus import EventBus
from src.log import configure_logging
from src.tasks import Tasks

logger = logging.getLogger("FACS")


def build_parser() -> argparse.ArgumentParser:
    settings = ServerSettings.from_env()
    parser = argparse.ArgumentParser(description="FACS CWMP/TR-069 ACS")
    parser.add_argument("-i", "--ip", default=settings.ip)
    parser.add_argument("-p", "--port", default=settings.port, type=int)
    parser.add_argument("--socket", default=settings.socket_path)
    parser.add_argument("--max-sessions", default=settings.max_sessions, type=int)
    parser.add_argument("--max-soap-bytes", default=settings.max_soap_bytes, type=int)
    parser.add_argument("--database-url", default=settings.database_url)
    parser.add_argument("--log-level", default=settings.log_level)
    parser.add_argument("--log-file", default=settings.log_file)
    parser.add_argument("-cli", "--cli", action="store_true", help="attach a detachable CLI to this ACS process")
    parser.add_argument("--manage-ip", default=settings.manage_ip)
    parser.add_argument("--manage-port", default=settings.manage_port, type=int)
    parser.add_argument("--tls-cert", default=settings.tls_cert)
    parser.add_argument("--tls-key", default=settings.tls_key)
    parser.add_argument("--admin-token", default=settings.admin_token)
    return parser


def remote_management_config(args) -> tuple[tuple[str, int] | None, ssl.SSLContext | None]:
    if not args.manage_ip:
        if args.tls_cert or args.tls_key:
            raise ValueError("set --manage-ip to enable remote TLS management")
        return None, None
    if not args.tls_cert or not args.tls_key:
        raise ValueError("remote management requires --tls-cert and --tls-key")
    if not args.admin_token or len(args.admin_token) < 32:
        raise ValueError("remote management requires an admin token of at least 32 characters")
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.minimum_version = ssl.TLSVersion.TLSv1_2
    context.load_cert_chain(args.tls_cert, args.tls_key)
    return (args.manage_ip, args.manage_port), context


async def serve(args, *, started: Future[None] | None = None, stop_signal: threading.Event | None = None) -> None:
    management_address, management_tls = remote_management_config(args)
    database = Database(args.database_url)
    server: CWMPServer | None = None
    try:
        try:
            await database.init_db()
        except Exception as exc:
            raise RuntimeError(f"could not initialize PostgreSQL: {exc}") from exc

        cpe_manager = CPEManager(database)
        task_manager = Tasks(database)
        event_bus = EventBus()
        cwmp = CWMP(cpe_manager, task_manager, event_bus, max_xml_bytes=args.max_soap_bytes)
        server = CWMPServer(
            server_address=(args.ip, args.port),
            cwmp=cwmp,
            cpe_manager=cpe_manager,
            task_manager=task_manager,
            event_bus=event_bus,
            sock_path=args.socket,
            max_sessions=args.max_sessions,
            max_soap_bytes=args.max_soap_bytes,
            management_address=management_address,
            management_ssl_context=management_tls,
            admin_token=args.admin_token,
        )
        await server.start()

        if started is not None:
            started.set_result(None)
        if stop_signal is not None:
            await asyncio.to_thread(stop_signal.wait)
            return
        stop = asyncio.Event()
        loop = asyncio.get_running_loop()
        for signum in (signal.SIGINT, signal.SIGTERM):
            with contextlib.suppress(NotImplementedError, RuntimeError):
                loop.add_signal_handler(signum, stop.set)
        await stop.wait()
    finally:
        if server is not None:
            await server.shutdown()
        await database.close()


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    configure_logging(args.log_level, log_file=args.log_file)
    if args.cli:
        # cmd2 owns stdin and signals on the main thread. The ACS runs on a
        # second loop; disconnecting the shell does not signal it to shut down.
        stop_signal = threading.Event()
        started: Future[None] = Future()

        def run_server() -> None:
            try:
                asyncio.run(serve(args, started=started, stop_signal=stop_signal))
            except BaseException as exc:
                if not started.done():
                    started.set_exception(exc)
                else:
                    logger.exception("ACS stopped unexpectedly")
                    stop_signal.set()

        server_thread = threading.Thread(target=run_server, name="facs-acs", daemon=False)
        server_thread.start()
        try:
            started.result(timeout=45)
        except BaseException:
            stop_signal.set()
            server_thread.join(timeout=15)
            raise

        def signal_stop(_signum, _frame) -> None:
            stop_signal.set()
            raise KeyboardInterrupt

        previous_handlers = {
            signum: signal.signal(signum, signal_stop)
            for signum in (signal.SIGINT, signal.SIGTERM)
        }
        try:
            try:
                run_cli(sock_path=args.socket)
            except KeyboardInterrupt:
                logger.info("interrupt received; shutting down ACS")
            except Exception:
                logger.exception("attached CLI stopped unexpectedly; ACS remains online")
            if not stop_signal.is_set():
                logger.info("CLI disconnected; ACS remains online. Press Ctrl-C to stop it.")
                with contextlib.suppress(KeyboardInterrupt):
                    stop_signal.wait()
        finally:
            stop_signal.set()
            server_thread.join(timeout=15)
            for signum, previous in previous_handlers.items():
                signal.signal(signum, previous)
    else:
        try:
            asyncio.run(serve(args))
        except KeyboardInterrupt:
            return 130
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
