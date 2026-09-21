"""Bounded, per-CPE HTTP session state."""

import asyncio
import contextlib
import logging
import re
import time
import uuid
from http.cookies import CookieError, SimpleCookie

from src.config import DEFAULT_MAX_SESSIONS

logger = logging.getLogger(__name__)
_SESSION_ID = re.compile(r"^[A-Za-z0-9_.-]{1,128}$")


class SessionCapacityError(RuntimeError):
    """The in-memory session limit was reached."""


class HttpSession:
    def __init__(self, session_id: str, ttl_seconds: int = 3600):
        self.session_id = session_id
        self.lock = asyncio.Lock()
        self.session_requests = 0
        self.created_at = time.monotonic()
        self.last_activity = self.created_at
        self.ttl_seconds = ttl_seconds

        # CWMP conversation state must be scoped to one CPE session.
        self.device_id: str | None = None
        self.informed = False
        self.pending_task_id: str | None = None
        self.pending_method: str | None = None
        self.cwmp_namespace: str | None = None

    def mark_activity(self) -> None:
        self.last_activity = time.monotonic()

    def is_expired(self, now: float | None = None) -> bool:
        return ((now if now is not None else time.monotonic()) - self.last_activity) > self.ttl_seconds

    @staticmethod
    def extract_session_id(headers) -> str | None:
        cookie = SimpleCookie()
        try:
            cookie.load(headers.get("Cookie", ""))
        except CookieError:
            return None
        value = cookie["session"].value if "session" in cookie else None
        return value if value and _SESSION_ID.fullmatch(value) else None


class SessionManager:
    def __init__(
        self,
        ttl_seconds: int = 3600,
        cleanup_period_seconds: int = 300,
        max_sessions: int = DEFAULT_MAX_SESSIONS,
    ):
        if max_sessions < 1:
            raise ValueError("max_sessions must be positive")
        self.sessions: dict[str, HttpSession] = {}
        self.ttl_seconds = ttl_seconds
        self.cleanup_period_seconds = cleanup_period_seconds
        self.max_sessions = max_sessions
        self.request_count = 0
        self.total_sessions_created = 0
        self.total_sessions_expired = 0
        self.total_cleanup_runs = 0
        self._lock = asyncio.Lock()
        self._cleanup_task: asyncio.Task[None] | None = None

    async def start_cleanup_task(self) -> None:
        if self._cleanup_task is None or self._cleanup_task.done():
            self._cleanup_task = asyncio.create_task(self._cleanup_loop(), name="session-cleanup")

    async def stop_cleanup_task(self) -> None:
        if self._cleanup_task is None:
            return
        self._cleanup_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await self._cleanup_task
        self._cleanup_task = None

    async def _cleanup_loop(self) -> None:
        while True:
            try:
                await asyncio.sleep(self.cleanup_period_seconds)
                await self.cleanup_expired_sessions()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("session cleanup failed")

    async def get_or_create_session(self, candidate_id: str | None = None) -> HttpSession:
        async with self._lock:
            self.request_count += 1
            if candidate_id:
                current = self.sessions.get(candidate_id)
                if current is not None and not current.is_expired():
                    current.mark_activity()
                    return current

            # Clear expired entries before refusing a new connection at capacity.
            if len(self.sessions) >= self.max_sessions:
                self._remove_expired_locked(time.monotonic())
            if len(self.sessions) >= self.max_sessions:
                raise SessionCapacityError("maximum active CWMP sessions reached")

            session_id = uuid.uuid4().hex
            session = HttpSession(session_id, self.ttl_seconds)
            self.sessions[session_id] = session
            self.total_sessions_created += 1
            return session

    def _remove_expired_locked(self, now: float) -> int:
        expired_ids = [
            session_id
            for session_id, session in self.sessions.items()
            if not session.lock.locked() and session.is_expired(now)
        ]
        for session_id in expired_ids:
            del self.sessions[session_id]
        self.total_sessions_expired += len(expired_ids)
        return len(expired_ids)

    async def cleanup_expired_sessions(self) -> int:
        async with self._lock:
            self.total_cleanup_runs += 1
            return self._remove_expired_locked(time.monotonic())

    async def invalidate_session(self, session_id: str) -> bool:
        async with self._lock:
            return self.sessions.pop(session_id, None) is not None

    def get_stats(self) -> dict[str, int]:
        return {
            "total_sessions_created": self.total_sessions_created,
            "total_sessions_expired": self.total_sessions_expired,
            "total_cleanup_runs": self.total_cleanup_runs,
            "current_active_sessions": len(self.sessions),
            "total_requests": self.request_count,
        }

    async def clear_all(self) -> None:
        async with self._lock:
            self.sessions.clear()
