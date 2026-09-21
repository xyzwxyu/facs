"""Environment-backed configuration for the ACS, database, and CLI.

The repository's .env file is optional. Exported variables take precedence;
set FACS_ENV_FILE to load a different file (useful for separate deployments).
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import dotenv_values

DEFAULT_ENV_FILE = Path(__file__).resolve().parents[1] / ".env"
DEFAULT_DATABASE_URL = "postgresql+asyncpg://facs:facs_password@localhost:5432/facs"
DEFAULT_SOCKET_PATH = "/tmp/facs.sock"
DEFAULT_MAX_SESSIONS = 100_000
DEFAULT_MAX_SOAP_BYTES = 16 * 1024 * 1024


def require_asyncpg_url(value: str) -> str:
    if not value.startswith("postgresql+asyncpg://"):
        raise ValueError("DATABASE_URL must use the postgresql+asyncpg:// scheme")
    return value


def load_environment(env_file: str | Path | None = None, *, override: bool = False) -> dict[str, str]:
    """Read dotenv plus exported values without mutating the process environment."""
    path = Path(env_file or os.getenv("FACS_ENV_FILE") or DEFAULT_ENV_FILE).expanduser()
    if path.is_file():
        file_values = {key: value for key, value in dotenv_values(path).items() if value is not None}
    elif env_file is not None or os.getenv("FACS_ENV_FILE"):
        raise FileNotFoundError(f"environment file does not exist: {path}")
    else:
        file_values = {}
    return {**os.environ, **file_values} if override else {**file_values, **os.environ}


def socket_path_from_env() -> str:
    return load_environment().get("FACS_SOCKET_PATH", DEFAULT_SOCKET_PATH)


def _integer(env: dict[str, str], name: str, default: int, *, minimum: int = 0, maximum: int | None = None) -> int:
    raw = env.get(name, str(default))
    try:
        value = int(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer, got {raw!r}") from exc
    if value < minimum or (maximum is not None and value > maximum):
        bound = f" between {minimum} and {maximum}" if maximum is not None else f" at least {minimum}"
        raise ValueError(f"{name} must be{bound}, got {value}")
    return value


@dataclass(frozen=True, slots=True)
class DatabaseSettings:
    url: str
    pool_size: int
    max_overflow: int
    task_lease_seconds: int
    task_max_retries: int

    @classmethod
    def from_env(cls) -> DatabaseSettings:
        env = load_environment()
        return cls(
            url=env.get("DATABASE_URL", DEFAULT_DATABASE_URL),
            pool_size=_integer(env, "FACS_DB_POOL_SIZE", 10, minimum=1),
            max_overflow=_integer(env, "FACS_DB_MAX_OVERFLOW", 20),
            task_lease_seconds=_integer(env, "FACS_TASK_LEASE_SECONDS", 900, minimum=1),
            task_max_retries=_integer(env, "FACS_TASK_MAX_RETRIES", 3),
        )


@dataclass(frozen=True, slots=True)
class ServerSettings:
    ip: str
    port: int
    socket_path: str
    max_sessions: int
    max_soap_bytes: int
    database_url: str
    log_level: str
    log_file: str | None
    manage_ip: str | None
    manage_port: int
    tls_cert: str | None
    tls_key: str | None
    admin_token: str | None

    @classmethod
    def from_env(cls) -> ServerSettings:
        env = load_environment()
        return cls(
            ip=env.get("FACS_IP", "127.0.0.1"),
            port=_integer(env, "FACS_PORT", 8000, minimum=1, maximum=65535),
            socket_path=env.get("FACS_SOCKET_PATH", DEFAULT_SOCKET_PATH),
            max_sessions=_integer(env, "FACS_MAX_SESSIONS", DEFAULT_MAX_SESSIONS, minimum=1),
            max_soap_bytes=_integer(env, "FACS_MAX_SOAP_BYTES", DEFAULT_MAX_SOAP_BYTES, minimum=1),
            database_url=env.get("DATABASE_URL", DEFAULT_DATABASE_URL),
            log_level=env.get("FACS_LOG_LEVEL", "INFO"),
            log_file=env.get("FACS_LOG_FILE") or None,
            manage_ip=env.get("FACS_MANAGE_IP") or None,
            manage_port=_integer(env, "FACS_MANAGE_PORT", 8443, minimum=1, maximum=65535),
            tls_cert=env.get("FACS_TLS_CERT") or None,
            tls_key=env.get("FACS_TLS_KEY") or None,
            admin_token=env.get("FACS_ADMIN_TOKEN") or None,
        )


@dataclass(frozen=True, slots=True)
class CliSettings:
    socket_path: str
    url: str | None
    token: str | None
    ca_file: str | None

    @classmethod
    def from_env(cls) -> CliSettings:
        env = load_environment()
        return cls(
            socket_path=env.get("FACS_SOCKET_PATH", DEFAULT_SOCKET_PATH),
            url=env.get("FACS_CLI_URL") or None,
            token=env.get("FACS_ADMIN_TOKEN") or None,
            ca_file=env.get("FACS_CA_FILE") or None,
        )
