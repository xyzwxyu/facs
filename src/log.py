"""Logging configuration without import-time files or duplicate handlers."""

import logging

from aiohttp.abc import AbstractAccessLogger


class CustomAccessLogger(AbstractAccessLogger):
    def log(self, request, response, time) -> None:
        self.logger.info(
            '%s "%s %s" %.3fs %s',
            request.remote,
            request.method,
            request.path,
            time,
            response.status,
        )


def configure_logging(level: str = "INFO", *, log_file: str | None = None) -> None:
    numeric_level = getattr(logging, level.upper(), logging.INFO)
    handlers: list[logging.Handler] = [logging.StreamHandler()]
    if log_file:
        handlers.append(logging.FileHandler(log_file))
    logging.basicConfig(
        level=numeric_level,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
        handlers=handlers,
        force=True,
    )


logger = logging.getLogger("FACS")
