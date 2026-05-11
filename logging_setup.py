import json
import logging
import os
import re
import sys
from datetime import datetime, timezone


EVENT_PATTERN = re.compile(r"^\[([A-Z0-9\-]+)\]\s*")
ENVIRONMENT = os.environ.get("APP_ENV") or os.environ.get("ENVIRONMENT") or "production"
SERVICE_VERSION = (
    os.environ.get("APP_VERSION")
    or os.environ.get("SERVICE_VERSION")
    or os.environ.get("COOLIFY_BRANCH")
    or "unknown"
)


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        message = record.getMessage()
        event = getattr(record, "event", None)
        if not event:
            m = EVENT_PATTERN.match(message)
            if m:
                event = m.group(1)

        ts = datetime.now(timezone.utc).isoformat()
        payload = {
            "@timestamp": ts,
            "timestamp": ts,
            "severity": record.levelname,
            "level": record.levelname,  # backward compatibility
            "message": message,
            "event": event,
            "service": record.name,  # backward compatibility
            "service.name": record.name,
            "service.version": SERVICE_VERSION,
            "deployment.environment": ENVIRONMENT,
            "process.pid": record.process,
            "process.thread.name": record.threadName,
            "log.logger": record.name,
            "log.origin.file.name": record.filename,
            "log.origin.function": record.funcName,
            "log.origin.line": record.lineno,
        }

        for key in ("video_id", "lang", "proxy", "attempt", "status"):
            value = getattr(record, key, None)
            if value is not None:
                payload[key] = value
        fields = getattr(record, "fields", None)
        if isinstance(fields, dict):
            payload.update(fields)

        if record.exc_info:
            payload["error.type"] = record.exc_info[0].__name__
            payload["error.message"] = str(record.exc_info[1])
            payload["error.stack_trace"] = self.formatException(record.exc_info)

        return json.dumps(payload, ensure_ascii=True)


def setup_logging(service_name: str) -> logging.Logger:
    logger = logging.getLogger(service_name)
    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    logger.addHandler(handler)
    logger.propagate = False
    return logger


def log_event(logger: logging.Logger, level: str, event: str, message: str, **fields) -> None:
    lvl = getattr(logging, level.upper(), logging.INFO)
    logger.log(lvl, message, extra={"event": event, "fields": fields})
