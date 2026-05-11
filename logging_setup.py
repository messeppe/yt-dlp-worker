import json
import logging
import re
import sys
from datetime import datetime, timezone


EVENT_PATTERN = re.compile(r"^\[([A-Z0-9\-]+)\]\s*")


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        message = record.getMessage()
        event = getattr(record, "event", None)
        if not event:
            m = EVENT_PATTERN.match(message)
            if m:
                event = m.group(1)

        payload = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "service": record.name,
            "event": event,
            "message": message,
        }

        for key in ("video_id", "lang", "proxy", "attempt", "status"):
            value = getattr(record, key, None)
            if value is not None:
                payload[key] = value

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
