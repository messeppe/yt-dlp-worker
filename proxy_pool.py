import os
import random
import threading
import time


PROXY_COOLDOWN = int(os.environ.get("PROXY_COOLDOWN", "60"))


class ProxyPool:
    def __init__(self, url_template: str, size: int, name: str = "proxy"):
        self.url_template = url_template
        self.size = size
        self.name = name
        self._cooldowns: dict[int, float] = {}
        self._lock = threading.Lock()

    def pick(self) -> int:
        """Return a random proxy idx not on cooldown. Falls back to soonest-expiring if all cooled."""
        now = time.time()
        with self._lock:
            available = [i for i in range(1, self.size + 1) if self._cooldowns.get(i, 0) <= now]
            if available:
                return random.choice(available)
            return min(range(1, self.size + 1), key=lambda i: self._cooldowns.get(i, 0))

    def has_available(self) -> bool:
        now = time.time()
        with self._lock:
            return any(self._cooldowns.get(i, 0) <= now for i in range(1, self.size + 1))

    def mark_failed(self, idx: int, cooldown_secs: int = PROXY_COOLDOWN) -> None:
        with self._lock:
            self._cooldowns[idx] = time.time() + cooldown_secs

    def make_proxies(self, idx: int) -> dict:
        url = (
            self.url_template.replace("-rotate", f"-{idx}", 1)
            if "-rotate" in self.url_template
            else self.url_template
        )
        return {"http": url, "https": url}


def build_pools() -> tuple["ProxyPool", "ProxyPool | None"]:
    """Return (primary_pool, secondary_pool_or_None) from env vars."""
    primary = ProxyPool(
        url_template=os.environ["PROXY_URL"],
        size=int(os.environ.get("PROXY_POOL_SIZE", "100")),
        name="webshare",
    )
    secondary = None
    if os.environ.get("PROXY_B_URL"):
        secondary = ProxyPool(
            url_template=os.environ["PROXY_B_URL"],
            size=int(os.environ.get("PROXY_B_POOL_SIZE", "100")),
            name=os.environ.get("PROXY_B_NAME", "proxy-b"),
        )
    return primary, secondary


def pick_pool(primary: "ProxyPool", secondary: "ProxyPool | None") -> "ProxyPool":
    """Use primary unless fully cooled and secondary is available."""
    if secondary and not primary.has_available():
        return secondary
    return primary
