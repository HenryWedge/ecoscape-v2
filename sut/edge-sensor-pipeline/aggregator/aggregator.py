"""
Aggregator — reads sensor-data streams from all edge zones, computes a
rolling average per zone, and exposes Prometheus metrics on :8000/metrics.

Metrics exported:
  aggregator_zone_avg{zone}              — rolling average of last WINDOW values
  aggregator_messages_consumed_total{zone} — total messages successfully read
  aggregator_zone_offline_seconds_total{zone} — cumulative seconds the zone was
                                                 considered offline (no data)
  aggregator_stream_lag{zone}            — consumer group lag: number of messages
                                           in the stream not yet delivered to the
                                           consumer group (via XINFO GROUPS, Redis 7+)
"""
import logging
import os
import time
import threading
from collections import deque
from typing import Optional

import redis
from prometheus_client import start_http_server, Gauge, Counter

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
ZONES: list[dict] = []
for raw in os.environ.get("ZONES", "").split(","):
    raw = raw.strip()
    if not raw:
        continue
    # Format: <name>:<redis-host>[:<port>]
    parts = raw.split(":")
    name = parts[0]
    host = parts[1] if len(parts) > 1 else "localhost"
    port = int(parts[2]) if len(parts) > 2 else 6379
    ZONES.append({"name": name, "host": host, "port": port})

if not ZONES:
    # Sensible default for local testing
    ZONES = [
        {"name": "berlin", "host": "localhost", "port": 6379},
    ]

STREAM_NAME = os.environ.get("STREAM_NAME", "sensor-data")
CONSUMER_GROUP = os.environ.get("CONSUMER_GROUP", "aggregator")
CONSUMER_NAME = os.environ.get("CONSUMER_NAME", "agg-0")
WINDOW = int(os.environ.get("WINDOW", "60"))          # rolling average window
INTERVAL_MS = int(os.environ.get("INTERVAL_MS", "500"))
OFFLINE_TIMEOUT_S = float(os.environ.get("OFFLINE_TIMEOUT_S", "5"))
METRICS_PORT = int(os.environ.get("METRICS_PORT", "8000"))
READ_COUNT = int(os.environ.get("READ_COUNT", "10"))  # messages per XREADGROUP call
LOG_INTERVAL_S = float(os.environ.get("LOG_INTERVAL_S", "5"))  # avg log frequency

# ---------------------------------------------------------------------------
# Prometheus metrics
# ---------------------------------------------------------------------------
zone_avg = Gauge(
    "aggregator_zone_avg",
    "Rolling average of sensor values over the last WINDOW readings",
    ["zone"],
)
messages_consumed = Counter(
    "aggregator_messages_consumed_total",
    "Total number of messages successfully consumed per zone",
    ["zone"],
)
zone_offline_seconds = Counter(
    "aggregator_zone_offline_seconds_total",
    "Cumulative seconds the zone produced no data (offline simulation)",
    ["zone"],
)
stream_lag = Gauge(
    "aggregator_stream_lag",
    "Approximate number of unread messages in the zone stream",
    ["zone"],
)


# ---------------------------------------------------------------------------
# Per-zone worker
# ---------------------------------------------------------------------------
class ZoneWorker:
    def __init__(self, zone: str, host: str, port: int) -> None:
        self.zone = zone
        self.host = host
        self.port = port
        self.client: Optional[redis.Redis] = None
        self.window: deque[float] = deque(maxlen=WINDOW)
        self.last_message_time: float = time.monotonic()
        self.consumed: int = 0
        self._last_log_time: float = 0.0  # force log on first poll

        # Pre-initialise label combinations so Prometheus shows them from start
        zone_avg.labels(zone=zone).set(0)
        messages_consumed.labels(zone=zone)
        zone_offline_seconds.labels(zone=zone)
        stream_lag.labels(zone=zone).set(0)

    # ------------------------------------------------------------------
    def _connect(self) -> redis.Redis:
        while True:
            try:
                client = redis.Redis(host=self.host, port=self.port, decode_responses=True)
                client.ping()
                log.info("[%s] connected to redis at %s:%d", self.zone, self.host, self.port)
                return client
            except redis.exceptions.ConnectionError:
                log.warning("[%s] redis not ready, retrying in 2s...", self.zone)
                time.sleep(2)

    def _ensure_group(self) -> None:
        try:
            self.client.xgroup_create(STREAM_NAME, CONSUMER_GROUP, id="0", mkstream=True)
            log.info("[%s] consumer group '%s' created", self.zone, CONSUMER_GROUP)
        except redis.exceptions.ResponseError as exc:
            if "BUSYGROUP" in str(exc):
                pass  # group already exists — fine
            else:
                raise

    # ------------------------------------------------------------------
    def _poll(self) -> None:
        messages = self.client.xreadgroup(
            CONSUMER_GROUP,
            CONSUMER_NAME,
            {STREAM_NAME: ">"},
            count=READ_COUNT,
            block=int(INTERVAL_MS),
        )
        if not messages:
            return

        for _stream, entries in messages:
            for msg_id, fields in entries:
                try:
                    value = float(fields["value"])
                except (KeyError, ValueError):
                    continue
                self.window.append(value)
                self.consumed += 1
                messages_consumed.labels(zone=self.zone).inc()
                self.client.xack(STREAM_NAME, CONSUMER_GROUP, msg_id)
                self.last_message_time = time.monotonic()

        if self.window:
            avg = sum(self.window) / len(self.window)
            zone_avg.labels(zone=self.zone).set(avg)

            now = time.monotonic()
            if now - self._last_log_time >= LOG_INTERVAL_S:
                log.info(
                    "[%s] avg=%.4f (window=%d/%d, consumed=%d)",
                    self.zone, avg, len(self.window), WINDOW, self.consumed,
                )
                self._last_log_time = now

    def _update_lag(self) -> None:
        try:
            groups = self.client.xinfo_groups(STREAM_NAME)
            for g in groups:
                if g["name"] == CONSUMER_GROUP:
                    # Redis 7.0+ exposes lag directly in XINFO GROUPS.
                    # lag = messages in the stream after last-delivered-id,
                    # i.e. not yet delivered to this consumer group.
                    lag = g.get("lag", 0) or 0
                    stream_lag.labels(zone=self.zone).set(lag)
                    return
            # Consumer group not found yet — lag unknown, report 0
            stream_lag.labels(zone=self.zone).set(0)
        except Exception:
            pass

    def _check_offline(self, interval_s: float) -> None:
        silent_s = time.monotonic() - self.last_message_time
        if silent_s >= OFFLINE_TIMEOUT_S:
            zone_offline_seconds.labels(zone=self.zone).inc(interval_s)
            log.warning("[%s] zone offline for %.1fs", self.zone, silent_s)

    # ------------------------------------------------------------------
    def run(self) -> None:
        interval_s = INTERVAL_MS / 1000.0
        self.client = self._connect()
        self._ensure_group()

        while True:
            start = time.monotonic()
            try:
                self._poll()
                self._update_lag()
            except redis.exceptions.ConnectionError as exc:
                log.error("[%s] connection lost: %s — reconnecting...", self.zone, exc)
                time.sleep(2)
                self.client = self._connect()
                self._ensure_group()
                continue
            except Exception as exc:
                log.error("[%s] unexpected error: %s", self.zone, exc)

            self._check_offline(interval_s)

            elapsed = time.monotonic() - start
            time.sleep(max(0.0, interval_s - elapsed))


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
def main() -> None:
    log.info("aggregator starting | zones=%s window=%d", [z["name"] for z in ZONES], WINDOW)
    start_http_server(METRICS_PORT)
    log.info("prometheus metrics available on :%d/metrics", METRICS_PORT)

    threads = []
    for zone_cfg in ZONES:
        worker = ZoneWorker(
            zone=zone_cfg["name"],
            host=zone_cfg["host"],
            port=zone_cfg["port"],
        )
        t = threading.Thread(target=worker.run, name=f"worker-{zone_cfg['name']}", daemon=True)
        t.start()
        threads.append(t)

    # Main thread just waits — daemon threads die with the process
    for t in threads:
        t.join()


if __name__ == "__main__":
    main()
