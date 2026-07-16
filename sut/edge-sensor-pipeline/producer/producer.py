import os
import random
import time
import logging

import redis

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

ZONE = os.environ.get("ZONE", "unknown")
REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")
REDIS_PORT = int(os.environ.get("REDIS_PORT", "6379"))
INTERVAL_MS = int(os.environ.get("INTERVAL_MS", "500"))
VALUE_BASE = float(os.environ.get("VALUE_BASE", "20.0"))
VALUE_DRIFT = float(os.environ.get("VALUE_DRIFT", "5.0"))
STREAM_NAME = os.environ.get("STREAM_NAME", "sensor-data")
# Keep the stream bounded so it doesn't grow unbounded on the redis instance
STREAM_MAXLEN = int(os.environ.get("STREAM_MAXLEN", "10000"))


def connect(host: str, port: int) -> redis.Redis:
    client = redis.Redis(host=host, port=port, decode_responses=True)
    # Retry until Redis is ready (useful during pod startup)
    while True:
        try:
            client.ping()
            log.info("connected to redis at %s:%d", host, port)
            return client
        except redis.exceptions.ConnectionError:
            log.warning("redis not ready, retrying in 2s...")
            time.sleep(2)


def main() -> None:
    log.info(
        "producer starting | zone=%s redis=%s:%d interval=%dms base=%.1f drift=%.1f",
        ZONE, REDIS_HOST, REDIS_PORT, INTERVAL_MS, VALUE_BASE, VALUE_DRIFT,
    )
    client = connect(REDIS_HOST, REDIS_PORT)
    interval_s = INTERVAL_MS / 1000.0

    while True:
        start = time.monotonic()
        value = VALUE_BASE + random.uniform(-VALUE_DRIFT, VALUE_DRIFT)
        timestamp_ms = int(time.time() * 1000)

        try:
            client.xadd(
                STREAM_NAME,
                {
                    "zone": ZONE,
                    "value": f"{value:.4f}",
                    "timestamp_ms": timestamp_ms,
                },
                maxlen=STREAM_MAXLEN,
                approximate=True,
            )
            log.debug("xadd zone=%s value=%.4f", ZONE, value)
        except redis.exceptions.ConnectionError as exc:
            log.error("redis connection lost: %s — retrying...", exc)
            time.sleep(2)
            client = connect(REDIS_HOST, REDIS_PORT)
            continue

        elapsed = time.monotonic() - start
        sleep_time = max(0.0, interval_s - elapsed)
        time.sleep(sleep_time)


if __name__ == "__main__":
    main()
