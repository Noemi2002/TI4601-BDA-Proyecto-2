import os

import redis


VALKEY_URL = os.getenv(
    "VALKEY_URL",
    "redis://valkey:6379/0",
)

LOCK_TTL_MS = int(
    os.getenv("LOCK_TTL_MS", "120000")
)


valkey = redis.Redis.from_url(
    VALKEY_URL,
    decode_responses=True,
    socket_connect_timeout=2,
    socket_timeout=2,
)