from __future__ import annotations

import hashlib
import hmac
import time


def auth_cookie_name(project_root: str) -> str:
    suffix = hashlib.sha256(project_root.encode("utf-8")).hexdigest()[:10]
    return "slurm_manager_auth_" + suffix


def make_auth_token(password: str, now: int | None = None) -> str:
    issued_at = int(time.time() if now is None else now)
    timestamp = str(issued_at)
    signature = hmac.new(password.encode("utf-8"), timestamp.encode("ascii"), hashlib.sha256).hexdigest()
    return timestamp + "." + signature


def valid_auth_token(token: str, password: str, max_age_seconds: int = 21600, now: int | None = None) -> bool:
    try:
        timestamp, supplied = token.split(".", 1)
        issued_at = int(timestamp)
    except (AttributeError, ValueError):
        return False
    current = int(time.time() if now is None else now)
    if issued_at > current + 60 or current - issued_at > max_age_seconds:
        return False
    expected = hmac.new(password.encode("utf-8"), timestamp.encode("ascii"), hashlib.sha256).hexdigest()
    return hmac.compare_digest(supplied, expected)
