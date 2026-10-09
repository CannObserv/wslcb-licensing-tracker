"""Report a backup run to its dead-man monitor (#185).

Ported from CannObserv/watcher's ``src/ops/checkin.py`` (watcher#296 D9, after
broker#3). A backup that fails loudly still says nothing when it stops running:
a disabled timer, a dead VM and a wedged interpreter all produce zero failures
and zero traffic. So the job checks in on **every** run, success or not, and
co-status alarms when a check-in fails to arrive.

``POST {base}/api/v1/monitors/{id}/checkin`` with ``{"status": "ok"|"alert",
"variables": {...}}`` and an ``X-API-Key`` header. An ``ok`` resets the
monitor's timer; an ``alert`` renders its template against ``variables``.
Retry-safe by contract: a replay overwrites the previous check-in.

**Failure never propagates.** A check-in never raises and never changes the
job's exit status: a monitoring path that fails the thing it monitors trains an
operator to ignore both.

**The key is a credential, never an environment variable.** The base URL and
monitor id live in ``/etc/wslcb-licensing-tracker/backup.env``; the key is the
unit's ``checkin-key`` credential, which systemd reads from a root-only file and
hands the run as a private copy under ``$CREDENTIALS_DIRECTORY``. So it is in no
process environment, and no ``/proc/<pid>/environ`` shows it.
"""

import logging
import re
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Literal
from urllib.parse import urlsplit

import httpx

logger = logging.getLogger(__name__)

BASE_URL_ENV = "WSLCB_BACKUP_CHECKIN_BASE_URL"
MONITOR_ID_ENV = "WSLCB_BACKUP_MONITOR_ID"
#: Set by systemd for a unit with credentials; the key is the file named below.
CREDENTIALS_DIRECTORY_ENV = "CREDENTIALS_DIRECTORY"
KEY_CREDENTIAL = "checkin-key"
_KEY_LABEL = f"the {KEY_CREDENTIAL} credential"
TIMEOUT_SECONDS = 10.0

#: One immediate retry on a transport error or a 5xx. A dropped check-in reads
#: as a dead job, and the replay is harmless.
_ATTEMPTS = 2
#: Monitor ids are ULIDs; the value becomes a URL path segment.
_MONITOR_ID_RE = re.compile(r"^[0-9A-Za-z]+$")
_HTTP_OK_MIN, _HTTP_OK_MAX, _HTTP_SERVER_ERROR = 200, 299, 500

Post = Callable[[str, dict, dict, float], int]


def _http_post(url: str, payload: dict, headers: dict, timeout: float) -> int:
    """One POST, returning the status code. The seam the tests replace."""
    with httpx.Client(timeout=timeout) as client:
        return client.post(url, json=payload, headers=headers).status_code


def _read_key(environ: Mapping[str, str]) -> str:
    """The key credential's contents, or "" when there is none to read.

    Under the unit the file always exists — a missing ``LoadCredential=`` source
    fails the start — and is empty until the monitor does. Outside one there is
    no directory.
    """
    directory = environ.get(CREDENTIALS_DIRECTORY_ENV, "").strip()
    if not directory:
        return ""
    try:
        return (Path(directory) / KEY_CREDENTIAL).read_text().strip()
    except FileNotFoundError:
        return ""


def _is_http_base(base: str) -> bool:
    """An http(s) URL with a host and, if any, a numeric port."""
    try:
        parts = urlsplit(base)
        _ = parts.port
    except ValueError:
        return False
    return parts.scheme in ("http", "https") and bool(parts.hostname)


def post_checkin(
    status: Literal["ok", "alert"],
    variables: dict,
    *,
    environ: Mapping[str, str],
    post: Post = _http_post,
) -> bool:
    """Report this run; return whether the check-in landed. Never raises.

    Whatever escapes the configuration checks is logged by type — never by
    message, which may quote the key — and reported as not landed.
    """
    try:
        return _post_checkin(status, variables, environ=environ, post=post)
    except Exception as e:  # noqa: BLE001 — the contract is "never raises"
        logger.error("backup check-in failed: %s — not checking in", type(e).__name__)  # noqa: TRY400
        return False


def _configured(environ: Mapping[str, str]) -> tuple[str, str, str] | None:
    """``(base, monitor_id, key)`` when all three are present and well-formed."""
    base = environ.get(BASE_URL_ENV, "").strip()
    monitor_id = environ.get(MONITOR_ID_ENV, "").strip()
    api_key = _read_key(environ)
    sources = f"{BASE_URL_ENV}, {MONITOR_ID_ENV} and {_KEY_LABEL}"
    present = {
        BASE_URL_ENV: bool(base),
        MONITOR_ID_ENV: bool(monitor_id),
        _KEY_LABEL: bool(api_key),
    }
    if not any(present.values()):
        logger.warning(
            "backup check-in not configured (%s unset) — a stopped backup will not be noticed",
            sources,
        )
        return None
    if not all(present.values()):
        missing = ", ".join(name for name, there in present.items() if not there)
        logger.error(
            "backup check-in half-configured — %s must all be set (missing: %s); not checking in",
            sources,
            missing,
        )
        return None
    if not _is_http_base(base):
        logger.error("%s is not an http(s) URL with a host; not checking in", BASE_URL_ENV)
        return None
    if not _MONITOR_ID_RE.match(monitor_id):
        logger.error("%s is not a bare monitor id; not checking in", MONITOR_ID_ENV)
        return None
    return base, monitor_id, api_key


def _post_checkin(
    status: Literal["ok", "alert"],
    variables: dict,
    *,
    environ: Mapping[str, str],
    post: Post,
) -> bool:
    config = _configured(environ)
    if config is None:
        return False
    base, monitor_id, api_key = config
    url = f"{base.rstrip('/')}/api/v1/monitors/{monitor_id}/checkin"
    payload = {"status": status, "variables": variables}
    headers = {"X-API-Key": api_key}
    failure = ""
    for _ in range(_ATTEMPTS):
        try:
            code = post(url, payload, headers, TIMEOUT_SECONDS)
        except httpx.TransportError as e:
            failure = f"{type(e).__name__}: {e}"
            continue
        if _HTTP_OK_MIN <= code <= _HTTP_OK_MAX:
            return True
        if code < _HTTP_SERVER_ERROR:
            logger.warning(
                "backup check-in rejected with HTTP %s — check the monitor id and key", code
            )
            return False
        failure = f"HTTP {code}"
    logger.warning("backup check-in failed: %s", failure)
    return False
