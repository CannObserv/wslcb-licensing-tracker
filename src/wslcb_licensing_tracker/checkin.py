"""Report a timer job's run to its co-status dead-man monitor (#185, #196, #207).

Ported from CannObserv/watcher's ``src/ops/checkin.py`` (watcher#296 D9, after
broker#3). A job that fails loudly still says nothing when it stops running:
a disabled timer, a dead VM and a wedged interpreter all produce zero failures
and zero traffic. So the job checks in on **every** run, success or not, and
co-status alarms when a check-in fails to arrive.

Three jobs check in, each to its own monitor: the nightly backup (``BACKUP``),
the twice-daily scrape (``SCRAPE``) and the weekly entity classification
(``CLASSIFY``; the post-scrape pass also alerts to it, never ``ok``). One
implementation; a ``Monitor`` names which environment variables configure it.

``POST {base}/api/v1/monitors/{id}/checkin`` with ``{"status": "ok"|"alert",
"variables": {...}}`` (plus ``"metadata"`` when given) and an ``X-API-Key``
header. An ``ok`` resets the monitor's timer; an ``alert`` renders its template
against ``variables``. co-status folds a run of ``alert``s into one fault, and
``metadata.fault`` is the one key it reads: a new value opens a new fault.
Retry-safe by contract: a replay overwrites the previous check-in.

**Failure never propagates.** A check-in never raises and never changes the
job's exit status: a monitoring path that fails the thing it monitors trains an
operator to ignore both.

**The key is a credential, never an environment variable.** The base URL and
monitor id are environment variables (the backup's in
``/etc/wslcb-licensing-tracker/backup.env``, the scrape's in the app ``.env``);
the key is the unit's ``checkin-key`` credential, which systemd reads from a
root-only file and hands the run as a private copy under
``$CREDENTIALS_DIRECTORY``. So it is in no process environment, and no
``/proc/<pid>/environ`` shows it.
"""

import logging
import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Literal
from urllib.parse import urlsplit

import httpx

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class Monitor:
    """Which job is checking in, and where its configuration lives."""

    #: Names the job in every log line ("backup check-in failed: …").
    label: str
    base_url_env: str
    monitor_id_env: str


BACKUP = Monitor("backup", "WSLCB_BACKUP_CHECKIN_BASE_URL", "WSLCB_BACKUP_MONITOR_ID")
SCRAPE = Monitor("scrape", "WSLCB_SCRAPE_CHECKIN_BASE_URL", "WSLCB_SCRAPE_MONITOR_ID")
# Prefixed, as every variable added from #204 on; the two above keep their names.
CLASSIFY = Monitor(
    "classify",
    "CO_WSLCB_LICENSING_TRACKER_CLASSIFY_CHECKIN_BASE_URL",
    "CO_WSLCB_LICENSING_TRACKER_CLASSIFY_MONITOR_ID",
)

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


def post_checkin(  # noqa: PLR0913 — the seams are keyword-only
    status: Literal["ok", "alert"],
    variables: dict,
    *,
    monitor: Monitor,
    environ: Mapping[str, str],
    metadata: dict | None = None,
    post: Post = _http_post,
) -> bool:
    """Report this run; return whether the check-in landed. Never raises.

    Whatever escapes the configuration checks is logged by type — never by
    message, which may quote the key — and reported as not landed.
    """
    try:
        return _post_checkin(
            status, variables, monitor=monitor, environ=environ, metadata=metadata, post=post
        )
    except Exception as e:  # noqa: BLE001 — the contract is "never raises"
        logger.error(  # noqa: TRY400
            "%s check-in failed: %s — not checking in", monitor.label, type(e).__name__
        )
        return False


def _configured(monitor: Monitor, environ: Mapping[str, str]) -> tuple[str, str, str] | None:
    """``(base, monitor_id, key)`` when all three are present and well-formed."""
    base = environ.get(monitor.base_url_env, "").strip()
    monitor_id = environ.get(monitor.monitor_id_env, "").strip()
    api_key = _read_key(environ)
    sources = f"{monitor.base_url_env}, {monitor.monitor_id_env} and {_KEY_LABEL}"
    present = {
        monitor.base_url_env: bool(base),
        monitor.monitor_id_env: bool(monitor_id),
        _KEY_LABEL: bool(api_key),
    }
    if not any(present.values()):
        logger.warning(
            "%s check-in not configured (%s unset) — a stopped %s will not be noticed",
            monitor.label,
            sources,
            monitor.label,
        )
        return None
    if not all(present.values()):
        missing = ", ".join(name for name, there in present.items() if not there)
        logger.error(
            "%s check-in half-configured — %s must all be set (missing: %s); not checking in",
            monitor.label,
            sources,
            missing,
        )
        return None
    if not _is_http_base(base):
        logger.error("%s is not an http(s) URL with a host; not checking in", monitor.base_url_env)
        return None
    if not _MONITOR_ID_RE.match(monitor_id):
        logger.error("%s is not a bare monitor id; not checking in", monitor.monitor_id_env)
        return None
    return base, monitor_id, api_key


def _post_checkin(  # noqa: PLR0913 — post_checkin's arguments, passed through
    status: Literal["ok", "alert"],
    variables: dict,
    *,
    monitor: Monitor,
    environ: Mapping[str, str],
    metadata: dict | None,
    post: Post,
) -> bool:
    config = _configured(monitor, environ)
    if config is None:
        return False
    base, monitor_id, api_key = config
    url = f"{base.rstrip('/')}/api/v1/monitors/{monitor_id}/checkin"
    payload: dict = {"status": status, "variables": variables}
    if metadata is not None:
        payload["metadata"] = metadata
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
                "%s check-in rejected with HTTP %s — check the monitor id and key",
                monitor.label,
                code,
            )
            return False
        failure = f"HTTP {code}"
    logger.warning("%s check-in failed: %s", monitor.label, failure)
    return False
