"""Admin authentication middleware for the WSLCB Licensing Tracker.

Leverages exe.dev proxy headers (``X-ExeDev-Email``, ``X-ExeDev-UserID``) for
authentication.  Falls back to ``ADMIN_DEV_EMAIL`` / ``ADMIN_DEV_USERID`` env
vars when the headers are absent (local development only).  Production deploys
always have the proxy headers present.

Public API
----------
- :func:`require_admin` — FastAPI dependency that enforces admin access.
- :func:`get_current_user` — non-enforcing version for conditional UI.
"""
import logging
import os

from fastapi import Request

from database import get_db

logger = logging.getLogger(__name__)


def _extract_user(request: Request) -> tuple[str | None, str | None]:
    """Return ``(email, user_id)`` from proxy headers or env-var fallback."""
    email = request.headers.get("X-ExeDev-Email")
    user_id = request.headers.get("X-ExeDev-UserID")
    if not email:
        email = os.environ.get("ADMIN_DEV_EMAIL")
        user_id = os.environ.get("ADMIN_DEV_USERID", "dev")
    return email, user_id


def _lookup_admin(email: str) -> dict | None:
    """Return the admin row for *email* (case-insensitive), or ``None``."""
    with get_db() as conn:
        row = conn.execute(
            "SELECT id, email, role FROM admin_users WHERE email = ? COLLATE NOCASE",
            (email,),
        ).fetchone()
    if row is None:
        return None
    return {"id": row[0], "email": row[1], "role": row[2]}


async def get_current_user(request: Request) -> dict | None:
    """Return the admin dict for the current request, or ``None``.

    Never raises or redirects — safe to use for conditional UI rendering.
    """
    email, user_id = _extract_user(request)
    if not email:
        return None
    admin = _lookup_admin(email)
    if admin:
        admin["user_id"] = user_id
    return admin


async def require_admin(request: Request) -> dict:
    """FastAPI dependency that enforces admin access.

    Returns the admin dict ``{id, email, role, user_id}`` on success.
    Redirects to the exe.dev login page if no credentials are present.
    Returns HTTP 403 if credentials are present but not in ``admin_users``.
    """
    email, user_id = _extract_user(request)
    if not email:
        redirect_path = str(request.url.path)
        if request.url.query:
            redirect_path += f"?{request.url.query}"
        raise _forbidden_redirect(redirect_path)
    admin = _lookup_admin(email)
    if admin is None:
        logger.warning("Admin access denied for email: %s", email)
        from fastapi import HTTPException
        raise HTTPException(status_code=403, detail="Forbidden")
    admin["user_id"] = user_id
    return admin


def _forbidden_redirect(path: str) -> "_RedirectException":
    """Return the sentinel exception that triggers a login redirect."""
    return _RedirectException(f"/__exe.dev/login?redirect={path}")


class _RedirectException(Exception):  # noqa: N818
    """Sentinel raised by :func:`require_admin` to trigger a login redirect."""

    def __init__(self, location: str) -> None:
        self.location = location



