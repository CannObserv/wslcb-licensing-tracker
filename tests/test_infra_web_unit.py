"""Tests for wslcb-web.service's ordering on PostgreSQL (#184).

The unit used to declare only ``After=network.target``, so at boot systemd was
free to start uvicorn before the local cluster accepted connections. On the
2026-09-29 reboot (#184) the app's startup landed 1.7 s after Postgres was
ready, which was luck, not configuration. CannObserv/address-validator#239 had
the same shape, and there the first start failed.

``After=postgresql.service`` waits for every cluster. Ubuntu's
``postgresql@.service`` declares ``Before=postgresql.service`` and is
``Type=forking`` via ``pg_ctlcluster``, which returns only once the cluster
accepts connections. The dependency is ordering only. Both units are
``WantedBy=multi-user.target``, so boot pulls both in without help. ``Wants=``
would make every start of the web unit, including ``wslcb-healthcheck``'s
automatic restart, start a cluster an operator stopped on purpose, because
``postgresql.service`` wants ``postgresql@16-main``. ``Requires=``, ``BindsTo=``
or ``PartOf=`` would stop the web app whenever Postgres stops or restarts,
including the security-update restarts #184 measured at 1.46 s. The app rides
those out on its own, with two 503s and no restart.
"""

from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
WEB_UNIT = REPO_ROOT / "infra" / "wslcb-web.service"
POSTGRES = "postgresql.service"


def _unit_list(key: str) -> list[str]:
    """Collect every space-separated value of a list directive in [Unit]."""
    values: list[str] = []
    section = None
    for raw in WEB_UNIT.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if line.startswith("[") and line.endswith("]"):
            section = line
            continue
        if section != "[Unit]" or line.startswith("#") or "=" not in line:
            continue
        name, _, value = line.partition("=")
        if name.strip() == key:
            values.extend(value.split())
    return values


def test_web_starts_after_postgres() -> None:
    assert POSTGRES in _unit_list("After")


def test_web_neither_pulls_in_nor_binds_to_postgres() -> None:
    """Starting the web app must not start Postgres, and a Postgres restart must
    not stop the web app."""
    for key in ("Wants", "Requires", "Requisite", "BindsTo", "PartOf", "Upholds"):
        assert not any(v.startswith("postgresql") for v in _unit_list(key)), key


def test_web_keeps_network_ordering() -> None:
    assert "network.target" in _unit_list("After")
