"""Every environment variable the project consumes carries ``ENV_PREFIX`` (#204).

Enforced, not just written down: ``.env.example`` (the canonical list), every
env read in ``src/`` and ``alembic/env.py``, the relay script and plist, and
the systemd units. Names the project does not own are allow-listed below,
each with its reason; ``.env.example`` names them too.

The scan is static: it resolves an env read's name from a literal, an
``f"{ENV_PREFIX}…"`` string, a module-level constant, or a ``for`` loop over
a tuple of those. A read it cannot resolve fails, so a new reader cannot slip
past by building its name some other way.
"""

import ast
import plistlib
import re
from pathlib import Path

import pytest

from wslcb_licensing_tracker.config import ENV_PREFIX, LEGACY_ENV_NAMES

REPO_ROOT = Path(__file__).resolve().parent.parent
SRC = REPO_ROOT / "src" / "wslcb_licensing_tracker"
ENV_EXAMPLE = REPO_ROOT / ".env.example"
RELAY_SCRIPT = REPO_ROOT / "scripts" / "relay" / "relay-capture.sh"
RELAY_PLIST = REPO_ROOT / "scripts" / "relay" / "wslcb-relay.plist"
INFRA = REPO_ROOT / "infra"

#: Read by this project but named by someone else. Keep in step with the
#: exceptions paragraph in ``.env.example``.
ALLOWED = {
    "GOOGLE_APPLICATION_CREDENTIALS": "the GCS SDK's; wslcb-backup.service sets it",
    "CREDENTIALS_DIRECTORY": "systemd's, for a unit with LoadCredential=",
    "PYTHONUNBUFFERED": "Python's; the units set it",
    "PYTHONDONTWRITEBYTECODE": "Python's; wslcb-backup.service sets it",
}

#: Functions whose body reads a *parameter* name: the helper itself.
_HELPERS = {("config.py", "env")}


def _files() -> list[Path]:
    return [*sorted(SRC.rglob("*.py")), REPO_ROOT / "alembic" / "env.py"]


def _resolve_fstring(
    node: ast.JoinedStr, consts: dict[str, str], local: dict[str, list[str]]
) -> list[str] | None:
    """``f"{ENV_PREFIX}NAME"``, resolved; None when any part isn't a known string."""
    parts = []
    for value in node.values:
        if isinstance(value, ast.Constant):
            parts.append(value.value)
            continue
        resolved = (
            _resolve(value.value, consts, local) if isinstance(value, ast.FormattedValue) else None
        )
        if resolved is None or len(resolved) != 1:
            return None
        parts.append(resolved[0])
    return ["".join(parts)]


def _resolve(
    node: ast.AST, consts: dict[str, str], local: dict[str, list[str]]
) -> list[str] | None:
    """The env-var name(s) *node* evaluates to, or None when it can't tell."""
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return [node.value]
    if isinstance(node, ast.JoinedStr):
        return _resolve_fstring(node, consts, local)
    if isinstance(node, ast.Name):
        if node.id in local:
            return local[node.id]
        if node.id in consts:
            return [consts[node.id]]
    return None


def _module_constants() -> dict[str, str]:
    """Every module-level ``NAME = <str>`` across the scanned files, resolved."""
    consts: dict[str, str] = {"ENV_PREFIX": ENV_PREFIX}
    trees = [ast.parse(p.read_text()) for p in _files()]
    for _ in range(3):  # constants built from constants in other modules
        for tree in trees:
            for stmt in tree.body:
                target = None
                if isinstance(stmt, ast.Assign) and len(stmt.targets) == 1:
                    target, value = stmt.targets[0], stmt.value
                elif isinstance(stmt, ast.AnnAssign) and stmt.value is not None:
                    target, value = stmt.target, stmt.value
                if isinstance(target, ast.Name):
                    resolved = _resolve(value, consts, {})
                    if resolved and len(resolved) == 1:
                        consts[target.id] = resolved[0]
    return consts


def _is_environ(node: ast.AST) -> bool:
    """``os.environ`` or a parameter named ``environ``."""
    if isinstance(node, ast.Attribute):
        return node.attr == "environ" and isinstance(node.value, ast.Name) and node.value.id == "os"
    return isinstance(node, ast.Name) and node.id == "environ"


class _Reads(ast.NodeVisitor):
    """Collect (line, how, name-node) for every env read, with loop-variable scopes."""

    def __init__(self, consts: dict[str, str]) -> None:
        self.consts = consts
        self.local: dict[str, list[str]] = {}
        self.func: list[str] = []
        self.reads: list[tuple[int, str, ast.AST, dict[str, list[str]], str]] = []
        self.monitors: list[tuple[int, ast.AST]] = []

    def _add(self, line: int, how: str, arg: ast.AST) -> None:
        self.reads.append((line, how, arg, dict(self.local), self.func[-1] if self.func else ""))

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        saved = self.local
        self.local = {}
        self.func.append(node.name)
        self.generic_visit(node)
        self.func.pop()
        self.local = saved

    visit_AsyncFunctionDef = visit_FunctionDef  # noqa: N815 — ast API

    def _bind(self, target: ast.AST, iterable: ast.AST) -> None:
        """``for name in (A_ENV, B_ENV)``: *name* is each of those."""
        if isinstance(target, ast.Name) and isinstance(iterable, ast.Tuple | ast.List):
            names = [_resolve(e, self.consts, self.local) for e in iterable.elts]
            if all(n and len(n) == 1 for n in names):
                self.local[target.id] = [n[0] for n in names]

    def visit_For(self, node: ast.For) -> None:
        self._bind(node.target, node.iter)
        self.generic_visit(node)

    def _visit_comp(
        self, node: ast.ListComp | ast.SetComp | ast.GeneratorExp | ast.DictComp
    ) -> None:
        # Bind first: the element expression is visited before the generators.
        for gen in node.generators:
            self._bind(gen.target, gen.iter)
        self.generic_visit(node)

    visit_ListComp = visit_SetComp = visit_GeneratorExp = visit_DictComp = _visit_comp  # noqa: N815

    def visit_Call(self, node: ast.Call) -> None:
        func = node.func
        for kw in node.keywords:  # click.option(..., envvar=NAME) reads it, bypassing env()
            if kw.arg == "envvar":
                self._add(node.lineno, "raw", kw.value)
        if node.args:
            first = node.args[0]
            if isinstance(func, ast.Attribute) and func.attr in ("get", "pop", "setdefault"):
                if _is_environ(func.value):
                    self._add(node.lineno, "raw", first)
            elif isinstance(func, ast.Attribute) and func.attr == "getenv":
                self._add(node.lineno, "raw", first)
            elif (isinstance(func, ast.Name) and func.id == "env") or (
                isinstance(func, ast.Attribute) and func.attr == "env"
            ):
                self._add(node.lineno, "env", first)
            if isinstance(func, ast.Name) and func.id == "Monitor":
                self.monitors.extend((node.lineno, a) for a in node.args[1:])
        self.generic_visit(node)

    def visit_Subscript(self, node: ast.Subscript) -> None:
        if isinstance(node.ctx, ast.Load) and _is_environ(node.value):
            self._add(node.lineno, "raw", node.slice)
        self.generic_visit(node)


def _ok(name: str) -> bool:
    return name.startswith(ENV_PREFIX) or name in ALLOWED


def _check_read(how: str, names: list[str]) -> list[str]:
    """What's wrong with one resolved read."""
    problems = []
    for name in names:
        if not _ok(name):
            problems.append(f"{name} lacks {ENV_PREFIX}")
        elif how == "raw" and name in LEGACY_ENV_NAMES:
            problems.append(f"read {name} with config.env (legacy fallback)")
    return problems


def _scan() -> tuple[list[str], list[str]]:
    """(problems, every name read)."""
    consts = _module_constants()
    problems: list[str] = []
    seen: list[str] = []
    for path in _files():
        rel = path.relative_to(REPO_ROOT) if path.is_relative_to(REPO_ROOT) else path
        visitor = _Reads(consts)
        visitor.visit(ast.parse(path.read_text()))
        for line, how, arg, local, func in visitor.reads:
            if (path.name, func) in _HELPERS:
                continue
            if isinstance(arg, ast.Attribute) and arg.attr in ("base_url_env", "monitor_id_env"):
                continue  # a Monitor's field: its constructor args are checked below
            names = _resolve(arg, consts, local)
            if names is None:
                problems.append(f"{rel}:{line}: can't resolve the env name ({ast.unparse(arg)})")
                continue
            seen.extend(names)
            problems.extend(f"{rel}:{line}: {p}" for p in _check_read(how, names))
        for line, arg in visitor.monitors:
            names = _resolve(arg, consts, {})
            if names is None or not all(_ok(n) for n in names):
                problems.append(
                    f"{rel}:{line}: Monitor env name {ast.unparse(arg)} lacks the prefix"
                )
            else:
                seen.extend(names)
    return problems, seen


class TestSource:
    def test_every_env_read_is_prefixed(self):
        problems, _ = _scan()
        assert problems == []

    def test_the_scan_sees_the_readers(self):
        """Guard the guard: a scan that finds nothing passes vacuously."""
        _, seen = _scan()
        for name in (
            "DATABASE_URL",
            "BUILD_ID",
            "BACKUP_BUCKET",
            "SCRAPE_MONITOR_ID",
            "RELAY_INBOX",
        ):
            assert f"{ENV_PREFIX}{name}" in seen
        assert "GOOGLE_APPLICATION_CREDENTIALS" in seen

    def test_the_scan_catches_an_unprefixed_read(self, tmp_path, monkeypatch):
        bad = tmp_path / "bad.py"
        bad.write_text(
            'import os\nNAME = "DATABASE_URL"\nos.environ.get(NAME)\nos.getenv("X_Y")\n'
            'click.option("--bucket", envvar="WSLCB_BACKUP_BUCKET")\n'
        )
        monkeypatch.setattr(f"{__name__}._files", lambda: [bad])
        problems, _ = _scan()
        assert len(problems) == 3

    def test_the_scan_refuses_what_it_cant_resolve(self, tmp_path, monkeypatch):
        bad = tmp_path / "bad.py"
        bad.write_text('import os\ndef f(n):\n    return os.environ.get("X_" + n)\n')
        monkeypatch.setattr(f"{__name__}._files", lambda: [bad])
        problems, _ = _scan()
        assert len(problems) == 1
        assert "can't resolve" in problems[0]


class TestEnvExample:
    def _names(self) -> list[str]:
        return re.findall(r"^#?([A-Z][A-Z0-9_]+)=", ENV_EXAMPLE.read_text(), re.MULTILINE)

    def test_every_name_is_prefixed(self):
        names = self._names()
        assert len(names) > 20
        assert [n for n in names if not _ok(n)] == []

    def test_every_renamed_variable_is_documented(self):
        assert set(LEGACY_ENV_NAMES) - set(self._names()) == set()

    def test_exceptions_are_documented(self):
        text = ENV_EXAMPLE.read_text()
        assert [n for n in ALLOWED if n not in text] == []


class TestLegacyMap:
    def test_new_names_prefixed_and_legacy_names_not(self):
        assert LEGACY_ENV_NAMES
        for new, old in LEGACY_ENV_NAMES.items():
            assert new.startswith(ENV_PREFIX)
            assert not old.startswith(ENV_PREFIX)

    def test_each_legacy_name_is_the_suffix_or_its_wslcb_form(self):
        """``DATABASE_URL`` → ``…_DATABASE_URL``; ``WSLCB_X`` drops the redundant ``WSLCB_``."""
        for new, old in LEGACY_ENV_NAMES.items():
            suffix = new.removeprefix(ENV_PREFIX)
            assert old in (suffix, f"WSLCB_{suffix}")


class TestRelay:
    def test_script_expands_no_legacy_name_directly(self):
        """Legacy names reach the script only through its fallback helper."""
        text = RELAY_SCRIPT.read_text()
        assert re.findall(r"\$\{?WSLCB_[A-Z_]+", text) == []

    def test_script_reads_each_relay_variable_by_suffix(self):
        text = RELAY_SCRIPT.read_text()
        for suffix in ("RELAY_HOST", "RELAY_INBOX", "RELAY_URL", "RELAY_CHECKOUT"):
            assert f"{ENV_PREFIX}{suffix}" in LEGACY_ENV_NAMES
            assert re.search(rf"env_or_legacy {suffix}\b", text), suffix

    def test_plist_sets_only_prefixed_names(self):
        names = plistlib.loads(RELAY_PLIST.read_bytes())["EnvironmentVariables"]
        assert [n for n in names if not n.startswith(ENV_PREFIX)] == []


class TestUnits:
    @pytest.mark.parametrize(
        "unit", sorted(p for p in INFRA.rglob("*") if p.suffix in (".service", ".conf")), ids=str
    )
    def test_environment_lines_prefixed_or_allowed(self, unit):
        names = re.findall(r"^Environment=([A-Z][A-Z0-9_]*)=", unit.read_text(), re.MULTILINE)
        assert [n for n in names if not _ok(n)] == []

    def test_web_unit_writes_the_prefixed_build_id(self):
        text = (INFRA / "wslcb-web.service").read_text()
        (pre,) = re.findall(r"^ExecStartPre=.*$", text, re.MULTILINE)
        assert re.findall(r"echo ([A-Z][A-Z0-9_]*)=", pre) == [f"{ENV_PREFIX}BUILD_ID"]


class TestSuitePromotion:
    """The suite runs the same under either spelling of its own variables (#204)."""

    def test_legacy_names_move_onto_the_prefixed_ones(self):
        from ._env import promote_legacy_env_names

        environ = {"TEST_DATABASE_URL": "t", "REQUIRE_PG_TESTS": "1", "UNRELATED": "u"}
        promote_legacy_env_names(environ)
        assert environ == {
            f"{ENV_PREFIX}TEST_DATABASE_URL": "t",
            f"{ENV_PREFIX}REQUIRE_PG_TESTS": "1",
            "UNRELATED": "u",
        }

    def test_a_prefixed_name_already_set_wins(self):
        from ._env import promote_legacy_env_names

        environ = {f"{ENV_PREFIX}DATABASE_URL": "new", "DATABASE_URL": "prod"}
        promote_legacy_env_names(environ)
        assert environ == {f"{ENV_PREFIX}DATABASE_URL": "new"}
