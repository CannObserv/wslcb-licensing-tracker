"""Tests for `wslcb ops user-agent` (#243): the relay's Mac asks the VM for the string."""

from click.testing import CliRunner

from wslcb_licensing_tracker.cli import main
from wslcb_licensing_tracker.config import CONTACT_EMAIL_ENV, user_agent


def test_prints_the_user_agent_from_the_environment():
    result = CliRunner().invoke(main, ["ops", "user-agent"])
    assert result.exit_code == 0, result.output
    assert result.stdout == user_agent() + "\n"


def test_reads_only_the_contact_from_an_env_file(tmp_path, monkeypatch):
    """The production .env holds DATABASE_URL too: parse the one key, never source it."""
    monkeypatch.delenv(CONTACT_EMAIL_ENV)
    env = tmp_path / ".env"
    env.write_text(
        "DATABASE_URL=postgresql://secret\n"
        "# a comment\n"
        f'{CONTACT_EMAIL_ENV}="code@cannabis.observer"\n'
    )
    result = CliRunner().invoke(main, ["ops", "user-agent", "--env-file", str(env)])
    assert result.exit_code == 0, result.output
    assert result.stdout.strip() == user_agent({CONTACT_EMAIL_ENV: "code@cannabis.observer"})
    assert "secret" not in result.output


def test_missing_contact_fails_naming_the_variable(tmp_path, monkeypatch):
    monkeypatch.delenv(CONTACT_EMAIL_ENV)
    env = tmp_path / ".env"
    env.write_text("DATABASE_URL=x\n")
    result = CliRunner().invoke(main, ["ops", "user-agent", "--env-file", str(env)])
    assert result.exit_code != 0
    assert CONTACT_EMAIL_ENV in result.output
    assert result.stdout == ""


def test_malformed_contact_fails(monkeypatch):
    monkeypatch.setenv(CONTACT_EMAIL_ENV, "not an email")
    result = CliRunner().invoke(main, ["ops", "user-agent"])
    assert result.exit_code != 0
    assert CONTACT_EMAIL_ENV in result.output
