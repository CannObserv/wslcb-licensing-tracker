"""Tests for address_client.py's shared HTTP client (#243)."""

import pytest

from wslcb_licensing_tracker import address_client
from wslcb_licensing_tracker.config import CONTACT_EMAIL_ENV, ContactEmailError, user_agent


@pytest.fixture(autouse=True)
def _fresh_shared_client(monkeypatch):
    """Each test builds the shared client from its own environment."""
    monkeypatch.setattr(address_client, "_shared_client", None)


@pytest.mark.asyncio
async def test_shared_client_identifies_us():
    client = address_client.get_shared_client()
    try:
        assert client.headers["User-Agent"] == user_agent()
        assert address_client.get_shared_client() is client  # one pool, reused
    finally:
        await address_client.close_shared_client()


@pytest.mark.asyncio
async def test_closing_lets_the_next_use_build_a_fresh_client():
    first = address_client.get_shared_client()
    await address_client.close_shared_client()
    second = address_client.get_shared_client()
    try:
        assert second is not first
        assert not second.is_closed
    finally:
        await address_client.close_shared_client()


@pytest.mark.asyncio
async def test_closing_an_unbuilt_client_is_a_noop():
    await address_client.close_shared_client()


def test_no_contact_email_refuses_to_build(monkeypatch):
    monkeypatch.delenv(CONTACT_EMAIL_ENV)
    with pytest.raises(ContactEmailError):
        address_client.get_shared_client()
