"""Shared pytest fixtures for I2T-library integration tests."""

import pytest

from semt_py import AuthManager

BASE_URL = "http://localhost:3003"
API_URL = f"{BASE_URL}/api"


@pytest.fixture(scope="session")
def auth():
    """Session-scoped AuthManager for user 'test'."""
    return AuthManager(API_URL, "test", "test")
