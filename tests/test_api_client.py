"""Basic tests for API client module stubs."""

from ingestion.api_client import APIClient


def test_api_client_initializes() -> None:
    """Client should preserve constructor values."""
    client = APIClient(base_url="https://example.test", api_key="demo")
    assert client.base_url == "https://example.test"
    assert client.api_key == "demo"
