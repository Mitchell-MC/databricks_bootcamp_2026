"""Shared pytest fixtures for Bronze layer tests."""

import json
from typing import Any

import pytest


@pytest.fixture
def sample_push_event() -> dict[str, Any]:
    """Sample GitHub PushEvent for testing.

    Returns:
        dict: A valid PushEvent JSON structure.
    """
    return {
        "id": "12345678901",
        "type": "PushEvent",
        "actor": {
            "id": 1234567,
            "login": "testuser",
            "display_login": "testuser",
            "gravatar_id": "",
            "url": "https://api.github.com/users/testuser",
            "avatar_url": "https://avatars.githubusercontent.com/u/1234567?",
        },
        "repo": {
            "id": 9876543,
            "name": "testuser/test-repo",
            "url": "https://api.github.com/repos/testuser/test-repo",
        },
        "payload": {
            "push_id": 11111111111,
            "size": 1,
            "distinct_size": 1,
            "ref": "refs/heads/main",
            "head": "abc123def456",
            "before": "000111222333",
            "commits": [
                {
                    "sha": "abc123def456",
                    "author": {"email": "test@example.com", "name": "Test User"},
                    "message": "Test commit message",
                    "distinct": True,
                    "url": "https://api.github.com/repos/testuser/test-repo/commits/abc123def456",
                }
            ],
        },
        "public": True,
        "created_at": "2024-01-15T10:30:00Z",
    }


@pytest.fixture
def sample_watch_event() -> dict[str, Any]:
    """Sample GitHub WatchEvent (star) for testing.

    Returns:
        dict: A valid WatchEvent JSON structure.
    """
    return {
        "id": "12345678902",
        "type": "WatchEvent",
        "actor": {
            "id": 2345678,
            "login": "stargazer",
            "display_login": "stargazer",
            "gravatar_id": "",
            "url": "https://api.github.com/users/stargazer",
            "avatar_url": "https://avatars.githubusercontent.com/u/2345678?",
        },
        "repo": {
            "id": 9876543,
            "name": "testuser/test-repo",
            "url": "https://api.github.com/repos/testuser/test-repo",
        },
        "payload": {"action": "started"},
        "public": True,
        "created_at": "2024-01-15T11:00:00Z",
    }


@pytest.fixture
def sample_pull_request_event() -> dict[str, Any]:
    """Sample GitHub PullRequestEvent for testing.

    Returns:
        dict: A valid PullRequestEvent JSON structure.
    """
    return {
        "id": "12345678903",
        "type": "PullRequestEvent",
        "actor": {
            "id": 3456789,
            "login": "contributor",
            "display_login": "contributor",
            "gravatar_id": "",
            "url": "https://api.github.com/users/contributor",
            "avatar_url": "https://avatars.githubusercontent.com/u/3456789?",
        },
        "repo": {
            "id": 9876543,
            "name": "testuser/test-repo",
            "url": "https://api.github.com/repos/testuser/test-repo",
        },
        "payload": {
            "action": "opened",
            "number": 42,
            "pull_request": {
                "url": "https://api.github.com/repos/testuser/test-repo/pulls/42",
                "id": 555666777,
                "number": 42,
                "state": "open",
                "title": "Add new feature",
                "user": {"login": "contributor", "id": 3456789},
                "body": "This PR adds a new feature",
                "created_at": "2024-01-15T12:00:00Z",
                "updated_at": "2024-01-15T12:00:00Z",
                "merged": False,
                "commits": 1,
                "additions": 50,
                "deletions": 10,
                "changed_files": 3,
            },
        },
        "public": True,
        "created_at": "2024-01-15T12:00:00Z",
    }


@pytest.fixture
def sample_events_jsonl(
    sample_push_event: dict[str, Any],
    sample_watch_event: dict[str, Any],
    sample_pull_request_event: dict[str, Any],
) -> str:
    """Sample newline-delimited JSON string with multiple events.

    Args:
        sample_push_event: Push event fixture.
        sample_watch_event: Watch event fixture.
        sample_pull_request_event: Pull request event fixture.

    Returns:
        str: Newline-delimited JSON string.
    """
    events = [sample_push_event, sample_watch_event, sample_pull_request_event]
    return "\n".join(json.dumps(event) for event in events)
