"""Rate limiting for the agent — per-session + global daily."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import streamlit as st

PER_SESSION_LIMIT = 10
GLOBAL_DAILY_LIMIT = 200
GLOBAL_STATE_PATH = Path("/tmp/gridsense_agent_global_state.json")


def get_session_count() -> int:
    return st.session_state.get("question_count", 0)


def increment_session_count() -> None:
    st.session_state["question_count"] = get_session_count() + 1


def session_limit_reached() -> bool:
    return get_session_count() >= PER_SESSION_LIMIT


def _today_key() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%d")


def _load_global_state() -> dict:
    if not GLOBAL_STATE_PATH.exists():
        return {}
    try:
        return json.loads(GLOBAL_STATE_PATH.read_text())
    except (json.JSONDecodeError, OSError):
        return {}


def _save_global_state(state: dict) -> None:
    try:
        GLOBAL_STATE_PATH.write_text(json.dumps(state))
    except OSError:
        pass


def get_global_count() -> int:
    state = _load_global_state()
    return state.get(_today_key(), 0)


def increment_global_count() -> None:
    state = _load_global_state()
    today = _today_key()
    state[today] = state.get(today, 0) + 1
    state = {k: v for k, v in state.items() if k >= today}
    _save_global_state(state)


def global_limit_reached() -> bool:
    return get_global_count() >= GLOBAL_DAILY_LIMIT


def check_can_ask() -> tuple[bool, str | None]:
    if session_limit_reached():
        return False, (
            f"You've reached the demo limit of {PER_SESSION_LIMIT} questions "
            "for this session. Refresh the page to start a new session, or "
            "check out the [GitHub repo](https://github.com/demonjd2026-afk/gridsense) "
            "to see the full project including code, architecture, and screenshots."
        )
    if global_limit_reached():
        return False, (
            f"This portfolio demo has reached its daily cap of "
            f"{GLOBAL_DAILY_LIMIT} questions across all users. Come back "
            "tomorrow, or explore the "
            "[GitHub repo](https://github.com/demonjd2026-afk/gridsense) "
            "for the full project."
        )
    return True, None
