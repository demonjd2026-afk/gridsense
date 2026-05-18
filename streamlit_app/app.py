"""GridSense Carbon Briefing Agent — Streamlit web app."""

from __future__ import annotations

import streamlit as st
from agent.llm import build_client, chat_with_tools
from agent.prompts import SYSTEM_PROMPT
from agent.rate_limit import (
    GLOBAL_DAILY_LIMIT,
    PER_SESSION_LIMIT,
    check_can_ask,
    get_global_count,
    get_session_count,
    increment_global_count,
    increment_session_count,
)
from agent.tools import TOOL_REGISTRY, TOOL_SCHEMAS

# ─────────────────────────────────────────────────────────────────────────────
# Page setup
# ─────────────────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="GridSense Carbon Briefing Agent",
    page_icon="🌍",
    layout="centered",
)

st.title("🌍 GridSense Carbon Briefing")
st.caption(
    "Ask anything about live grid carbon intensity in the EU (5 countries) "
    "or UK (18 regions). The agent queries a live Databricks lakehouse."
)


# ─────────────────────────────────────────────────────────────────────────────
# Load secrets
# ─────────────────────────────────────────────────────────────────────────────
try:
    AZURE_OPENAI_ENDPOINT = st.secrets["azure_openai"]["endpoint"]
    AZURE_OPENAI_KEY = st.secrets["azure_openai"]["api_key"]
    AZURE_OPENAI_DEPLOYMENT = st.secrets["azure_openai"]["deployment"]
    DATABRICKS_HOST = st.secrets["databricks"]["host"]
    DATABRICKS_HTTP_PATH = st.secrets["databricks"]["http_path"]
    DATABRICKS_TOKEN = st.secrets["databricks"]["token"]
except KeyError as e:
    st.error(
        f"Missing secret: {e}. " "Configure `.streamlit/secrets.toml` (see `secrets.toml.example`)."
    )
    st.stop()


# ─────────────────────────────────────────────────────────────────────────────
# Build connection dicts (passed to every tool call)
# ─────────────────────────────────────────────────────────────────────────────
client = build_client(AZURE_OPENAI_ENDPOINT, AZURE_OPENAI_KEY)
db_connection = {
    "server_hostname": DATABRICKS_HOST,
    "http_path": DATABRICKS_HTTP_PATH,
    "access_token": DATABRICKS_TOKEN,
}


# ─────────────────────────────────────────────────────────────────────────────
# Session state for chat history
# ─────────────────────────────────────────────────────────────────────────────
if "messages" not in st.session_state:
    st.session_state.messages = [{"role": "system", "content": SYSTEM_PROMPT}]


# ─────────────────────────────────────────────────────────────────────────────
# Sidebar with example questions
# ─────────────────────────────────────────────────────────────────────────────
with st.sidebar:
    st.header("Example questions")
    examples = [
        "What's the cleanest EU country right now?",
        "Why is Germany so dirty right now?",
        "Which UK region is cleanest?",
        "How has France's carbon trended over the last 24 hours?",
        "When should I run my UK batch job for lowest carbon?",
        "Will Germany be cleaner tomorrow than today?",
    ]
    for ex in examples:
        if st.button(ex, key=f"ex_{ex}", use_container_width=True):
            st.session_state.pending_question = ex

    st.divider()
    if st.button("Clear conversation"):
        st.session_state.messages = [{"role": "system", "content": SYSTEM_PROMPT}]
        st.session_state.question_count = 0
        st.rerun()

    st.divider()
    st.caption(
        f"**Session usage:** {get_session_count()} / {PER_SESSION_LIMIT} questions  \n"
        f"**Daily quota:** {get_global_count()} / {GLOBAL_DAILY_LIMIT} questions"
    )

    st.divider()
    st.caption(
        "Data sources: UK Carbon Intensity API, ENTSO-E Transparency Platform, "
        "Open-Meteo. Lakehouse on Azure with Databricks Unity Catalog. "
        "[Repo](https://github.com/demonjd2026-afk/gridsense)"
    )


# ─────────────────────────────────────────────────────────────────────────────
# Render chat history (skip the system prompt)
# ─────────────────────────────────────────────────────────────────────────────
for msg in st.session_state.messages:
    if msg["role"] in ("system", "tool"):
        continue
    if msg["role"] == "assistant" and not msg.get("content"):
        continue
    with st.chat_message(msg["role"]):
        st.markdown(msg["content"])


# ─────────────────────────────────────────────────────────────────────────────
# Input + agent loop
# ─────────────────────────────────────────────────────────────────────────────
user_input = st.chat_input("Ask about grid carbon...")
if "pending_question" in st.session_state:
    user_input = st.session_state.pop("pending_question")

if user_input:
    allowed, deny_reason = check_can_ask()
    if not allowed:
        with st.chat_message("assistant"):
            st.warning(deny_reason)
        st.stop()

    increment_session_count()
    increment_global_count()

    with st.chat_message("user"):
        st.markdown(user_input)
    st.session_state.messages.append({"role": "user", "content": user_input})

    # Run agent
    with st.chat_message("assistant"):
        with st.spinner("Querying the lakehouse..."):
            try:
                answer, tool_log = chat_with_tools(
                    client=client,
                    deployment_name=AZURE_OPENAI_DEPLOYMENT,
                    messages=st.session_state.messages,
                    tool_schemas=TOOL_SCHEMAS,
                    tool_registry=TOOL_REGISTRY,
                    connection=db_connection,
                )
            except Exception as e:
                st.error(f"Agent error: {e}")
                st.stop()

        st.markdown(answer)
        st.session_state.messages.append({"role": "assistant", "content": answer})

        # Show data sources used in a quiet footer (transparency without noise)
        if tool_log:
            n = len(tool_log)
            label = f"Show data source{'s' if n != 1 else ''} used ({n})"
            with st.expander(label, expanded=False):
                lines = []
                for call in tool_log:
                    args_str = ", ".join(f"{k}={v!r}" for k, v in call["args"].items())
                    line = (
                        f"`{call['name']}({args_str})` returned "
                        f"**{call.get('rows', 0)} row{'s' if call.get('rows', 0) != 1 else ''}**"
                    )
                    if "error" in call:
                        line += f" — error: {call['error']}"
                    lines.append(line)
                st.caption("  \n".join(lines))
