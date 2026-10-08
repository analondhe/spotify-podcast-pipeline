"""Streamlit chat UI over the warehouse.

    streamlit run agent/app.py
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import streamlit as st  # noqa: E402

from agent.catalog import DEFAULT_DB  # noqa: E402
from agent.chat import (  # noqa: E402
    AgentError, DataAgent, DEFAULT_EFFORT, DEFAULT_MODEL, DEFAULT_MODELS, DEFAULT_PROVIDER, PROVIDERS,
    TextDelta, ToolFinished, ToolStarted, TurnComplete,
)

EXAMPLES = [
    "Which shows chart in the most regions?",
    "Compare pct_audio by country as a bar chart",
    "How does average episode duration change over time per region?",
    "Which publishers have the best average rank, and how many shows do they have?",
]

st.set_page_config(page_title="Podcast warehouse chat", page_icon="🎧", layout="wide")


def get_agent(provider: str, model: str | None) -> DataAgent:
    key = (provider, model)
    if st.session_state.get("agent_key") != key:
        old = st.session_state.pop("agent", None)
        if old is not None:
            old.close()
        st.session_state.agent = DataAgent(db_path=DEFAULT_DB, provider=provider, model=model, effort=DEFAULT_EFFORT)
        st.session_state.agent_key = key
        st.session_state.history = []
    return st.session_state.agent


def choose_provider() -> tuple[str, str | None]:
    with st.sidebar:
        st.title("🎧 Podcast warehouse")
        provider = st.selectbox("Provider", PROVIDERS, index=PROVIDERS.index(DEFAULT_PROVIDER),
                                format_func=lambda p: {"anthropic": "Claude (Anthropic API)",
                                                       "ollama": "Open-source via Ollama"}[p])
        default_model = DEFAULT_MODEL if (DEFAULT_MODEL and provider == DEFAULT_PROVIDER) else DEFAULT_MODELS[provider]
        model = st.text_input("Model", value=default_model, key=f"model-{provider}").strip() or None
    return provider, model


def render_block(block: dict) -> None:
    kind = block["type"]
    if kind == "text":
        st.markdown(block["text"])
    elif kind == "sql":
        label = f"SQL · {block['name']}" + (" · result capped" if block["truncated"] else "")
        with st.expander(label, expanded=block.get("expanded", False)):
            st.code(block["sql"], language="sql")
            if block["df"] is not None:
                st.dataframe(block["df"], hide_index=True)
    elif kind == "chart":
        st.plotly_chart(block["figure"], key=block["key"])
        if block.get("notes"):
            st.caption(" ".join(block["notes"]))
    elif kind == "error":
        st.error(block["text"])
    elif kind == "usage":
        st.caption(block["text"])


def render_history() -> None:
    for i, msg in enumerate(st.session_state.history):
        with st.chat_message(msg["role"]):
            for j, block in enumerate(msg["blocks"]):
                if block["type"] == "chart":
                    block["key"] = f"chart-{i}-{j}"
                render_block(block)


def answer(agent: DataAgent, question: str) -> None:
    blocks: list[dict] = []
    with st.chat_message("assistant"):
        slot = st.empty()
        text = ""
        chart_n = 0
        for event in agent.ask(question):
            if isinstance(event, TextDelta):
                text += event.text
                slot.markdown(text + " ▌")
            elif isinstance(event, ToolStarted):
                if text:
                    slot.markdown(text)
                    blocks.append({"type": "text", "text": text})
                    text = ""
                slot = st.empty()
                slot.caption(f"Running {event.name}…")
            elif isinstance(event, ToolFinished):
                out = event.outcome
                slot.empty()
                if out.is_error:
                    block = {"type": "error", "text": f"{event.name}: {out.content}"}
                elif out.figure is not None:
                    chart_n += 1
                    block = {"type": "chart", "figure": out.figure, "notes": out.chart.notes,
                             "key": f"live-{len(st.session_state.history)}-{chart_n}"}
                    blocks.append(block)
                    render_block(block)
                    block = {"type": "sql", "name": event.name, "sql": out.sql,
                             "df": out.dataframe, "truncated": out.truncated}
                else:
                    block = {"type": "sql", "name": event.name, "sql": out.sql or "",
                             "df": out.dataframe, "truncated": out.truncated,
                             "expanded": False}
                    if out.sql is None:
                        block = {"type": "text", "text": f"```\n{out.content}\n```"}
                blocks.append(block)
                render_block(block)
                slot = st.empty()
            elif isinstance(event, AgentError):
                block = {"type": "error", "text": event.message}
                blocks.append(block)
                slot.empty()
                render_block(block)
                slot = st.empty()
            elif isinstance(event, TurnComplete):
                if text:
                    slot.markdown(text)
                    blocks.append({"type": "text", "text": text})
                    text = ""
                usage = (f"{event.model} · in {event.input_tokens:,} "
                         f"(cached {event.cache_read_tokens:,}) · out {event.output_tokens:,}")
                blocks.append({"type": "usage", "text": usage})
                st.caption(usage)
        if text:
            slot.markdown(text)
            blocks.append({"type": "text", "text": text})
    st.session_state.history.append({"role": "assistant", "blocks": blocks})


def sidebar(agent: DataAgent) -> None:
    with st.sidebar:
        st.caption(f"{agent.provider_name} · {agent.model}"
                   + (f" · effort {agent.effort}" if agent.provider_name == "anthropic" else ""))
        cov = agent.catalog.coverage()
        if cov:
            st.caption(f"{cov['first_date']} → {cov['last_date']} · {cov['regions']} regions · {cov['days']} days")
        if st.button("New conversation", width="stretch"):
            agent.reset()
            st.session_state.history = []
            st.rerun()
        st.subheader("Entities")
        for e in agent.catalog.entities(include_supporting=True):
            rows = f" · {e.row_count:,} rows" if e.row_count is not None else ""
            with st.expander(f"{e.qualified}{rows}"):
                st.caption(e.description)
                if e.grain:
                    st.caption(f"Grain: one row per {e.grain}")
                st.table(
                    [{"column": c.name, "type": c.data_type, "description": c.description}
                     for c in e.columns]
                )


def main() -> None:
    provider, model = choose_provider()
    try:
        agent = get_agent(provider, model)
    except FileNotFoundError as exc:
        st.error(str(exc))
        st.stop()
        return
    except Exception as exc:  # missing API key, bad model name, etc.
        st.error(f"Could not start the {provider} provider: {exc}")
        st.stop()
        return

    sidebar(agent)
    st.markdown("Ask a question about the podcast charts, or ask for a chart.")
    if not st.session_state.history:
        cols = st.columns(2)
        for i, example in enumerate(EXAMPLES):
            if cols[i % 2].button(example, key=f"ex-{i}", width="stretch"):
                st.session_state.pending = example

    render_history()

    question = st.chat_input("e.g. Which countries have the most explicit content on their charts?")
    question = question or st.session_state.pop("pending", None)
    if question:
        st.session_state.history.append({"role": "user", "blocks": [{"type": "text", "text": question}]})
        with st.chat_message("user"):
            st.markdown(question)
        answer(agent, question)
        st.rerun()


main()
