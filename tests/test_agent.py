"""Tests for the warehouse chat agent that run without an API key.

The agent loop is exercised with a fake Anthropic client that replays a scripted
sequence of responses, so the tool round-trip and history bookkeeping are checked
end to end against the real DuckDB warehouse.
"""

from __future__ import annotations

from types import SimpleNamespace

import duckdb
import pandas as pd
import pytest

from agent.catalog import DEFAULT_DB, Catalog, connect
from agent.charts import ChartSpec, build_figure, validate_spec
from agent.chat import AgentError, DataAgent, TextDelta, ToolFinished, TurnComplete
from agent.providers import OllamaProvider
from agent.tools import (
    MAX_LIMIT, SqlRejected, ToolExecutor, frame_to_text, run_query, validate_sql,
)

pytestmark = pytest.mark.skipif(not DEFAULT_DB.exists(), reason="warehouse not built")


@pytest.fixture(scope="module")
def con():
    c = connect(DEFAULT_DB)
    yield c
    c.close()


@pytest.fixture(scope="module")
def catalog(con):
    return Catalog(con).build()


# -- SQL guard ----------------------------------------------------------------

@pytest.mark.parametrize("sql", [
    "select 1",
    "SELECT * FROM analytics.fct_show_insights;",
    "-- a comment\nwith t as (select 1 as x) select * from t",
    "/* block */ select count(*) from analytics.fct_region_insights",
    "select * from analytics.fct_show_insights where show_name = 'offset reset'",
])
def test_validate_sql_accepts_selects(sql):
    assert validate_sql(sql)


@pytest.mark.parametrize("sql", [
    "drop table analytics.fct_show_insights",
    "select 1; select 2",
    "insert into analytics.fct_show_insights values (1)",
    "copy (select 1) to '/tmp/x.csv'",
    "select 1 union all select 2; drop table x",
    "attach 'other.duckdb'",
    "",
    "   ",
])
def test_validate_sql_rejects_non_selects(sql):
    with pytest.raises(SqlRejected):
        validate_sql(sql)


def test_run_query_caps_rows_and_reports_truncation(con):
    df, truncated = run_query(con, "select * from staging.stg_podcast_episodes", limit=5)
    assert len(df) == 5 and truncated
    df, truncated = run_query(con, "select 1 as x", limit=MAX_LIMIT + 1000)
    assert len(df) == 1 and not truncated


def test_run_query_rejects_writes_even_on_read_only(con):
    with pytest.raises(SqlRejected):
        run_query(con, "create table t as select 1")


def test_frame_to_text_mentions_cap():
    text = frame_to_text(pd.DataFrame({"a": [1, 2]}), truncated=True)
    assert "capped" in text and "2 row(s)" in text


# -- catalog --------------------------------------------------------------------

def test_catalog_lists_marts_with_descriptions(catalog):
    marts = {e.name for e in catalog.entities()}
    assert marts == {"fct_region_insights", "fct_show_insights", "fct_publisher_insights"}
    show = catalog.get("fct_show_insights")
    assert show.grain == "show_uri"
    assert show.column("best_rank_ever").description.startswith("Best (lowest)")
    assert show.row_count and show.row_count > 0


def test_catalog_hides_raw_by_default(catalog):
    names = {e.qualified for e in catalog.entities(include_supporting=True)}
    assert "raw.top_podcasts" not in names
    assert "staging.stg_podcast_episodes" in names
    assert "raw.top_podcasts" in {e.qualified for e in catalog.entities(include_raw=True)}


def test_catalog_get_accepts_qualified_and_bare_names(catalog):
    assert catalog.get("analytics.fct_region_insights") is catalog.get("fct_region_insights")
    with pytest.raises(KeyError):
        catalog.get("nope")


def test_dictionary_and_describe_render(catalog):
    text = catalog.dictionary()
    assert "### analytics.fct_region_insights" in text
    assert "`pct_audio`" in text
    desc = catalog.describe(catalog.get("fct_publisher_insights"))
    assert "Sample rows" in desc and "show_publisher" in desc


# -- tools ----------------------------------------------------------------------

def test_executor_run_sql(catalog, con):
    ex = ToolExecutor(catalog, con)
    out = ex.execute("run_sql", {"sql": "select show_name, best_rank_ever from analytics.fct_show_insights order by 2 limit 3"})
    assert not out.is_error
    assert out.dataframe is not None and len(out.dataframe) <= 3
    assert "best_rank_ever" in out.content


def test_executor_reports_errors_as_tool_errors(catalog, con):
    ex = ToolExecutor(catalog, con)
    assert ex.execute("run_sql", {"sql": "drop table x"}).is_error
    assert ex.execute("run_sql", {"sql": "select nope from analytics.fct_show_insights"}).is_error
    assert ex.execute("describe_entity", {"name": "missing"}).is_error
    assert ex.execute("run_sql", "not a dict").is_error
    assert ex.execute("nonexistent", {}).is_error


def test_executor_render_chart_builds_figure(catalog, con):
    ex = ToolExecutor(catalog, con)
    out = ex.execute("render_chart", {
        "sql": "select country_name, avg(pct_audio) as pct_audio from analytics.fct_region_insights group by 1 order by 2 desc",
        "chart_type": "bar", "x": "country_name", "y": ["pct_audio"], "title": "Audio share by country",
    })
    assert not out.is_error, out.content
    assert out.figure is not None and out.chart.chart_type == "bar"
    assert "Chart rendered" in out.content


def test_render_chart_rejects_bad_columns(catalog, con):
    ex = ToolExecutor(catalog, con)
    out = ex.execute("render_chart", {
        "sql": "select country_name from analytics.fct_region_insights",
        "chart_type": "bar", "x": "country_name", "y": ["missing"], "title": "x",
    })
    assert out.is_error and "missing" in out.content


def test_chart_folds_series_beyond_palette():
    df = pd.DataFrame({
        "day": [1, 2] * 12,
        "value": range(24),
        "series": [f"s{i}" for i in range(12) for _ in range(2)],
    })
    spec = validate_spec({"chart_type": "line", "x": "day", "y": "value", "color": "series", "title": "t"},
                         list(df.columns))
    fig = build_figure(df, spec)
    names = [t.name for t in fig.data]
    assert len(names) == 8 and names[-1] == "Other"
    assert spec.notes


# -- agent loop with a fake client -------------------------------------------

class _Block(SimpleNamespace):
    pass


class _FakeStream:
    def __init__(self, message):
        self._message = message

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def __iter__(self):
        for block in self._message.content:
            if block.type == "text":
                yield SimpleNamespace(type="text", text=block.text)

    def get_final_message(self):
        return self._message


class _FakeMessages:
    def __init__(self, script):
        self.script = list(script)
        self.requests = []

    def stream(self, **kwargs):
        # The agent mutates its message list in place; snapshot what was sent.
        self.requests.append({**kwargs, "messages": list(kwargs["messages"])})
        return _FakeStream(self.script.pop(0))


def _message(content, stop_reason):
    return SimpleNamespace(
        content=content, stop_reason=stop_reason, model="fake-model",
        usage=SimpleNamespace(input_tokens=10, output_tokens=5, cache_read_input_tokens=0),
    )


def _fake_client(script):
    messages = _FakeMessages(script)
    return SimpleNamespace(beta=SimpleNamespace(messages=messages)), messages


def test_agent_runs_tool_round_trip():
    script = [
        _message([
            _Block(type="text", text="Looking that up."),
            _Block(type="tool_use", id="t1", name="run_sql",
                   input={"sql": "select count(*) as n from analytics.fct_show_insights"}),
        ], "tool_use"),
        _message([_Block(type="text", text="There are some shows.")], "end_turn"),
    ]
    client, fake = _fake_client(script)
    agent = DataAgent(provider="anthropic", client=client, model="fake-model", effort="low")
    events = list(agent.ask("How many shows?"))
    agent.close()

    text = "".join(e.text for e in events if isinstance(e, TextDelta))
    assert text == "Looking that up.There are some shows."
    finished = [e for e in events if isinstance(e, ToolFinished)]
    assert len(finished) == 1 and not finished[0].outcome.is_error
    assert finished[0].outcome.dataframe["n"].iloc[0] > 0
    assert isinstance(events[-1], TurnComplete)

    # Second request carried the assistant turn and the tool result back.
    second = fake.requests[1]["messages"]
    assert [m["role"] for m in second] == ["user", "assistant", "user"]
    assert second[2]["content"][0]["tool_use_id"] == "t1"
    assert fake.requests[0]["fallbacks"] == "default"
    assert fake.requests[0]["system"][0]["cache_control"] == {"type": "ephemeral"}
    assert fake.requests[0]["output_config"] == {"effort": "low"}


def test_agent_refusal_keeps_history_valid():
    script = [_message([_Block(type="text", text="")], "refusal")]
    client, fake = _fake_client(script)
    agent = DataAgent(provider="anthropic", client=client)
    events = list(agent.ask("hi"))
    assert any(isinstance(e, AgentError) for e in events)
    assert agent.messages[-1]["role"] == "assistant"
    assert isinstance(agent.messages[-1]["content"], str)
    agent.close()


def test_agent_tool_error_is_returned_to_model():
    script = [
        _message([_Block(type="tool_use", id="t1", name="run_sql", input={"sql": "drop table x"})], "tool_use"),
        _message([_Block(type="text", text="I can only read data.")], "end_turn"),
    ]
    client, fake = _fake_client(script)
    agent = DataAgent(provider="anthropic", client=client)
    list(agent.ask("delete everything"))
    result = fake.requests[1]["messages"][2]["content"][0]
    assert result["is_error"] is True and "SELECT" in result["content"]
    agent.close()


# -- Ollama provider with a fake client ----------------------------------------

class _FakeOllama:
    """Replays scripted chunk lists; records every request."""

    def __init__(self, script):
        self.script = list(script)
        self.requests = []

    def chat(self, **kwargs):
        self.requests.append({**kwargs, "messages": list(kwargs["messages"])})
        return iter(self.script.pop(0))


def _chunk(content="", tool_calls=None, done=False, done_reason=None):
    calls = [
        SimpleNamespace(function=SimpleNamespace(name=n, arguments=a)) for n, a in (tool_calls or [])
    ]
    return SimpleNamespace(
        message=SimpleNamespace(content=content, tool_calls=calls, thinking=None),
        done=done, done_reason=done_reason, prompt_eval_count=20, eval_count=7,
    )


def test_ollama_tool_schema_conversion():
    tools = OllamaProvider._convert_tool({"name": "run_sql", "description": "d",
                                          "eager_input_streaming": True,
                                          "input_schema": {"type": "object", "properties": {}}})
    assert tools == {"type": "function", "function": {"name": "run_sql", "description": "d",
                                                      "parameters": {"type": "object", "properties": {}}}}


def test_agent_ollama_round_trip():
    script = [
        [_chunk("Checking. "),
         _chunk(tool_calls=[("run_sql", {"sql": "select count(*) as n from analytics.fct_show_insights"})]),
         _chunk(done=True, done_reason="stop")],
        [_chunk("There are "), _chunk("a few shows."), _chunk(done=True, done_reason="stop")],
    ]
    fake = _FakeOllama(script)
    agent = DataAgent(provider="ollama", client=fake, model="fake-qwen", num_ctx=4096)
    events = list(agent.ask("How many shows?"))
    agent.close()

    text = "".join(e.text for e in events if isinstance(e, TextDelta))
    assert text == "Checking. There are a few shows."
    finished = [e for e in events if isinstance(e, ToolFinished)]
    assert len(finished) == 1 and finished[0].outcome.dataframe["n"].iloc[0] > 0
    assert isinstance(events[-1], TurnComplete) and events[-1].model == "fake-qwen"

    first = fake.requests[0]
    assert first["messages"][0]["role"] == "system" and "function calling" in first["messages"][0]["content"]
    assert first["tools"][0]["type"] == "function"
    assert first["options"]["num_ctx"] == 4096
    second = fake.requests[1]["messages"]
    assert [m["role"] for m in second] == ["system", "user", "assistant", "tool"]
    assert second[2]["tool_calls"][0]["function"]["name"] == "run_sql"
    assert second[3]["tool_name"] == "run_sql" and "n" in second[3]["content"]


def test_agent_ollama_parses_string_arguments_and_errors():
    script = [
        [_chunk(tool_calls=[("run_sql", '{"sql": "drop table x"}')]), _chunk(done=True, done_reason="stop")],
        [_chunk("Read-only."), _chunk(done=True, done_reason="stop")],
    ]
    fake = _FakeOllama(script)
    agent = DataAgent(provider="ollama", client=fake, model="fake-qwen")
    list(agent.ask("delete it"))
    tool_msg = fake.requests[1]["messages"][3]
    assert tool_msg["role"] == "tool" and tool_msg["content"].startswith("ERROR:")
    agent.close()


def test_agent_ollama_connection_failure_rolls_back_history():
    import httpx

    class _Down:
        def chat(self, **kwargs):
            raise httpx.ConnectError("refused")

    agent = DataAgent(provider="ollama", client=_Down(), model="fake-qwen")
    events = list(agent.ask("hi"))
    assert isinstance(events[-1], AgentError) and "Ollama" in events[-1].message
    assert agent.messages == []
    agent.close()


def test_unknown_provider_rejected():
    with pytest.raises(ValueError):
        DataAgent(provider="gpt")
