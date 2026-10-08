"""Conversation loop: streams the model's answer, runs tools against DuckDB, repeats.

The loop is provider-agnostic; see ``providers.py`` for the Claude and Ollama backends.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator

from agent.catalog import DEFAULT_DB, Catalog, connect
from agent.providers import (
    DEFAULT_MODELS, PROVIDERS, Provider, ProviderError, ToolResult, make_provider,
)
from agent.tools import TOOLS, ToolExecutor, ToolOutcome

DEFAULT_PROVIDER = os.environ.get("PODCAST_AGENT_PROVIDER", "anthropic")
DEFAULT_MODEL = os.environ.get("PODCAST_AGENT_MODEL") or None  # None -> provider default
DEFAULT_EFFORT = os.environ.get("PODCAST_AGENT_EFFORT", "medium")
MAX_TOOL_ROUNDS = 12


# -- events yielded to the UI -------------------------------------------------

@dataclass
class TextDelta:
    text: str


@dataclass
class ToolStarted:
    name: str
    args: dict


@dataclass
class ToolFinished:
    name: str
    args: dict
    outcome: ToolOutcome


@dataclass
class TurnComplete:
    stop_reason: str
    model: str
    input_tokens: int
    output_tokens: int
    cache_read_tokens: int


@dataclass
class AgentError:
    message: str


Event = TextDelta | ToolStarted | ToolFinished | TurnComplete | AgentError


# -- prompt -------------------------------------------------------------------

def build_system_prompt(catalog: Catalog) -> str:
    coverage = catalog.coverage()
    coverage_text = ""
    if coverage:
        coverage_text = (
            f"\n## Data coverage\n"
            f"Charts span {coverage['first_date']} to {coverage['last_date']} "
            f"({coverage['days']} snapshot day(s), {coverage['regions']} region(s)).\n"
        )
    return f"""You are a data analyst assistant for a Spotify podcast charts warehouse. \
Users ask questions in plain language; you answer them by querying DuckDB with the tools \
provided and, when asked for a chart, graph, plot, or visualization, by calling render_chart.

## How to work
- Answer from data, never from memory. Call run_sql for every number you report.
- Prefer the marts in the analytics schema. Reach for staging or intermediate entities only \
when a mart cannot answer the question (for example episode-level or per-day-per-show detail).
- Call describe_entity before querying any entity whose columns you have not seen in this \
conversation and that is not fully listed below.
- Write DuckDB SQL. Qualify every table as schema.table. Aggregate or use ORDER BY + LIMIT; \
results are capped.
- When the question is ambiguous, pick the most reasonable reading, answer it, and say what \
you assumed in one sentence. Ask a question only if no reasonable reading exists.
- For charts, run a single render_chart call with tidy, ordered rows. Use bar for category \
comparison, hbar for ranked lists, line for change over time, area for stacked composition \
over time, scatter for two measures. Never put two different units on one chart; make two \
charts instead. Do not request pie charts.
- If the data cannot answer the question, say so plainly and suggest the closest thing it can.

## How to answer
- Lead with the answer, then the key numbers. Keep it short.
- The user interface already displays the SQL and result table for every tool call, so do \
not paste SQL into your prose unless the user asks for it. Name the table you used.
- Mention grain and caveats when they matter: ranks are per region per day; show and \
publisher marts are lifetime aggregates; population is an annual country total; \
best_rank_ever and avg_rank are lower-is-better.
- Use plain text and short markdown lists. No headers.
{coverage_text}
## Data dictionary
{catalog.dictionary()}
"""


# -- agent --------------------------------------------------------------------

class DataAgent:
    """One conversation against the warehouse. Not thread-safe."""

    def __init__(
        self,
        db_path: Path | str = DEFAULT_DB,
        provider: str = DEFAULT_PROVIDER,
        model: str | None = DEFAULT_MODEL,
        effort: str = DEFAULT_EFFORT,
        client=None,
        **provider_kwargs,
    ):
        if provider not in PROVIDERS:
            raise ValueError(f"unknown provider {provider!r}; choose from {PROVIDERS}")
        self.con = connect(db_path)
        self.catalog = Catalog(self.con).build()
        self.executor = ToolExecutor(self.catalog, self.con)
        self.system = build_system_prompt(self.catalog)
        self.provider: Provider = make_provider(
            provider, self.system, TOOLS, model=model, client=client, effort=effort, **provider_kwargs
        )
        self.effort = effort

    @property
    def provider_name(self) -> str:
        return self.provider.name

    @property
    def model(self) -> str:
        return self.provider.model

    @property
    def messages(self) -> list[dict]:
        return self.provider.messages

    def reset(self) -> None:
        self.provider.reset()

    def close(self) -> None:
        self.con.close()

    def ask(self, question: str) -> Iterator[Event]:
        mark = self.provider.mark()
        self.provider.add_user(question)
        for _round in range(MAX_TOOL_ROUNDS + 1):
            turn = None
            try:
                for item in self.provider.stream():
                    if isinstance(item, str):
                        yield TextDelta(item)
                    else:
                        turn = item
            except ProviderError as exc:
                self.provider.rollback(mark)  # keep history valid: drop the unanswered turn
                yield AgentError(str(exc))
                return
            if turn is None:
                self.provider.rollback(mark)
                yield AgentError("The model returned no response.")
                return

            done = TurnComplete(
                stop_reason=turn.stop_reason, model=turn.model,
                input_tokens=turn.input_tokens, output_tokens=turn.output_tokens,
                cache_read_tokens=turn.cache_read_tokens,
            )
            if turn.stop_reason == "refusal":
                yield AgentError("The model declined this request.")
                yield done
                return
            if turn.stop_reason == "max_tokens":
                yield AgentError("The response hit the output limit mid tool-call; ask a narrower question.")
                yield done
                return
            if not turn.tool_calls:
                yield done
                return

            results = []
            for call in turn.tool_calls:
                args = call.args if isinstance(call.args, dict) else {}
                yield ToolStarted(call.name, args)
                outcome = self.executor.execute(call.name, call.args)
                yield ToolFinished(call.name, args, outcome)
                results.append(ToolResult(call.id, call.name, outcome.content, outcome.is_error))
            self.provider.add_tool_results(results)

        yield AgentError(f"Stopped after {MAX_TOOL_ROUNDS} tool rounds without a final answer.")


__all__ = [
    "AgentError", "DataAgent", "DEFAULT_EFFORT", "DEFAULT_MODEL", "DEFAULT_MODELS",
    "DEFAULT_PROVIDER", "PROVIDERS", "TextDelta", "ToolFinished", "ToolStarted", "TurnComplete",
    "build_system_prompt",
]
