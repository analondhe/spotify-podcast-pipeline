"""Terminal chat over the warehouse.

    python -m agent.cli
    python -m agent.cli --db warehouse/spotify_podcasts.duckdb --effort low
    python -m agent.cli --provider ollama --model qwen3:8b

Commands inside the chat: /entities, /describe <name>, /reset, /quit.
Charts are written as HTML files under .context/charts/.
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

from agent.catalog import DEFAULT_DB, ROOT
from agent.chat import (
    AgentError, DataAgent, DEFAULT_EFFORT, DEFAULT_MODEL, DEFAULT_PROVIDER, PROVIDERS,
    TextDelta, ToolFinished, ToolStarted, TurnComplete,
)

CHART_DIR = ROOT / ".context" / "charts"


def _print(text: str = "", end: str = "\n") -> None:
    print(text, end=end, flush=True)


def handle_command(agent: DataAgent, line: str) -> bool:
    """Return True if the line was a slash command."""
    cmd, _, rest = line.partition(" ")
    if cmd == "/quit":
        raise EOFError
    if cmd == "/reset":
        agent.reset()
        _print("Conversation cleared.")
    elif cmd == "/entities":
        for e in agent.catalog.entities(include_supporting=True):
            _print(f"  {e.qualified:40s} {e.kind:10s} {e.description[:70]}")
    elif cmd == "/describe":
        try:
            _print(agent.catalog.describe(agent.catalog.get(rest.strip())))
        except KeyError as exc:
            _print(str(exc))
    else:
        return False
    return True


def run_turn(agent: DataAgent, question: str) -> None:
    for event in agent.ask(question):
        if isinstance(event, TextDelta):
            _print(event.text, end="")
        elif isinstance(event, ToolStarted):
            _print(f"\n  [{event.name}]")
            if event.name in ("run_sql", "render_chart") and isinstance(event.args.get("sql"), str):
                for line in event.args["sql"].strip().splitlines():
                    _print(f"    {line}")
        elif isinstance(event, ToolFinished):
            out = event.outcome
            if out.is_error:
                _print(f"  ! {out.content}")
            elif out.figure is not None:
                CHART_DIR.mkdir(parents=True, exist_ok=True)
                path = CHART_DIR / f"chart-{int(time.time())}.html"
                out.figure.write_html(path, include_plotlyjs="cdn")
                _print(f"  chart saved: {path}")
            elif out.dataframe is not None:
                _print("  " + out.dataframe.head(15).to_string(index=False).replace("\n", "\n  "))
                if len(out.dataframe) > 15:
                    _print(f"  ... {len(out.dataframe)} rows")
            _print()
        elif isinstance(event, AgentError):
            _print(f"\n  ! {event.message}")
        elif isinstance(event, TurnComplete):
            _print(
                f"\n  -- {event.model} | in {event.input_tokens} "
                f"(cached {event.cache_read_tokens}) out {event.output_tokens}"
            )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Chat with the podcast warehouse.")
    parser.add_argument("--db", default=str(DEFAULT_DB), help="Path to the DuckDB file.")
    parser.add_argument("--provider", default=DEFAULT_PROVIDER, choices=PROVIDERS)
    parser.add_argument("--model", default=DEFAULT_MODEL, help="Model name; defaults per provider.")
    parser.add_argument("--effort", default=DEFAULT_EFFORT, choices=["low", "medium", "high", "xhigh", "max"])
    parser.add_argument("question", nargs="*", help="Ask one question and exit.")
    args = parser.parse_args(argv)

    try:
        agent = DataAgent(db_path=Path(args.db), provider=args.provider, model=args.model, effort=args.effort)
    except (FileNotFoundError, ValueError) as exc:
        _print(str(exc))
        return 1

    if args.question:
        run_turn(agent, " ".join(args.question))
        return 0

    _print(f"Podcast warehouse chat ({agent.provider_name}/{agent.model}, effort={agent.effort}). "
           "Type a question, or /entities, /describe <name>, /reset, /quit.")
    try:
        while True:
            line = input("\nyou> ").strip()
            if not line:
                continue
            if line.startswith("/") and handle_command(agent, line):
                continue
            _print()
            run_turn(agent, line)
    except (EOFError, KeyboardInterrupt):
        _print("\nbye")
    finally:
        agent.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
