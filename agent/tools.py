"""Tool definitions the model can call, and the code that executes them."""

from __future__ import annotations

import json
import re
import threading
from dataclasses import dataclass

import duckdb
import pandas as pd

from agent.catalog import Catalog
from agent.charts import CHART_TYPES, ChartSpec, build_figure, validate_spec

DEFAULT_LIMIT = 100
MAX_LIMIT = 500
QUERY_TIMEOUT_S = 30
MODEL_ROW_PREVIEW = 100

_FORBIDDEN = re.compile(
    r"\b(insert|update|delete|merge|drop|create|alter|truncate|attach|detach|copy|export|import|"
    r"install|load|pragma|set|reset|call|vacuum|checkpoint|begin|commit|rollback|use)\b",
    re.IGNORECASE,
)
_LEADING_COMMENTS = re.compile(r"^(\s*(--[^\n]*\n|/\*.*?\*/))*\s*", re.DOTALL)
_STRING_LITERALS = re.compile(r"'(?:[^']|'')*'")


class SqlRejected(ValueError):
    """Raised when a statement is not a single read-only SELECT."""


def validate_sql(sql: str) -> str:
    if not isinstance(sql, str) or not sql.strip():
        raise SqlRejected("sql must be a non-empty string")
    cleaned = sql.strip().rstrip(";").strip()
    if ";" in cleaned:
        raise SqlRejected("only a single statement is allowed")
    body = _LEADING_COMMENTS.sub("", cleaned)
    first = body.split(None, 1)[0].lower() if body.split() else ""
    if first not in ("select", "with"):
        raise SqlRejected("only SELECT (or WITH ... SELECT) statements are allowed")
    # Keyword scan ignores string literals so a value like 'offset reset' passes.
    hit = _FORBIDDEN.search(_STRING_LITERALS.sub("''", body))
    if hit:
        raise SqlRejected(f"statement contains a disallowed keyword: {hit.group(0).upper()}")
    return cleaned


def run_query(
    con: duckdb.DuckDBPyConnection,
    sql: str,
    limit: int = DEFAULT_LIMIT,
    timeout_s: float = QUERY_TIMEOUT_S,
) -> tuple[pd.DataFrame, bool]:
    """Run a validated SELECT with a row cap and a wall-clock timeout.

    Returns the frame and whether more rows were available than the cap.
    """
    cleaned = validate_sql(sql)
    limit = max(1, min(int(limit), MAX_LIMIT))
    wrapped = f"select * from (\n{cleaned}\n) as q limit {limit + 1}"
    cursor = con.cursor()
    timer = threading.Timer(timeout_s, cursor.interrupt)
    timer.start()
    try:
        df = cursor.execute(wrapped).df()
    except duckdb.InterruptException as exc:
        raise TimeoutError(f"query exceeded {timeout_s:.0f}s and was cancelled") from exc
    finally:
        timer.cancel()
        cursor.close()
    truncated = len(df) > limit
    return df.head(limit), truncated


def frame_to_text(df: pd.DataFrame, truncated: bool, max_rows: int = MODEL_ROW_PREVIEW) -> str:
    """Compact text rendering of a result for the model's context."""
    shown = df.head(max_rows)
    header = f"{len(df)} row(s), {len(df.columns)} column(s)"
    if truncated:
        header += " (result was capped; more rows exist - add ORDER BY/LIMIT or aggregate)"
    if len(df) > max_rows:
        header += f"; showing first {max_rows}"
    if shown.empty:
        return header + "\ncolumns: " + ", ".join(df.columns)
    return header + "\n" + shown.to_string(index=False, max_colwidth=60)


# -- tool schemas ------------------------------------------------------------

TOOLS: list[dict] = [
    {
        "name": "list_entities",
        "description": (
            "List the tables and views the agent can query, with descriptions and grain. "
            "Marts in the analytics schema are returned by default; set include_supporting "
            "to also see staging, intermediate, and seed entities."
        ),
        "eager_input_streaming": True,
        "input_schema": {
            "type": "object",
            "properties": {
                "include_supporting": {
                    "type": "boolean",
                    "description": "Also list staging, intermediate, and seed entities.",
                }
            },
            "required": [],
            "additionalProperties": False,
        },
    },
    {
        "name": "describe_entity",
        "description": (
            "Return the columns, types, descriptions, grain, row count, and a few sample rows "
            "for one entity. Use before querying a table whose columns you are unsure of."
        ),
        "eager_input_streaming": True,
        "input_schema": {
            "type": "object",
            "properties": {
                "name": {
                    "type": "string",
                    "description": "Entity name, e.g. analytics.fct_show_insights or fct_show_insights.",
                }
            },
            "required": ["name"],
            "additionalProperties": False,
        },
    },
    {
        "name": "run_sql",
        "description": (
            "Run a read-only DuckDB SELECT against the warehouse and return the rows. "
            "Always qualify tables as schema.table. Results are capped, so aggregate or "
            "ORDER BY + LIMIT rather than selecting raw rows. The user sees the SQL and the "
            "full result table alongside your answer."
        ),
        "eager_input_streaming": True,
        "input_schema": {
            "type": "object",
            "properties": {
                "sql": {"type": "string", "description": "A single SELECT statement in DuckDB SQL."},
                "limit": {
                    "type": "integer",
                    "description": f"Max rows to return (default {DEFAULT_LIMIT}, max {MAX_LIMIT}).",
                },
            },
            "required": ["sql"],
            "additionalProperties": False,
        },
    },
    {
        "name": "render_chart",
        "description": (
            "Run a SELECT and render its result as a chart shown to the user. Pick the form by "
            "the data's job: bar for comparing categories, hbar for ranked lists with long labels, "
            "line for change over time, area for stacked composition over time, scatter for two "
            "measures. Provide the x column, one or more y columns (same unit, plotted on a single "
            "axis), or one y column plus a color column that splits it into series. At most 8 "
            "series are drawn; smaller ones fold into 'Other'. Keep rows ordered and small "
            "(ORDER BY + LIMIT)."
        ),
        "eager_input_streaming": True,
        "input_schema": {
            "type": "object",
            "properties": {
                "sql": {"type": "string", "description": "SELECT that returns the columns to plot."},
                "chart_type": {"type": "string", "enum": list(CHART_TYPES)},
                "x": {"type": "string", "description": "Column for the x axis (category or date)."},
                "y": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "One or more numeric columns to plot, all in the same unit.",
                },
                "color": {
                    "type": "string",
                    "description": "Optional column whose values become separate series (only with a single y).",
                },
                "title": {"type": "string", "description": "Short chart title stating what is shown."},
                "x_label": {"type": "string"},
                "y_label": {"type": "string"},
            },
            "required": ["sql", "chart_type", "x", "y", "title"],
            "additionalProperties": False,
        },
    },
]


# -- execution ---------------------------------------------------------------

@dataclass
class ToolOutcome:
    content: str
    is_error: bool = False
    sql: str | None = None
    dataframe: pd.DataFrame | None = None
    truncated: bool = False
    chart: ChartSpec | None = None
    figure: object | None = None


class ToolExecutor:
    def __init__(self, catalog: Catalog, con: duckdb.DuckDBPyConnection):
        self.catalog = catalog
        self.con = con

    def execute(self, name: str, args: object) -> ToolOutcome:
        if not isinstance(args, dict):
            return ToolOutcome(json.dumps({"INVALID_JSON": json.dumps(args)}), is_error=True)
        handler = getattr(self, f"_tool_{name}", None)
        if handler is None:
            return ToolOutcome(f"Unknown tool: {name}", is_error=True)
        try:
            return handler(args)
        except (SqlRejected, ValueError, KeyError, TimeoutError) as exc:
            return ToolOutcome(f"Error: {exc}", is_error=True)
        except duckdb.Error as exc:
            return ToolOutcome(f"DuckDB error: {exc}", is_error=True, sql=args.get("sql"))

    def _tool_list_entities(self, args: dict) -> ToolOutcome:
        include = bool(args.get("include_supporting", False))
        lines = []
        for e in self.catalog.entities(include_supporting=include):
            grain = f" Grain: one row per {e.grain}." if e.grain else ""
            rows = f" Rows: {e.row_count:,}." if e.row_count is not None else ""
            lines.append(f"- {e.qualified} ({e.kind}, {e.table_type.lower()}): {e.description}{grain}{rows}")
        return ToolOutcome("\n".join(lines) or "No entities found.")

    def _tool_describe_entity(self, args: dict) -> ToolOutcome:
        name = args.get("name")
        if not isinstance(name, str):
            raise ValueError("name must be a string")
        entity = self.catalog.get(name)
        return ToolOutcome(self.catalog.describe(entity))

    def _tool_run_sql(self, args: dict) -> ToolOutcome:
        sql = args.get("sql")
        limit = args.get("limit", DEFAULT_LIMIT)
        if not isinstance(limit, int) or isinstance(limit, bool):
            raise ValueError("limit must be an integer")
        df, truncated = run_query(self.con, sql, limit=limit)
        return ToolOutcome(frame_to_text(df, truncated), sql=validate_sql(sql),
                           dataframe=df, truncated=truncated)

    def _tool_render_chart(self, args: dict) -> ToolOutcome:
        sql = args.get("sql")
        df, truncated = run_query(self.con, sql, limit=MAX_LIMIT)
        spec = validate_spec(args, list(df.columns))
        if df.empty:
            raise ValueError("the SQL returned no rows, so there is nothing to chart")
        for col in spec.y:
            if not pd.api.types.is_numeric_dtype(df[col]):
                raise ValueError(f"y column '{col}' is not numeric ({df[col].dtype})")
        fig = build_figure(df, spec)
        summary = (
            f"Chart rendered for the user: {spec.chart_type} of {', '.join(spec.y)} by {spec.x}"
            + (f" split by {spec.color}" if spec.color else "")
            + f"; {len(df)} row(s)."
        )
        if truncated:
            summary += f" Result was capped at {MAX_LIMIT} rows."
        if spec.notes:
            summary += " " + " ".join(spec.notes)
        summary += "\nData preview:\n" + df.head(20).to_string(index=False, max_colwidth=40)
        return ToolOutcome(summary, sql=validate_sql(sql), dataframe=df,
                           truncated=truncated, chart=spec, figure=fig)
