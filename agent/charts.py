"""Build Plotly figures from a query result and a chart spec chosen by the model."""

from __future__ import annotations

from dataclasses import dataclass, field

import pandas as pd
import plotly.graph_objects as go

CHART_TYPES = ("bar", "hbar", "line", "area", "scatter")

# Fixed categorical order; hues are assigned by position and never cycled.
PALETTE = [
    "#2a78d6",  # blue
    "#eb6834",  # orange
    "#1baf7a",  # aqua
    "#eda100",  # yellow
    "#e87ba4",  # magenta
    "#008300",  # green
    "#4a3aa7",  # violet
    "#e34948",  # red
]
MAX_SERIES = len(PALETTE)
SERIES_COL = "series"
TEXT_PRIMARY = "#0b0b0b"
TEXT_SECONDARY = "#52514e"
GRID = "#e6e5e1"


@dataclass
class ChartSpec:
    chart_type: str
    x: str
    y: list[str]
    title: str = ""
    color: str | None = None
    x_label: str | None = None
    y_label: str | None = None
    notes: list[str] = field(default_factory=list)


def validate_spec(raw: dict, columns: list[str]) -> ChartSpec:
    """Check a model-supplied chart spec against the columns the SQL returned."""
    chart_type = raw.get("chart_type")
    if chart_type not in CHART_TYPES:
        raise ValueError(f"chart_type must be one of {CHART_TYPES}, got {chart_type!r}")
    x = raw.get("x")
    if not isinstance(x, str) or x not in columns:
        raise ValueError(f"x must be a column returned by the SQL; got {x!r}, available: {columns}")
    y = raw.get("y")
    if isinstance(y, str):
        y = [y]
    if not isinstance(y, list) or not y or not all(isinstance(c, str) for c in y):
        raise ValueError("y must be a non-empty list of column names")
    missing = [c for c in y if c not in columns]
    if missing:
        raise ValueError(f"y columns not in SQL result: {missing}; available: {columns}")
    color = raw.get("color")
    if color is not None:
        if not isinstance(color, str) or color not in columns:
            raise ValueError(f"color must be a column returned by the SQL; got {color!r}")
        if len(y) > 1:
            raise ValueError("use either several y columns or a color column, not both")
    title = raw.get("title") or ""
    if not isinstance(title, str):
        raise ValueError("title must be a string")
    return ChartSpec(
        chart_type=chart_type,
        x=x,
        y=y,
        title=title,
        color=color,
        x_label=raw.get("x_label") if isinstance(raw.get("x_label"), str) else None,
        y_label=raw.get("y_label") if isinstance(raw.get("y_label"), str) else None,
    )


def _to_long(df: pd.DataFrame, spec: ChartSpec) -> tuple[pd.DataFrame, str]:
    """Return a long-format frame with one value column and a series column."""
    if spec.color:
        long = df[[spec.x, spec.y[0], spec.color]].rename(columns={spec.color: SERIES_COL})
        long[SERIES_COL] = long[SERIES_COL].astype(str)
        return long, spec.y[0]
    if len(spec.y) == 1:
        long = df[[spec.x, spec.y[0]]].copy()
        long[SERIES_COL] = spec.y[0]
        return long, spec.y[0]
    long = df[[spec.x, *spec.y]].melt(id_vars=[spec.x], var_name=SERIES_COL, value_name="value")
    return long, "value"


def _fold_series(long: pd.DataFrame, value_col: str, spec: ChartSpec) -> pd.DataFrame:
    """Keep the top series by total value; fold the remainder into 'Other'."""
    totals = long.groupby(SERIES_COL)[value_col].sum().sort_values(ascending=False)
    if len(totals) <= MAX_SERIES:
        return long
    keep = set(totals.index[: MAX_SERIES - 1])
    spec.notes.append(
        f"{len(totals)} series exceeded the {MAX_SERIES}-series limit; "
        f"{len(totals) - (MAX_SERIES - 1)} smaller series were folded into 'Other'."
    )
    folded = long.copy()
    folded.loc[~folded[SERIES_COL].isin(keep), SERIES_COL] = "Other"
    return folded.groupby([spec.x, SERIES_COL], as_index=False, sort=False)[value_col].sum()


def build_figure(df: pd.DataFrame, spec: ChartSpec) -> go.Figure:
    long, value_col = _to_long(df, spec)
    long = _fold_series(long, value_col, spec)
    series_names = list(dict.fromkeys(long[SERIES_COL].tolist()))
    if "Other" in series_names:
        series_names = [s for s in series_names if s != "Other"] + ["Other"]
    multi = len(series_names) > 1

    fig = go.Figure()
    for i, name in enumerate(series_names):
        part = long[long[SERIES_COL] == name]
        colour = PALETTE[i]
        xs, ys = part[spec.x], part[value_col]
        if spec.chart_type == "bar":
            fig.add_bar(x=xs, y=ys, name=name, marker_color=colour)
        elif spec.chart_type == "hbar":
            fig.add_bar(x=ys, y=xs, name=name, marker_color=colour, orientation="h")
        elif spec.chart_type == "line":
            fig.add_scatter(x=xs, y=ys, name=name, mode="lines+markers",
                            line={"color": colour, "width": 2}, marker={"size": 8})
        elif spec.chart_type == "area":
            fig.add_scatter(x=xs, y=ys, name=name, mode="lines", stackgroup="one",
                            line={"color": colour, "width": 2})
        elif spec.chart_type == "scatter":
            fig.add_scatter(x=xs, y=ys, name=name, mode="markers",
                            marker={"color": colour, "size": 9,
                                    "line": {"color": "#ffffff", "width": 1}})

    horizontal = spec.chart_type == "hbar"
    x_title = spec.x_label or (value_col if horizontal else spec.x)
    y_title = spec.y_label or (spec.x if horizontal else value_col)
    fig.update_layout(
        title={"text": spec.title, "x": 0, "font": {"color": TEXT_PRIMARY, "size": 16}},
        template="plotly_white",
        paper_bgcolor="#fcfcfb",
        plot_bgcolor="#fcfcfb",
        font={"color": TEXT_SECONDARY, "size": 12},
        showlegend=multi,
        legend={"orientation": "h", "y": -0.2, "title": None},
        margin={"l": 48, "r": 16, "t": 48 if spec.title else 16, "b": 48},
        bargap=0.35,
        bargroupgap=0.08,
        barmode="group",
        hovermode="x unified" if spec.chart_type in ("line", "area") else "closest",
    )
    fig.update_xaxes(title_text=x_title, showgrid=False, linecolor=GRID, zeroline=False)
    fig.update_yaxes(title_text=y_title, gridcolor=GRID, linecolor=GRID, zeroline=False)
    if horizontal:
        fig.update_yaxes(autorange="reversed", showgrid=False)
        fig.update_xaxes(showgrid=True, gridcolor=GRID)
    return fig
