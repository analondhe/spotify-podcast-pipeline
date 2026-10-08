"""Catalog of warehouse entities, enriched with descriptions from the dbt YAML.

The catalog is the agent's semantic layer: it combines the live schema from
DuckDB's information_schema with model, grain, and column descriptions written
in the dbt project's schema files.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path

import duckdb
import yaml

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DB = ROOT / "warehouse" / "spotify_podcasts.duckdb"
DBT_DIR = ROOT / "dbt_project"

MART_SCHEMA = "analytics"
SUPPORTING_SCHEMAS = ("staging", "intermediate", "seeds")
RAW_SCHEMA = "raw"


def _clean(text: str | None) -> str:
    """Collapse the folded whitespace that YAML block scalars leave behind."""
    return re.sub(r"\s+", " ", text or "").strip()


@dataclass
class Column:
    name: str
    data_type: str
    description: str = ""


@dataclass
class Entity:
    schema: str
    name: str
    kind: str  # "mart", "supporting", or "raw"
    table_type: str  # "BASE TABLE" or "VIEW"
    description: str = ""
    grain: str = ""
    columns: list[Column] = field(default_factory=list)
    row_count: int | None = None

    @property
    def qualified(self) -> str:
        return f"{self.schema}.{self.name}"

    def column(self, name: str) -> Column | None:
        return next((c for c in self.columns if c.name == name), None)


def load_dbt_docs(dbt_dir: Path = DBT_DIR) -> dict[str, dict]:
    """Read model and seed descriptions from every schema YAML in the dbt project."""
    docs: dict[str, dict] = {}
    patterns = ("models/**/*.yml", "models/**/*.yaml", "seeds/*.yml", "seeds/*.yaml")
    for pattern in patterns:
        for path in sorted(dbt_dir.glob(pattern)):
            try:
                data = yaml.safe_load(path.read_text()) or {}
            except yaml.YAMLError:
                continue
            for section in ("models", "seeds"):
                for node in data.get(section) or []:
                    name = node.get("name")
                    if not name:
                        continue
                    meta = node.get("meta") or {}
                    docs[name] = {
                        "description": _clean(node.get("description")),
                        "grain": _clean(meta.get("grain")),
                        "columns": {
                            c["name"]: _clean(c.get("description"))
                            for c in node.get("columns") or []
                            if c.get("name")
                        },
                    }
    return docs


class Catalog:
    """Entities available to the agent, keyed by schema.table."""

    def __init__(self, con: duckdb.DuckDBPyConnection, dbt_dir: Path = DBT_DIR):
        self.con = con
        self.dbt_dir = dbt_dir
        self._entities: dict[str, Entity] = {}

    def build(self) -> "Catalog":
        docs = load_dbt_docs(self.dbt_dir)
        tables = self.con.execute(
            """
            select table_schema, table_name, table_type
            from information_schema.tables
            where table_schema not in ('information_schema', 'pg_catalog', 'main')
            order by table_schema, table_name
            """
        ).fetchall()
        columns = self.con.execute(
            """
            select table_schema, table_name, column_name, data_type
            from information_schema.columns
            order by table_schema, table_name, ordinal_position
            """
        ).fetchall()
        cols_by_table: dict[tuple[str, str], list[tuple[str, str]]] = {}
        for schema, table, col, dtype in columns:
            cols_by_table.setdefault((schema, table), []).append((col, dtype))

        self._entities = {}
        for schema, table, table_type in tables:
            if schema == MART_SCHEMA:
                kind = "mart"
            elif schema in SUPPORTING_SCHEMAS:
                kind = "supporting"
            elif schema == RAW_SCHEMA:
                kind = "raw"
            else:
                continue
            doc = docs.get(table, {})
            entity = Entity(
                schema=schema,
                name=table,
                kind=kind,
                table_type=table_type,
                description=doc.get("description", ""),
                grain=doc.get("grain", ""),
                columns=[
                    Column(col, dtype, doc.get("columns", {}).get(col, ""))
                    for col, dtype in cols_by_table.get((schema, table), [])
                ],
                row_count=self._count(schema, table),
            )
            self._entities[entity.qualified] = entity
        return self

    def _count(self, schema: str, table: str) -> int | None:
        try:
            return self.con.execute(f'select count(*) from "{schema}"."{table}"').fetchone()[0]
        except duckdb.Error:
            return None

    # -- lookup -------------------------------------------------------------

    def entities(self, include_supporting: bool = False, include_raw: bool = False) -> list[Entity]:
        kinds = {"mart"}
        if include_supporting:
            kinds.add("supporting")
        if include_raw:
            kinds.add("raw")
        order = {"mart": 0, "supporting": 1, "raw": 2}
        return sorted(
            (e for e in self._entities.values() if e.kind in kinds),
            key=lambda e: (order[e.kind], e.qualified),
        )

    def get(self, name: str) -> Entity:
        key = name.strip().strip('"').lower()
        if key in self._entities:
            return self._entities[key]
        matches = [e for e in self._entities.values() if e.name == key]
        if len(matches) == 1:
            return matches[0]
        if len(matches) > 1:
            raise KeyError(
                f"'{name}' is ambiguous; use one of: " + ", ".join(e.qualified for e in matches)
            )
        known = ", ".join(sorted(self._entities))
        raise KeyError(f"Unknown entity '{name}'. Known entities: {known}")

    # -- text renderings ----------------------------------------------------

    def coverage(self) -> dict[str, object]:
        """Summarize the date range and regions in the data, when the region mart exists."""
        if f"{MART_SCHEMA}.fct_region_insights" not in self._entities:
            return {}
        row = self.con.execute(
            f"""
            select min(snapshot_date), max(snapshot_date),
                   count(distinct snapshot_date), count(distinct region_code)
            from {MART_SCHEMA}.fct_region_insights
            """
        ).fetchone()
        return {
            "first_date": row[0],
            "last_date": row[1],
            "days": row[2],
            "regions": row[3],
        }

    def describe(self, entity: Entity, sample_rows: int = 3) -> str:
        lines = [
            f"# {entity.qualified} ({entity.table_type.lower()}, {entity.kind})",
            entity.description or "(no description)",
        ]
        if entity.grain:
            lines.append(f"Grain: one row per {entity.grain}")
        if entity.row_count is not None:
            lines.append(f"Rows: {entity.row_count:,}")
        lines.append("")
        lines.append("Columns:")
        for c in entity.columns:
            desc = f" - {c.description}" if c.description else ""
            lines.append(f"- {c.name} ({c.data_type}){desc}")
        if sample_rows > 0 and entity.row_count:
            try:
                rel = self.con.execute(
                    f'select * from "{entity.schema}"."{entity.name}" limit {int(sample_rows)}'
                )
                names = [d[0] for d in rel.description]
                rows = rel.fetchall()
                lines.append("")
                lines.append(f"Sample rows ({len(rows)}):")
                lines.append(" | ".join(names))
                for r in rows:
                    lines.append(" | ".join(_fmt(v) for v in r))
            except duckdb.Error as exc:
                lines.append(f"(could not fetch sample rows: {exc})")
        return "\n".join(lines)

    def dictionary(self) -> str:
        """Markdown data dictionary for the system prompt: marts in full, others as one-liners."""
        parts = ["## Marts (schema `analytics`) - query these first"]
        for e in self.entities():
            parts.append(f"\n### {e.qualified}")
            parts.append(e.description or "(no description)")
            if e.grain:
                parts.append(f"Grain: one row per {e.grain}.")
            if e.row_count is not None:
                parts.append(f"Rows: {e.row_count:,}.")
            for c in e.columns:
                desc = f": {c.description}" if c.description else ""
                parts.append(f"- `{c.name}` ({c.data_type}){desc}")
        supporting = self.entities(include_supporting=True)
        supporting = [e for e in supporting if e.kind == "supporting"]
        if supporting:
            parts.append("\n## Supporting entities - use describe_entity before querying")
            for e in supporting:
                cols = ", ".join(c.name for c in e.columns)
                grain = f" Grain: {e.grain}." if e.grain else ""
                parts.append(f"- `{e.qualified}` ({e.table_type.lower()}): {e.description}{grain} Columns: {cols}")
        return "\n".join(parts)


def _fmt(value: object) -> str:
    text = str(value)
    return text if len(text) <= 60 else text[:57] + "..."


def connect(db_path: Path | str = DEFAULT_DB) -> duckdb.DuckDBPyConnection:
    path = Path(db_path)
    if not path.exists():
        raise FileNotFoundError(
            f"Warehouse not found at {path}. Run the pipeline first "
            "(python scripts/load_to_duckdb.py && cd dbt_project && dbt build --profiles-dir .)."
        )
    return duckdb.connect(str(path), read_only=True)
