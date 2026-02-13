"""Load all CSVs from data/ into DuckDB under a 'raw' schema."""

import glob
import os
import sys

import duckdb

# Paths relative to this script's location
BASE_DIR = os.path.join(os.path.dirname(__file__), os.pardir)
DATA_DIR = os.path.abspath(os.path.join(BASE_DIR, "data"))
DB_PATH = os.path.abspath(os.path.join(BASE_DIR, "warehouse", "spotify_podcasts.duckdb"))


def sanitize(name: str) -> str:
    """Turn a filename into a valid, consistent table name."""
    return name.lower().replace(" ", "_").replace("-", "_")


def load_csv(con: duckdb.DuckDBPyConnection, csv_path: str) -> None:
    """Load a single CSV into the raw schema, replacing any existing table."""
    table_name = sanitize(os.path.splitext(os.path.basename(csv_path))[0])
    filename = os.path.basename(csv_path)

    print(f"  {filename} -> raw.{table_name}")

    try:
        con.execute(f"DROP TABLE IF EXISTS raw.{table_name}")
        con.execute(
            f"CREATE TABLE raw.{table_name} AS SELECT * FROM read_csv_auto('{csv_path}')"
        )
        row_count = con.execute(f"SELECT COUNT(*) FROM raw.{table_name}").fetchone()[0]
        print(f"    {row_count:,} rows loaded")
    except duckdb.Error as e:
        print(f"    ERROR loading {filename}: {e}", file=sys.stderr)
        raise


def print_column_inventory(con: duckdb.DuckDBPyConnection) -> None:
    """Print every column and its type for all tables in the raw schema."""
    tables = con.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'raw'"
    ).fetchall()

    print("\n--- Column inventory ---")
    for (tbl,) in tables:
        cols = con.execute(
            f"SELECT column_name, data_type FROM information_schema.columns "
            f"WHERE table_schema = 'raw' AND table_name = '{tbl}' ORDER BY ordinal_position"
        ).fetchall()
        print(f"\nraw.{tbl}:")
        for col_name, col_type in cols:
            print(f"  {col_name:40s} {col_type}")


def main():
    # Find CSVs
    csv_files = sorted(glob.glob(os.path.join(DATA_DIR, "*.csv")))
    if not csv_files:
        print(f"No CSV files found in {DATA_DIR}", file=sys.stderr)
        sys.exit(1)

    # Ensure the warehouse directory exists
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

    print(f"Loading {len(csv_files)} CSV(s) into {DB_PATH}")

    try:
        con = duckdb.connect(DB_PATH)
    except duckdb.Error as e:
        print(f"Failed to connect to DuckDB at {DB_PATH}: {e}", file=sys.stderr)
        sys.exit(1)

    try:
        con.execute("CREATE SCHEMA IF NOT EXISTS raw")
        for csv_path in csv_files:
            load_csv(con, csv_path)
        print_column_inventory(con)
    finally:
        con.close()

    print("\nDone.")


if __name__ == "__main__":
    main()
