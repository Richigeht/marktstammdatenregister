#!/usr/bin/env python3
"""
MaStR Gesamtdatenexport → DuckDB ETL

Usage:
    python3 etl.py [--db mastr.duckdb] [--data-dir <path>] [--tables <name> ...]

Examples:
    python3 etl.py
    python3 etl.py --tables EinheitenStromSpeicher Katalogwerte
    python3 etl.py --db /tmp/mastr.duckdb --data-dir /data/Gesamtdatenexport_20260317_25.2
"""

import argparse
import sys
from pathlib import Path
import zipfile
from lxml import etree
import duckdb
import pandas as pd
from tqdm import tqdm

# ── Paths (relative to this script) ──────────────────────────────────────────
SCRIPT_DIR = Path(__file__).parent
XSD_DIR = SCRIPT_DIR / "Dokumentation MaStR Gesamtdatenexport" / "xsd"
XSD_ZIP = SCRIPT_DIR / "Dokumentation MaStR Gesamtdatenexport" / "xsd.zip"
DEFAULT_DB = SCRIPT_DIR / "mastr.duckdb"

# Tables to import (in dependency order — lookups first)
DEFAULT_TABLES = [
    "Katalogkategorien",
    "Katalogwerte",
    "Marktakteure",
    "Netze",
    "Netzanschlusspunkte",
    "EinheitenStromSpeicher",
    "AnlagenEegSpeicher",
    "AnlagenStromSpeicher",
    "EinheitenAenderungNetzbetreiberzuordnungen",
]

DEFAULT_BATCH_SIZE = 200_000

# ── XSD type → DuckDB type ────────────────────────────────────────────────────
XS_TYPE_MAP = {
    "xs:string": "VARCHAR",
    "xs:int": "INTEGER",
    "xs:integer": "INTEGER",
    "xs:short": "SMALLINT",
    "xs:byte": "TINYINT",
    "xs:long": "BIGINT",
    "xs:float": "FLOAT",
    "xs:double": "DOUBLE",
    "xs:decimal": "DECIMAL",
    "xs:date": "DATE",
    "xs:dateTime": "TIMESTAMP",
    "xs:boolean": "BOOLEAN",
    "xs:positiveInteger": "INTEGER",
    "xs:nonNegativeInteger": "INTEGER",
}

XS_NS = {"xs": "http://www.w3.org/2001/XMLSchema"}


# ── Schema parsing ────────────────────────────────────────────────────────────

def xsd_type_to_sql(elem) -> str:
    """Determine SQL type for an xs:element node."""
    xs_type = elem.get("type")
    if xs_type:
        return XS_TYPE_MAP.get(xs_type, "VARCHAR")
    restriction = elem.find(".//xs:restriction", XS_NS)
    if restriction is not None:
        base = restriction.get("base", "xs:string")
        return XS_TYPE_MAP.get(base, "VARCHAR")
    return "VARCHAR"


def load_xsd_bytes(table_name: str) -> bytes | None:
    """Load an XSD either from the extracted xsd/ directory or the bundled zip."""
    xsd_name = f"{table_name}.xsd"
    xsd_path = XSD_DIR / xsd_name
    if xsd_path.exists():
        return xsd_path.read_bytes()

    if XSD_ZIP.exists():
        member = f"xsd/{xsd_name}"
        with zipfile.ZipFile(XSD_ZIP) as zf:
            try:
                return zf.read(member)
            except KeyError:
                return None

    return None


def parse_xsd(xsd_name: str, xsd_bytes: bytes) -> tuple[str, str, list[tuple[str, str]]]:
    """
    Parse an XSD file and return:
        table_name  — root element name (e.g. 'EinheitenStromSpeicher')
        row_tag     — row element name  (e.g. 'EinheitStromSpeicher')
        fields      — [(field_name, sql_type), ...]
    """
    root = etree.fromstring(xsd_bytes)

    # Root element = table container
    root_el = root.find("xs:element", XS_NS)
    if root_el is None:
        raise ValueError(f"No root xs:element in {xsd_name}")
    table_name = root_el.get("name")

    # Row element (maxOccurs="unbounded")
    row_el = root_el.find("xs:complexType/xs:sequence/xs:element", XS_NS)
    row_tag = row_el.get("name") if row_el is not None else table_name

    # Field elements — either in xs:choice or xs:sequence inside the row
    field_container = None
    if row_el is not None:
        field_container = row_el.find("xs:complexType/xs:choice", XS_NS)
        if field_container is None:
            field_container = row_el.find("xs:complexType/xs:sequence", XS_NS)

    fields: list[tuple[str, str]] = []
    seen: set[str] = set()
    if field_container is not None:
        for elem in field_container.findall("xs:element", XS_NS):
            name = elem.get("name")
            if not name or name in seen:
                continue
            seen.add(name)
            fields.append((name, xsd_type_to_sql(elem)))

    return table_name, row_tag, fields


# ── Database helpers ──────────────────────────────────────────────────────────

def create_table(conn: duckdb.DuckDBPyConnection, table_name: str, fields: list[tuple[str, str]]):
    col_defs = ",\n    ".join(f'"{name}" {sql_type}' for name, sql_type in fields)
    conn.execute(f'CREATE TABLE IF NOT EXISTS "{table_name}" (\n    {col_defs}\n)')


def ensure_progress_table(conn: duckdb.DuckDBPyConnection):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS _import_progress (
            table_name VARCHAR,
            file_name  VARCHAR,
            rows       BIGINT,
            finished_at TIMESTAMP DEFAULT current_timestamp
        )
    """)


def completed_files(conn: duckdb.DuckDBPyConnection, table_name: str) -> set[str]:
    rows = conn.execute(
        "SELECT file_name FROM _import_progress WHERE table_name = ?", [table_name]
    ).fetchall()
    return {r[0] for r in rows}


def mark_complete(conn: duckdb.DuckDBPyConnection, table_name: str, file_name: str, rows: int):
    conn.execute(
        "INSERT INTO _import_progress (table_name, file_name, rows) VALUES (?, ?, ?)",
        [table_name, file_name, rows],
    )


# ── XML streaming ─────────────────────────────────────────────────────────────

def iter_records(xml_path: Path, row_tag: str, field_names: list[str]):
    """
    Stream-parse a (potentially UTF-16) XML file and yield one tuple per row.
    Tuple values are in the same order as field_names, missing fields → None.
    """
    field_index = {name: idx for idx, name in enumerate(field_names)}
    with open(xml_path, "rb") as f:
        context = etree.iterparse(f, events=("end",), tag=row_tag, huge_tree=True)
        for _event, elem in context:
            record = [None] * len(field_names)
            for child in elem:
                idx = field_index.get(child.tag)
                if idx is not None:
                    record[idx] = child.text
            yield tuple(record)
            elem.clear()
            while elem.getprevious() is not None:
                del elem.getparent()[0]


# ── Import ────────────────────────────────────────────────────────────────────

def import_table(
    conn: duckdb.DuckDBPyConnection,
    table_name: str,
    row_tag: str,
    fields: list[tuple[str, str]],
    data_dir: Path,
    batch_size: int,
) -> int:
    xml_files = sorted(data_dir.glob(f"{table_name}.xml")) + sorted(
        data_dir.glob(f"{table_name}_*.xml"),
        key=lambda p: int(p.stem.rsplit("_", 1)[-1]),
    )
    if not xml_files:
        print(f"  [skip] No XML files found for {table_name}")
        return 0

    done = completed_files(conn, table_name)
    field_names = [name for name, _ in fields]

    total_rows = 0
    for xml_path in xml_files:
        if xml_path.name in done:
            print(f"  {xml_path.name}: already imported, skipping")
            total_rows += conn.execute(
                "SELECT rows FROM _import_progress WHERE table_name=? AND file_name=?",
                [table_name, xml_path.name],
            ).fetchone()[0]
            continue

        batch: list[tuple] = []
        file_rows = 0
        conn.execute("BEGIN")
        try:
            with tqdm(
                desc=f"  {xml_path.name}",
                unit=" rows",
                unit_scale=True,
                leave=False,
            ) as pbar:
                for row in iter_records(xml_path, row_tag, field_names):
                    batch.append(row)
                    file_rows += 1
                    if len(batch) >= batch_size:
                        conn.append(table_name, pd.DataFrame.from_records(batch, columns=field_names))
                        pbar.update(len(batch))
                        batch = []
                if batch:
                    conn.append(table_name, pd.DataFrame.from_records(batch, columns=field_names))
                    pbar.update(len(batch))

            mark_complete(conn, table_name, xml_path.name, file_rows)
            conn.execute("COMMIT")
            print(f"  {xml_path.name}: {file_rows:,} rows")
            total_rows += file_rows

        except KeyboardInterrupt:
            conn.execute("ROLLBACK")
            print(f"\n  Interrupted during {xml_path.name} — rolled back, progress saved.")
            raise
        except Exception:
            conn.execute("ROLLBACK")
            raise

    return total_rows


# ── Main ──────────────────────────────────────────────────────────────────────

def find_data_dir(base: Path) -> Path:
    candidates = sorted(
        [p for p in base.glob("Gesamtdatenexport_*") if p.is_dir()],
        reverse=True,
    )
    if not candidates:
        sys.exit("Error: no Gesamtdatenexport_* directory found. Use --data-dir.")
    return candidates[0]


def main():
    parser = argparse.ArgumentParser(description="Import MaStR XML export into DuckDB")
    parser.add_argument("--db", type=Path, default=DEFAULT_DB, help="DuckDB file path")
    parser.add_argument("--data-dir", type=Path, help="Directory with XML files (auto-detected if omitted)")
    parser.add_argument("--tables", nargs="+", metavar="TABLE", help="Only import these tables")
    parser.add_argument("--drop", action="store_true", help="Drop existing tables before import")
    parser.add_argument(
        "--batch-size",
        type=int,
        default=DEFAULT_BATCH_SIZE,
        help=f"Rows per append batch (default: {DEFAULT_BATCH_SIZE})",
    )
    args = parser.parse_args()

    data_dir = args.data_dir or find_data_dir(SCRIPT_DIR)
    tables = args.tables or DEFAULT_TABLES

    print(f"Data dir : {data_dir}")
    print(f"Database : {args.db}")
    print(f"Tables   : {', '.join(tables)}")
    print(f"Batch    : {args.batch_size:,}")
    print()

    # Parse XSD schemas
    schemas: dict[str, tuple[str, list[tuple[str, str]]]] = {}
    for table_name in tables:
        xsd_bytes = load_xsd_bytes(table_name)
        if xsd_bytes is None:
            print(f"  [warn] No XSD found for {table_name}, skipping")
            continue
        _, row_tag, fields = parse_xsd(f"{table_name}.xsd", xsd_bytes)
        schemas[table_name] = (row_tag, fields)
        print(f"  Schema: {table_name} — {len(fields)} columns, row tag <{row_tag}>")

    print()

    # Open database
    conn = duckdb.connect(str(args.db))
    conn.execute("PRAGMA threads = 4")
    ensure_progress_table(conn)

    # Create / reset tables
    for table_name, (row_tag, fields) in schemas.items():
        if args.drop:
            conn.execute(f'DROP TABLE IF EXISTS "{table_name}"')
            conn.execute(
                "DELETE FROM _import_progress WHERE table_name = ?", [table_name]
            )
        create_table(conn, table_name, fields)
    print(f"Tables ready in {args.db}\n")

    # Import data
    grand_total = 0
    try:
        for table_name, (row_tag, fields) in schemas.items():
            print(f"Importing {table_name} ...")
            n = import_table(conn, table_name, row_tag, fields, data_dir, args.batch_size)
            print(f"  → {n:,} rows total\n")
            grand_total += n
    except KeyboardInterrupt:
        print(f"\nStopped. Re-run to resume — completed files will be skipped.")
    finally:
        conn.close()

    print(f"Done. {grand_total:,} rows imported into {args.db}")


if __name__ == "__main__":
    main()
