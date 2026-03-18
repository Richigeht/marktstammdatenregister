#!/usr/bin/env python3
"""
Export a compact BESS dataset from MaStR DuckDB for static hosting.
"""

import argparse
import json
from pathlib import Path

import pandas as pd

from bess_data import DEFAULT_DB, load_bess_dataframe, open_db


EXPORT_COLUMNS = [
    "unit_id",
    "plant_name",
    "storage_id",
    "operator_id",
    "operator_name",
    "bundesland",
    "district",
    "municipality",
    "postal_code",
    "city",
    "address",
    "latitude",
    "longitude",
    "operating_status",
    "battery_technology",
    "technology",
    "energy_source",
    "commissioning_date",
    "registration_date",
    "net_power_mw",
    "gross_power_mw",
    "usable_capacity_mwh",
]


def normalize_records(df: pd.DataFrame) -> pd.DataFrame:
    trimmed = df[EXPORT_COLUMNS].copy()
    for column in ["commissioning_date", "registration_date"]:
        trimmed[column] = trimmed[column].astype("string")
    trimmed = trimmed.sort_values(
        ["net_power_mw", "usable_capacity_mwh", "plant_name"],
        ascending=[False, False, True],
        na_position="last",
    )
    return trimmed


def build_geojson(df: pd.DataFrame) -> dict:
    mapped = df.dropna(subset=["latitude", "longitude"]).copy()
    features = []
    for record in mapped.to_dict(orient="records"):
        lon = float(record.pop("longitude"))
        lat = float(record.pop("latitude"))
        features.append(
            {
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [lon, lat]},
                "properties": record,
            }
        )
    return {"type": "FeatureCollection", "features": features}


def write_summary(df: pd.DataFrame, out_path: Path):
    mapped = df.dropna(subset=["latitude", "longitude"])
    summary = {
        "plants": int(len(df)),
        "mapped_plants": int(len(mapped)),
        "total_net_power_mw": round(float(df["net_power_mw"].fillna(0).sum()), 3),
        "total_usable_capacity_mwh": round(float(df["usable_capacity_mwh"].fillna(0).sum()), 3),
        "bundeslaender": sorted(value for value in df["bundesland"].dropna().unique() if value),
        "operating_statuses": sorted(
            value for value in df["operating_status"].dropna().unique() if value
        ),
    }
    out_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def main():
    parser = argparse.ArgumentParser(description="Export compact MaStR BESS artifacts")
    parser.add_argument("--db", type=Path, default=DEFAULT_DB, help="DuckDB file path")
    parser.add_argument("--out-dir", type=Path, default=Path("dist"), help="Output directory")
    parser.add_argument(
        "--format",
        nargs="+",
        choices=["parquet", "geojson", "json"],
        default=["parquet", "geojson", "json"],
        help="Export one or more artifact formats",
    )
    args = parser.parse_args()

    conn = open_db(args.db, read_only=True)
    try:
        df = load_bess_dataframe(conn)
    finally:
        conn.close()

    if df.empty:
        raise SystemExit("No BESS records found in the database.")

    out_dir = args.out_dir.expanduser()
    out_dir.mkdir(parents=True, exist_ok=True)
    export_df = normalize_records(df)

    if "parquet" in args.format:
        parquet_path = out_dir / "bess.parquet"
        export_df.to_parquet(parquet_path, index=False)
        print(f"Wrote {parquet_path}")

    if "json" in args.format:
        json_path = out_dir / "bess.json"
        records = export_df.to_dict(orient="records")
        json_path.write_text(json.dumps(records, ensure_ascii=False), encoding="utf-8")
        print(f"Wrote {json_path}")

    if "geojson" in args.format:
        geojson_path = out_dir / "bess.geojson"
        geojson = build_geojson(export_df)
        geojson_path.write_text(json.dumps(geojson, ensure_ascii=False), encoding="utf-8")
        print(f"Wrote {geojson_path}")

    summary_path = out_dir / "summary.json"
    write_summary(export_df, summary_path)
    print(f"Wrote {summary_path}")


if __name__ == "__main__":
    main()
