#!/usr/bin/env python3
"""
Export BESS datasets from MaStR DuckDB.
"""

import argparse
import json
from pathlib import Path

import pandas as pd

from .data import DEFAULT_DB, load_bess_dataframe, open_db
from .paths import DIST_DATA_DIR


FULL_EXPORT_COLUMNS = [
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

WEB_EXPORT_COLUMNS = [
    "unit_id",
    "plant_name",
    "operator_name",
    "bundesland",
    "district",
    "city",
    "latitude",
    "longitude",
    "operating_status",
    "battery_technology",
    "commissioning_date",
    "net_power_mw",
    "usable_capacity_mwh",
]


def round_coordinates(df: pd.DataFrame, decimals: int) -> pd.DataFrame:
    rounded = df.copy()
    rounded["latitude"] = rounded["latitude"].map(
        lambda value: None if pd.isna(value) else float(f"{float(value):.{decimals}f}")
    )
    rounded["longitude"] = rounded["longitude"].map(
        lambda value: None if pd.isna(value) else float(f"{float(value):.{decimals}f}")
    )
    return rounded


def normalize_records(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    trimmed = df[columns].copy()
    for column in ["commissioning_date", "registration_date"]:
        if column in trimmed.columns:
            trimmed[column] = trimmed[column].astype("string")
    return trimmed.sort_values(
        ["net_power_mw", "usable_capacity_mwh", "plant_name"],
        ascending=[False, False, True],
        na_position="last",
    )


def build_geojson(df: pd.DataFrame) -> dict:
    df = df.dropna(subset=["latitude", "longitude"]).copy()
    features = []
    for record in df.to_dict(orient="records"):
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


def write_summary(all_df: pd.DataFrame, public_df: pd.DataFrame, out_path: Path, coordinate_precision: int):
    summary = {
        "plants": int(len(all_df)),
        "mapped_plants": int(len(public_df)),
        "public_plants": int(len(public_df)),
        "total_net_power_mw": round(float(all_df["net_power_mw"].fillna(0).sum()), 3),
        "total_usable_capacity_mwh": round(float(all_df["usable_capacity_mwh"].fillna(0).sum()), 3),
        "public_net_power_mw": round(float(public_df["net_power_mw"].fillna(0).sum()), 3),
        "public_usable_capacity_mwh": round(float(public_df["usable_capacity_mwh"].fillna(0).sum()), 3),
        "coordinate_precision_decimals": coordinate_precision,
        "bundeslaender": sorted(value for value in all_df["bundesland"].dropna().unique() if value),
        "operating_statuses": sorted(
            value for value in all_df["operating_status"].dropna().unique() if value
        ),
    }
    out_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def main():
    parser = argparse.ArgumentParser(description="Export MaStR BESS artifacts")
    parser.add_argument("--db", type=Path, default=DEFAULT_DB, help="DuckDB file path")
    parser.add_argument("--out-dir", type=Path, default=DIST_DATA_DIR, help="Output directory")
    parser.add_argument(
        "--profile",
        choices=["public", "internal", "full"],
        default="public",
        help="Export profile: public-safe web payload, internal exact payload, or full dataset export",
    )
    parser.add_argument(
        "--format",
        nargs="+",
        choices=["parquet", "geojson", "json"],
        default=None,
        help="Export one or more artifact formats",
    )
    parser.add_argument(
        "--coordinate-decimals",
        type=int,
        default=2,
        help="Decimal precision for rounded public coordinates (default: 2)",
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
    full_df = normalize_records(df, FULL_EXPORT_COLUMNS)
    mapped_exact_df = normalize_records(
        df.dropna(subset=["latitude", "longitude"]).copy(),
        WEB_EXPORT_COLUMNS,
    )
    public_df = round_coordinates(mapped_exact_df, args.coordinate_decimals)

    if args.profile == "public":
        export_df = public_df
        formats = args.format or ["geojson"]
    elif args.profile == "internal":
        export_df = mapped_exact_df
        formats = args.format or ["geojson", "json"]
    else:
        export_df = full_df
        formats = args.format or ["parquet", "json", "geojson"]

    if "parquet" in formats:
        parquet_path = out_dir / "bess.parquet"
        export_df.to_parquet(parquet_path, index=False)
        print(f"Wrote {parquet_path}")

    if "json" in formats:
        json_path = out_dir / "bess.json"
        json_path.write_text(json.dumps(export_df.to_dict(orient="records"), ensure_ascii=False), encoding="utf-8")
        print(f"Wrote {json_path}")

    if "geojson" in formats:
        geojson_path = out_dir / "bess.geojson"
        geojson_path.write_text(json.dumps(build_geojson(export_df), ensure_ascii=False), encoding="utf-8")
        print(f"Wrote {geojson_path}")

    summary_path = out_dir / "summary.json"
    write_summary(full_df, public_df, summary_path, args.coordinate_decimals)
    print(f"Wrote {summary_path}")
