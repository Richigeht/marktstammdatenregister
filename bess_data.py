from pathlib import Path

import duckdb
import pandas as pd


BASE_DIR = Path(__file__).parent
DEFAULT_DB = BASE_DIR / "mastr.duckdb"


BESS_QUERY = """
    WITH storage AS (
        SELECT
            e.EinheitMastrNummer AS unit_id,
            e.NameStromerzeugungseinheit AS plant_name,
            e.SpeMastrNummer AS storage_id,
            e.AnlagenbetreiberMastrNummer AS operator_id,
            m.Firmenname AS operator_name,
            e.Ort AS city,
            e.Postleitzahl AS postal_code,
            e.Gemeinde AS municipality,
            e.Landkreis AS district,
            e.Strasse AS street,
            e.Hausnummer AS house_number,
            e.Breitengrad AS latitude,
            e.Laengengrad AS longitude,
            e.Inbetriebnahmedatum AS commissioning_date,
            e.Registrierungsdatum AS registration_date,
            e.Nettonennleistung AS net_power_kw,
            e.Bruttoleistung AS gross_power_kw,
            stor.NutzbareSpeicherkapazitaet AS usable_capacity_kwh,
            COALESCE(bl.Wert, CAST(e.Bundesland AS VARCHAR)) AS bundesland,
            COALESCE(bt.Wert, CAST(e.Batterietechnologie AS VARCHAR)) AS battery_technology,
            COALESCE(bs.Wert, CAST(e.EinheitBetriebsstatus AS VARCHAR)) AS operating_status,
            COALESCE(tech.Wert, CAST(e.Technologie AS VARCHAR)) AS technology,
            COALESCE(energy.Wert, CAST(e.Energietraeger AS VARCHAR)) AS energy_source
        FROM EinheitenStromSpeicher e
        LEFT JOIN AnlagenStromSpeicher stor
            ON e.SpeMastrNummer = stor.MaStRNummer
        LEFT JOIN Marktakteure m
            ON e.AnlagenbetreiberMastrNummer = m.MastrNummer
        LEFT JOIN Katalogwerte bl
            ON e.Bundesland = bl.Id
        LEFT JOIN Katalogwerte bt
            ON e.Batterietechnologie = bt.Id
        LEFT JOIN Katalogwerte bs
            ON e.EinheitBetriebsstatus = bs.Id
        LEFT JOIN Katalogwerte tech
            ON e.Technologie = tech.Id
        LEFT JOIN Katalogwerte energy
            ON e.Energietraeger = energy.Id
    )
    SELECT
        *,
        net_power_kw / 1000.0 AS net_power_mw,
        gross_power_kw / 1000.0 AS gross_power_mw,
        usable_capacity_kwh / 1000.0 AS usable_capacity_mwh
    FROM storage
"""


def table_exists(conn: duckdb.DuckDBPyConnection, table_name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM information_schema.tables WHERE table_name = ? LIMIT 1",
        [table_name],
    ).fetchone()
    return row is not None


def open_db(db_path: str | Path, read_only: bool = True) -> duckdb.DuckDBPyConnection:
    return duckdb.connect(str(Path(db_path).expanduser()), read_only=read_only)


def load_bess_dataframe(conn: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    required_tables = [
        "EinheitenStromSpeicher",
        "AnlagenStromSpeicher",
        "Marktakteure",
        "Katalogwerte",
    ]
    missing = [table for table in required_tables if not table_exists(conn, table)]
    if missing:
        raise RuntimeError("Missing required tables: " + ", ".join(missing))

    df = conn.execute(BESS_QUERY).df()
    if df.empty:
        return df

    for column in ["latitude", "longitude", "net_power_mw", "usable_capacity_mwh", "gross_power_mw"]:
        df[column] = pd.to_numeric(df[column], errors="coerce")

    df["operator_name"] = df["operator_name"].fillna("Unknown")
    df["plant_name"] = df["plant_name"].fillna("Unnamed plant")
    df["address"] = (
        df["street"].fillna("")
        + " "
        + df["house_number"].fillna("")
    ).str.strip()
    return df
