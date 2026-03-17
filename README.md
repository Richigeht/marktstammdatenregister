# Marktstammdatenregister → DuckDB

Imports the German energy asset registry ([MaStR](https://www.marktstammdatenregister.de)) bulk export into a local [DuckDB](https://duckdb.org) database for fast SQL analysis.

## What's in the database

| Table | Description |
|---|---|
| `EinheitenStromSpeicher` | Battery storage units (BESS) — location, power, technology, status |
| `AnlagenEegSpeicher` | EEG subsidy data for storage systems |
| `AnlagenStromSpeicher` | Storage system registrations (capacity, linked units) |
| `EinheitenAenderungNetzbetreiberzuordnungen` | Changes to grid operator assignments |
| `Netzanschlusspunkte` | Grid connection points |
| `Netze` | Grid networks |
| `Marktakteure` | Market participants (operators, grid operators, etc.) |
| `Katalogkategorien` | Lookup: enum category names |
| `Katalogwerte` | Lookup: enum code → human-readable label |
| `_import_progress` | Internal: tracks which files have been imported |

Enum fields (e.g. `Bundesland`, `Batterietechnologie`) are stored as integer codes.
Join to `Katalogwerte` to resolve them to text — see [Resolving enum codes](#resolving-enum-codes) below.

## Requirements

- [uv](https://docs.astral.sh/uv/) — `brew install uv`
- [DuckDB CLI](https://duckdb.org/docs/installation) — `brew install duckdb`

## Setup

```bash
git clone <this-repo>
cd marktstammdatenregister

# Download the bulk export from:
# https://www.marktstammdatenregister.de/MaStR/Datendownload
# Unzip into the project directory — folder name: Gesamtdatenexport_YYYYMMDD_XX.X/

uv sync
```

## Import data

```bash
uv run python3 etl.py
```

This reads all XSD schemas, creates the DuckDB tables, and streams the XML files into `mastr.duckdb`.
Large tables (e.g. `EinheitenStromSpeicher`) take a few minutes.

**Resume after interruption** — already-completed files are tracked in `_import_progress` and skipped automatically:
```bash
uv run python3 etl.py   # safe to re-run
```

**Import specific tables only:**
```bash
uv run python3 etl.py --tables EinheitenStromSpeicher AnlagenEegSpeicher
```

**Re-import from scratch (drop + recreate):**
```bash
uv run python3 etl.py --drop
```

**Check import progress:**
```sql
SELECT table_name, COUNT(*) AS files, SUM(rows) AS total_rows
FROM _import_progress
GROUP BY table_name
ORDER BY table_name;
```

## Querying the database

### Open the UI

```bash
duckdb -ui mastr.duckdb      # browser-based UI (recommended)
duckdb mastr.duckdb          # CLI
```

Or use the **SQLTools + DuckDB** VS Code extension to query from the editor.

### Resolving enum codes

Many fields store integer codes. Resolve them like this:

```sql
SELECT kw.Id, kw.Wert
FROM Katalogwerte kw
JOIN Katalogkategorien kk ON kw.KatalogKategorieId = kk.Id
WHERE kk.Name = 'Batterietechnologie';
```

Use in a query:
```sql
SELECT
    e.EinheitMastrNummer,
    e.NameStromerzeugungseinheit,
    e.Nettonennleistung,
    bt.Wert AS Batterietechnologie,
    bl.Wert AS Bundesland
FROM EinheitenStromSpeicher e
LEFT JOIN Katalogwerte bt ON e.Batterietechnologie = bt.Id
LEFT JOIN Katalogwerte bl ON e.Bundesland          = bl.Id
LIMIT 20;
```

### Example queries

**Find a plant by name:**
```sql
SELECT EinheitMastrNummer, NameStromerzeugungseinheit, Ort, Nettonennleistung
FROM EinheitenStromSpeicher
WHERE NameStromerzeugungseinheit ILIKE '%smareg4%';
```

**Large commercial BESS (> 1 MW):**
```sql
SELECT
    e.EinheitMastrNummer,
    e.NameStromerzeugungseinheit,
    e.Ort,
    e.Nettonennleistung / 1000.0 AS MW,
    bt.Wert AS Batterietechnologie
FROM EinheitenStromSpeicher e
LEFT JOIN Katalogwerte bt ON e.Batterietechnologie = bt.Id
WHERE e.Nettonennleistung > 1000
ORDER BY e.Nettonennleistung DESC;
```

**Storage capacity by federal state:**
```sql
SELECT
    bl.Wert AS Bundesland,
    COUNT(*)                           AS Anlagen,
    ROUND(SUM(Nettonennleistung) / 1e6, 2) AS GW_Leistung
FROM EinheitenStromSpeicher e
LEFT JOIN Katalogwerte bl ON e.Bundesland = bl.Id
WHERE e.EinheitBetriebsstatus = 35   -- in Betrieb
GROUP BY bl.Wert
ORDER BY GW_Leistung DESC;
```

**Find who operates a plant (Netzbetreiber):**
```sql
SELECT
    e.EinheitMastrNummer,
    e.NameStromerzeugungseinheit,
    m.Firmenname AS Betreiber
FROM EinheitenStromSpeicher e
LEFT JOIN Marktakteure m ON e.AnlagenbetreiberMastrNummer = m.MastrNummer
WHERE e.NameStromerzeugungseinheit ILIKE '%smareg4%';
```

**Grid connection point for a unit:**
```sql
SELECT
    e.EinheitMastrNummer,
    e.NameStromerzeugungseinheit,
    n.NetzanschlusspunktBezeichnung,
    n.Spannungsebene,
    n.MaximaleEinspeiseleistung
FROM EinheitenStromSpeicher e
LEFT JOIN Lokationen l   ON e.LokationMaStRNummer = l.LokationMaStRNummer   -- if Lokationen imported
LEFT JOIN Netzanschlusspunkte n ON n.LokationMaStRNummer = e.LokationMaStRNummer
WHERE e.NameStromerzeugungseinheit ILIKE '%smareg4%';
```

**EEG subsidy data for a storage unit:**
```sql
SELECT
    e.EinheitMastrNummer,
    e.NameStromerzeugungseinheit,
    eeg.EegMaStRNummer,
    eeg.EegInbetriebnahmedatum,
    eeg.AusschreibungZuschlag,
    stor.NutzbareSpeicherkapazitaet
FROM EinheitenStromSpeicher e
LEFT JOIN AnlagenEegSpeicher  eeg  ON e.EegMaStRNummer  = eeg.EegMaStRNummer
LEFT JOIN AnlagenStromSpeicher stor ON e.SpeMastrNummer  = stor.MaStRNummer
WHERE e.NameStromerzeugungseinheit ILIKE '%smareg4%';
```

## Data source

- **Marktstammdatenregister (MaStR)** — Bundesnetzagentur
- Bulk export: https://www.marktstammdatenregister.de/MaStR/Datendownload
- Data definitions: https://www.marktstammdatenregister.de/MaStRHilfe/subpages/GrundlagenDatendefinition.html
- Export format version: 25.2
