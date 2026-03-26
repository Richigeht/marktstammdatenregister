# Marktstammdatenregister → DuckDB

Imports the German energy asset registry ([MaStR](https://www.marktstammdatenregister.de)) bulk export into a local [DuckDB](https://duckdb.org) database for fast SQL analysis.

## Repository layout

- `src/marktstammdatenregister/` — Python package for ETL, export, and app logic
- `site/` — source files for the static browser
- `dist/` — generated static site output for GitHub Pages and the container image
- `dist/data/` — exported static data artifacts consumed by the browser

## What's in the database

| Table                                        | Description                                                        |
| -------------------------------------------- | ------------------------------------------------------------------ |
| `EinheitenStromSpeicher`                     | Battery storage units (BESS) — location, power, technology, status |
| `AnlagenEegSpeicher`                         | EEG subsidy data for storage systems                               |
| `AnlagenStromSpeicher`                       | Storage system registrations (capacity, linked units)              |
| `EinheitenAenderungNetzbetreiberzuordnungen` | Changes to grid operator assignments                               |
| `Netzanschlusspunkte`                        | Grid connection points                                             |
| `Netze`                                      | Grid networks                                                      |
| `Marktakteure`                               | Market participants (operators, grid operators, etc.)              |
| `Katalogkategorien`                          | Lookup: enum category names                                        |
| `Katalogwerte`                               | Lookup: enum code → human-readable label                           |
| `_import_progress`                           | Internal: tracks which files have been imported                    |

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
uv run etl
```

This reads all XSD schemas, creates the DuckDB tables, and streams the XML files into `mastr.duckdb`.
Large tables (e.g. `EinheitenStromSpeicher`) take a few minutes.
The importer reads schemas from either the extracted `xsd/` folder or the bundled `xsd.zip`.

**Resume after interruption** — already-completed files are tracked in `_import_progress` and skipped automatically:

```bash
uv run etl   # safe to re-run
```

**Import specific tables only:**

```bash
uv run etl --tables EinheitenStromSpeicher AnlagenEegSpeicher
```

**Re-import from scratch (drop + recreate):**

```bash
uv run etl --drop
```

**Check import progress:**

```sql
SELECT table_name, COUNT(*) AS files, SUM(rows) AS total_rows
FROM _import_progress
GROUP BY table_name
ORDER BY table_name;
```

## Querying the database

### Browse BESS plants in Streamlit

```bash
uv run streamlit run src/marktstammdatenregister/streamlit_app.py
```

The app opens the generated `mastr.duckdb`, lets you filter storage plants by state, status, battery technology, power, and capacity, and shows matching assets on a map using the MaStR latitude/longitude fields.

Features:

- map view of BESS plants with size-scaled markers
- browsable table with export to CSV
- detail view for a selected plant with operator, status, power, capacity, and address fields
- works against any MaStR DuckDB path you provide in the sidebar

If your DuckDB is stored elsewhere, change the path in the sidebar after the app starts.

### Export compact static data

```bash
uv run export-bess
```

By default this now writes a public-safe payload directly into `dist/data/`:

- `dist/data/bess.geojson` with mapped plants only and rounded coordinates
- `dist/data/summary.json` with total and public summary counts

That default is intended for GitHub Pages, static hosting, and the container.

You can override input and output paths:

```bash
uv run export-bess --db /path/to/mastr.duckdb --out-dir public/data
```

If you want exact mapped coordinates for internal use:

```bash
uv run export-bess --profile internal
```

This writes `bess.geojson`, `bess.json`, and `summary.json` with exact mapped coordinates.

If you explicitly want the heavy full export again:

```bash
uv run export-bess --profile full --format parquet json geojson
```

For a static website, run ETL and export in a build step somewhere else, then publish only `dist/` including `dist/data/`.

### Static site for GitHub Pages

The repo includes a static frontend source in [site/index.html](site/index.html). The generated publish output lives in `dist/` and reads exported data from `dist/data/`.

Build the static payload like this:

```bash
uv run etl
uv run export-bess --out-dir dist/data
uv run build-static-site
```

Then preview locally:

```bash
python3 -m http.server 8000 -d dist
```

Open `http://localhost:8000`.

To host on GitHub Pages:

- commit `site/` and the workflow files
- enable GitHub Pages in repo settings
- [pages.yml](.github/workflows/pages.yml) rebuilds the static shell from `site/` on pushes to `release/main` and uploads it as a validation artifact
- [refresh-pages-data.yml](.github/workflows/refresh-pages-data.yml) is the workflow that deploys the live Pages site: it checks out `release/main`, downloads the latest MaStR ZIP, runs ETL/export/site build, and publishes the complete `dist/` artifact including `dist/data/`
- [smoke-test-pages.yml](.github/workflows/smoke-test-pages.yml) is a fast manual smoke test that skips the large download, generates a tiny fixture dataset, runs the ETL/export/site build, and uploads a build artifact without deploying Pages

This route does not need Streamlit, DuckDB, or Python on the host. The data is precomputed during export, and the browser filters the GeoJSON client-side.
The public static site uses approximate coordinates by default to avoid republishing exact points in a clean bulk form.
The generated `summary.json` also carries metadata for the source export date and the UTC build timestamp so the site can show when the underlying MaStR dump was published and when the static site was rebuilt.

Notes:

- GitHub Actions artifacts are separate from git history; they are not stored "inside" the repository tree.
- `dist/` is generated output and should not be committed.

### Run the static browser in a container

Once `dist/` and `dist/data/` are populated, build and run the container:

```bash
docker build -t mastr-bess-static .
docker run --rm -p 8080:80 mastr-bess-static
```

Then open `http://localhost:8080`.

The container serves the static `dist/` output with Nginx. It does not run ETL or DuckDB inside the container. The intended flow is:

1. run `uv run etl`
2. run `uv run export-bess --out-dir dist/data`
3. run `uv run build-static-site`
4. build the container or deploy `dist/` to GitHub Pages

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
