from pathlib import Path


PACKAGE_DIR = Path(__file__).resolve().parent
SRC_DIR = PACKAGE_DIR.parent
PROJECT_ROOT = SRC_DIR.parent
DEFAULT_DB = PROJECT_ROOT / "mastr.duckdb"
SITE_DIR = PROJECT_ROOT / "site"
DIST_DIR = PROJECT_ROOT / "dist"
DIST_DATA_DIR = DIST_DIR / "data"
XSD_DIR = PROJECT_ROOT / "Dokumentation MaStR Gesamtdatenexport" / "xsd"
XSD_ZIP = PROJECT_ROOT / "Dokumentation MaStR Gesamtdatenexport" / "xsd.zip"
