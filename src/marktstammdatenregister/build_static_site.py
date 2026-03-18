#!/usr/bin/env python3
"""
Copy the static site source into the Pages/container publish directory.
"""

import argparse
import shutil
from pathlib import Path

from .paths import DOCS_DIR, SITE_DIR


ASSET_FILES = ["index.html", "app.js", "styles.css"]


def main():
    parser = argparse.ArgumentParser(description="Build the static BESS site into docs/")
    parser.add_argument("--source-dir", type=Path, default=SITE_DIR, help="Static site source directory")
    parser.add_argument("--publish-dir", type=Path, default=DOCS_DIR, help="Publish directory")
    args = parser.parse_args()

    source_dir = args.source_dir.expanduser()
    publish_dir = args.publish_dir.expanduser()
    publish_dir.mkdir(parents=True, exist_ok=True)

    for name in ASSET_FILES:
        src = source_dir / name
        dst = publish_dir / name
        if not src.exists():
            raise SystemExit(f"Missing site asset: {src}")
        shutil.copy2(src, dst)
        print(f"Copied {src} -> {dst}")

    nojekyll = publish_dir / ".nojekyll"
    nojekyll.write_text("", encoding="utf-8")
    print(f"Wrote {nojekyll}")

