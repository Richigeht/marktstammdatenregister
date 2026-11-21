#!/usr/bin/env python3
"""Build Astro into an isolated staging directory, preserving exported dist/data."""

import argparse
import os
import shutil
import subprocess
import tempfile
from pathlib import Path

from .paths import DIST_DIR, PROJECT_ROOT, SITE_DIR


def main():
    parser = argparse.ArgumentParser(description="Build the Astro BESS site into dist/")
    parser.add_argument("--source-dir", type=Path, default=SITE_DIR, help="Astro site source directory")
    parser.add_argument("--publish-dir", type=Path, default=DIST_DIR, help="Publish directory")
    args = parser.parse_args()
    source_dir = args.source_dir.expanduser().resolve()
    publish_dir = args.publish_dir.expanduser().resolve()
    if not (source_dir / "astro.config.mjs").is_file():
        raise SystemExit(f"Missing Astro site configuration: {source_dir}")
    if not (PROJECT_ROOT / "node_modules" / "astro").is_dir():
        raise SystemExit("Astro dependencies are missing. Run npm ci with Node 24 first.")
    # Astro cleans its output directory. Never point it at existing exported data.
    with tempfile.TemporaryDirectory(prefix=".astro-stage-", dir=PROJECT_ROOT) as staging:
        command = ["npm", "run", "build", "--", "--outDir", staging]
        if source_dir != SITE_DIR.resolve():
            command += ["--root", str(source_dir)]
        subprocess.run(
            command,
            cwd=PROJECT_ROOT,
            env={**os.environ, "ASTRO_TELEMETRY_DISABLED": "1"},
            check=True,
        )
        if not (Path(staging) / "index.html").is_file():
            raise SystemExit("Astro did not produce index.html; publish directory left unchanged.")
        shutil.copytree(staging, publish_dir, dirs_exist_ok=True)
    (publish_dir / ".nojekyll").write_text("", encoding="utf-8")
    print(f"Built Astro site into {publish_dir}; existing data preserved")
