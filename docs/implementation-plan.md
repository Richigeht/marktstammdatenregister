# Astro + ECharts rework

Approved direction: Astro + ECharts with sensible defaults; retain DuckDB export and map. Implement on an isolated feature branch; deliver a reviewable PR before production integration.

1. Build/test pure dataset parsing, shared filters, aggregates, sorting, pagination and safe CSV.
2. Replace legacy frontend with an Astro static route, bundled Leaflet/Supercluster map, selective ECharts imports, responsive styling and error/retry states.
3. Keep build-static-site as the compatible entry point. Build Astro in a temporary directory and copy the shell into dist without removing exported data. Update all CI build paths with pinned Node/npm dependencies.
4. Validate Python serialization tests, Node tests, full XML ETL/export/build fixture, browser interactions, mobile layout, malformed/missing data, and a production-data sample where accessible.
5. Review desktop/mobile captures, fix material issues, document deployment and package provenance, and publish the PR.

Package research: Firecrawl located Astro's official GitHub Pages guide and ECharts download documentation. npm registry resolves exact current releases into package-lock.json. Use Node 24. Keep Python ETL separate because Astro is a frontend/static build framework, not a replacement for DuckDB ingestion.

Boundary source: Germany feature (`ADM0_A3=DEU`) extracted without simplifying geometry from https://github.com/nvkelso/natural-earth-vector/blob/master/geojson/ne_110m_admin_0_countries.geojson (Natural Earth 1:110m, public domain). Only geometry and source/name metadata are bundled; it is a contextual fallback, not a navigation boundary.

## Verification and review (2026-11-21)

- 5 Node data tests; 3 Python tests including a real Astro build preserving existing export files; 6 Playwright interaction cases across desktop/mobile passed.
- Real published 8,108-feature payload reviewed locally after converting legacy non-standard missing constants to null (same boundary behavior as merged export fix). No runtime exceptions or page-level horizontal overflow at 1440px and 390px. External tiles fail TLS/policy in this execution environment; captures show the functioning local boundary fallback.
- Independent finish reviewer: **ship**, no material blockers. Nonblocking follow-up: mobile table selection updates the details section below the table rather than moving focus automatically.
- Build emits a size advisory for the selected ECharts bundle (~530 KB minified, ~180 KB gzip); map library is split separately. No build errors.
- `npm audit --omit=dev`: zero known vulnerabilities in the production dependency tree at verification time.
- Fixed `.dockerignore` excluding `dist/`, which previously prevented Docker from copying the built app. Container build succeeded. Runtime test exposed restrictive source-directory permissions (HTTP 403); the image now grants nginx workers read/traverse access to the copied static files.
- Production data refresh started from main (run 44928235554) because prior deployed data was built from pre-fix commit ad97c8c. New Astro UI is a separate branch pending integration.
