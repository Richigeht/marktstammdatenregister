---
name: BESS Atlas
description: A measured workspace for German battery storage registrations.
colors:
  canvas: "#f3f5f1"
  surface: "#fff"
  ink: "#18352c"
  muted: "#53645c"
  line: "#d7dfd6"
  green: "#17694f"
  lime: "#c4f16b"
  selected: "#edf6df"
  focus: "#357b58"
  error: "#9e302c"
  error-surface: "#fff1ed"
  error-line: "#e6c2bd"
  notice: "#edf2e7"
  status-surface: "#edf3e8"
  status-text: "#285d43"
  map-surface: "#e4eae1"
  selected-marker: "#93b83d"
  chart-olive: "#8fae43"
  chart-amber: "#d4a34e"
  chart-blue: "#698cb2"
  chart-rose: "#ac7284"
  chart-grey: "#6c7972"
  boundary-stroke: "#91a78d"
  boundary-fill: "#cddcc3"
  nav-text: "#d7e2d9"
  placeholder: "#64736a"
  hover-line: "#a4b994"
  table-head: "#f7f9f5"
  table-line: "#e8ede5"
  table-hover: "#f6f9f2"
  empty-icon: "#718a6f"
  scroll-thumb: "#a6b8a8"
  scroll-track: "#eef2ec"
typography:
  display:
    fontFamily: "Manrope Variable, sans-serif"
    fontSize: "clamp(26px, 2.5vw, 38px)"
    fontWeight: 600
    lineHeight: 1.2
    letterSpacing: "-0.035em"
  headline:
    fontFamily: "Manrope Variable, sans-serif"
    fontSize: "18px"
    fontWeight: 650
    lineHeight: 1.3
    letterSpacing: "-0.025em"
  body:
    fontFamily: "Manrope Variable, sans-serif"
    fontSize: "13px"
    lineHeight: 1.65
  label:
    fontFamily: "Manrope Variable, sans-serif"
    fontSize: "11px"
    fontWeight: 600
rounded:
  tag: "4px"
  control: "6px"
  notice: "8px"
  panel: "12px"
spacing:
  step-4: "4px"
  step-8: "8px"
  step-12: "12px"
  step-14: "14px"
  step-16: "16px"
  step-18: "18px"
  step-20: "20px"
  step-22: "22px"
  step-24: "24px"
  step-28: "28px"
  step-32: "32px"
  step-36: "36px"
  step-48: "48px"
components:
  button-primary:
    backgroundColor: "{colors.ink}"
    textColor: "{colors.surface}"
    rounded: "{rounded.control}"
    padding: "0 16px"
  button-primary-hover:
    backgroundColor: "{colors.green}"
  panel:
    backgroundColor: "{colors.surface}"
    rounded: "{rounded.panel}"
  status-tag:
    backgroundColor: "{colors.status-surface}"
    textColor: "{colors.status-text}"
    rounded: "{rounded.tag}"
    padding: "4px 7px"
---
# Design System: BESS Atlas

## Overview

**Creative North Star: “The Energy Research Workspace”** — a descriptive name for the implemented, code-led direction, not an approved visual comp. The user approved Astro + ECharts with sensible defaults on 2026-11-21. This refresh records the finished implementation rather than the earlier provisional sizes.

A light mineral canvas and forest ink support dense, measured comparisons. Lime provides small signals of identity and map clustering; green carries links and data. Separation comes from spacing, white panels and quiet rules. The workspace leads with shared filters and a working map, rather than a marketing hero.

**Key Characteristics:** compact navigation; visible data provenance; aligned measurements; restrained surfaces; coordinated map, charts and registry.

## Colors

The normative palette is in the frontmatter, extracted from `site/src/styles/atlas.css` and `site/src/scripts/app.js`.

### Primary

- **Forest green** (`green`) carries links, regional bars, ordinary markers and button hover. **Deep forest** (`ink`) carries body ink, the header and export button.
- **Electric lime** (`lime`) appears in the battery mark, country dot, cluster outlines and text selection. Selected individual markers use `selected-marker`, not the brighter cluster outline.

### Secondary

- The six-color status chart sequence is `green`, `chart-olive`, `chart-amber`, `chart-blue`, `chart-rose`, `chart-grey`. It follows returned category order; it is not a permanent status-to-color semantic mapping. Always accompany it with status names and counts.

### Neutral

- `canvas`, `surface`, `muted` and `line` establish page, panels, secondary text and divisions. `selected` marks the selected registry row; `table-hover` marks row hover.
- Error notices use `error`, `error-surface` and `error-line`. The contextual Germany fallback uses `boundary-stroke` and `boundary-fill` at 0.65 fill opacity over `map-surface`.

## Typography

Self-hosted **Manrope Variable** is imported through `@fontsource-variable/manrope` in the Astro route; the fallback is `sans-serif`. The same family is specified for Leaflet and ECharts. Font synthesis is disabled.

- **Display:** route h1 uses the frontmatter clamp, weight 600, 1.2 line height and −0.035em tracking. Intro copy is 14px, falling to 12px on narrow screens; its desktop measure is 65ch.
- **Headline:** h2 is 18px/1.3, weight 650, −0.025em tracking. Standard h3 is 16px/1.4 at 650.
- **Body:** paragraphs are 13px/1.65; contextual copy is commonly 10–12px. This is a dense explorer, not a 16px body-text system.
- **Measurements:** metric values start at 30px/1.3, weight 600 and −0.035em tracking. Tabular numerals align metrics, numeric table cells, pagination, detail values and cluster counts.
- **Labels:** filter labels are 11px/600; desktop fields are 12px. At 600px and below fields become 16px while labels become 10px.

## Layout

The maximum outer layout width is 1544px, including 32px side padding; initial top padding is 36px. The dark header is 80px tall. The composition is identity/navigation, title/provenance, shared filter band, four-column divided metric strip, map/insights, registry/details and source limitations.

Desktop overview and registry grids share `minmax(0, 2.3fr) minmax(320px, 1fr)` with a 22px gap. Filters use `1.7fr 1.1fr 1fr 1fr 0.75fr 0.75fr auto`, 14px gaps and 20px padding. Panel headings use 22px 24px 18px padding. Table scrolling is contained in a focusable region with a 650px maximum height and sticky header; rows paginate in groups of 25.

- **At ≥1600px:** map height becomes 600px (default 570px).
- **At ≤1200px:** outer padding becomes 24px, top padding 28px; filters become six equal columns with selected fields spanning two. Main grids become `minmax(0, 1.8fr) minmax(300px, 1fr)`; panel headings use 18px padding.
- **At ≤900px:** overview and registry become single-column. Insights temporarily arrange regional/status charts side by side. Map height is 480px; detail description columns become 1fr/2fr.
- **At ≤600px:** header wraps navigation onto its own row; layout padding is 24px 16px 0. Provenance moves under the introduction. Filters use two columns with full-width search; metrics become 2×2. Insights stack, map height becomes 400px, export moves below the registry heading, and footer becomes one column. The table retains internal horizontal scrolling.

## Elevation & Depth

Panels are flat, separated by 1px borders and tonal surfaces, with no ornamental panel shadows. The custom cluster marker has the only application-authored shadow: `0 3px 8px #18352c24`. Leaflet retains its bundled control and popup styling. The table header is sticky at z-index 1; the contextual map alert overlays the map at z-index 500 and ignores pointer input.

## Shapes

Panels and the filter band use 12px corners; notices use 8px; controls and map alerts use 6px; status tags use 4px. Clusters are circular 44px markers with a 3px lime border. Ordinary individual map points have radius 6px and a 1px white stroke; selection enlarges them to radius 10px with a 3px white stroke. Regional bars are 12px thick with 3px rounded right ends.

## Components

### Buttons, fields and navigation

The primary export button uses forest ink, white type, 0 16px padding and a 44px minimum height, switching to green on hover. The outlined subtle button uses a white surface, line border, 0 12px padding and a 40px minimum height; hover uses pale selection green and `hover-line`. The transparent text button is green, 44px high, with a pale hover background. Disabled buttons use opacity 0.45 and a default cursor. Mobile export is 40px high; compact pagination, panel actions and table actions use their implemented smaller heights rather than a universal 44px guarantee.

Fields are native, visibly labelled, 44px high, with a line border, 6px corners and 0 10px padding. Placeholder ink is explicit. Header links are muted light green, turning lime on hover; there is no route-active state in this single-page navigation.

### Focus and accessible alternatives

Buttons, links, inputs, selects, disclosure summaries and the table region receive a 3px `focus` outline with 3px offset on `:focus-visible`. A focus-revealed skip link reaches the main explorer. Sort headers expose `aria-sort`; plant buttons expose `aria-pressed`. Notices, metric changes and result counts use polite live announcements. Map cluster markers are keyboard enabled; individual canvas points also have a selectable plant-name equivalent in the table.

### Charts and map

ECharts uses the SVG renderer with ARIA enabled and confined rich-text tooltips. Regional bars show the top eight states, with value labels, and click to apply the shared state filter. A disclosure lists all state counts as text. The status donut uses 54%/83% radii, white 3px separators, a centered total, and a persistent named count legend; hover scaling is disabled.

Leaflet uses OpenStreetMap tiles with visible attribution; scroll-wheel zoom starts disabled. Supercluster groups points. Map and registry selection share the selected unit; a popup offers a plant-details action. If tiles fail, a noninteractive bundled Germany geometry appears with a visible warning and Natural Earth attribution. Its source is the DEU feature from Natural Earth's public-domain 1:110m countries dataset, extracted without geometry simplification: https://github.com/nvkelso/natural-earth-vector/blob/master/geojson/ne_110m_admin_0_countries.geojson. This is geographic context, not a navigation boundary.

### Loading, error, empty and motion states

Loading placeholders use em dashes and plain loading copy. Data errors show a tinted notice and retry action. Zero matches explain how to recover; empty selection invites a map-point or plant-name selection. Selected rows use pale green; row hover uses a separate quieter tint. Missing measurements remain “—” and totals exclude unknown values.

Charts animate for 350ms unless reduced motion was requested at initialization. Map fitting, zooming and selection scrolling also respect that preference. CSS switches smooth anchor scrolling to automatic and removes animations/transitions under `prefers-reduced-motion: reduce`; no decorative transitions are otherwise prescribed.

## Do's and Don'ts

- **Do** preserve visible labels, tabular measurements, keyboard focus and chart text alternatives.
- **Do** keep shared filters consistent across map, totals, charts, table and export.
- **Do** distinguish public mapped units from the whole registry, units from physical sites, and approximate locations from precise coordinates.
- **Do** use flat bordered panels and the implemented density; extend existing tokens before inventing new surface treatments.
- **Don't** replace unknown measurements with zero or imply that registry status guarantees actual operation.
- **Don't** rely on status color or map interaction alone to communicate data.
- **Don't** treat the code-led direction as a separately approved visual comp or introduce ornamental panel shadows.
