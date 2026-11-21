# BESS Atlas

A public explorer of German battery storage registrations from the Bundesnetzagentur's Marktstammdatenregister. Visitors compare power, capacity, operating status, technology, geography, and operators, then inspect or export matching plants.

The Python ETL imports the source bulk XML into DuckDB. Public web exports contain only mapped records with rounded, approximate coordinates. An asset count means a registry unit, not necessarily a unique physical site. Missing values are unknown, never confirmed zero. The whole registry and the mapped public subset are different scopes.

Hosting is static GitHub Pages at bess-atlas.yeah-cool.io, with daily data refresh and optional container hosting. Keep the Python analysis/Streamlit paths usable. User approved Astro + ECharts with sensible implementation/design defaults on 2026-11-21.
