#!/usr/bin/env python3
from pathlib import Path

import pandas as pd
import pydeck as pdk
import streamlit as st

from .data import DEFAULT_DB, load_bess_dataframe, open_db


st.set_page_config(
    page_title="MaStR BESS Browser",
    page_icon="🔋",
    layout="wide",
)


@st.cache_resource(show_spinner=False)
def connect_db(db_path: str):
    return open_db(db_path, read_only=True)


@st.cache_data(show_spinner=False)
def load_storage_dataset(db_path: str) -> pd.DataFrame:
    conn = connect_db(db_path)
    return load_bess_dataframe(conn)


def format_metric(value: float | int | None, suffix: str) -> str:
    if value is None or pd.isna(value):
        return "n/a"
    return f"{value:,.1f} {suffix}"


def display_value(value) -> str:
    if value is None or pd.isna(value) or value == "":
        return "n/a"
    return str(value)


def filter_dataset(df: pd.DataFrame) -> pd.DataFrame:
    st.sidebar.header("Filters")
    search = st.sidebar.text_input("Search plant or operator")

    state_options = sorted(value for value in df["bundesland"].dropna().unique() if value)
    tech_options = sorted(value for value in df["battery_technology"].dropna().unique() if value)
    status_options = sorted(value for value in df["operating_status"].dropna().unique() if value)

    selected_states = st.sidebar.multiselect("Bundesland", state_options)
    selected_tech = st.sidebar.multiselect("Battery technology", tech_options)
    selected_status = st.sidebar.multiselect("Operating status", status_options)

    power_min, power_max = df["net_power_mw"].fillna(0).min(), df["net_power_mw"].fillna(0).max()
    capacity_min, capacity_max = (
        df["usable_capacity_mwh"].fillna(0).min(),
        df["usable_capacity_mwh"].fillna(0).max(),
    )

    power_range = st.sidebar.slider(
        "Net power (MW)",
        min_value=float(power_min),
        max_value=float(max(power_max, power_min + 0.1)),
        value=(float(power_min), float(max(power_max, power_min + 0.1))),
    )
    capacity_range = st.sidebar.slider(
        "Usable capacity (MWh)",
        min_value=float(capacity_min),
        max_value=float(max(capacity_max, capacity_min + 0.1)),
        value=(float(capacity_min), float(max(capacity_max, capacity_min + 0.1))),
    )

    filtered = df.copy()
    if search:
        needle = search.strip().lower()
        filtered = filtered[
            filtered["plant_name"].str.lower().str.contains(needle)
            | filtered["operator_name"].str.lower().str.contains(needle)
            | filtered["unit_id"].str.lower().str.contains(needle)
        ]
    if selected_states:
        filtered = filtered[filtered["bundesland"].isin(selected_states)]
    if selected_tech:
        filtered = filtered[filtered["battery_technology"].isin(selected_tech)]
    if selected_status:
        filtered = filtered[filtered["operating_status"].isin(selected_status)]

    filtered = filtered[
        filtered["net_power_mw"].fillna(0).between(*power_range)
        & filtered["usable_capacity_mwh"].fillna(0).between(*capacity_range)
    ]
    return filtered.sort_values(["net_power_mw", "usable_capacity_mwh"], ascending=False)


def render_map(df: pd.DataFrame):
    mapped = df.dropna(subset=["latitude", "longitude"]).copy()
    if mapped.empty:
        st.info("No coordinates available for the current filter selection.")
        return

    mapped["radius_m"] = (mapped["net_power_mw"].fillna(0).clip(lower=0.1).pow(0.5) * 700).clip(
        lower=120, upper=4000
    )

    view_state = pdk.ViewState(
        latitude=float(mapped["latitude"].median()),
        longitude=float(mapped["longitude"].median()),
        zoom=5.4,
        pitch=0,
    )
    layer = pdk.Layer(
        "ScatterplotLayer",
        data=mapped,
        get_position="[longitude, latitude]",
        get_radius="radius_m",
        get_fill_color="[235, 87, 87, 180]",
        pickable=True,
        stroked=True,
        get_line_color="[135, 28, 28, 220]",
        line_width_min_pixels=1,
    )
    tooltip = {
        "html": (
            "<b>{plant_name}</b><br/>"
            "{operator_name}<br/>"
            "{bundesland} / {city}<br/>"
            "Power: {net_power_mw} MW<br/>"
            "Capacity: {usable_capacity_mwh} MWh"
        )
    }
    st.pydeck_chart(pdk.Deck(layers=[layer], initial_view_state=view_state, tooltip=tooltip))


def render_table(df: pd.DataFrame):
    columns = [
        "plant_name",
        "unit_id",
        "operator_name",
        "bundesland",
        "city",
        "net_power_mw",
        "usable_capacity_mwh",
        "battery_technology",
        "operating_status",
        "commissioning_date",
        "latitude",
        "longitude",
    ]
    st.dataframe(
        df[columns],
        use_container_width=True,
        hide_index=True,
        column_config={
            "plant_name": "Plant",
            "unit_id": "Unit ID",
            "operator_name": "Operator",
            "bundesland": "Bundesland",
            "city": "City",
            "net_power_mw": st.column_config.NumberColumn("Net Power (MW)", format="%.2f"),
            "usable_capacity_mwh": st.column_config.NumberColumn("Capacity (MWh)", format="%.2f"),
            "battery_technology": "Battery Technology",
            "operating_status": "Status",
            "commissioning_date": "Commissioned",
            "latitude": st.column_config.NumberColumn("Lat", format="%.5f"),
            "longitude": st.column_config.NumberColumn("Lon", format="%.5f"),
        },
    )


def render_details(df: pd.DataFrame):
    if df.empty:
        st.info("No plant details available for the current filter selection.")
        return

    options = {f"{row.plant_name} [{row.unit_id}]": row.unit_id for row in df.head(500).itertuples()}
    selected_label = st.selectbox("Plant details", list(options.keys()))
    selected = df[df["unit_id"] == options[selected_label]].iloc[0]

    left, right = st.columns(2)
    with left:
        st.markdown(f"### {selected['plant_name']}")
        st.write(f"**Operator:** {display_value(selected['operator_name'])}")
        st.write(f"**Unit ID:** {display_value(selected['unit_id'])}")
        st.write(f"**Storage ID:** {display_value(selected['storage_id'])}")
        st.write(f"**Status:** {display_value(selected['operating_status'])}")
        st.write(f"**Battery technology:** {display_value(selected['battery_technology'])}")
        st.write(f"**Energy source:** {display_value(selected['energy_source'])}")
    with right:
        st.write(f"**Net power:** {format_metric(selected['net_power_mw'], 'MW')}")
        st.write(f"**Gross power:** {format_metric(selected['gross_power_mw'], 'MW')}")
        st.write(f"**Usable capacity:** {format_metric(selected['usable_capacity_mwh'], 'MWh')}")
        st.write(f"**Commissioning date:** {display_value(selected['commissioning_date'])}")
        st.write(f"**Registered:** {display_value(selected['registration_date'])}")
        st.write(
            f"**Location:** {display_value(selected['postal_code'])} {display_value(selected['city'])}, {display_value(selected['bundesland'])}"
        )
        if display_value(selected["address"]) != "n/a":
            st.write(f"**Address:** {selected['address']}")


def main():
    st.title("MaStR Battery Storage Browser")
    st.caption("Browse storage plants from the imported Marktstammdatenregister DuckDB.")

    db_path = st.sidebar.text_input("DuckDB path", value=str(DEFAULT_DB))
    db_file = Path(db_path).expanduser()
    if not db_file.exists():
        st.error(
            f"Database not found at `{db_file}`. Run `uv run etl` first or point the app to an existing DuckDB file."
        )
        st.stop()

    try:
        df = load_storage_dataset(str(db_file))
    except Exception as exc:
        st.error(f"Failed to load data from `{db_file}`: {exc}")
        st.stop()

    if df.empty:
        st.warning("The storage dataset is empty in this database.")
        st.stop()

    filtered = filter_dataset(df)
    if filtered.empty:
        st.warning("The current filters returned no storage plants.")

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Plants", f"{len(filtered):,}")
    c2.metric("Mapped plants", f"{filtered[['latitude', 'longitude']].dropna().shape[0]:,}")
    c3.metric("Net power", format_metric(filtered["net_power_mw"].sum(), "MW"))
    c4.metric("Usable capacity", format_metric(filtered["usable_capacity_mwh"].sum(), "MWh"))

    map_tab, table_tab, detail_tab = st.tabs(["Map", "Table", "Details"])
    with map_tab:
        render_map(filtered)
    with table_tab:
        render_table(filtered)
        st.download_button(
            "Download filtered CSV",
            data=filtered.to_csv(index=False).encode("utf-8"),
            file_name="mastr_bess_filtered.csv",
            mime="text/csv",
        )
    with detail_tab:
        render_details(filtered)
