const DATA_DIR = "./data";

const state = {
  records: [],
  filtered: [],
  markers: [],
  selectedId: null,
};

const elements = {
  statusBanner: document.getElementById("status-banner"),
  datasetMeta: document.getElementById("dataset-meta"),
  plantsCount: document.getElementById("plants-count"),
  mappedCount: document.getElementById("mapped-count"),
  powerSum: document.getElementById("power-sum"),
  capacitySum: document.getElementById("capacity-sum"),
  tableCount: document.getElementById("table-count"),
  tableBody: document.getElementById("table-body"),
  selection: document.getElementById("selection"),
  search: document.getElementById("search"),
  stateFilter: document.getElementById("state-filter"),
  statusFilter: document.getElementById("status-filter"),
  technologyFilter: document.getElementById("technology-filter"),
  minPower: document.getElementById("min-power"),
  minCapacity: document.getElementById("min-capacity"),
  resetFilters: document.getElementById("reset-filters"),
};

const datasetSummary = {
  plants: 0,
  mappedPlants: 0,
  coordinatePrecision: null,
  sourceExportDate: null,
  builtAtUtc: null,
};

const map = L.map("map", { preferCanvas: true }).setView([51.1, 10.3], 6);
L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
  maxZoom: 18,
  attribution: "&copy; OpenStreetMap contributors",
}).addTo(map);
const markerLayer = L.layerGroup().addTo(map);

function formatNumber(value, digits = 1) {
  return new Intl.NumberFormat("de-DE", {
    maximumFractionDigits: digits,
    minimumFractionDigits: digits,
  }).format(value || 0);
}

function metricText(value, suffix) {
  return `${formatNumber(value, 1)} ${suffix}`;
}

function formatIsoDate(value, options) {
  if (!value) {
    return null;
  }
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return value;
  }
  return new Intl.DateTimeFormat("de-DE", options).format(date);
}

function renderDatasetMeta() {
  const parts = [];
  const sourceDate = formatIsoDate(datasetSummary.sourceExportDate, {
    day: "2-digit",
    month: "2-digit",
    year: "numeric",
    timeZone: "UTC",
  });
  const builtAt = formatIsoDate(datasetSummary.builtAtUtc, {
    day: "2-digit",
    month: "2-digit",
    year: "numeric",
    hour: "2-digit",
    minute: "2-digit",
    timeZoneName: "short",
  });

  if (sourceDate) {
    parts.push(`Source export: ${sourceDate}`);
  }
  if (builtAt) {
    parts.push(`Site build: ${builtAt}`);
  }

  elements.datasetMeta.textContent = parts.join(" | ");
}

function setStatusBanner(message) {
  if (!message) {
    elements.statusBanner.hidden = true;
    elements.statusBanner.textContent = "";
    return;
  }
  elements.statusBanner.hidden = false;
  elements.statusBanner.textContent = message;
}

function optionValues(records, key) {
  return [...new Set(records.map((record) => record[key]).filter(Boolean))].sort((a, b) =>
    a.localeCompare(b, "de")
  );
}

function fillSelect(select, values) {
  for (const value of values) {
    const option = document.createElement("option");
    option.value = value;
    option.textContent = value;
    select.appendChild(option);
  }
}

function selectedFilters() {
  return {
    search: elements.search.value.trim().toLowerCase(),
    bundesland: elements.stateFilter.value,
    status: elements.statusFilter.value,
    technology: elements.technologyFilter.value,
    minPower: Number(elements.minPower.value || 0),
    minCapacity: Number(elements.minCapacity.value || 0),
  };
}

function applyFilters() {
  const filters = selectedFilters();
  state.filtered = state.records.filter((record) => {
    if (filters.search) {
      const haystack = [
        record.plant_name,
        record.operator_name,
        record.unit_id,
        record.city,
      ]
        .filter(Boolean)
        .join(" ")
        .toLowerCase();
      if (!haystack.includes(filters.search)) {
        return false;
      }
    }

    if (filters.bundesland && record.bundesland !== filters.bundesland) {
      return false;
    }
    if (filters.status && record.operating_status !== filters.status) {
      return false;
    }
    if (filters.technology && record.battery_technology !== filters.technology) {
      return false;
    }
    if ((record.net_power_mw || 0) < filters.minPower) {
      return false;
    }
    if ((record.usable_capacity_mwh || 0) < filters.minCapacity) {
      return false;
    }
    return true;
  });

  renderStats();
  renderMap();
  renderTable();
  renderSelection();
}

function renderStats() {
  const mapped = state.filtered.filter((record) => Number.isFinite(record.latitude) && Number.isFinite(record.longitude));
  const powerSum = state.filtered.reduce((sum, record) => sum + (record.net_power_mw || 0), 0);
  const capacitySum = state.filtered.reduce((sum, record) => sum + (record.usable_capacity_mwh || 0), 0);

  elements.plantsCount.textContent = state.filtered.length.toLocaleString("de-DE");
  const mappedText = datasetSummary.mappedPlants
    ? `${mapped.length.toLocaleString("de-DE")} / ${datasetSummary.mappedPlants.toLocaleString("de-DE")}`
    : mapped.length.toLocaleString("de-DE");
  elements.mappedCount.textContent = mappedText;
  elements.powerSum.textContent = metricText(powerSum, "MW");
  elements.capacitySum.textContent = metricText(capacitySum, "MWh");
  elements.tableCount.textContent = `${state.filtered.length.toLocaleString("de-DE")} visible`;
}

function markerRadius(record) {
  const power = Math.max(record.net_power_mw || 0.1, 0.1);
  return Math.max(5, Math.min(20, Math.sqrt(power) * 2.2));
}

function popupHtml(record) {
  return `
    <strong>${record.plant_name || "Unnamed plant"}</strong><br>
    ${record.operator_name || "Unknown operator"}<br>
    ${record.bundesland || "n/a"} / ${record.city || "n/a"}<br>
    Net power: ${metricText(record.net_power_mw || 0, "MW")}<br>
    Capacity: ${metricText(record.usable_capacity_mwh || 0, "MWh")}
  `;
}

function renderMap() {
  markerLayer.clearLayers();
  state.markers = [];

  const mapped = state.filtered.filter((record) => Number.isFinite(record.latitude) && Number.isFinite(record.longitude));
  for (const record of mapped) {
    const marker = L.circleMarker([record.latitude, record.longitude], {
      radius: markerRadius(record),
      weight: 1,
      color: "#742713",
      fillColor: "#b14d2d",
      fillOpacity: 0.65,
    }).bindPopup(popupHtml(record));

    marker.on("click", () => {
      state.selectedId = record.unit_id;
      renderSelection();
      renderTable();
    });

    markerLayer.addLayer(marker);
    state.markers.push({ id: record.unit_id, marker });
  }

  if (mapped.length > 0) {
    const group = L.featureGroup(state.markers.map((item) => item.marker));
    map.fitBounds(group.getBounds().pad(0.2));
  } else {
    map.setView([51.1, 10.3], 6);
  }
}

function renderTable() {
  elements.tableBody.innerHTML = "";
  const topRows = state.filtered.slice(0, 500);

  for (const record of topRows) {
    const row = document.createElement("tr");
    if (record.unit_id === state.selectedId) {
      row.classList.add("selected");
    }
    row.innerHTML = `
      <td>${record.plant_name || "Unnamed plant"}</td>
      <td>${record.bundesland || "n/a"}</td>
      <td>${record.operator_name || "Unknown"}</td>
      <td>${formatNumber(record.net_power_mw || 0, 2)}</td>
      <td>${formatNumber(record.usable_capacity_mwh || 0, 2)}</td>
      <td>${record.operating_status || "n/a"}</td>
    `;
    row.addEventListener("click", () => {
      state.selectedId = record.unit_id;
      renderSelection();
      renderTable();
      const marker = state.markers.find((item) => item.id === record.unit_id);
      if (marker) {
        map.setView(marker.marker.getLatLng(), Math.max(map.getZoom(), 10));
        marker.marker.openPopup();
      }
    });
    elements.tableBody.appendChild(row);
  }
}

function renderSelection() {
  const record =
    state.filtered.find((item) => item.unit_id === state.selectedId) ||
    state.filtered[0];

  if (!record) {
    elements.selection.className = "selection empty";
    elements.selection.textContent = "No plants match the current filters.";
    return;
  }

  state.selectedId = record.unit_id;
  elements.selection.className = "selection";
  elements.selection.innerHTML = `
    <strong>${record.plant_name || "Unnamed plant"}</strong><br>
    <span>${record.operator_name || "Unknown operator"}</span><br><br>
    Unit ID: ${record.unit_id || "n/a"}<br>
    Bundesland: ${record.bundesland || "n/a"}<br>
    City: ${record.city || "n/a"}<br>
    Status: ${record.operating_status || "n/a"}<br>
    Battery technology: ${record.battery_technology || "n/a"}<br>
    Net power: ${metricText(record.net_power_mw || 0, "MW")}<br>
    Capacity: ${metricText(record.usable_capacity_mwh || 0, "MWh")}<br>
    Map location: approximate
  `;
}

function attachEvents() {
  [
    elements.search,
    elements.stateFilter,
    elements.statusFilter,
    elements.technologyFilter,
    elements.minPower,
    elements.minCapacity,
  ].forEach((element) => {
    element.addEventListener("input", applyFilters);
    element.addEventListener("change", applyFilters);
  });

  elements.resetFilters.addEventListener("click", () => {
    elements.search.value = "";
    elements.stateFilter.value = "";
    elements.statusFilter.value = "";
    elements.technologyFilter.value = "";
    elements.minPower.value = "0";
    elements.minCapacity.value = "0";
    applyFilters();
  });
}

async function loadData() {
  const [geojsonResponse, summaryResponse] = await Promise.all([
    fetch(`${DATA_DIR}/bess.geojson`),
    fetch(`${DATA_DIR}/summary.json`),
  ]);

  if (!geojsonResponse.ok) {
    throw new Error(`Failed to load ${DATA_DIR}/bess.geojson`);
  }
  if (!summaryResponse.ok) {
    throw new Error(`Failed to load ${DATA_DIR}/summary.json`);
  }

  const geojson = await geojsonResponse.json();
  const summary = await summaryResponse.json();
  datasetSummary.plants = summary.plants || 0;
  datasetSummary.mappedPlants = summary.mapped_plants || 0;
  datasetSummary.coordinatePrecision = summary.coordinate_precision_decimals || null;
  datasetSummary.sourceExportDate = summary.source_export_date || null;
  datasetSummary.builtAtUtc = summary.built_at_utc || null;

  state.records = geojson.features.map((feature) => ({
    ...feature.properties,
    longitude: feature.geometry.coordinates[0],
    latitude: feature.geometry.coordinates[1],
  }));
  state.records.sort((a, b) => (b.net_power_mw || 0) - (a.net_power_mw || 0));

  if (window.location.protocol === "file:") {
    setStatusBanner("Open this site through http://localhost or GitHub Pages. Browser fetches are often blocked from file:// pages.");
  } else if (datasetSummary.plants > state.records.length) {
    setStatusBanner(
      `Showing ${datasetSummary.mappedPlants.toLocaleString("de-DE")} plants with approximate coordinates out of ${datasetSummary.plants.toLocaleString("de-DE")} exported BESS records.`
    );
  } else {
    setStatusBanner("");
  }

  fillSelect(elements.stateFilter, optionValues(state.records, "bundesland"));
  fillSelect(elements.statusFilter, optionValues(state.records, "operating_status"));
  fillSelect(elements.technologyFilter, optionValues(state.records, "battery_technology"));
  renderDatasetMeta();
}

async function init() {
  attachEvents();
  try {
    await loadData();
    applyFilters();
  } catch (error) {
    setStatusBanner(error.message);
    elements.selection.className = "selection empty";
    elements.selection.textContent = error.message;
    console.error(error);
  }
}

init();
