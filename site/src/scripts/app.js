import L from "leaflet";
import "leaflet/dist/leaflet.css";
import Supercluster from "supercluster";
import germany from "./germany.json";
import * as echarts from "echarts/core";
import { BarChart, PieChart } from "echarts/charts";
import {
  GridComponent,
  TooltipComponent,
  AriaComponent,
  GraphicComponent,
} from "echarts/components";
import { SVGRenderer } from "echarts/renderers";
import {
  parsePlants,
  filterPlants,
  summarize,
  sortPlants,
  csv,
} from "./data.js";

echarts.use([
  BarChart,
  PieChart,
  GridComponent,
  TooltipComponent,
  AriaComponent,
  GraphicComponent,
  SVGRenderer,
]);
const $ = (id) => document.getElementById(id);
const form = $("filters");
const state = {
  records: [],
  filtered: [],
  page: 0,
  selected: null,
  sort: "net_power_mw",
  direction: "desc",
  loaded: false,
};
const PAGE_SIZE = 25;
const count = (value) => new Intl.NumberFormat("en-GB").format(value);
const measurement = (value) =>
  value === null
    ? "—"
    : new Intl.NumberFormat("en-GB", { maximumFractionDigits: 2 }).format(
        value,
      );
const metric = (value, unit) =>
  value >= 1000000
    ? `${measurement(value / 1000000)} T${unit}`
    : value >= 1000
      ? `${measurement(value / 1000)} G${unit}`
      : `${measurement(value)} M${unit}`;
const controls = {
  search: $("search"),
  state: $("state-filter"),
  status: $("status-filter"),
  technology: $("technology-filter"),
  minPower: $("min-power"),
  minCapacity: $("min-capacity"),
};
let index = null;
let searchTimer;
const map = L.map("map", {
  preferCanvas: true,
  scrollWheelZoom: false,
}).setView([51.1, 10.3], 6);
const tiles = L.tileLayer(
  "https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png",
  {
    maxZoom: 18,
    attribution:
      '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors',
  },
).addTo(map);
let boundaryShown = false;
tiles.on("tileerror", () => {
  if (!boundaryShown) {
    map.createPane("boundary");
    map.getPane("boundary").style.zIndex = 350;
    L.geoJSON(germany, {
      pane: "boundary",
      interactive: false,
      style: {
        color: "#91a78d",
        weight: 1.5,
        fillColor: "#cddcc3",
        fillOpacity: 0.65,
      },
    }).addTo(map);
    map.attributionControl.addAttribution(
      'Boundary: <a href="https://www.naturalearthdata.com/about/terms-of-use/">Natural Earth</a>',
    );
    boundaryShown = true;
  }
  $("map-alert").hidden = false;
});
const layer = L.layerGroup().addTo(map);
const renderer = L.canvas({ padding: 0.5 });
const colors = [
  "#17694f",
  "#8fae43",
  "#d4a34e",
  "#698cb2",
  "#ac7284",
  "#6c7972",
];
const chartOptions = { renderer: "svg" };
const regionalChart = echarts.init($("state-chart"), null, chartOptions);
const statusChart = echarts.init($("status-chart"), null, chartOptions);
const reducedMotion = window.matchMedia(
  "(prefers-reduced-motion: reduce)",
).matches;
const chartBase = {
  animation: !reducedMotion,
  animationDuration: 350,
  textStyle: { fontFamily: "Manrope Variable, sans-serif", color: "#53645c" },
  aria: { enabled: true },
  tooltip: { trigger: "item", renderMode: "richText", confine: true },
};
function node(tag, text, className) {
  const element = document.createElement(tag);
  if (text !== undefined) element.textContent = text;
  if (className) element.className = className;
  return element;
}
function notice(message, error = false) {
  $("status-banner").hidden = !message;
  $("status-banner").classList.toggle("error", error);
  $("status-message").textContent = message;
  $("retry").hidden = !error;
}
function filters() {
  return Object.fromEntries(
    Object.entries(controls).map(([key, el]) => [
      key,
      key.startsWith("min") ? Math.max(0, Number(el.value) || 0) : el.value,
    ]),
  );
}
function updateUrl() {
  const url = new URL(location.href);
  for (const [key, el] of Object.entries(controls)) {
    if (el.value) url.searchParams.set(key, el.value);
    else url.searchParams.delete(key);
  }
  history.replaceState(null, "", url);
}
function applyFilters({ fit = false } = {}) {
  if (!state.loaded) return;
  state.filtered = sortPlants(
    filterPlants(state.records, filters()),
    state.sort,
    state.direction,
  );
  state.page = 0;
  if (!state.filtered.some((r) => r.unit_id === state.selected))
    state.selected = null;
  updateUrl();
  renderStatsAndCharts();
  renderTable();
  renderSelection();
  index = new Supercluster({ radius: 90, maxZoom: 16 }).load(
    state.filtered.map((r, i) => ({
      type: "Feature",
      geometry: { type: "Point", coordinates: [r.longitude, r.latitude] },
      properties: { recordIndex: i },
    })),
  );
  renderMap();
  if (fit) fitMap();
}
function renderStatsAndCharts() {
  const summary = summarize(state.filtered);
  $("plants-count").textContent = count(state.filtered.length);
  $("scope-label").textContent =
    `of ${count(state.records.length)} public mapped units`;
  $("power-sum").textContent = metric(summary.power, "W");
  $("capacity-sum").textContent = metric(summary.capacity, "Wh");
  $("power-note").textContent = summary.missingPower
    ? `${count(summary.missingPower)} units with unknown power`
    : "Known net nominal power";
  $("capacity-note").textContent = summary.missingCapacity
    ? `${count(summary.missingCapacity)} units with unknown capacity`
    : "Known usable storage capacity";
  const topStates = summary.states.slice(0, 8).reverse();
  regionalChart.setOption(
    {
      ...chartBase,
      grid: { left: 12, right: 42, top: 8, bottom: 8, containLabel: true },
      xAxis: { type: "value", show: false },
      yAxis: {
        type: "category",
        data: topStates.map((x) => x[0]),
        axisLine: { show: false },
        axisTick: { show: false },
        axisLabel: {
          width: 135,
          overflow: "truncate",
          fontSize: 11,
          color: "#53645c",
        },
      },
      series: [
        {
          type: "bar",
          data: topStates.map((x) => ({ value: x[1], name: x[0] })),
          barWidth: 12,
          itemStyle: { color: "#17694f", borderRadius: [0, 3, 3, 0] },
          label: {
            show: true,
            position: "right",
            fontSize: 10,
            color: "#18352c",
            formatter: (p) => count(p.value),
          },
        },
      ],
    },
    true,
  );
  statusChart.setOption(
    {
      ...chartBase,
      color: colors,
      series: [
        {
          type: "pie",
          radius: ["54%", "83%"],
          center: ["50%", "50%"],
          label: { show: false },
          emphasis: { scale: false },
          data: summary.statuses.map(([name, value]) => ({ name, value })),
          itemStyle: { borderColor: "#fff", borderWidth: 3 },
        },
      ],
      graphic: [
        {
          type: "text",
          left: "center",
          top: "41%",
          style: {
            text: count(state.filtered.length),
            fill: "#18352c",
            font: '600 22px "Manrope Variable"',
          },
        },
        {
          type: "text",
          left: "center",
          top: "58%",
          style: {
            text: "units",
            fill: "#53645c",
            font: '12px "Manrope Variable"',
          },
        },
      ],
    },
    true,
  );
  $("state-summary").replaceChildren(
    ...summary.states.map(([name, value]) =>
      node("li", `${name}: ${count(value)} units`),
    ),
  );
  $("status-summary").replaceChildren(
    ...summary.statuses.map(([name, value], i) => {
      const item = node("li");
      const dot = node("i");
      dot.style.background = colors[i % colors.length];
      item.append(dot, node("span", name), node("strong", count(value)));
      return item;
    }),
  );
  $("table-count").textContent = state.filtered.length
    ? `${count(state.filtered.length)} matching units · sorted by ${state.sort === "plant_name" ? "plant name" : state.sort === "net_power_mw" ? "net power" : "capacity"}`
    : "No matching units. Try changing or resetting the filters.";
  $("download").disabled = !state.filtered.length;
}
regionalChart.on("click", (params) => {
  controls.state.value = params.name;
  applyFilters({ fit: true });
});
function fitMap() {
  if (state.filtered.length)
    map.fitBounds(
      L.latLngBounds(state.filtered.map((r) => [r.latitude, r.longitude])),
      { padding: [24, 24], maxZoom: 12, animate: !reducedMotion },
    );
  else map.setView([51.1, 10.3], 6);
}
function renderMap() {
  layer.clearLayers();
  if (!index) return;
  const bounds = map.getBounds();
  const clusters = index.getClusters(
    [
      Math.max(-180, bounds.getWest()),
      Math.max(-90, bounds.getSouth()),
      Math.min(180, bounds.getEast()),
      Math.min(90, bounds.getNorth()),
    ],
    Math.floor(map.getZoom()),
  );
  for (const f of clusters) {
    const [lon, lat] = f.geometry.coordinates;
    if (f.properties.cluster) {
      const total = f.properties.point_count;
      const icon = L.divIcon({
        className: "cluster-icon",
        html: `<span>${count(total)}</span>`,
        iconSize: [44, 44],
      });
      L.marker([lat, lon], {
        icon,
        title: `${count(total)} storage units; zoom in`,
        keyboard: true,
      })
        .on("click", () =>
          map.setView(
            [lat, lon],
            Math.min(
              index.getClusterExpansionZoom(f.properties.cluster_id),
              18,
            ),
            { animate: !reducedMotion },
          ),
        )
        .addTo(layer);
    } else {
      const r = state.filtered[f.properties.recordIndex];
      const point = L.circleMarker([lat, lon], {
        renderer,
        radius: state.selected === r.unit_id ? 10 : 6,
        weight: state.selected === r.unit_id ? 3 : 1,
        color: "#fff",
        fillColor: state.selected === r.unit_id ? "#93b83d" : "#17694f",
        fillOpacity: 0.85,
      });
      const popup = node("div", undefined, "plant-popup");
      popup.append(
        node("strong", r.plant_name || "Unnamed plant"),
        node("p", r.city || r.bundesland || "Location not provided"),
      );
      const button = node("button", "View plant details", "subtle-button");
      button.addEventListener("click", () => selectPlant(r, true));
      popup.append(button);
      point
        .bindPopup(popup)
        .on("click", () => selectPlant(r, false))
        .addTo(layer);
    }
  }
}
map.on("moveend", renderMap);
function selectPlant(record, scroll = false) {
  state.selected = record.unit_id;
  renderSelection();
  renderTable();
  if (scroll)
    $("selection").scrollIntoView({
      behavior: reducedMotion ? "instant" : "smooth",
      block: "center",
    });
}
function renderTable() {
  const pages = Math.max(1, Math.ceil(state.filtered.length / PAGE_SIZE));
  state.page = Math.min(state.page, pages - 1);
  const start = state.page * PAGE_SIZE;
  const fragment = document.createDocumentFragment();
  for (const r of state.filtered.slice(start, start + PAGE_SIZE)) {
    const row = node("tr");
    if (r.unit_id === state.selected) row.classList.add("selected");
    const plant = node("td");
    const button = node(
      "button",
      r.plant_name || "Unnamed plant",
      "plant-link",
    );
    button.setAttribute("aria-pressed", String(r.unit_id === state.selected));
    button.addEventListener("click", () => {
      selectPlant(r);
      map.setView([r.latitude, r.longitude], 12, { animate: !reducedMotion });
    });
    plant.append(button, node("small", r.operator_name || "Unknown operator"));
    const place = node("td", r.bundesland || "—");
    place.append(node("small", r.city || "—"));
    const status = node("td");
    status.append(node("span", r.operating_status || "Unknown", "status-tag"));
    row.append(
      plant,
      place,
      node("td", measurement(r.net_power_mw), "numeric"),
      node("td", measurement(r.usable_capacity_mwh), "numeric"),
      status,
    );
    fragment.append(row);
  }
  if (!state.filtered.length) {
    const row = node("tr");
    const cell = node(
      "td",
      "No plants match these filters. Reset filters to explore all mapped units.",
      "table-empty",
    );
    cell.colSpan = 5;
    row.append(cell);
    fragment.append(row);
  }
  $("table-body").replaceChildren(fragment);
  $("page-label").textContent = state.filtered.length
    ? `${count(start + 1)}–${count(Math.min(start + PAGE_SIZE, state.filtered.length))} of ${count(state.filtered.length)}`
    : "0 results";
  $("previous").disabled = state.page === 0;
  $("next").disabled = state.page >= pages - 1;
}
const emptySelection = $("selection").cloneNode(true);
function renderSelection() {
  const r = state.filtered.find((r) => r.unit_id === state.selected);
  $("clear-selection").hidden = !r;
  if (!r) {
    $("selection").replaceChildren(
      ...Array.from(emptySelection.childNodes).map((n) => n.cloneNode(true)),
    );
    return;
  }
  const title = node("h3", r.plant_name || "Unnamed plant");
  const subtitle = node(
    "p",
    r.operator_name || "Unknown operator",
    "detail-operator",
  );
  const list = node("dl");
  for (const [label, value] of [
    ["MaStR unit ID", r.unit_id],
    ["Federal state", r.bundesland],
    ["City", r.city],
    ["District", r.district],
    ["Operating status", r.operating_status],
    ["Battery technology", r.battery_technology],
    [
      "Net power",
      r.net_power_mw === null ? "Unknown" : `${measurement(r.net_power_mw)} MW`,
    ],
    [
      "Usable capacity",
      r.usable_capacity_mwh === null
        ? "Unknown"
        : `${measurement(r.usable_capacity_mwh)} MWh`,
    ],
    ["Commissioning date", r.commissioning_date?.slice(0, 10)],
  ])
    list.append(node("dt", label), node("dd", value || "Not provided"));
  $("selection").replaceChildren(
    title,
    subtitle,
    list,
    node(
      "p",
      "Location is approximate. Measurements and status are as registered in MaStR.",
      "detail-note",
    ),
  );
}
function populateControls() {
  for (const [name, key] of [
    ["state", "bundesland"],
    ["status", "operating_status"],
    ["technology", "battery_technology"],
  ]) {
    const select = controls[name];
    while (select.options.length > 1) select.remove(1);
    for (const value of [
      ...new Set(state.records.map((r) => r[key]).filter(Boolean)),
    ].sort((a, b) => a.localeCompare(b, "de")))
      select.add(new Option(value, value));
  }
  const params = new URL(location.href).searchParams;
  for (const [key, el] of Object.entries(controls))
    el.value = params.get(key) || "";
}
async function fetchJson(path) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), 60000);
  try {
    const response = await fetch(new URL(`data/${path}`, document.baseURI), {
      signal: controller.signal,
    });
    if (!response.ok) throw new Error(`${path}: HTTP ${response.status}`);
    return await response.json();
  } finally {
    clearTimeout(timer);
  }
}
async function load() {
  notice("Loading the public registry…");
  $("retry").disabled = true;
  form
    .querySelectorAll("input,select,button")
    .forEach((el) => (el.disabled = true));
  try {
    // Provenance is useful, but a missing summary must not hide a valid plant dataset.
    const [geo, summary] = await Promise.all([
      fetchJson("bess.geojson"),
      fetchJson("summary.json").catch(() => null),
    ]);
    state.records = parsePlants(geo);
    state.loaded = true;
    populateControls();
    const date =
      typeof summary?.source_export_date === "string" &&
      /^\d{4}-\d{2}-\d{2}$/.test(summary.source_export_date)
        ? summary.source_export_date
        : null;
    const built =
      typeof summary?.built_at_utc === "string" &&
      !Number.isNaN(Date.parse(summary.built_at_utc))
        ? new Date(summary.built_at_utc).toLocaleDateString("en-GB", {
            timeZone: "UTC",
            day: "numeric",
            month: "short",
            year: "numeric",
          })
        : null;
    $("dataset-meta").textContent = [
      date ? `Source export ${date}` : "Source date unavailable",
      built ? `Built ${built}` : null,
    ]
      .filter(Boolean)
      .join(" · ");
    const decimals = summary?.coordinate_precision_decimals;
    $("precision-note").textContent = Number.isInteger(decimals)
      ? `Coordinates rounded to ${decimals} decimal places`
      : "Public locations are rounded";
    const total = Number.isFinite(summary?.plants) ? summary.plants : null;
    const omitted = geo.features.length - state.records.length;
    notice(
      !summary
        ? "Dataset loaded. Source metadata is unavailable."
        : omitted
          ? `Excluded ${count(omitted)} records with invalid coordinates or IDs.`
          : total > state.records.length
            ? `Showing ${count(state.records.length)} public mapped units from ${count(total)} registry units. Units without valid coordinates are not included in this explorer.`
            : "",
    );
    form
      .querySelectorAll("input,select,button")
      .forEach((el) => (el.disabled = false));
    applyFilters();
    if (Object.values(filters()).some(Boolean)) fitMap();
    else
      map.fitBounds(
        [
          [47.1, 5.5],
          [55.2, 15.5],
        ],
        { padding: [20, 20], animate: false },
      );
  } catch (error) {
    state.loaded = false;
    notice(
      "The plant dataset could not be loaded. Try again. If this continues, the published export needs to be rebuilt.",
      true,
    );
    $("dataset-meta").textContent = "Dataset unavailable";
    $("plants-count").textContent = "—";
    $("power-sum").textContent = "—";
    $("capacity-sum").textContent = "—";
    $("scope-label").textContent = "No dataset loaded";
    $("table-body").replaceChildren();
    const row = node("tr");
    const cell = node(
      "td",
      "Plant data is unavailable. Use “Try again” above to reload.",
      "table-empty",
    );
    cell.colSpan = 5;
    row.append(cell);
    $("table-body").append(row);
    $("table-count").textContent = "Data unavailable";
    console.error("Dataset load failed:", error);
  } finally {
    $("retry").disabled = false;
  }
}
form.addEventListener("submit", (event) => event.preventDefault());
form.addEventListener("input", (event) => {
  clearTimeout(searchTimer);
  searchTimer = setTimeout(
    () => applyFilters(),
    event.target === controls.search ? 180 : 0,
  );
});
form.addEventListener("reset", () => {
  clearTimeout(searchTimer);
  setTimeout(() => applyFilters({ fit: true }), 0);
});
$("retry").addEventListener("click", load);
$("fit-map").addEventListener("click", fitMap);
$("clear-selection").addEventListener("click", () => {
  state.selected = null;
  renderSelection();
  renderTable();
  renderMap();
});
$("previous").addEventListener("click", () => {
  state.page--;
  renderTable();
});
$("next").addEventListener("click", () => {
  state.page++;
  renderTable();
});
for (const button of document.querySelectorAll("[data-sort]"))
  button.addEventListener("click", () => {
    state.direction =
      state.sort === button.dataset.sort && state.direction === "desc"
        ? "asc"
        : "desc";
    state.sort = button.dataset.sort;
    for (const b of document.querySelectorAll("[data-sort]")) {
      b.parentElement.setAttribute(
        "aria-sort",
        b === button
          ? state.direction === "asc"
            ? "ascending"
            : "descending"
          : "none",
      );
      b.querySelector("span").textContent =
        b === button ? (state.direction === "asc" ? "↑" : "↓") : "↕";
    }
    state.filtered = sortPlants(state.filtered, state.sort, state.direction);
    state.page = 0;
    renderTable();
    renderStatsAndCharts();
    // Cluster indices refer to the previous sorted array; rebuild after a sort.
    index = new Supercluster({ radius: 90, maxZoom: 16 }).load(
      state.filtered.map((r, i) => ({
        type: "Feature",
        geometry: { type: "Point", coordinates: [r.longitude, r.latitude] },
        properties: { recordIndex: i },
      })),
    );
    renderMap();
  });
$("download").addEventListener("click", () => {
  const url = URL.createObjectURL(
    new Blob([csv(state.filtered)], { type: "text/csv;charset=utf-8" }),
  );
  const link = node("a");
  link.href = url;
  link.download = "bess-atlas-filtered.csv";
  link.click();
  setTimeout(() => URL.revokeObjectURL(url), 1000);
});
new ResizeObserver(() => {
  map.invalidateSize();
  regionalChart.resize();
  statusChart.resize();
}).observe($("main"));
window.addEventListener("popstate", () => {
  const params = new URL(location.href).searchParams;
  for (const [key, el] of Object.entries(controls))
    el.value = params.get(key) || "";
  applyFilters({ fit: true });
});
load();
