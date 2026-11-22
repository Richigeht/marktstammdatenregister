const number = (value) =>
  typeof value === "number" && Number.isFinite(value) && value >= 0
    ? value
    : null;
const text = (value) => (typeof value === "string" ? value : "");
const fold = (value) =>
  value
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLocaleLowerCase("de");
export function parsePlants(payload) {
  if (payload?.type !== "FeatureCollection" || !Array.isArray(payload.features))
    throw new Error("Expected a GeoJSON FeatureCollection.");
  return payload.features.flatMap((feature) => {
    const p = feature?.properties;
    const c = feature?.geometry?.coordinates;
    if (
      !p ||
      feature.geometry?.type !== "Point" ||
      !Array.isArray(c) ||
      !Number.isFinite(c[0]) ||
      !Number.isFinite(c[1]) ||
      Math.abs(c[0]) > 180 ||
      Math.abs(c[1]) > 90 ||
      !text(p.unit_id)
    )
      return [];
    const record = Object.fromEntries(
      [
        "unit_id",
        "plant_name",
        "operator_name",
        "bundesland",
        "district",
        "city",
        "operating_status",
        "battery_technology",
        "commissioning_date",
      ].map((key) => [key, text(p[key])]),
    );
    record.net_power_mw = number(p.net_power_mw);
    record.usable_capacity_mwh = number(p.usable_capacity_mwh);
    record.longitude = c[0];
    record.latitude = c[1];
    record.searchText = fold(
      [
        record.plant_name,
        record.operator_name,
        record.unit_id,
        record.city,
        record.district,
      ].join(" "),
    );
    return [record];
  });
}
export function filterPlants(rows, filters = {}) {
  const query = fold((filters.search || "").trim());
  return rows.filter(
    (r) =>
      (!query || r.searchText.includes(query)) &&
      (!filters.state || r.bundesland === filters.state) &&
      (!filters.status || r.operating_status === filters.status) &&
      (!filters.technology || r.battery_technology === filters.technology) &&
      (!(filters.minPower > 0) ||
        (r.net_power_mw !== null && r.net_power_mw >= filters.minPower)) &&
      (!(filters.minCapacity > 0) ||
        (r.usable_capacity_mwh !== null &&
          r.usable_capacity_mwh >= filters.minCapacity)),
  );
}
export function summarize(rows) {
  let power = 0,
    capacity = 0,
    missingPower = 0,
    missingCapacity = 0;
  const states = new Map(),
    statuses = new Map();
  for (const r of rows) {
    if (r.net_power_mw === null) missingPower++;
    else power += r.net_power_mw;
    if (r.usable_capacity_mwh === null) missingCapacity++;
    else capacity += r.usable_capacity_mwh;
    const name = r.bundesland || "Unknown";
    states.set(name, (states.get(name) || 0) + 1);
    const status = r.operating_status || "Unknown";
    statuses.set(status, (statuses.get(status) || 0) + 1);
  }
  return {
    power,
    capacity,
    missingPower,
    missingCapacity,
    states: [...states].sort((a, b) => b[1] - a[1]),
    statuses: [...statuses].sort((a, b) => b[1] - a[1]),
  };
}
export function sortPlants(rows, key = "net_power_mw", direction = "desc") {
  return [...rows].sort((a, b) => {
    if (a[key] === null || a[key] === "")
      return b[key] === null || b[key] === "" ? 0 : 1;
    if (b[key] === null || b[key] === "") return -1;
    const comparison =
      typeof a[key] === "number"
        ? a[key] - b[key]
        : a[key].localeCompare(b[key], "de");
    return direction === "asc" ? comparison : -comparison;
  });
}
export const CSV_COLUMNS = [
  "unit_id",
  "plant_name",
  "operator_name",
  "bundesland",
  "city",
  "operating_status",
  "battery_technology",
  "net_power_mw",
  "usable_capacity_mwh",
  "commissioning_date",
];
export function csv(rows) {
  const cell = (value) => {
    let s = value == null ? "" : String(value);
    if (/^[\s]*[=+@-]/.test(s)) s = "'" + s;
    return '"' + s.replaceAll('"', '""') + '"';
  };
  return (
    "\uFEFF" +
    [
      CSV_COLUMNS.join(","),
      ...rows.map((r) => CSV_COLUMNS.map((key) => cell(r[key])).join(",")),
    ].join("\r\n")
  );
}
