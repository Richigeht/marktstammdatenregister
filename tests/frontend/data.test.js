import test from "node:test";
import assert from "node:assert/strict";
import {
  parsePlants,
  filterPlants,
  summarize,
  csv,
  sortPlants,
} from "../../site/src/scripts/data.js";
const feature = (id, properties = {}, coordinates = [13.4, 52.5]) => ({
  type: "Feature",
  geometry: { type: "Point", coordinates },
  properties: { unit_id: id, plant_name: "Berlin storage", ...properties },
});
test("validates payload and excludes invalid geometry without inventing zero measurements", () => {
  assert.throws(() => parsePlants({}), /FeatureCollection/);
  const rows = parsePlants({
    type: "FeatureCollection",
    features: [
      feature("one", { net_power_mw: null }),
      feature("two", {}, [181, 52]),
      feature("three", {}, [null, 52]),
    ],
  });
  assert.equal(rows.length, 1);
  assert.equal(rows[0].net_power_mw, null);
  assert.equal(
    parsePlants({ type: "FeatureCollection", features: [] }).length,
    0,
  );
});
test("shared filters handle accents, missing values and minimums", () => {
  const rows = parsePlants({
    type: "FeatureCollection",
    features: [
      feature("a", { plant_name: "Köln", bundesland: "NRW", net_power_mw: 2 }),
      feature("b", { net_power_mw: null }),
    ],
  });
  assert.equal(
    filterPlants(rows, { search: "koln", state: "NRW", minPower: 1 }).length,
    1,
  );
  assert.equal(filterPlants(rows, { minPower: 1 }).length, 1);
  assert.equal(filterPlants(rows, { status: "Planned" }).length, 0);
});
test("aggregation reports unknown values separately from known totals", () => {
  const result = summarize([
    { net_power_mw: 2, usable_capacity_mwh: null, bundesland: "Berlin" },
    { net_power_mw: null, usable_capacity_mwh: 4, bundesland: "Berlin" },
  ]);
  assert.equal(result.power, 2);
  assert.equal(result.capacity, 4);
  assert.equal(result.missingPower, 1);
  assert.equal(result.missingCapacity, 1);
  assert.deepEqual(result.states, [["Berlin", 2]]);
});
test("sorting leaves unknown values last in either direction", () => {
  const rows = [
    { net_power_mw: null },
    { net_power_mw: 2 },
    { net_power_mw: 1 },
  ];
  assert.deepEqual(
    sortPlants(rows, "net_power_mw", "asc").map((x) => x.net_power_mw),
    [1, 2, null],
  );
  assert.deepEqual(
    sortPlants(rows, "net_power_mw", "desc").map((x) => x.net_power_mw),
    [2, 1, null],
  );
});
test("CSV escapes quotes and neutralizes spreadsheet formulas", () => {
  const output = csv([
    {
      unit_id: "a",
      plant_name: "=SUM(1,2)",
      operator_name: '"Quoted"',
      net_power_mw: null,
    },
  ]);
  assert.ok(output.includes('"\'=SUM(1,2)"'));
  assert.ok(output.includes('"""Quoted"""'));
  assert.ok(output.startsWith("\uFEFF"));
  assert.ok(!output.includes("undefined"));
});
