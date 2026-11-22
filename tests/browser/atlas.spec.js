import { test, expect } from "@playwright/test";
const features = Array.from({ length: 61 }, (_, i) => ({
  type: "Feature",
  geometry: {
    type: "Point",
    coordinates: [i % 2 ? 13.4 : 11.58, i % 2 ? 52.52 : 48.14],
  },
  properties: {
    unit_id: `SEE${i}`,
    plant_name: i === 0 ? "<img src=x onerror=alert(1)>" : `Storage ${i}`,
    operator_name: "Atlas storage operator",
    city: i % 2 ? "Berlin" : "München",
    bundesland: i % 2 ? "Berlin" : "Bayern",
    operating_status: i % 3 ? "In Betrieb" : "In Planung",
    battery_technology: "Lithium",
    net_power_mw: i === 1 ? null : i + 1,
    usable_capacity_mwh: i === 1 ? null : (i + 1) * 2,
  },
}));
const geo = { type: "FeatureCollection", features };
const summary = {
  plants: 100,
  mapped_plants: 61,
  source_export_date: "2026-11-21",
  coordinate_precision_decimals: 2,
};
async function setup(page, { broken = false, missingSummary = false } = {}) {
  await page.route("**/data/bess.geojson", (route) =>
    route.fulfill({
      status: 200,
      contentType: "application/json",
      body: broken ? "not JSON" : JSON.stringify(geo),
    }),
  );
  await page.route("**/data/summary.json", (route) =>
    route.fulfill({
      status: missingSummary ? 404 : 200,
      contentType: "application/json",
      body: JSON.stringify(summary),
    }),
  );
  await page.route("**/*.tile.openstreetmap.org/**", (route) => route.abort());
}
test("loads, filters, paginates, selects, sorts and exports safely", async ({
  page,
}) => {
  const errors = [];
  page.on("pageerror", (e) => errors.push(e.message));
  await setup(page);
  await page.goto("/");
  await expect(page.locator("#plants-count")).toHaveText("61");
  await expect(page.locator("#page-label")).toHaveText("1–25 of 61");
  await page.getByRole("button", { name: "Next", exact: true }).click();
  await expect(page.locator("#page-label")).toHaveText("26–50 of 61");
  await page.locator(".plant-link").first().click();
  await expect(page.locator("#selection")).toContainText("MaStR unit ID");
  await page.locator("#state-filter").selectOption("Berlin");
  await expect(page.locator("#plants-count")).toHaveText("30");
  await expect(page.locator("#power-note")).toContainText(
    "1 units with unknown power",
  );
  await page.locator("#search").fill("does not exist");
  await expect(page.locator("#plants-count")).toHaveText("0");
  await expect(page.locator("#download")).toBeDisabled();
  await page.getByRole("button", { name: "Reset", exact: true }).click();
  await expect(page.locator("#plants-count")).toHaveText("61");
  await page.locator('[data-sort="net_power_mw"]').click();
  await expect(page.locator(".plant-link").first()).toHaveText(
    "<img src=x onerror=alert(1)>",
  );
  await expect(page.locator("#table-body img")).toHaveCount(0);
  const downloadPromise = page.waitForEvent("download");
  await page.locator("#download").click();
  expect((await downloadPromise).suggestedFilename()).toBe(
    "bess-atlas-filtered.csv",
  );
  await expect(page.locator("#state-chart svg")).toBeVisible();
  expect(
    await page.evaluate(
      () => document.documentElement.scrollWidth <= window.innerWidth,
    ),
  ).toBeTruthy();
  expect(errors).toEqual([]);
});
test("handles a failed dataset and recovers on retry", async ({ page }) => {
  await setup(page, { broken: true });
  await page.goto("/");
  await expect(page.locator("#status-message")).toContainText(
    "could not be loaded",
  );
  await expect(page.locator("#plants-count")).toHaveText("—");
  await page.unroute("**/data/bess.geojson");
  await page.route("**/data/bess.geojson", (route) =>
    route.fulfill({ json: geo }),
  );
  await page.getByRole("button", { name: "Try again", exact: true }).click();
  await expect(page.locator("#plants-count")).toHaveText("61");
});
test("summary is optional and URL filters survive reload", async ({ page }) => {
  await setup(page, { missingSummary: true });
  await page.goto("/?state=Berlin&minPower=10");
  await expect(page.locator("#plants-count")).toHaveText("26");
  await expect(page.locator("#status-message")).toContainText(
    "metadata is unavailable",
  );
  await page.reload();
  await expect(page.locator("#plants-count")).toHaveText("26");
});
