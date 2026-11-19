import json
import unittest

import pandas as pd

from marktstammdatenregister.export_bess import build_geojson


class GeoJsonTests(unittest.TestCase):
    def test_missing_properties_are_json_null(self):
        frame = pd.DataFrame({
            "latitude": [52.52, 51.1], "longitude": [13.4, 10.3],
            "net_power_mw": [float("nan"), float("inf")],
            "plant_name": ["NaN is a valid name", pd.NA],
        })
        payload = build_geojson(frame)
        encoded = json.dumps(payload, allow_nan=False)
        records = json.loads(encoded)["features"]
        self.assertIsNone(records[0]["properties"]["net_power_mw"])
        self.assertEqual(records[0]["properties"]["plant_name"], "NaN is a valid name")
        self.assertIsNone(records[1]["properties"]["plant_name"])

    def test_invalid_coordinates_are_excluded(self):
        frame = pd.DataFrame({
            "latitude": [52.52, float("inf"), 91, None],
            "longitude": [13.4, 10, 10, 10],
        })
        self.assertEqual(len(build_geojson(frame)["features"]), 1)
