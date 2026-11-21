import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


class StaticBuildTests(unittest.TestCase):
    def test_astro_build_preserves_exported_data(self):
        with tempfile.TemporaryDirectory() as directory:
            publish = Path(directory) / 'dist'
            data = publish / 'data'
            data.mkdir(parents=True)
            sentinel = data / 'bess.geojson'
            sentinel.write_text('{"type":"FeatureCollection","features":[]}')
            subprocess.run([sys.executable, '-c',
                            'from marktstammdatenregister.build_static_site import main; main()',
                            '--publish-dir', str(publish)], check=True, capture_output=True)
            self.assertEqual(sentinel.read_text(), '{"type":"FeatureCollection","features":[]}')
            self.assertIn('Battery storage, in perspective.', (publish / 'index.html').read_text())
            self.assertTrue(list((publish / '_astro').glob('*.js')))
            self.assertEqual((publish / 'CNAME').read_text().strip(), 'bess-atlas.yeah-cool.io')
