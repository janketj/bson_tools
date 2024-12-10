import unittest
from pathlib import Path
from ..processor.transformer import BSONTransformer

class TestBSONTransformer(unittest.TestCase):
    def setUp(self):
        self.test_file = Path(__file__).parent / 'data' / 'test.bson'
        self.output_file = Path(__file__).parent / 'data' / 'output.bson'
        self.transformer = BSONTransformer(str(self.test_file), str(self.output_file))

    def tearDown(self):
        if self.output_file.exists():
            self.output_file.unlink()

    def test_export_json(self):
        json_output = self.output_file.with_suffix('.json')
        self.transformer.export_json()
        self.assertTrue(json_output.exists())

    def test_deduplicate(self):
        self.transformer.deduplicate()
        self.assertTrue(self.output_file.exists())
