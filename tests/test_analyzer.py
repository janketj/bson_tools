import unittest
from pathlib import Path
from ..processor.analyzer import BSONAnalyzer

class TestBSONAnalyzer(unittest.TestCase):
    def setUp(self):
        self.test_file = Path(__file__).parent / 'data' / 'test.bson'
        self.analyzer = BSONAnalyzer(str(self.test_file))

    def test_analyze(self):
        stats = self.analyzer.analyze()
        self.assertIn('total_documents', stats)
        self.assertIn('field_names', stats)
        self.assertIn('data_types', stats)

    def test_compare(self):
        other_file = Path(__file__).parent / 'data' / 'test2.bson'
        diff = self.analyzer.compare(str(other_file))
        self.assertIn('document_count_diff', diff)
        self.assertIn('field_differences', diff)
