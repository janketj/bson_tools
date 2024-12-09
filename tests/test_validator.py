import unittest
from pathlib import Path
from ..processor.validator import BSONValidator

class TestBSONValidator(unittest.TestCase):
    def setUp(self):
        self.test_file = Path(__file__).parent / 'data' / 'test.bson'
        self.validator = BSONValidator(str(self.test_file))

    def test_validate(self):
        report = self.validator.validate()
        self.assertIn('valid_documents', report)
        self.assertIn('invalid_documents', report)
        self.assertIn('errors', report)
        self.assertIn('warnings', report)
