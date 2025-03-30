import unittest
from unittest.mock import patch
from ingestion import SnowflakeIngestion

class TestIngestion(unittest.TestCase):

    def setUp(self):
        self.ingestor = SnowflakeIngestion()

    def test_generate_record_hash_consistency(self):
        record = {"country": "Canada", "cases": 100}
        hash1 = self.ingestor.generate_record_hash(record)
        hash2 = self.ingestor.generate_record_hash(record)
        self.assertEqual(hash1, hash2)  # Same input, same hash

    def test_generate_record_hash_variation(self):
        record1 = {"country": "Canada", "cases": 100}
        record2 = {"country": "Canada", "cases": 101}
        hash1 = self.ingestor.generate_record_hash(record1)
        hash2 = self.ingestor.generate_record_hash(record2)
        self.assertNotEqual(hash1, hash2)  # Different records, different hashes

if __name__ == '__main__':
    unittest.main()
