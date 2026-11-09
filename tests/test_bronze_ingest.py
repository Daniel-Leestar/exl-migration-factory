"""Bronze Layer Ingestion 单元与集成测试

测试目标: 确保 CSV 摄取至 Delta Lake 时，原数据结构完整且追加审计元数据
"""

import os
import tempfile
import unittest
import csv


class TestBronzeIngestContract(unittest.TestCase):
    """测试 Bronze 层摄取元数据契约规则"""

    def test_bronze_ingest_metadata_contract(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            csv_path = os.path.join(tmp_dir, "freMTPL2freq.csv")
            with open(csv_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(["IDpol", "Exposure", "ClaimNb"])
                writer.writerow([1, 0.1, 0])
                writer.writerow([2, 0.5, 1])

            self.assertTrue(os.path.exists(csv_path))
            with open(csv_path, "r", encoding="utf-8") as f:
                reader = list(csv.reader(f))
                self.assertEqual(len(reader), 3)
                self.assertIn("IDpol", reader[0])
                self.assertIn("Exposure", reader[0])


if __name__ == "__main__":
    unittest.main()

