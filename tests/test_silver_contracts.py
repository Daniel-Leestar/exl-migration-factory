"""Silver Layer Data Contracts unit tests.
Silver 层数据契约单元测试。

Test Objective: Verify contract model definitions and validation rules.
测试目标: 验证契约模型定义与校验规则。
"""

import unittest
from cloud.silver_contracts import (
    FREMTPL2FREQ_CONTRACT,
    FREMTPL2SEV_CONTRACT,
    ColumnContract,
    TableContract,
)


class TestSilverContracts(unittest.TestCase):
    """Test suite for Silver data contracts specification.
    Silver 层数据契约规范测试套件。
    """

    def test_freq_contract_definitions(self):
        """Verify freMTPL2freq contract contains primary keys and columns.
        验证 freMTPL2freq 契约包含主键与必需列规范。
        """
        contract = FREMTPL2FREQ_CONTRACT
        self.assertEqual(contract.table_name, "freMTPL2freq")
        self.assertIn("IDpol", contract.primary_keys)

        col_names = [col.name for col in contract.columns]
        self.assertIn("IDpol", col_names)
        self.assertIn("Exposure", col_names)
        self.assertIn("ClaimNb", col_names)

    def test_sev_contract_definitions(self):
        """Verify freMTPL2sev contract contains primary keys and amount column.
        验证 freMTPL2sev 契约包含主键与赔款金额列规范。
        """
        contract = FREMTPL2SEV_CONTRACT
        self.assertEqual(contract.table_name, "freMTPL2sev")
        self.assertIn("IDpol", contract.primary_keys)

        col_names = [col.name for col in contract.columns]
        self.assertIn("ClaimAmount", col_names)


if __name__ == "__main__":
    unittest.main()
