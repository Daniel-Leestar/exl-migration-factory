"""Shared test fixtures.
共用的測試設備。
"""
from pathlib import Path

import pytest

from engine.model import Column

ROOT = Path(__file__).resolve().parent.parent


@pytest.fixture(scope="session")
def schemas():
    """Two small input tables shaped like the real ones.
    兩張與真實資料同形狀的小型輸入表。
    """
    return {
        "policy": [
            Column("IDpol", "int"), Column("ClaimNb", "int"), Column("Exposure", "float"),
            Column("VehAge", "int"), Column("BonusMalus", "int"), Column("VehGas", "string"), Column("Region", "string"),
        ],
        "claims": [Column("IDpol", "int"), Column("ClaimAmount", "decimal(18,2)")],
    }


@pytest.fixture(scope="session")
def spark():
    """One Spark session for the whole test run (starting it takes several seconds).
    整個測試過程共用一個 Spark session(啟動需要好幾秒)。
    """
    from engine.sandbox import get_spark

    return get_spark()
