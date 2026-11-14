"""Build and load the "base" dataset: a fixed sample of the public freMTPL2 data.
建立與載入「base」資料集:公開 freMTPL2 資料的固定樣本。

This is real (public, non-personal) data, so it is marked non-synthetic and is
never shown to an LLM.
這是真實(公開、非個資)資料,因此標記為非合成,絕不會給 LLM 看。
"""
from __future__ import annotations

import csv
from decimal import Decimal
from pathlib import Path

from engine.model import Column, Table, parse_value, read_table, write_table

FREQ_FILE, SEV_FILE = "freMTPL2freq.csv", "freMTPL2sev.csv"


def make_base(data_dir: Path, target_dir: Path, schemas: dict[str, list[Column]],
              every: int = 200, with_claims: int = 600, sample: bool = True) -> dict[str, int]:
    """Sample the raw CSVs into `target_dir/policy.csv` and `claims.csv`.
    從原始 CSV 取樣,寫成 `target_dir/policy.csv` 與 `claims.csv`。

    Picks every `every`-th policy plus the first `with_claims` policies that have claims,
    then every claim of those policies and every 100th claim regardless (so some claims
    have no matching policy, as in the full data). No randomness: same input, same sample.
    取每第 `every` 張保單,加上前 `with_claims` 張有理賠的保單;
    再取這些保單的所有理賠,以及每第 100 筆理賠(因此有些理賠對不到保單,和全量資料一樣)。
    不使用亂數:相同輸入永遠得到相同樣本。
    """
    freq_path, sev_path = Path(data_dir) / FREQ_FILE, Path(data_dir) / SEV_FILE
    for path in (freq_path, sev_path):
        if not path.exists():
            raise FileNotFoundError(f"raw data file not found: {path}")

    policy_columns, claim_columns = schemas["policy"], schemas["claims"]
    policies, kept_ids, claim_holders = [], set(), 0
    for index, record in enumerate(_records(freq_path)):
        row = tuple(parse_value(record[c.name], c.type) for c in policy_columns)
        has_claim = row[1] > 0 and claim_holders < with_claims
        if not sample or index % every == 0 or has_claim:
            claim_holders += has_claim
            policies.append(row)
            kept_ids.add(row[0])

    claims = []
    for index, record in enumerate(_records(sev_path)):
        policy_id = parse_value(record["IDpol"], "int")
        if not sample or policy_id in kept_ids or index % 100 == 0:
            amount = Decimal(record["ClaimAmount"]).quantize(Decimal("0.01"))
            claims.append((policy_id, amount))

    write_table(Table(policy_columns, policies), Path(target_dir) / "policy.csv")
    write_table(Table(claim_columns, claims), Path(target_dir) / "claims.csv")
    return {"policy": len(policies), "claims": len(claims)}


def load_base(target_dir: Path) -> dict[str, Table] | None:
    """Load the base dataset, or None when it has not been built.
    載入 base 資料集;尚未建立時回傳 None。
    """
    target_dir = Path(target_dir)
    files = sorted(target_dir.glob("*.csv")) if target_dir.is_dir() else []
    if not files:
        return None
    return {path.stem: read_table(path, synthetic=False) for path in files}


def _records(path: Path):
    with path.open(newline="", encoding="utf-8-sig") as handle:
        yield from csv.DictReader(handle)
