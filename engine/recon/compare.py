"""Compare the legacy output with the migrated output under fixed rules.
依固定規則比對舊系統輸出與遷移後輸出。

Rules (also written into every report):
- row order is ignored; column order is ignored; column names match ignoring case
- with a primary key: rows are paired by key, then compared cell by cell
- without one (or when keys repeat): every distinct row must appear the same number of times
- DECIMAL / integer cells must be exactly equal; FLOAT cells may differ by 1e-9
規則(同時寫入每份報告):
- 不比列順序、不比欄順序;欄名不分大小寫
- 有主鍵:依主鍵配對後逐格比對
- 沒有主鍵(或主鍵重複):每一種列出現的次數必須相同
- DECIMAL 與整數必須完全相等;FLOAT 容許 1e-9 誤差
"""
from __future__ import annotations

import datetime as dt
import math
from collections import Counter
from dataclasses import dataclass, field
from decimal import Decimal

from engine.model import Table, is_numeric

SAMPLE_LIMIT = 20


@dataclass
class CompareRules:
    primary_key: list[str] = field(default_factory=list)
    ci_columns: list[str] = field(default_factory=list)
    float_tolerance: float = 1e-9


def compare(legacy: Table, new: Table, rules: CompareRules) -> dict:
    """Return a JSON-ready report; `passed` is True only if every check holds.
    回傳可直接存成 JSON 的報告;所有檢查都成立時 `passed` 才為 True。
    """
    legacy_idx = {c.name.lower(): i for i, c in enumerate(legacy.columns)}
    new_idx = {c.name.lower(): i for i, c in enumerate(new.columns)}
    common = [c.name for c in legacy.columns if c.name.lower() in new_idx]
    schema = {
        "missing_in_new": [c.name for c in legacy.columns if c.name.lower() not in new_idx],
        "extra_in_new": [c.name for c in new.columns if c.name.lower() not in legacy_idx],
    }
    schema["ok"] = not schema["missing_in_new"] and not schema["extra_in_new"]

    ci = {c.lower() for c in rules.ci_columns}
    kinds = {}
    for name in common:
        types = (legacy.columns[legacy_idx[name.lower()]].type, new.columns[new_idx[name.lower()]].type)
        if name.lower() in ci:
            kinds[name] = "ci_text"
        elif "float" in types:
            kinds[name] = "float"
        elif "string" in types:
            kinds[name] = "text"
        else:
            kinds[name] = "exact"

    def project(table: Table, index: dict) -> list[tuple]:
        positions = [index[name.lower()] for name in common]
        return [tuple(_normalise(row[p], kinds[n]) for p, n in zip(positions, common)) for row in table.rows]

    legacy_rows, new_rows = project(legacy, legacy_idx), project(new, new_idx)
    tol = rules.float_tolerance

    row_count = {"legacy": len(legacy_rows), "new": len(new_rows), "ok": len(legacy_rows) == len(new_rows)}
    totals = _totals(common, kinds, legacy, legacy_idx, legacy_rows, new_rows, tol)

    key_positions = _key_positions(rules.primary_key, common)
    mode, note = "multiset", ""
    if rules.primary_key and key_positions is None:
        note = "primary key column missing from output; compared as whole rows"
    elif key_positions is not None:
        legacy_keyed, new_keyed = _by_key(legacy_rows, key_positions), _by_key(new_rows, key_positions)
        if legacy_keyed is None or new_keyed is None:
            note = "primary key values repeat; compared as whole rows"
        else:
            mode = "primary_key"
    if mode == "primary_key":
        rows, samples = _diff_keyed(legacy_keyed, new_keyed, common, kinds, key_positions, tol)
    else:
        rows, samples = _diff_multiset(legacy_rows, new_rows, common, kinds)

    rows["ok"] = not (rows["missing"] or rows["extra"] or rows["different"])
    passed = schema["ok"] and row_count["ok"] and rows["ok"] and all(t["ok"] for t in totals.values())
    return {
        "passed": passed,
        "mode": mode,
        "note": note,
        "rules": {
            "primary_key": rules.primary_key,
            "case_insensitive_columns": rules.ci_columns,
            "float_tolerance": tol,
            "row_order": "ignored",
        },
        "checks": {"schema": schema, "row_count": row_count, "totals": totals, "rows": rows},
        "samples": samples,
    }


def _normalise(value, kind: str):
    if value is None:
        return None
    if isinstance(value, bool):
        value = int(value)
    if kind == "float":
        return float(value)
    if kind == "ci_text":
        return str(value).rstrip().upper()
    if kind == "text":
        return str(value)
    if isinstance(value, (int, float, Decimal)):
        # Decimal("1.50") == Decimal("1.5") and both hash alike, so scale differences are ignored.
        # Decimal("1.50") 與 Decimal("1.5") 相等且雜湊相同,因此不受小數位數寫法影響。
        return Decimal(str(value))
    return value


def _same(a, b, kind: str, tol: float) -> bool:
    if a is None or b is None:
        return a is None and b is None
    if kind == "float":
        if math.isnan(a) or math.isnan(b):
            return math.isnan(a) and math.isnan(b)
        return math.isclose(a, b, rel_tol=tol, abs_tol=tol)
    return a == b


def _totals(common, kinds, legacy, legacy_idx, legacy_rows, new_rows, tol) -> dict:
    totals = {}
    for pos, name in enumerate(common):
        if kinds[name] not in ("float", "exact") or not is_numeric(legacy.columns[legacy_idx[name.lower()]].type):
            continue
        sums = []
        for rows in (legacy_rows, new_rows):
            values = [r[pos] for r in rows if r[pos] is not None]
            sums.append(math.fsum(values) if kinds[name] == "float" else sum(values, Decimal(0)))
        totals[name] = {"legacy": _json(sums[0]), "new": _json(sums[1]), "ok": _same(sums[0], sums[1], kinds[name], tol)}
    return totals


def _key_positions(primary_key, common):
    if not primary_key:
        return None
    lowered = [c.lower() for c in common]
    try:
        return [lowered.index(k.lower()) for k in primary_key]
    except ValueError:
        return None


def _by_key(rows, key_positions):
    """Index rows by key; None when a key repeats.
    依主鍵建立索引;主鍵重複時回傳 None。
    """
    keyed = {}
    for row in rows:
        key = tuple(row[p] for p in key_positions)
        if key in keyed:
            return None
        keyed[key] = row
    return keyed


def _diff_keyed(legacy_keyed, new_keyed, common, kinds, key_positions, tol):
    key_names = [common[p] for p in key_positions]
    missing = [k for k in legacy_keyed if k not in new_keyed]
    extra = [k for k in new_keyed if k not in legacy_keyed]
    different = []
    for key, legacy_row in legacy_keyed.items():
        new_row = new_keyed.get(key)
        if new_row is None:
            continue
        cells = [
            {"column": name, "legacy": _json(a), "new": _json(b)}
            for name, a, b in zip(common, legacy_row, new_row)
            if not _same(a, b, kinds[name], tol)
        ]
        if cells:
            different.append({"key": dict(zip(key_names, map(_json, key))), "cells": cells})
    samples = {
        "missing": [dict(zip(common, map(_json, legacy_keyed[k]))) for k in missing[:SAMPLE_LIMIT]],
        "extra": [dict(zip(common, map(_json, new_keyed[k]))) for k in extra[:SAMPLE_LIMIT]],
        "different": different[:SAMPLE_LIMIT],
    }
    return {"missing": len(missing), "extra": len(extra), "different": len(different)}, samples


def _diff_multiset(legacy_rows, new_rows, common, kinds):
    def hashable(row):
        # Floats cannot be hashed "with tolerance", so they are rounded to 9 significant digits here.
        # 浮點數無法「帶容忍度」做雜湊,這裡改為四捨五入到 9 位有效數字。
        return tuple(
            float(f"{v:.9g}") if kinds[n] == "float" and v is not None else v for v, n in zip(row, common)
        )

    legacy_bag, new_bag = Counter(map(hashable, legacy_rows)), Counter(map(hashable, new_rows))
    missing, extra = legacy_bag - new_bag, new_bag - legacy_bag

    def sample(bag):
        return [{"row": dict(zip(common, map(_json, row))), "times": n} for row, n in list(bag.items())[:SAMPLE_LIMIT]]

    counts = {"missing": sum(missing.values()), "extra": sum(extra.values()), "different": 0}
    return counts, {"missing": sample(missing), "extra": sample(extra), "different": []}


def _json(value):
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, (dt.date, dt.datetime)):
        return value.isoformat()
    return value
