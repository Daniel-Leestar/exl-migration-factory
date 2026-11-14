"""Shrink failing input tables to the fewest rows that still show the difference.
把造成不一致的輸入表縮減到仍能重現差異的最少列數。

This is the classic "delta debugging" idea: drop a chunk of rows, re-run both
systems, and keep the smaller input whenever the mismatch is still there.
這是經典的 delta debugging 做法:拿掉一塊資料列後兩邊重跑,
只要不一致仍然存在,就保留較小的輸入。
"""
from __future__ import annotations

from typing import Callable

from engine.model import Table


def shrink(
    inputs: dict[str, Table],
    still_fails: Callable[[dict[str, Table]], bool],
    max_trials: int = 120,
) -> tuple[dict[str, Table], int]:
    """Return (smallest failing inputs found, number of re-runs used).
    回傳(找到的最小失敗輸入, 重跑次數)。

    `still_fails` must return True only when the mismatch is reproduced; any other
    outcome (match, legacy error, crash) counts as "not this failure".
    `still_fails` 只有在重現不一致時才回傳 True;其他結果(一致、舊系統報錯、當掉)都算「不是這個失敗」。
    """
    rows = [(name, i) for name in sorted(inputs) for i in range(len(inputs[name].rows))]
    cache: dict[frozenset, bool] = {}
    trials = 0

    def build(kept: list) -> dict[str, Table]:
        wanted = set(kept)
        return {
            name: table.with_rows([row for i, row in enumerate(table.rows) if (name, i) in wanted])
            for name, table in inputs.items()
        }

    def fails(kept: list) -> bool:
        nonlocal trials
        key = frozenset(kept)
        if key not in cache:
            trials += 1
            cache[key] = bool(still_fails(build(kept)))
        return cache[key]

    chunks = 2
    while len(rows) >= 2 and trials < max_trials:
        size = max(1, len(rows) // chunks)
        pieces = [rows[i:i + size] for i in range(0, len(rows), size)]
        reduced = False
        for piece in pieces:
            dropped = set(piece)
            remaining = [r for r in rows if r not in dropped]
            if trials >= max_trials:
                break
            if fails(remaining):
                rows, chunks, reduced = remaining, max(chunks - 1, 2), True
                break
        if not reduced:
            if size == 1:
                break
            chunks = min(len(rows), chunks * 2)
    # Some mistakes show up even on empty tables (for example an empty SUM returning 0 instead of NULL).
    # 有些錯誤連空表都會出現(例如空集合的 SUM 回傳 0 而不是 NULL)。
    if len(rows) == 1 and trials < max_trials and fails([]):
        rows = []
    return build(rows), trials
