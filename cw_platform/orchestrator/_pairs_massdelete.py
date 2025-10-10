from __future__ import annotations
from typing import Any, Dict, List

# Single-purpose guard to keep dangerous delete storms at bay.


def maybe_block_mass_delete(
    rem_list: List[Dict[str, Any]],
    baseline_size: int,
    *,
    allow_mass_delete: bool,
    suspect_ratio: float,
    emit,
    dbg,
    dst_name: str,
    feature: str,
) -> List[Dict[str, Any]]:
    """
    If mass deletes are disabled and the planned removals exceed the threshold,
    block them and emit a clear breadcrumb. This function never raises.
    """
    try:
        if allow_mass_delete or not rem_list:
            return rem_list

        ratio = suspect_ratio if suspect_ratio > 0 else 0.10
        threshold = int(baseline_size * ratio)

        if len(rem_list) > max(threshold, 0):
            try:
                emit("mass_delete:blocked",
                     dst=dst_name, feature=feature,
                     attempted=len(rem_list), baseline=baseline_size, threshold=threshold)
            except Exception:
                pass
            try:
                dbg("mass_delete.block",
                    dst=dst_name, feature=feature,
                    attempted=len(rem_list), baseline=baseline_size, threshold=threshold)
            except Exception:
                pass
            return []
    except Exception:
        # Better to let the caller proceed than to crash the run.
        return rem_list

    return rem_list


# Backwards-compat alias
_maybe_block_mass_delete = maybe_block_mass_delete
