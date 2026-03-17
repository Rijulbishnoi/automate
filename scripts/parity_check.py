#!/usr/bin/env python3
"""Run strict parity checks between legacy and pandas engines."""

import argparse
import sys
from pathlib import Path

import openpyxl

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from pipeline import (
    auto_detect_dates,
    auto_detect_sheets,
    compare_engine_outputs,
    reset_hitl_queue,
    run_legacy_engine,
    run_pandas_engine,
    snapshot_hitl_queue,
)


def main():
    parser = argparse.ArgumentParser(description="Run legacy vs pandas parity checks")
    parser.add_argument("input", help="Path to source workbook")
    parser.add_argument("--start", help="Effect start date (YYYY-MM-DD)")
    parser.add_argument("--end", help="Effect end date (YYYY-MM-DD)")
    args = parser.parse_args()

    source_path = Path(args.input)
    if not source_path.exists():
        print(f"✗ File not found: {source_path}")
        return 1

    if args.start and args.end:
        effect_start, effect_end = args.start, args.end
    else:
        effect_start, effect_end = auto_detect_dates(str(source_path))
        if args.start:
            effect_start = args.start
        if args.end:
            effect_end = args.end

    print("=" * 60)
    print("  PARITY CHECK")
    print("=" * 60)
    print(f"  Input:  {source_path}")
    print(f"  Period: {effect_start} to {effect_end}")

    wb = openpyxl.load_workbook(str(source_path), data_only=True)
    sheets = auto_detect_sheets(wb)
    if "rto_2w" not in sheets:
        print("✗ Could not find 2W RTO sheet. Aborting.")
        return 1

    print("\nRunning legacy engine...")
    reset_hitl_queue()
    legacy_rows = run_legacy_engine(wb, sheets, effect_start, effect_end)
    legacy_hitl = snapshot_hitl_queue()

    print("\nRunning pandas engine...")
    reset_hitl_queue()
    try:
        pandas_rows = run_pandas_engine(str(source_path), sheets, effect_start, effect_end)
    except RuntimeError as exc:
        print(f"✗ {exc}")
        return 1
    pandas_hitl = snapshot_hitl_queue()

    ok, messages = compare_engine_outputs(legacy_rows, pandas_rows, legacy_hitl, pandas_hitl)
    print("\nParity diagnostics:")
    for message in messages:
        print(f"  - {message}")

    if not ok:
        print("\n✗ PARITY CHECK FAILED")
        return 2

    print("\n✓ PARITY CHECK PASSED")
    return 0


if __name__ == "__main__":
    sys.exit(main())
