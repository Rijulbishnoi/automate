import subprocess
import sys
from pathlib import Path

import pytest

pytest.importorskip("openpyxl")
pytest.importorskip("pandas")

import openpyxl

from pipeline import (
    auto_detect_dates,
    auto_detect_sheets,
    compare_engine_outputs,
    reset_hitl_queue,
    run_legacy_engine,
    run_pandas_engine,
    snapshot_hitl_queue,
)


LLM_ENV_KEYS = [
    "OPENAI_API_KEY",
    "ANTHROPIC_API_KEY",
    "GEMINI_API_KEY",
    "LITELLM_API_KEY",
    "AZURE_API_KEY",
]


def _dataset_path() -> Path:
    return Path(__file__).resolve().parents[1] / "test_data" / "HM_DIGIT_MAR26_GRID.xlsx"


def test_engine_parity_mar26(monkeypatch):
    for key in LLM_ENV_KEYS:
        monkeypatch.delenv(key, raising=False)

    source_path = _dataset_path()
    if not source_path.exists():
        pytest.skip("MAR26 parity dataset is missing")

    effect_start, effect_end = auto_detect_dates(str(source_path))
    wb = openpyxl.load_workbook(str(source_path), data_only=True)
    sheets = auto_detect_sheets(wb)
    assert "rto_2w" in sheets

    reset_hitl_queue()
    legacy_rows = run_legacy_engine(wb, sheets, effect_start, effect_end)
    legacy_hitl = snapshot_hitl_queue()

    reset_hitl_queue()
    pandas_rows = run_pandas_engine(str(source_path), sheets, effect_start, effect_end)
    pandas_hitl = snapshot_hitl_queue()

    ok, messages = compare_engine_outputs(legacy_rows, pandas_rows, legacy_hitl, pandas_hitl)
    assert ok, "\n".join(messages)


def test_cli_legacy_and_pandas_and_compare(monkeypatch, tmp_path):
    for key in LLM_ENV_KEYS:
        monkeypatch.delenv(key, raising=False)

    source_path = _dataset_path()
    if not source_path.exists():
        pytest.skip("MAR26 parity dataset is missing")

    pipeline_path = Path(__file__).resolve().parents[1] / "pipeline.py"

    commands = [
        [sys.executable, str(pipeline_path), str(source_path), "--dry-run", "--engine", "legacy"],
        [sys.executable, str(pipeline_path), str(source_path), "--dry-run", "--engine", "pandas"],
        [sys.executable, str(pipeline_path), str(source_path), "--dry-run", "--compare-engine"],
    ]

    for cmd in commands:
        run = subprocess.run(cmd, cwd=str(Path(__file__).resolve().parents[1]), capture_output=True, text=True)
        assert run.returncode == 0, f"Command failed: {' '.join(cmd)}\nSTDOUT:\n{run.stdout}\nSTDERR:\n{run.stderr}"
