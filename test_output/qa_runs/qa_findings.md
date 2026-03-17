# QA Mutation Findings (2026-03-13)

Baseline source: `test_data/HM_DIGIT_MAR26_GRID.xlsx`
Mutated sources: `test_data/qa_cases/*.xlsx`

## Summary
- Legacy engine is stable for all tested mutations (no crashes).
- Legacy vs pandas parity fails even on baseline:
  - Legacy rows: 167
  - Pandas rows: 163
  - Major mismatch bucket: Maharashtra (legacy 36 vs pandas 32)

## Case Results
- baseline:
  - Legacy: pass, 167 rows, 4 files written
  - Compare: fail (167 vs 163; Maharashtra 36 vs 32)
- cluster_whitespace_case (`B5` changed to `" dl_good  "`):
  - Legacy: unchanged behavior
  - Compare: same mismatch pattern as baseline
- segment_spaced_sc_ev (`C5` changed to `"SC / EV"`):
  - Legacy rows dropped to 165
  - Compare: 165 vs 161
  - Limitation: parser special duplication for `SC/EV` is exact-token sensitive
- invalid_cd2_text (`E6` changed to `"N/A"`):
  - Legacy rows dropped to 166
  - Compare: 166 vs 162
  - Limitation: invalid payout values are silently dropped (no warning)
- unknown_mh_rto_prefix (`B8/B9` changed to `ZZ01/ZZ02`):
  - Legacy: pass, 167 rows, only 3 files written, 36 ungrouped
  - Compare: fail (167 vs 163)
  - Limitation: unknown prefixes route rows to ungrouped bucket

## Root-Cause Clue for Baseline Parity Gap
Diff inspection showed pandas misses 4 legacy rows in Maharashtra, all from `MH_RURAL` entries in `TW 1+1 & SATP`.
Likely trigger: pandas reads `N/A` as NaN by default and strips blank/whitespace-only segments that legacy still processes.

## Artifacts
- Structured report JSON: `test_output/qa_runs/qa_summary.json`
- This markdown summary: `test_output/qa_runs/qa_findings.md`
