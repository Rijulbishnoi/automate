# Broker Payout Pipeline — Phase 1

## 1. Data Audit & Schema Mapping
- [x] Audit all 15 sheets in `HM_DIGIT_FEB26_GRID.xlsx`
- [x] Audit 17 output reference files to extract exact broker portal schema (45 columns)
- [x] Map source headers → target payout schema

## 2. Implementation Plan
- [x] Write implementation plan with all 6 rule systems
- [x] User approved plan

## 3. Core Pipeline (`pipeline.py`)
- [x] Load all 4 TW grid sheets + `2W RTO's` lookup
- [x] Build RTO→Cluster lookup (case-insensitive, comma-joined)
- [x] Segment normalization (SC/EV → SCOOTER+ELETRIC, MC CC-range parsing)
- [x] Make handling (`Others` → `EXCLUDE:` logic)
- [x] Value transform (decimal → %, D→0 for make-specific, MISP→2.5)
- [x] Rule Name generation (`{Cluster}_{BizType}_{CoverType}`)
- [x] Cover Type / Business Type routing per grid sheet

## 4. Geographic Routing (State-wise Output)
- [x] State detection from RTO prefix
- [x] Write 20 state-wise `.xlsx` files matching reference format

## 5. Validation Results
- [x] BIHAR TP rules: **100% match** (14/14 rows)
- [x] BIHAR rule names: **100% match** (7/7 names)
- [x] All core transformations verified working
- [ ] Minor edge cases: EXCLUDE scope for EV-only segments, SC_EV SAOD split count
