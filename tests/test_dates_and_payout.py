from datetime import datetime
import calendar

from pipeline import auto_detect_dates, cd2_to_payout, get_state_from_rtos


def test_auto_detect_dates_feb26():
    start, end = auto_detect_dates("HM_DIGIT_FEB26_GRID.xlsx")
    assert start == "2026-02-01"
    assert end == "2026-02-28"


def test_auto_detect_dates_mar2026():
    start, end = auto_detect_dates("my_grid_mar2026.xlsx")
    assert start == "2026-03-01"
    assert end == "2026-03-31"


def test_auto_detect_dates_jan_apostrophe_26():
    start, end = auto_detect_dates("Digit Jan'26_h&M.xlsx")
    assert start == "2026-01-01"
    assert end == "2026-01-31"


def test_auto_detect_dates_fallback_current_month():
    start, end = auto_detect_dates("grid_without_month_hint.xlsx")
    start_dt = datetime.strptime(start, "%Y-%m-%d")
    end_dt = datetime.strptime(end, "%Y-%m-%d")

    now = datetime.now()
    expected_end_day = calendar.monthrange(now.year, now.month)[1]

    assert start_dt.year == now.year
    assert start_dt.month == now.month
    assert start_dt.day == 1

    assert end_dt.year == now.year
    assert end_dt.month == now.month
    assert end_dt.day == expected_end_day


def test_cd2_to_payout_decimal_value():
    assert cd2_to_payout(0.075) == "7.5"
    assert cd2_to_payout(0.1) == "10"


def test_cd2_to_payout_special_values():
    assert cd2_to_payout("D", skip_d=True) is None
    assert cd2_to_payout("D", skip_d=False) == "0"
    assert cd2_to_payout("MISP") == "2.5"


def test_cd2_to_payout_invalid_string():
    assert cd2_to_payout("abc") is None


def test_get_state_from_rtos_multiple_prefixes():
    states = get_state_from_rtos(["AP01", "TS09", "AP16", "AR03"])
    assert states == "ANDHRA PRADESH, TELANGANA, ARUNACHAL PRADESH"
