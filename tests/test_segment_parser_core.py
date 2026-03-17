import pytest

from segment_parser import parse_segment


LLM_ENV_KEYS = [
    "OPENAI_API_KEY",
    "ANTHROPIC_API_KEY",
    "GEMINI_API_KEY",
    "LITELLM_API_KEY",
    "AZURE_API_KEY",
]


@pytest.fixture
def no_llm_keys(monkeypatch):
    for key in LLM_ENV_KEYS:
        monkeypatch.delenv(key, raising=False)


def test_parse_sc_ev_expands_to_two_segments(no_llm_keys):
    parsed = parse_segment("SC/EV")
    assert len(parsed) == 2
    fuels = {item.fuel for item in parsed}
    assert fuels == {"Petrol", "Electric"}
    assert all(item.category == "Scooter" for item in parsed)
    assert all(item.duplication_required for item in parsed)


def test_parse_mc_180_with_makes(no_llm_keys):
    parsed = parse_segment("MC <= 180 Hero/Honda")
    assert len(parsed) == 1
    item = parsed[0]
    assert item.category == "Bike"
    assert item.fuel == "Petrol"
    assert item.cc_range == {"from": 1, "to": 180}
    assert set(item.manufacturers["include"]) == {"HERO MOTOCORP", "HONDA"}


def test_parse_others_make_column_uses_exclusion(no_llm_keys):
    parsed = parse_segment(
        "MC <= 180 Others",
        make_column="Others",
        all_cluster_makes={"HONDA", "TVS"},
        sheet_context="make_specific",
    )
    assert len(parsed) == 1
    item = parsed[0]
    assert item.manufacturers["exclude"] == ["HONDA", "TVS"]


def test_parse_re_maps_to_royal_enfield(no_llm_keys):
    parsed = parse_segment("RE")
    assert len(parsed) == 1
    item = parsed[0]
    assert item.manufacturers["include"] == ["ROYAL ENFIELD"]
    assert item.category == "Bike"


def test_parse_ambiguous_3w_defaults_to_all_petrol(no_llm_keys):
    parsed = parse_segment("3W > 350")
    assert len(parsed) == 1
    item = parsed[0]
    assert item.category == "All"
    assert item.fuel == "Petrol"
