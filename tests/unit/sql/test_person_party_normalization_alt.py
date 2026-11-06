from __future__ import annotations

import importlib.util
import sys
from dataclasses import is_dataclass
from pathlib import Path


def load_module():
    filename = Path(__file__).resolve().parents[3] / "pyriksprot/sql/other/05_person_party_normalization_alt.py"
    spec = importlib.util.spec_from_file_location("person_party_normalization_alt", filename)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


normalization_alt = load_module()


def normalize(rows: list[dict]) -> list[dict]:
    return normalization_alt.normalize_person_affiliation_periods(rows)


def test_empty_person_rows_return_empty_list():
    assert normalize([]) == []


def test_part_affiliation_item_is_a_dataclass():
    assert is_dataclass(normalization_alt.PartAffiliationItem)


def test_single_undated_affiliation_stays_open_ended():
    rows = [
        {
            "person_id": "P1",
            "party_id": 1,
            "swerik_party_id": "A",
            "party": "Party A",
            "start": None,
            "end": None,
        }
    ]

    assert normalize(rows) == [
        {
            "person_id": "P1",
            "party_id": 1,
            "swerik_party_id": "A",
            "party": "Party A",
            "start": None,
            "end": None,
            "start_precision": None,
            "end_precision": None,
        }
    ]


def test_undated_same_party_affiliation_opens_matching_dated_period():
    rows = [
        {
            "person_id": "P1",
            "party_id": 1,
            "swerik_party_id": "A",
            "party": "Party A",
            "start": "2000",
            "end": "2001",
        },
        {
            "person_id": "P1",
            "party_id": 1,
            "swerik_party_id": "A",
            "party": "Party A",
            "start": None,
            "end": None,
        },
    ]

    assert normalize(rows) == [
        {
            "person_id": "P1",
            "party_id": 1,
            "swerik_party_id": "A",
            "party": "Party A",
            "start": None,
            "end": None,
            "start_precision": None,
            "end_precision": None,
        }
    ]


def test_unmatched_undated_affiliation_stays_open_ended_with_dated_other_party():
    rows = [
        {
            "person_id": "P1",
            "party_id": 1,
            "swerik_party_id": "A",
            "party": "Party A",
            "start": None,
            "end": None,
        },
        {
            "person_id": "P1",
            "party_id": 2,
            "swerik_party_id": "B",
            "party": "Party B",
            "start": "2000",
            "end": "2001",
        },
    ]

    assert normalize(rows) == [
        {
            "person_id": "P1",
            "party_id": 1,
            "swerik_party_id": "A",
            "party": "Party A",
            "start": None,
            "end": None,
            "start_precision": None,
            "end_precision": None,
        },
        {
            "person_id": "P1",
            "party_id": 2,
            "swerik_party_id": "B",
            "party": "Party B",
            "start": "2000-01-01",
            "end": "2001-12-31",
            "start_precision": "day",
            "end_precision": "day",
        },
    ]


def test_same_party_fragments_are_merged_across_gaps():
    rows = [
        {
            "person_id": "P1",
            "party_id": 1,
            "swerik_party_id": "A",
            "party": "Party A",
            "start": "1978-10-01",
            "end": "1978-10-10",
        },
        {
            "person_id": "P1",
            "party_id": 1,
            "swerik_party_id": "A",
            "party": "Party A",
            "start": "1978-10-15",
            "end": "1978-10-20",
        },
    ]

    assert normalize(rows) == [
        {
            "person_id": "P1",
            "party_id": 1,
            "swerik_party_id": "A",
            "party": "Party A",
            "start": "1978-10-01",
            "end": "1978-10-20",
            "start_precision": "day",
            "end_precision": "day",
        }
    ]


def test_same_year_handoff_with_year_precision_splits_at_midyear():
    rows = [
        {
            "person_id": "P1",
            "party_id": 1,
            "swerik_party_id": "A",
            "party": "Party A",
            "start": "2000",
            "end": "2002",
        },
        {
            "person_id": "P1",
            "party_id": 2,
            "swerik_party_id": "B",
            "party": "Party B",
            "start": "2002",
            "end": "2003",
        },
    ]

    assert normalize(rows) == [
        {
            "person_id": "P1",
            "party_id": 1,
            "swerik_party_id": "A",
            "party": "Party A",
            "start": "2000-01-01",
            "end": "2002-06-30",
            "start_precision": "day",
            "end_precision": "day",
        },
        {
            "person_id": "P1",
            "party_id": 2,
            "swerik_party_id": "B",
            "party": "Party B",
            "start": "2002-07-01",
            "end": "2003-12-31",
            "start_precision": "day",
            "end_precision": "day",
        },
    ]


def test_same_year_handoff_with_outer_missing_bounds_splits_at_midyear():
    rows = [
        {
            "person_id": "P1",
            "party_id": 1,
            "swerik_party_id": "A",
            "party": "Party A",
            "start": None,
            "end": "2002",
        },
        {
            "person_id": "P1",
            "party_id": 2,
            "swerik_party_id": "B",
            "party": "Party B",
            "start": "2002",
            "end": None,
        },
    ]

    assert normalize(rows) == [
        {
            "person_id": "P1",
            "party_id": 1,
            "swerik_party_id": "A",
            "party": "Party A",
            "start": None,
            "end": "2002-06-30",
            "start_precision": None,
            "end_precision": "day",
        },
        {
            "person_id": "P1",
            "party_id": 2,
            "swerik_party_id": "B",
            "party": "Party B",
            "start": "2002-07-01",
            "end": None,
            "start_precision": "day",
            "end_precision": None,
        },
    ]


def test_different_party_overlap_with_day_precision_trims_previous_period():
    rows = [
        {
            "person_id": "P1",
            "party_id": 1,
            "swerik_party_id": "A",
            "party": "Party A",
            "start": "2000-01-01",
            "end": "2002-06-15",
        },
        {
            "person_id": "P1",
            "party_id": 2,
            "swerik_party_id": "B",
            "party": "Party B",
            "start": "2002-06-01",
            "end": "2003-12-31",
        },
    ]

    assert normalize(rows) == [
        {
            "person_id": "P1",
            "party_id": 1,
            "swerik_party_id": "A",
            "party": "Party A",
            "start": "2000-01-01",
            "end": "2002-05-31",
            "start_precision": "day",
            "end_precision": "day",
        },
        {
            "person_id": "P1",
            "party_id": 2,
            "swerik_party_id": "B",
            "party": "Party B",
            "start": "2002-06-01",
            "end": "2003-12-31",
            "start_precision": "day",
            "end_precision": "day",
        },
    ]


def test_missing_bounded_end_is_inferred_from_next_start():
    rows = [
        {
            "person_id": "P1",
            "party_id": 1,
            "swerik_party_id": "A",
            "party": "Party A",
            "start": "2000",
            "end": None,
        },
        {
            "person_id": "P1",
            "party_id": 2,
            "swerik_party_id": "B",
            "party": "Party B",
            "start": "2002",
            "end": "2003",
        },
    ]

    assert normalize(rows)[0] == {
        "person_id": "P1",
        "party_id": 1,
        "swerik_party_id": "A",
        "party": "Party A",
        "start": "2000-01-01",
        "end": "2001-12-31",
        "start_precision": "day",
        "end_precision": "day",
    }
