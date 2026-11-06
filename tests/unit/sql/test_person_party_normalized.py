from __future__ import annotations

import importlib.util
from pathlib import Path

import pandas as pd


def load_module():
    filename = Path(__file__).resolve().parents[3] / "pyriksprot/sql/other/05_person_party_normalized.py"
    spec = importlib.util.spec_from_file_location("person_party_normalized", filename)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


normalized = load_module()


def frame(rows: list[dict]) -> pd.DataFrame:
    return pd.DataFrame(rows, columns=["person_id", "party_id", "swerik_party_id", "party", "start", "end"])


def records(df: pd.DataFrame) -> list[dict]:
    return df.astype(object).where(pd.notna(df), None).to_dict("records")


def test_parse_year_precision_expands_start_and_end_boundaries():
    assert normalized.parse_start_date("1982", "year") == pd.Timestamp("1982-01-01")
    assert normalized.parse_end_date("1982", "year") == pd.Timestamp("1982-12-31")


def test_same_party_requires_a_shared_non_conflicting_identifier():
    assert normalized.same_party({"party_id": 1, "party": "A"}, {"party_id": 1, "party": "A"})
    assert not normalized.same_party({"party_id": 1, "party": "A"}, {"party_id": 1, "party": "B"})
    assert not normalized.same_party({"party_id": None, "party": "A"}, {"party_id": None, "party": None})


def test_normalize_empty_input_returns_empty_frame_with_precision_columns():
    result = normalized.normalize_affiliation_periods(frame([]))

    assert result.empty
    assert list(result.columns) == [
        "person_id",
        "party_id",
        "swerik_party_id",
        "party",
        "start",
        "end",
        "start_precision",
        "end_precision",
    ]


def test_single_undated_affiliation_stays_open_ended():
    data = frame(
        [
            {
                "person_id": "P1",
                "party_id": 1,
                "swerik_party_id": "A",
                "party": "Party A",
                "start": None,
                "end": None,
            }
        ]
    )

    assert records(normalized.normalize_affiliation_periods(data)) == [
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
    data = frame(
        [
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
    )

    assert records(normalized.normalize_affiliation_periods(data)) == [
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


def test_undated_same_party_affiliation_opens_matching_dated_period():
    data = frame(
        [
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
    )

    assert records(normalized.normalize_affiliation_periods(data)) == [
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


def test_same_party_fragments_are_merged_across_gaps():
    data = frame(
        [
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
    )

    assert records(normalized.normalize_affiliation_periods(data)) == [
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
    data = frame(
        [
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
    )

    assert records(normalized.normalize_affiliation_periods(data)) == [
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
    data = frame(
        [
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
    )

    assert records(normalized.normalize_affiliation_periods(data)) == [
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
    data = frame(
        [
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
    )

    assert records(normalized.normalize_affiliation_periods(data)) == [
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
    data = frame(
        [
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
    )

    assert records(normalized.normalize_affiliation_periods(data))[0] == {
        "person_id": "P1",
        "party_id": 1,
        "swerik_party_id": "A",
        "party": "Party A",
        "start": "2000-01-01",
        "end": "2001-12-31",
        "start_precision": "day",
        "end_precision": "day",
    }
