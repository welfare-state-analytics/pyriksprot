from __future__ import annotations

from typing import Any

import pandas as pd
from .normalize_person import normalize_person_affiliation_periods
from .utility import format_date, is_year_precision, is_missing

DEFAULT_PARTY_COLS = ("party_id", "swerik_party_id", "party")
INTERNAL_COLUMNS = (
    "_start",
    "_end",
    "_open_start",
    "_open_end",
    "_start_is_year_precision",
    "_end_is_year_precision",
    "_sort_start",
    "_sort_end",
)


def format_normalized_rows(
    rows: list[dict[str, Any]],
    *,
    source_columns: list[str],
    person_col: str,
    start_col: str,
    end_col: str,
    start_precision_col: str,
    end_precision_col: str,
) -> pd.DataFrame:
    if not rows:
        return empty_output_frame(source_columns, start_precision_col, end_precision_col)

    result = pd.DataFrame(rows)
    result["_sort_start"] = [pd.Timestamp.min if is_missing(value) else value for value in result["_start"]]
    result["_sort_end"] = [pd.Timestamp.max if is_missing(value) else value for value in result["_end"]]
    result = result.sort_values([person_col, "_sort_start", "_sort_end"]).reset_index(drop=True)

    result[start_col] = [format_date(value) for value in result["_start"]]
    result[end_col] = [format_date(value) for value in result["_end"]]
    result[start_precision_col] = ["day" if not is_missing(value) else pd.NA for value in result[start_col]]
    result[end_precision_col] = ["day" if not is_missing(value) else pd.NA for value in result[end_col]]
    result = result.drop(columns=[column for column in INTERNAL_COLUMNS if column in result])

    return result[output_columns(source_columns, start_precision_col, end_precision_col)]

def empty_output_frame(
    source_columns: list[str],
    start_precision_col: str,
    end_precision_col: str,
) -> pd.DataFrame:
    return pd.DataFrame(columns=output_columns(source_columns, start_precision_col, end_precision_col))


def optional_column(df: pd.DataFrame, column: str) -> pd.Series:
    if column in df:
        return df[column]
    return pd.Series(pd.NA, index=df.index, dtype="object")

def output_columns(
    source_columns: list[str],
    start_precision_col: str,
    end_precision_col: str,
) -> list[str]:
    columns = [column for column in source_columns if not column.startswith("_")]

    for column in (start_precision_col, end_precision_col):
        if column not in columns:
            columns.append(column)

    return columns


def parse_start_date(value: Any, precision: Any = None) -> Any:
    return parse_date_boundary(value, precision, is_start=True)


def parse_end_date(value: Any, precision: Any = None) -> Any:
    return parse_date_boundary(value, precision, is_start=False)


def parse_date_boundary(value: Any, precision: Any = None, *, is_start: bool) -> Any:
    if is_missing(value):
        return pd.NaT

    value_text = str(value).strip()
    precision_text = "" if is_missing(precision) else str(precision).strip().lower()

    if is_year_precision(value_text, precision_text):
        year = value_text[:4]
        if not year.isdigit():
            return pd.NaT
        return pd.Timestamp(f"{year}-{'01-01' if is_start else '12-31'}")

    return pd.to_datetime(value_text, errors="coerce")

def with_normalized_dates(
    df: pd.DataFrame,
    *,
    start_col: str,
    end_col: str,
    start_precision_col: str,
    end_precision_col: str,
) -> pd.DataFrame:
    result = df.copy()
    start_precisions = optional_column(result, start_precision_col)
    end_precisions = optional_column(result, end_precision_col)

    result["_start"] = [
        parse_start_date(value, precision) for value, precision in zip(result[start_col], start_precisions)
    ]
    result["_end"] = [parse_end_date(value, precision) for value, precision in zip(result[end_col], end_precisions)]
    result["_start_is_year_precision"] = [
        is_year_precision(value, precision) for value, precision in zip(result[start_col], start_precisions)
    ]
    result["_end_is_year_precision"] = [
        is_year_precision(value, precision) for value, precision in zip(result[end_col], end_precisions)
    ]
    result["_open_start"] = False
    result["_open_end"] = False

    return result


def normalize_affiliation_periods(
    df: pd.DataFrame,
    person_col="person_id",
    party_cols=DEFAULT_PARTY_COLS,
    start_col="start",
    end_col="end",
    start_precision_col="start_precision",
    end_precision_col="end_precision",
) -> pd.DataFrame:
    """
    Normalize party affiliation periods into day-level, mostly non-overlapping intervals.

    Year-only starts expand to YYYY-01-01 and year-only ends expand to
    YYYY-12-31. Fully undated rows are treated as open-ended evidence: if they
    match a dated row for the same party, that dated row becomes open-ended; if
    they do not match a dated row, they are preserved as open-ended rows.
    Consecutive same-party rows are merged. Bounded overlaps between different
    parties are resolved by truncating the earlier row, except that same-year
    handoffs based on year precision split at YYYY-06-30 / YYYY-07-01.
    """
    source_columns = list(df.columns)
    dated_df = with_normalized_dates(
        df,
        start_col=start_col,
        end_col=end_col,
        start_precision_col=start_precision_col,
        end_precision_col=end_precision_col,
    )

    output_rows: list[dict[str, Any]] = []

    for _, group in dated_df.groupby(person_col, dropna=False, sort=False):
        person_rows = group.to_dict("records")
        output_rows.extend(normalize_person_affiliation_periods(person_rows))

    return format_normalized_rows(
        output_rows,
        source_columns=source_columns,
        person_col=person_col,
        start_col=start_col,
        end_col=end_col,
        start_precision_col=start_precision_col,
        end_precision_col=end_precision_col,
    )
