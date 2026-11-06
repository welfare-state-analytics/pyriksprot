import re
import calendar
import numpy as np
import pandas as pd


MISSING_MARKERS = {
    "", "unknown", "?", "na", "n/a", "null", "none"
}


def normalize_temporal_dataframe_vectorized(
    df: pd.DataFrame,
    entity_col: str = "entity_id",
    start_col: str = "start",
    end_col: str = "end",
) -> pd.DataFrame:
    required = {entity_col, start_col, end_col}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {sorted(missing)}")

    out = pd.DataFrame({
        entity_col: df[entity_col],
        "start_raw": df[start_col],
        "end_raw": df[end_col],
    })

    def normalize_side(series: pd.Series, side: str):
        raw = series.astype("string").str.strip()
        lower = raw.str.lower()

        is_missing = series.isna() | lower.isin(MISSING_MARKERS)

        is_year = raw.str.fullmatch(r"\d{4}", na=False)
        is_month = raw.str.fullmatch(r"\d{4}-\d{2}", na=False)
        is_day = raw.str.fullmatch(r"\d{4}-\d{2}-\d{2}", na=False)

        date = pd.Series(pd.NaT, index=series.index, dtype="datetime64[ns]")
        precision = pd.Series("invalid", index=series.index, dtype="object")
        warning = pd.Series("", index=series.index, dtype="object")

        precision[is_missing] = "missing"

        # YYYY
        years = raw[is_year].astype(int)
        if side == "start":
            date.loc[is_year] = pd.to_datetime(
                years.astype(str) + "-01-01",
                errors="coerce",
            )
        else:
            date.loc[is_year] = pd.to_datetime(
                years.astype(str) + "-12-31",
                errors="coerce",
            )
        precision.loc[is_year] = "year"

        # YYYY-MM
        months = raw[is_month]
        month_dates = pd.to_datetime(months + "-01", errors="coerce")

        if side == "start":
            date.loc[is_month] = month_dates
        else:
            date.loc[is_month] = month_dates + pd.offsets.MonthEnd(0)

        precision.loc[is_month] = "month"

        # YYYY-MM-DD
        day_dates = pd.to_datetime(raw[is_day], errors="coerce")
        date.loc[is_day] = day_dates
        precision.loc[is_day] = "day"

        # invalid calendar dates among syntactically valid dates/months
        syntactic = is_year | is_month | is_day
        invalid_syntactic = syntactic & date.isna() & ~is_missing
        precision.loc[invalid_syntactic] = "invalid"
        warning.loc[invalid_syntactic] = f"{side}:invalid_temporal_value"

        # unsupported / ambiguous formats
        ambiguous = ~(is_missing | syntactic)
        precision.loc[ambiguous] = "ambiguous"
        warning.loc[ambiguous] = f"{side}:ambiguous_temporal_value"

        return date, precision, warning

    out["start_date"], out["start_precision"], start_warning = normalize_side(
        df[start_col], "start"
    )
    out["end_date"], out["end_precision"], end_warning = normalize_side(
        df[end_col], "end"
    )

    start_missing = out["start_date"].isna()
    end_missing = out["end_date"].isna()

    out["temporal_status"] = np.select(
        [
            start_missing & end_missing,
            start_missing,
            end_missing,
            out["start_date"] > out["end_date"],
        ],
        [
            "unknown_period",
            "open_start",
            "open_end",
            "invalid_interval",
        ],
        default="bounded",
    )

    warnings = (
        start_warning.fillna("").astype(str)
        + ";"
        + end_warning.fillna("").astype(str)
    )

    start_after_end = (
        out["start_date"].notna()
        & out["end_date"].notna()
        & (out["start_date"] > out["end_date"])
    )

    warnings = warnings.mask(
        start_after_end,
        warnings + ";start_after_end",
    )

    out["temporal_warnings"] = (
        warnings
        .str.strip(";")
        .str.replace(r";+", ";", regex=True)
        .replace("", pd.NA)
    )

    return out