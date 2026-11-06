import math
from datetime import date, datetime
from typing import Any

def is_year_precision(value: Any, precision: Any = None) -> bool:
    if is_missing(value):
        return False

    value_text: str = str(value).strip()
    precision_text: str = "" if is_missing(precision) else str(precision).strip().lower()

    return precision_text == "year" or is_year_value(value_text)


def is_year_value(value: str) -> bool:
    return len(value) == 4 and value.isdigit()


def format_date(value: Any) -> str | None:
    if is_missing(value):
        return None

    if isinstance(value, datetime):
        return value.date().isoformat()

    if isinstance(value, date):
        return value.isoformat()

    return str(value)



# def format_date(value: Any) -> str | None:
#     if is_missing(value):
#         return None
#     return pd.Timestamp(value).strftime("%Y-%m-%d")


def is_missing(value: Any) -> bool:
    if value is None:
        return True

    if isinstance(value, str):
        return value.strip() == ""

    if isinstance(value, float):
        return math.isnan(value)

    try:
        return bool(value != value)
    except (TypeError, ValueError):
        return False

def is_valid_period(row: dict[str, Any]) -> bool:
    return is_missing(row["_start"]) or is_missing(row["_end"]) or row["_start"] <= row["_end"]
