from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from typing import Any, Iterable

from pyriksprot.metadata.party.utility import format_date, is_missing, is_year_precision

DEFAULT_PARTY_COLS = ("party_id", "swerik_party_id", "party")


@dataclass
class AffiliationPeriod:
    data: dict[str, Any]
    start_col: str = "start"
    end_col: str = "end"
    start_precision_col: str = "start_precision"
    end_precision_col: str = "end_precision"

    start: date | None = field(init=False)
    end: date | None = field(init=False)
    open_start: bool = field(init=False, default=False)
    open_end: bool = field(init=False, default=False)
    start_is_year_precision: bool = field(init=False, default=False)
    end_is_year_precision: bool = field(init=False, default=False)

    def __post_init__(self) -> None:
        self.data = self.data.copy()

        raw_start = self.data.get(self.start_col)
        raw_end = self.data.get(self.end_col)
        start_precision = self.data.get(self.start_precision_col)
        end_precision = self.data.get(self.end_precision_col)

        self.start, self.start_is_year_precision = parse_boundary(raw_start, start_precision, is_start=True)
        self.end, self.end_is_year_precision = parse_boundary(raw_end, end_precision, is_start=False)

        if self.start is None and self.end is None:
            self.open_start = True
            self.open_end = True
            self.start_is_year_precision = False
            self.end_is_year_precision = False

    def to_dict(self) -> dict[str, Any]:
        result = self.data.copy()

        result[self.start_col] = format_date(self.start)
        result[self.end_col] = format_date(self.end)
        result[self.start_precision_col] = "day" if self.start else None
        result[self.end_precision_col] = "day" if self.end else None

        return result

    def is_valid_period(self) -> bool:
        return self.start is None or self.end is None or self.start <= self.end


def normalize_person_affiliation_periods(
    rows: list[dict[str, Any]],
    *,
    party_cols: Iterable[str] = DEFAULT_PARTY_COLS,
    start_col: str = "start",
    end_col: str = "end",
    start_precision_col: str = "start_precision",
    end_precision_col: str = "end_precision",
) -> list[dict[str, Any]]:
    party_cols = tuple(party_cols)

    items: list[AffiliationPeriod] = [
        AffiliationPeriod(
            row,
            start_col=start_col,
            end_col=end_col,
            start_precision_col=start_precision_col,
            end_precision_col=end_precision_col,
        )
        for row in rows
    ]

    items = sorted(items, key=lambda item: period_sort_key(item, party_cols))
    return [item.to_dict() for item in reduce_periods(items, party_cols)]


def reduce_periods(
    items: list[AffiliationPeriod], party_cols: Iterable[str] = DEFAULT_PARTY_COLS
) -> list[AffiliationPeriod]:
    party_cols = tuple(party_cols)
    output: list[AffiliationPeriod] = []
    active: AffiliationPeriod | None = None

    for item in items:
        if active is None:
            active = item
            continue

        if same_party(active, item, party_cols):
            merge_same_party(active, item)
            continue

        infer_current_start(active, item)
        resolve_handoff(active, item)

        if active.is_valid_period():
            output.append(active)

        active = item

    if active is not None and active.is_valid_period():
        output.append(active)

    return output


def merge_same_party(target: AffiliationPeriod, source: AffiliationPeriod) -> None:
    merge_boundary(target, source, boundary="start")
    merge_boundary(target, source, boundary="end")
    fill_missing_values(target, source)


def merge_boundary(target: AffiliationPeriod, source: AffiliationPeriod, *, boundary: str) -> None:
    is_start: bool = boundary == "start"

    target_value = getattr(target, boundary)
    source_value = getattr(source, boundary)
    open_attr: str = f"open_{boundary}"
    precision_attr: str = f"{boundary}_is_year_precision"

    if getattr(target, open_attr) or getattr(source, open_attr) or target_value is None or source_value is None:
        setattr(target, boundary, None)
        setattr(target, open_attr, True)
        setattr(target, precision_attr, False)
        return

    should_replace: bool = source_value < target_value if is_start else source_value > target_value

    if should_replace:
        setattr(target, boundary, source_value)
        setattr(target, precision_attr, getattr(source, precision_attr))
    elif source_value == target_value:
        setattr(
            target,
            precision_attr,
            getattr(target, precision_attr) or getattr(source, precision_attr),
        )


def infer_current_start(previous: AffiliationPeriod, current: AffiliationPeriod) -> None:
    if current.start is None and not current.open_start and previous.end is not None:
        current.start = previous.end + timedelta(days=1)
        current.start_is_year_precision = False


def resolve_handoff(previous: AffiliationPeriod, current: AffiliationPeriod) -> None:
    if current.start is None:
        return

    if previous.end is not None and previous.end >= current.start:
        if should_split_same_year_handoff(previous, current):
            split_same_year_handoff(previous, current)
        else:
            previous.end = current.start - timedelta(days=1)
            previous.end_is_year_precision = False
        return

    if previous.end is None and not previous.open_end:
        previous.end = current.start - timedelta(days=1)
        previous.end_is_year_precision = False


def should_split_same_year_handoff(previous: AffiliationPeriod, current: AffiliationPeriod) -> bool:
    return (
        previous.end_is_year_precision
        and current.start_is_year_precision
        and previous.end is not None
        and current.start is not None
        and previous.end.year == current.start.year
    )


def split_same_year_handoff(previous: AffiliationPeriod, current: AffiliationPeriod) -> None:
    assert current.start is not None

    year = current.start.year
    previous.end = date(year, 6, 30)
    current.start = date(year, 7, 1)
    previous.end_is_year_precision = False
    current.start_is_year_precision = False


def same_party(
    first: AffiliationPeriod, second: AffiliationPeriod, party_cols: Iterable[str] = DEFAULT_PARTY_COLS
) -> bool:
    compared = False

    for column in party_cols:
        first_value = first.data.get(column)
        second_value = second.data.get(column)

        if is_missing(first_value) or is_missing(second_value):
            continue

        compared = True

        if first_value != second_value:
            return False

    return compared


def fill_missing_values(target: AffiliationPeriod, source: AffiliationPeriod) -> None:
    for column, value in source.data.items():
        if is_missing(target.data.get(column)) and not is_missing(value):
            target.data[column] = value


def period_sort_key(item: AffiliationPeriod, party_cols: Iterable[str] = DEFAULT_PARTY_COLS) -> tuple[Any, ...]:
    party_key = tuple("" if is_missing(item.data.get(column)) else str(item.data[column]) for column in party_cols)

    return (
        item.start or date.min,
        item.end or date.max,
        *party_key,
    )


def parse_boundary(value: Any, precision: Any = None, *, is_start: bool) -> tuple[date | None, bool]:
    if is_missing(value):
        return None, False

    if isinstance(value, datetime):
        return value.date(), False

    if isinstance(value, date):
        return value, False

    text: str = str(value).strip()
    is_year: bool = is_year_precision(text, precision)

    if is_year:
        year: str = text[:4]
        if not year.isdigit():
            return None, False

        month, day = (1, 1) if is_start else (12, 31)
        return date(int(year), month, day), True

    try:
        return date.fromisoformat(text[:10]), False
    except ValueError:
        return None, False
