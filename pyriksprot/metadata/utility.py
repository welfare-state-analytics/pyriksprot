from __future__ import annotations

from typing import Any, Literal, Type

import numpy as np
import pandas as pd

COLUMN_TYPES = {
    'year_of_birth': np.int16,
    'year_of_death': np.int16,
    'gender_id': np.int8,
    'party_id': np.int8,
    'chamber_id': np.int8,
    'office_type_id': np.int8,
    'sub_office_type_id': np.int8,
    'start_year': np.int16,
    'end_year': np.int16,
    'district_id': np.int16,
}

COLUMN_DEFAULTS = {
    'gender_id': 0,
    'year_of_birth': 0,
    'year_of_death': 0,
    'district_id': 0,
    'party_id': 0,
    'chamber_id': 0,
    'office_type_id': 0,
    'sub_office_type_id': 0,
    'start_year': 0,
    'end_year': 0,
}

DATE_COLUMNS = ['start_date', 'end_date']


def slim_table_types(
    tables: list[pd.DataFrame] | pd.DataFrame,
    defaults: dict[str, Any] = None,
    types: dict[str, Any] = None,
) -> None:
    """Slims types and sets default value for NaN entries"""

    if isinstance(tables, pd.DataFrame):
        tables = [tables]

    defaults = COLUMN_DEFAULTS if defaults is None else defaults
    types = COLUMN_TYPES if types is None else types

    for table in tables:
        for column_name, value in defaults.items():
            if column_name in table.columns:
                table[column_name].fillna(value, inplace=True)

        for column_name, dt in types.items():
            if column_name in table.columns:
                if table[column_name].dtype != dt:
                    table[column_name] = table[column_name].astype(dt)


def group_to_list_of_records2(df: pd.DataFrame, key: str) -> dict[str | int, list[dict]]:
    """Groups `df` by `key` and aggregates each group to list of row records (dicts)"""
    return {q: df.loc[ds].to_dict(orient='records') for q, ds in df.groupby(key).groups.items()}  # type: ignore


def group_to_list_of_records(
    df: pd.DataFrame, key: str, properties: list[str] = None, ctor: Type = None
) -> dict[str | int, list[dict]]:
    """Groups `df` by `key` and aggregates each group to list of row records (dicts)"""
    key_rows: pd.DataFrame = pd.DataFrame(
        data={
            key: df[key],
            'data': (df[properties] if properties else df).to_dict("records"),
        }
    )
    if ctor is not None:
        key_rows['data'] = key_rows['data'].apply(lambda x: ctor(**x))

    return key_rows.groupby(key)['data'].apply(list).to_dict()


def fx_or_url(url: Any, tag: str) -> str:
    return url(tag) if callable(url) else url


def fix_incomplete_datetime_series(
    df: pd.DataFrame, column_name: str, action: Literal['extend', 'truncate'] = 'truncate', inplace: bool = True
) -> pd.DataFrame:
    """Handles incomplete string dates of format yyyy, yyyy-mm or yyyy-mm-dd by truncating (or extending) to beginning (or end) of year or month
    Stores original column in `column_name0` and adds a flag indicating action made:
    X: no action due to missing or invalid date
    D: existing date was already complete
    M: days was missing and date was truncated to first-day in month or last day of month
    """
    ds: pd.Series[str] = df[column_name]

    df = df if inplace else df.copy()

    df[f"{column_name}0"] = ds
    df[column_name] = ''
    df[f"{column_name}_flag"] = 'X'

    mask_year: pd.Series[bool] = ds.str.len() == 4
    mask_yearmonth: pd.Series[bool] = ds.str.len() == 7
    mask_yearmonthday: pd.Series[bool] = ds.str.len() == 10

    """Truncate to beginning of year/month"""
    df.loc[mask_year, column_name] = ds[mask_year].astype(str) + '-01-01'
    df.loc[mask_yearmonth, column_name] = ds[mask_yearmonth].astype(str) + '-01'
    df.loc[mask_yearmonthday, column_name] = ds[mask_yearmonthday]

    if action == 'extend':
        dt: pd.Series = pd.to_datetime(df[column_name], errors='coerce')
        dt.loc[mask_year] = dt + pd.DateOffset(years=1) - pd.DateOffset(days=1)
        dt.loc[mask_yearmonth] = dt + pd.DateOffset(months=1) - pd.DateOffset(days=1)
        df[column_name] = dt.dt.strftime("%Y-%m-%d")

    df.loc[mask_year, f"{column_name}_flag"] = 'Y'
    df.loc[mask_yearmonth, f"{column_name}_flag"] = 'M'
    df.loc[mask_yearmonthday, f"{column_name}_flag"] = 'D'

    return df
