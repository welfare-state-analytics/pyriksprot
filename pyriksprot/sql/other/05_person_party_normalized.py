import pandas as pd
from pandas.tseries.offsets import Day


def normalize_affiliation_periods(
    df: pd.DataFrame,
    person_col="person_id",
    party_cols=("party_id", "swerik_party_id", "party"),
    start_col="start",
    end_col="end",
    start_precision_col="start_precision",
    end_precision_col="end_precision",
) -> pd.DataFrame:
    """
    Normalize temporal party affiliation data into consistent, non-overlapping periods.

    This function processes a Pandas DataFrame containing potentially incomplete,
    overlapping, and inconsistently specified time intervals describing a person's
    affiliation with political parties. It returns a cleaned DataFrame where all
    time periods are expressed as proper dates, are logically ordered, and do not overlap.

    ----------------------------------------------------------------------
    INPUT DATA CHARACTERISTICS
    ----------------------------------------------------------------------
    Each row represents a person's affiliation to a party and may contain:

    - start: may be:
        * missing / empty
        * a year (e.g. "1982")
        * a full date (e.g. "1982-10-04")

    - end: same possibilities as start

    - start_precision / end_precision:
        * "year"  → value should be interpreted as year-level precision
        * "day"   → value is a full date (YYYY-MM-DD)
        * missing → infer based on value format

    - party identity:
        Defined using one or more columns (default: party_id, swerik_party_id, party)

    ----------------------------------------------------------------------
    NORMALIZATION RULES
    ----------------------------------------------------------------------

    1. DATE NORMALIZATION
       - Year-only values are expanded:
            start: YYYY      → YYYY-01-01
            end:   YYYY      → YYYY-12-31
       - Full dates are parsed as-is.
       - Missing values are represented as NaT.

    2. OPEN-ENDED PERIODS (CRITICAL BEHAVIOR)
       - Rows with both start and end missing represent "unknown bounds".
       - If a person has exactly ONE such row:
            → interpreted as "always affiliated" with that party
            → preserved as (start=NULL, end=NULL)
       - If undated rows match a dated row with the same party:
            → they extend that affiliation to be open-ended
              (start=NULL and/or end=NULL)

    3. INFERRING MISSING BOUNDS
       For rows that are not explicitly open-ended:
       - Missing start:
            → inferred as previous row's end + 1 day (if available)
       - Missing end:
            → inferred as next row's start - 1 day (if available)

    4. SORTING
       - Rows are processed per person
       - Sorted chronologically by start, then end
       - Open-ended starts are treated as earliest
       - Open-ended ends are treated as latest

    5. OVERLAP RESOLUTION
       - If two consecutive rows (different parties) overlap:
            → previous row is truncated to (next.start - 1 day)

       - If a previous row is open-ended but a new party starts:
            → previous row is closed at (next.start - 1 day),
              unless explicitly marked as truly open-ended

    6. SAME-PARTY MERGING ("SQUEEZING")
       - Consecutive or overlapping rows with the same party are merged into one
       - Gaps between same-party rows are assumed to be continuous affiliation
       - Result:
            multiple fragmented rows → one continuous interval

       - If any contributing row is open-ended:
            → merged result becomes open-ended

    7. INVALID PERIOD REMOVAL
       - Rows where start > end are dropped (after adjustments)

    ----------------------------------------------------------------------
    PARTY IDENTITY MATCHING
    ----------------------------------------------------------------------
    Two rows are considered to represent the same party if:

    - They share at least one non-null identifier among party_cols, AND
    - No shared identifiers conflict

    Example:
        party_id matches → same party
        party name matches → same party
        conflicting non-null values → different party

    ----------------------------------------------------------------------
    OUTPUT
    ----------------------------------------------------------------------
    Returns a DataFrame with:

    - start, end:
        ISO date strings ("YYYY-MM-DD") or NULL

    - start_precision, end_precision:
        always "day" if date is present, else NULL

    - no overlapping intervals per person

    - minimized number of rows via merging

    ----------------------------------------------------------------------
    KEY ASSUMPTIONS
    ----------------------------------------------------------------------
    - Same-party records imply continuous affiliation unless contradicted
    - Missing bounds are inferred conservatively using neighboring data
    - Undated rows are treated as strong evidence of open-ended affiliation
    - Time is discrete at day-level granularity

    ----------------------------------------------------------------------
    LIMITATIONS
    ----------------------------------------------------------------------
    - Cannot infer affiliation gaps if no data exists
    - Assumes no simultaneous multi-party affiliation
    - Relies on data quality of party identifiers

    ----------------------------------------------------------------------
    EXAMPLE TRANSFORMATION
    ----------------------------------------------------------------------

    Input:
        1978-10-01 → 1978-10-10  Party A
        1978-10-15 → 1978-10-20  Party A

    Output:
        1978-10-01 → 1978-10-20  Party A

    Input:
        NULL → NULL  Party B

    Output:
        NULL → NULL  Party B  (always affiliated)

    ----------------------------------------------------------------------
    """

    df = df.copy()

    def is_missing(x):
        return pd.isna(x) or str(x).strip() == ""

    def parse_start(value, precision):
        if is_missing(value):
            return pd.NaT

        value = str(value).strip()
        precision = "" if is_missing(precision) else str(precision).strip()

        if precision == "year" or len(value) == 4:
            return pd.Timestamp(f"{value[:4]}-01-01")

        return pd.to_datetime(value, errors="coerce")

    def parse_end(value, precision):
        if is_missing(value):
            return pd.NaT

        value = str(value).strip()
        precision = "" if is_missing(precision) else str(precision).strip()

        if precision == "year" or len(value) == 4:
            return pd.Timestamp(f"{value[:4]}-12-31")

        return pd.to_datetime(value, errors="coerce")

    def same_party(a, b):
        """
        Compare party identity conservatively.
        If shared non-null identifiers disagree, rows are not same-party.
        """
        saw_shared_value = False

        for col in party_cols:
            av = a.get(col)
            bv = b.get(col)

            if not is_missing(av) and not is_missing(bv):
                saw_shared_value = True
                if av != bv:
                    return False

        return saw_shared_value

    df["_start"] = [
        parse_start(v, p)
        for v, p in zip(
            df[start_col],
            df[start_precision_col] if start_precision_col in df else pd.Series(index=df.index),
        )
    ]

    df["_end"] = [
        parse_end(v, p)
        for v, p in zip(
            df[end_col],
            df[end_precision_col] if end_precision_col in df else pd.Series(index=df.index),
        )
    ]

    output_rows = []

    for _, g in df.groupby(person_col, dropna=False):
        g = g.copy()

        g["_sort_start"] = g["_start"].fillna(pd.Timestamp.min)
        g["_sort_end"] = g["_end"].fillna(pd.Timestamp.max)
        g = g.sort_values(["_sort_start", "_sort_end"])

        rows = g.to_dict("records")

        undated = [r for r in rows if pd.isna(r["_start"]) and pd.isna(r["_end"])]

        dated = [r for r in rows if not (pd.isna(r["_start"]) and pd.isna(r["_end"]))]

        # Only one fully open row: keep it as "always affiliated".
        if not dated and len(undated) == 1:
            output_rows.append(undated[0])
            continue

        # Several fully open rows: keep one per distinct party.
        if not dated:
            kept = []
            for row in undated:
                if not any(same_party(row, prev) for prev in kept):
                    kept.append(row)
            output_rows.extend(kept)
            continue

        # Use undated same-party rows as evidence that the dated period is open.
        for u in undated:
            matched = False

            for d in dated:
                if same_party(u, d):
                    d["_open_start"] = True
                    d["_open_end"] = True
                    matched = True
                    break

            if not matched:
                dated.append(u)

        rows = sorted(
            dated,
            key=lambda r: (
                pd.Timestamp.min if pd.isna(r["_start"]) else r["_start"],
                pd.Timestamp.max if pd.isna(r["_end"]) else r["_end"],
            ),
        )

        # Infer bounded missing starts/ends from neighbors.
        for i, row in enumerate(rows):
            prev = rows[i - 1] if i > 0 else None
            nxt = rows[i + 1] if i + 1 < len(rows) else None

            if pd.isna(row["_start"]) and not row.get("_open_start") and prev is not None and pd.notna(prev["_end"]):
                row["_start"] = prev["_end"] + Day(1)

            if pd.isna(row["_end"]) and not row.get("_open_end") and nxt is not None and pd.notna(nxt["_start"]):
                row["_end"] = nxt["_start"] - Day(1)

        # Remove impossible bounded rows.
        rows = [r for r in rows if not (pd.notna(r["_start"]) and pd.notna(r["_end"]) and r["_start"] > r["_end"])]

        merged = []

        for row in rows:
            row = row.copy()

            if not merged:
                merged.append(row)
                continue

            prev = merged[-1]

            if same_party(prev, row):
                # Merge same-party records into one continuous/open interval.
                if (
                    prev.get("_open_start")
                    or row.get("_open_start")
                    or pd.isna(prev["_start"])
                    or pd.isna(row["_start"])
                ):
                    prev["_start"] = pd.NaT
                    prev["_open_start"] = True
                else:
                    prev["_start"] = min(prev["_start"], row["_start"])

                if prev.get("_open_end") or row.get("_open_end") or pd.isna(prev["_end"]) or pd.isna(row["_end"]):
                    prev["_end"] = pd.NaT
                    prev["_open_end"] = True
                else:
                    prev["_end"] = max(prev["_end"], row["_end"])

                continue

            # Different party: prevent overlap.
            if pd.notna(prev["_end"]) and pd.notna(row["_start"]):
                if prev["_end"] >= row["_start"]:
                    prev["_end"] = row["_start"] - Day(1)

            # If previous is open-ended and a new different party starts,
            # close previous the day before the new one.
            if pd.isna(prev["_end"]) and pd.notna(row["_start"]) and not prev.get("_open_end"):
                prev["_end"] = row["_start"] - Day(1)

            if pd.notna(prev["_start"]) and pd.notna(prev["_end"]) and prev["_start"] > prev["_end"]:
                merged.pop()

            merged.append(row)

        output_rows.extend(merged)

    result = pd.DataFrame(output_rows)

    result[start_col] = result["_start"].dt.strftime("%Y-%m-%d")
    result[end_col] = result["_end"].dt.strftime("%Y-%m-%d")

    result[start_precision_col] = result[start_col].notna().map({True: "day", False: pd.NA})
    result[end_precision_col] = result[end_col].notna().map({True: "day", False: pd.NA})

    drop_cols = [c for c in result.columns if c.startswith("_")]

    result = result.drop(columns=drop_cols)

    result = result.sort_values(
        [person_col, start_col, end_col],
        na_position="last",
    ).reset_index(drop=True)

    return result
