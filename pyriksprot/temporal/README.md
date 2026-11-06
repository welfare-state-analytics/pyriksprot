
# Step 1: Formal rules for parse/normalize dates

## Core rule

```text
A start boundary resolves to the earliest valid date implied by its input.
An end boundary resolves to the latest valid date implied by its input.
Missing boundaries remain missing.
Precision is always recorded separately from the normalized date.
```

## Goal

Let each row have two temporal fields:

```text
start_raw
end_raw
```

Normalization produces:

```text
start_date
end_date
start_precision
end_precision
temporal_status
temporal_warnings
```

## 1. Raw preservation

```text
start_raw and end_raw are preserved unchanged.
```

## 2. Missing values

A value is missing if it is null, empty, whitespace-only, or a configured missing marker.

```text
missing start → start_date = NULL, start_precision = missing
missing end   → end_date = NULL, end_precision = missing
```

## 3. Year precision

For a valid year `YYYY`:

```text
start_raw = YYYY → start_date = YYYY-01-01, start_precision = year
end_raw   = YYYY → end_date   = YYYY-12-31, end_precision   = year
```

## 4. Month precision

For a valid year-month `YYYY-MM`:

```text
start_raw = YYYY-MM → start_date = YYYY-MM-01, start_precision = month
end_raw   = YYYY-MM → end_date   = last day of YYYY-MM, end_precision = month
```

## 5. Day precision

For a valid date `YYYY-MM-DD`:

```text
start_raw = YYYY-MM-DD → start_date = YYYY-MM-DD, start_precision = day
end_raw   = YYYY-MM-DD → end_date   = YYYY-MM-DD, end_precision   = day
```

## 6. Partial-date expansion

```text
start values are expanded to the earliest date compatible with the input.
end values are expanded to the latest date compatible with the input.
```

## 7. Invalid values

For an unparseable or invalid value:

```text
date = NULL
precision = invalid
warning = invalid_temporal_value
```

## 8. Open periods

```text
known start + missing end → open_end
missing start + known end → open_start
missing start + missing end → unknown_period
```

No missing boundary is inferred from the other boundary.

## 9. Interval validity

If both normalized boundaries are known:

```text
start_date <= end_date
```

Otherwise:

```text
warning = start_after_end
status = invalid_interval
```

## 10. Inclusivity

All normalized periods are inclusive:

```text
start_date <= t <= end_date
```

## 11. Mixed precision

Different precision levels may coexist in the same interval.

```text
start_precision ≠ end_precision is valid
```

## 12. Ambiguous formats

Ambiguous date literals are not normalized unless a source-specific format is configured.

```text
ambiguous value → date = NULL, precision = ambiguous, warning = ambiguous_temporal_value
```

## 13. Normalization status

Suggested status values:

```text
bounded
open_start
open_end
unknown_period
invalid_interval
invalid_temporal_value
ambiguous_temporal_value
```
