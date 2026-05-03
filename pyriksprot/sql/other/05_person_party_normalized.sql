DROP TABLE IF EXISTS normalized_affiliations;

create table normalized_affiliations as
with parsed as (
    select
        person_id,
        party,
        party_id,
        swerik_party_id,

        case
            when start is null or trim(start) = '' then null
            when start_precision = 'year' or length(start) = 4
                then printf('%04d-01-01', cast(substr(start, 1, 4) as integer))
            else date(start)
        end as start_date,

        case
            when end is null or trim(end) = '' then null
            when end_precision = 'year' or length(end) = 4
                then printf('%04d-12-31', cast(substr(end, 1, 4) as integer))
            else date(end)
        end as end_date,

        start as original_start,
        end as original_end,
        start_precision as original_start_precision,
        end_precision as original_end_precision
    from person_party_affiliations
),

party_keyed AS (
    SELECT
        *,
        COALESCE(party_id, swerik_party_id, party) AS party_key
    FROM parsed
),

split_rows AS (
    SELECT
        *,
        CASE WHEN start_date IS NULL AND end_date IS NULL THEN 1 ELSE 0 END AS is_undated
    FROM party_keyed
),

dated as (
    select *
    from split_rows
    where is_undated = 0
),

undated as (
    select *
    from split_rows
    where is_undated = 1
),

dated_with_open_flags AS (
    select
        d.*,

        case
            when exists (
                select 1
                from undated u
                where u.person_id = d.person_id
                  and u.party_key = d.party_key
            )
            then 1 else 0
        end as open_start,

        case
            when exists (
                select 1
                from undated u
                where u.person_id = d.person_id
                  and u.party_key = d.party_key
            )
            then 1 else 0
        end as open_end
    from dated d
),

undated_without_dated_match as (
    select u.*
    from undated u
    where not exists (
        select 1
        from dated d
        where d.person_id = u.person_id
          and d.party_key = u.party_key
    )
),

combined AS (
    SELECT
        person_id,
        party,
        party_id,
        swerik_party_id,
        start_date,
        end_date,
        party_key,
        open_start,
        open_end
    FROM dated_with_open_flags

    UNION ALL

    SELECT
        person_id,
        party,
        party_id,
        swerik_party_id,
        NULL AS start_date,
        NULL AS end_date,
        party_key,
        1 AS open_start,
        1 AS open_end
    FROM undated_without_dated_match
),

ordered AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY person_id
            ORDER BY
                COALESCE(start_date, '0001-01-01'),
                COALESCE(end_date, '9999-12-31'),
                party_key
        ) AS rn
    FROM combined
),

with_neighbors AS (
    SELECT
        *,
        LAG(end_date) OVER (
            PARTITION BY person_id
            ORDER BY rn
        ) AS prev_end_date,

        LEAD(start_date) OVER (
            PARTITION BY person_id
            ORDER BY rn
        ) AS next_start_date
    FROM ordered
),

bounded AS (
    SELECT
        person_id,
        party,
        party_id,
        swerik_party_id,
        party_key,
        open_start,
        open_end,

        CASE
            WHEN start_date IS NULL
             AND open_start = 0
             AND prev_end_date IS NOT NULL
                THEN date(prev_end_date, '+1 day')
            ELSE start_date
        END AS start_date,

        CASE
            WHEN end_date IS NULL
             AND open_end = 0
             AND next_start_date IS NOT NULL
                THEN date(next_start_date, '-1 day')
            ELSE end_date
        END AS end_date
    FROM with_neighbors
),

valid AS (
    SELECT *
    FROM bounded
    WHERE start_date IS NULL
       OR end_date IS NULL
       OR start_date <= end_date
),

same_party_groups AS (
    SELECT
        *,
        SUM(is_new_group) OVER (
            PARTITION BY person_id
            ORDER BY
                COALESCE(start_date, '0001-01-01'),
                COALESCE(end_date, '9999-12-31'),
                party_key
        ) AS grp
    FROM (
        SELECT
            *,
            CASE
                WHEN LAG(party_key) OVER (
                    PARTITION BY person_id
                    ORDER BY
                        COALESCE(start_date, '0001-01-01'),
                        COALESCE(end_date, '9999-12-31'),
                        party_key
                ) = party_key
                THEN 0
                ELSE 1
            END AS is_new_group
        FROM valid
    )
),

merged_same_party AS (
    SELECT
        person_id,
        party,
        party_id,
        swerik_party_id,
        party_key,

        CASE
            WHEN MAX(open_start) = 1 THEN NULL
            ELSE MIN(start_date)
        END AS start_date,

        CASE
            WHEN MAX(open_end) = 1 THEN NULL
            ELSE MAX(end_date)
        END AS end_date
    FROM same_party_groups
    GROUP BY
        person_id,
        grp,
        party_key
),

final_ordered AS (
    SELECT
        *,
        LEAD(start_date) OVER (
            PARTITION BY person_id
            ORDER BY
                COALESCE(start_date, '0001-01-01'),
                COALESCE(end_date, '9999-12-31'),
                party_key
        ) AS next_start_date
    FROM merged_same_party
),

non_overlapping AS (
    SELECT
        person_id,
        party,
        party_id,
        swerik_party_id,

        start_date AS start,

        CASE
            WHEN end_date IS NOT NULL
             AND next_start_date IS NOT NULL
             AND end_date >= next_start_date
                THEN date(next_start_date, '-1 day')
            ELSE end_date
        END AS end
    FROM final_ordered
)

SELECT
    person_id,
    start,
    end,
    party,
    party_id,
    swerik_party_id,
    CASE WHEN start IS NULL THEN NULL ELSE 'day' END AS start_precision,
    CASE WHEN end IS NULL THEN NULL ELSE 'day' END AS end_precision
FROM non_overlapping
WHERE start IS NULL
   OR end IS NULL
   OR start <= end
ORDER BY
    person_id,
    COALESCE(start, '0001-01-01'),
    COALESCE(end, '9999-12-31');