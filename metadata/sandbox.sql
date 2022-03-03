with minister_years as (
    select *, cast(strftime('%Y',start) as integer) as start_year,
              cast(strftime('%Y',end) as integer) as end_year
    from _minister
) select *
  from minister_years
  join _person using (person_id)
  join years
   on years.year between start_year and start_year
;

/* Check for gaps in governments */
select g1.*, case when g2.start < g1.[end] then 'overlap' when g2.start > g1.[end] then 'gap' else 'ok' end as ok
from _government g1
join _government g2
  on g2.government_id = g1.government_id + 1
;

with persons_with_many_partys as (
    select person_id
    from person_party
    -- join utterances using (hash)
    group by person_id
    having count(distinct party_id) > 1
)
    select distinct person_party.person_id, party_id, null, null
    from person_party
    left join persons_with_many_partys using (person_id)
    where persons_with_many_partys.person_id  is null
--  2959
;
select count(*) from persons_of_interest
-- 3170
;
with persons_with_many_partys as (
    select person_id, group_concat(distinct party.party_abbrev) as partys
    from person_party
    join party using (party_id)
    group by person_id
    having count(distinct party_id) > 1
)
    select persons_of_interest.person_id,
        party.party_abbrev,
        min(persons_of_interest.name) as name,
        min(person_party.start_year) as start_year,
        max(person_party.end_year) as end_year
    from person_party
    join party using (party_id)
    join persons_of_interest using (person_id)
    join persons_with_many_partys using (person_id)
--    where not (start_year is null or end_year is null)
    group by person_id, party_abbrev;
    -- UNION
    select persons_of_interest.person_id,
        party.party_abbrev,
        persons_of_interest.name,
        person_party.start_year,
        person_party.end_year
    from person_party
    join party using (party_id)
    join persons_of_interest using (person_id)
    join persons_with_many_partys using (person_id)
    --where (start_year is null or end_year is null)
    ;
--  2959
select *
from _member_of_parliament
where [start] = [end];


select *
from person_party
join persons_with_many_partys using (person_id)
join years
    on years.year between person_party.start_year and person_party.end_year;



select *
from person_party
join years
  on years.year between person_party.start_year and person_party.end_year;

create index idx_utterances_speaker_hash on utterances(speaker_hash);
select
    utterances.*,
    cast(strftime('%Y',date) as integer) as year
from utterances
join protocols using (document_id)
left join unknown_utterance_party on (speaker_hash)
where person_id = 'Q4953847'
limit 10
;

