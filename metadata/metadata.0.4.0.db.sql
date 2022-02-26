
with person_name as (
    select person_id, coalesce(
           max(case when primary_name = 1 then _name.name else null end),
           max(name)) as name
    from _name
    group by person_id
), person_location as (
    select person_id, max(location) as location
    from _location_specifier
    group by person_id
), person_party as (
    select person_id, group_concat(party) as party, count(*)
    from _party_affiliation
    group by person_id
    having count(*) > 1
)
    select _person.person_id, _person.born, _person.dead, _person.gender,
           person_name.name,
           coalesce(person_location.location, 'unknown') as location
    from _person
    join _
    left join person_name using (person_id)
    left join person_location using (person_id)
    left join person_party using (person_id)
;

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

select *
from _minister
left join _government using (government)
where _government.government is null;

select *
from _minister
where government not in (
    select government
    from _government
);


select *
from person_location;


select person_id, group_concat(location, ',')
from _location_specifier
group by person_id
select * from _government;

select parties, count(*)
from (
    select person_id, count(*) as parties
    from _party_affiliation
    group by person_id
) as v
group by parties;

select person_id, group_concat(party), count(*)
from _party_affiliation
group by person_id
having count(*) > 1;

select *
from input_unknown
-- join utterances using (hash)
limit 100
;
