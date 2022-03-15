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
select *
from utterances
where speaker_hash = 'fd225650';
select speaker_hash, document_id, count(*)
from utterances
--where person_id != 'unknown'
group by speaker_hash, document_id
having count(distinct person_id) > 1;

select count(*)
from utterances
where speaker_hash is null
-- 5984
;
select *
from unknown_utterance_party
join unknown_utterance_party using u_id;
/* Check for gaps in governments */
select g1.*, case when g2.start < g1.[end] then 'overlap' when g2.start > g1.[end] then 'gap' else 'ok' end as ok
from _government g1
join _government g2
  on g2.government_id = g1.government_id + 1
;

select person_id, party_id, min(ifnull(start_year,0)), max(ifnull(end_year,9999))
from person_multiple_party
group by person_id, party_id
;
select *
from person_multiple_party
where (start_year is null) != (end_year is null);
--  2959
select *
from _member_of_parliament
where [start] = [end];

select *
from person_party
join person_multiple_party using (person_id)
join years
    on years.year between person_party.start_year and person_party.end_year;

select *
from person_party
join years
  on years.year between person_party.start_year and person_party.end_year;

select
    utterances.*,
    cast(strftime('%Y',date) as integer) as year
from utterances
join protocols using (document_id)
left join unknown_utterance_party on (speaker_hash)
where person_id = 'Q4953847'
limit 10
;

select *
from persons_of_interest
where person_id = 'unknown'
limit 10
;

select person_id, year, party_id, count(*), min(year), max(year)
from person_yearly_party
group by  person_id, year, party_id;

    select person_id, min([year]), max([year])
    from utterances
    join protocols using (document_id)
    join persons_of_interest using (person_id)
    group by person_id;


    select distinct person_id, [year]
    from utterances
    join protocols using (document_id)
    join persons_of_interest using (person_id)
