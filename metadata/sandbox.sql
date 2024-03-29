/* Various SQL queries */
select *
from persons_of_interest
where person_id = 'Q4956353'
;
;
from _unknowns
where protocol_id in (
    'prot-1933--fk--5.xml',
    'prot-1955--ak--22.xml',
    'prot-197879--14.xml',
    'prot-199596--35.xml',
    'prot-199192--127.xml',
    'prot-199192--21.xml'
);

SELECT speaker_hash, count(distinct person_id)
FROM utterances
group by speaker_hash
having count(distinct person_id) > 1;
select * from party;
select u_id, count(*), min(party_id), max(party_id)
from unknown_utterance_party
group by u_id
having COUNT(*) > 1
limit 10;
select count(*)
from unknown_utterance_gender
limit 10;
with dupes as (
    select protocol_id, uuid
    from unknowns
    group by protocol_id, uuid
    having COUNT(*) > 1
) select *
from unknowns
join dupes using (protocol_id, uuid);
select *
from unknowns
order by protocol_id, uuid
limit 10;
with unknown_speaker_note_party (speaker_hash, party_id) as (
    select [uuid], party_id
    from unknowns
    join _party_abbreviation pa using (party)
    join party on party.party_abbrev = pa.abbreviation
)
    insert into unknown_utterance_party(u_id, party_id)
        select u_id, party_id
        from utterances
        join unknown_speaker_note_party using (speaker_hash);

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
;

select *
from speech_index
limit 10;

