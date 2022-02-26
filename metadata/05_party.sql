
drop table if exists party;
create table party (
    party_id integer primary key,
    party varchar,
    party_abbrev varchar
);

insert into party
    values (0, 'unknown', '?'), (1, 'Other', 'X');

insert into party (party, party_abbrev)
    select party, coalesce(abbreviation, 'X') as party_abbrev
    from _party_affiliation
    join person_of_interest using (person_id)
    left join _party_abbreviation using (party)
    group by party, coalesce(abbreviation, 'X')
    union
    select party, abbreviation
    from _member_of_parliament
    join person_of_interest using (person_id)
    join _party_abbreviation using (party)
    where party is not null
    group by party, abbreviation
;

select party, coalesce(abbreviation, 'X')
from input_unknown
left join _party_abbreviation using (party)
group by party, coalesce(abbreviation, 'X');
select party
from _party_affiliation
group by party

with parties(person_id, party) as (
    select person_id, party
    from _member_of_parliament
    where party is not null
    union
    select person_id, party
    from _party_affiliation
)
    select count(*)
    from parties
    join person_of_interest using (person_id)
    left join _party_abbreviation using (party)
    group by party
    order by party;
select party, coalesce(abbreviation, 'X') as party_abbrev, count(*)
from _party_affiliation
join person_of_interest using (person_id)
left join _party_abbreviation using (party)
group by  party,coalesce(abbreviation, 'X');
-- Lantmanna- och borgarpartiet

select document_id, hash, gender_id, party
from input_unknown
left join gender using (gender)
left join _party_abbreviation using (party)
join protocols
  on protocols.document_name = input_unknown.protocol_id
limit 10;

select *
from _party_abbreviation
limit 10;


select *
from _party_affiliation
join _minister using (person_id)
group by count(*)