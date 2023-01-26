select *
from _member_of_parliament
where party not in (select party from _party_abbreviation)
;

select distinct party
from _party_affiliation
where party not in (select party from _party_abbreviation)
  and coalesce([end], [start], 2099) >= 2020
order by party
;

select distinct party
from _unknowns
where party not in (select party from _party_abbreviation)
order by party
;

select s.person_id, s.gender_id, s.name, pp.party_id, pp.start_year, pp.end_year, p.party_id, p.party, p.party_abbrev
from persons_of_interest s
left join person_party pp using (person_id)
left join party p using (party_id)
where person_id = 'Q6212174'
limit 10

select *
from persons_of_interest
where person_id = 'Q6212174'

select *
from _party_affiliation
where person_id = 'Q6212174'

select *
from person_party
where person_id = 'Q6212174'

with persons_with_single_party as (
    select person_id, max(party_id) as party_id
    from person_party
    group by person_id
    having count(distinct party_id) = 1
)
  --  insert into persons_of_interest (person_id, [party_id])
        select person_id, persons_with_single_party.party_id
        from persons_of_interest
        join persons_with_single_party using (person_id)
        where person_id = 'Q6212174'

  --        on conflict(person_id) do update set party_id=excluded.[party_id];