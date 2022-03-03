
delete from party;

/* Ansats: endast partier i MOPS blir egna poster, övriga blir "Other" */
insert into party
    values (0, 'unknown', '?'), (1, 'Other', 'X');

insert into party (party, party_abbrev)
    select party, coalesce(abbreviation, party)
    from _member_of_parliament
    join persons_of_interest using (person_id)
    left join _party_abbreviation using (party)
    where party is not null
    group by party, abbreviation;


drop table if exists person_party;
create table person_party (
    person_party_id integer primary key,
    person_id varchar not null references persons_of_interest(person_id),
    party_id integer not null references party(party_id),
    source_id integer not null,
    start_year int null,
    end_year int null
);

insert into person_party (person_id, source_id, party_id, start_year, end_year)
    select
        mops.person_id, 1 as source_id, party.party_id,
        cast(strftime('%Y', mops.[start]) as integer) as start_year,
        cast(strftime('%Y', mops.[end]) as integer) as end_year
    from _member_of_parliament mops
    join persons_of_interest using (person_id)
    left join _party_abbreviation pa using (party)
    left join party on party.party_abbrev = pa.abbreviation
    where mops.party is not null;

insert into person_party (person_id, source_id, party_id, start_year, end_year)
    select
        _party_affiliation.person_id, 2 as source_id,
        coalesce(party.party_id, 1), -- 1 is code for "Other", 84 records
        null as start_year,
        null as end_year
    from _party_affiliation
    join persons_of_interest using (person_id)
    left join _party_abbreviation pa using (party)
    left join party on party.party_abbrev = pa.abbreviation
    ;

/* Update party_id for persons hjaving only one party */
with persons_with_single_party as (
    select person_id, max(party_id) as party_id
    from person_party
    group by person_id
    having count(distinct party_id) = 1
)
    insert into persons_of_interest (person_id, [party_id])
        select person_id, persons_with_single_party.party_id
        from persons_of_interest
        join persons_with_single_party using (person_id)
          on conflict(person_id) do update set party_id=excluded.[party_id];


-- drop table if exists person_multiple_party;
-- create table person_multiple_party (
--     person_id varchar primary key,
--     party_id integer not null
-- );
-- with persons_with_many_partys as (
--     select person_id
--     from person_party
--     group by person_id
--     having count(distinct party_id) > 1
-- )
--     select distinct person_party.person_id, party_id
--     from person_party
--     left join persons_with_many_partys using (person_id)
--     where persons_with_many_partys.person_id  is null;
-- --  2959
