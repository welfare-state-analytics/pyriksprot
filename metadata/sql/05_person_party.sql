
delete from party;

/* Ansats: endast partier i MOPS blir egna poster, övriga blir "Other" */
insert into party (party_id, party, party_abbrev, party_color)
    values (0, 'unknown', '?', '#000000'), (1, 'Other', 'X', '#000000');

insert into party (party, party_abbrev, party_color)
    select party, coalesce(abbreviation, party), '#3f1105'
    from _member_of_parliament
    join persons_of_interest using (person_id)
    left join _party_abbreviation using (party)
    where party is not null
    group by party, abbreviation;

update party set party_color = '#E8112d' where party_abbrev = 'S';
update party set party_color = '#52BDEC' where party_abbrev = 'M';
update party set party_color = '#009933' where party_abbrev = 'C';
update party set party_color = '#006AB3' where party_abbrev = 'L';
update party set party_color = '#DA291C' where party_abbrev = 'V';
update party set party_color = '#83CF39' where party_abbrev = 'MP';
update party set party_color = '#000077' where party_abbrev = 'KD';
update party set party_color = '#007700' where party_abbrev = 'NYD';
update party set party_color = '#DDDD00' where party_abbrev = 'SD';

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
        cast(substr(mops.[start], 1, 4) as integer) as start_year,
        cast(substr(mops.[end], 1, 4) as integer) as end_year
    from _member_of_parliament mops
    join persons_of_interest using (person_id)
    join _party_abbreviation pa using (party)
    join party on party.party_abbrev = pa.abbreviation
    where mops.party is not null;

insert into person_party (person_id, source_id, party_id, start_year, end_year)
    with affiliation as (
        select
            person_id,
            party,
            cast(substr(_party_affiliation.[start], 1, 4) as integer) as start_year,
            cast(substr(_party_affiliation.[end], 1, 4) as integer) as end_year
        from _party_affiliation
    )
        select
            affiliation.person_id, 2 as source_id,
            coalesce(party.party_id, 1), -- 1 is code for "Other", 84 records
            affiliation.start_year,
            affiliation.end_year
        from affiliation
        join persons_of_interest using (person_id)
        left join _party_abbreviation using (party)
        left join party on party.party_abbrev = _party_abbreviation.abbreviation
        ;



/* Update party_id for persons having only one party */
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


drop table if exists person_multiple_party;
create table person_multiple_party (
    person_multiple_party_id integer primary key,
    person_id varchar not null,
    party_id integer not null,
    start_year int null,
    end_year int null
);
with persons_with_many_partys as (
    select person_id
    from person_party
    group by person_id
    having count(distinct party_id) > 1
)
    insert into person_multiple_party (person_id, party_id, start_year, end_year)
        select distinct person_party.person_id, party_id, start_year, end_year
        from person_party
        join persons_with_many_partys using (person_id);

drop table if exists person_yearly_party;
create table person_yearly_party (
    person_yearly_party_id integer primary key,
    person_id varchar not null references persons_of_interest(person_id),
    [year] int null,
    party_id integer not null
);

insert into person_yearly_party (person_id, [year], party_id)
    select distinct person_id, years.[year], party_id
    from person_multiple_party
    join years
    on years.[year] between person_multiple_party.[start_year] and ifnull(person_multiple_party.[end_year], 2030)
    union
    select distinct person_id, null, party_id
    from person_multiple_party
    where start_year is null
      and end_year is null
    union
    select person_id, null, party_id
    from persons_of_interest
    where party_id is not null;

insert into person_yearly_party (person_id, [year], party_id)
    select person_id, null, 0
    from persons_of_interest
    where person_id not in (select person_id from person_yearly_party);

