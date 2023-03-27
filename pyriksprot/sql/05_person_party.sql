

drop table if exists person_party;
create table person_party (
    person_party_id integer primary key,
    person_id varchar not null references persons_of_interest(person_id),
    party_id integer not null references party(party_id),
    source_id integer not null,
    [start_date] date null,
    [start_flag] text not null default('X'),
    [end_date] date null,
    [end_flag] text not null default('X'),
    [start_year] int null,
    [end_year] int null,
    has_multiple_parties bool not null default(FALSE)
);


insert into person_party (
    person_id, source_id, party_id,
    [start_date], [start_flag], [end_date], [end_flag], [start_year], [end_year]
)
    with affiliation as (
        select
            person_id,
            party,
            _party_affiliation.[start],
            _party_affiliation.[start_flag],
            _party_affiliation.[end],
            _party_affiliation.[end_flag],
            cast(substr(_party_affiliation.[start], 1, 4) as integer) as start_year,
            cast(substr(_party_affiliation.[end], 1, 4) as integer) as end_year
        from _party_affiliation
    )
        select
            affiliation.person_id, 2 as source_id,
            coalesce(party.party_id, 1), -- 1 is code for "Other", 84 records
            affiliation.start,
            affiliation.start_flag,
            affiliation.end,
            affiliation.end_flag,
            affiliation.start_year,
            affiliation.end_year
        from affiliation
        join persons_of_interest using (person_id)
        left join _party_abbreviation using (party)
        left join party on party.party_abbrev = _party_abbreviation.abbreviation
        ;

with persons_with_many_partys as (
    select person_id
    from person_party
    group by person_id
    having count(distinct party_id) > 1
)
    update persons_of_interest set has_multiple_parties = TRUE
        where person_id in (select person_id from persons_with_many_partys);

with persons_with_many_partys as (
    select person_id
    from person_party
    group by person_id
    having count(distinct party_id) > 1
)
    update person_party set has_multiple_parties = TRUE
        where person_id in (select person_id from persons_with_many_partys);

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
        where persons_of_interest.has_multiple_parties = FALSE
    on conflict(person_id) do update set party_id=excluded.[party_id];

