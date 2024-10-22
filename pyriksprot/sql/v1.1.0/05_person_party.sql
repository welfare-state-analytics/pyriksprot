
/* Party: records are added in 05_person_party.sql */


drop table if exists party;

create table if not exists party (
    "party_id" integer primary key,
    "party" varchar not null unique,
    "party_abbrev" varchar not null unique,
    "party_color" varchar default('#3f1105'),
    "sort_order" integer default(0)
);

insert into party("party_id", "party", "party_abbrev", "party_color", "sort_order")
    values (0, 'Okänt', '[?]', '#000000', 5);

insert into party("party_id", "party", "party_abbrev", "party_color", "sort_order")
    select
        row_number() over (order by sort_order, party) as party_id,
        party,
        max(abbreviation) as abbreviation,
        '#000000' as party_color,
        max(sort_order) as sort_order
    from swedeb_parties
    group by party;

-- update party
-- set "party_color" = party_colors."party_color"
-- from (values
--     ('C', '#009933'),
--     ('Kd', '#000077'),
--     ('Sp', '#FF112d'),
--     ('L', '#006AB3'),
--     ('Mp', '#83CF39'),
--     ('M', '#52BDEC'),
--     ('Nyd', '#007700'),
--     ('S', '#E8112d'),
--     ('Sd', '#DDDD00'),
--     ('V', '#DA291C')
-- ) as party_colors("party_abbrev", "party_color")
-- where party_colors."party_abbrev" = party."party_abbrev";

update party set "party_color" = '#009933' where "party_abbrev" = 'C';
update party set "party_color" = '#000077' where "party_abbrev" = 'Kd';
update party set "party_color" = '#FF112d' where "party_abbrev" = 'Sp';
update party set "party_color" = '#006AB3' where "party_abbrev" = 'L';
update party set "party_color" = '#83CF39' where "party_abbrev" = 'Mp';
update party set "party_color" = '#52BDEC' where "party_abbrev" = 'M';
update party set "party_color" = '#007700' where "party_abbrev" = 'Nyd';
update party set "party_color" = '#E8112d' where "party_abbrev" = 'S';
update party set "party_color" = '#DDDD00' where "party_abbrev" = 'Sd';
update party set "party_color" = '#DA291C' where "party_abbrev" = 'V';

drop table if exists person_party;

create table person_party (
    "person_party_id" integer primary key,
    "person_id" varchar not null references persons_of_interest(person_id),
    "party_id" integer not null references party(party_id),
    "source_id" integer not null,
    "start_date" date null,
    "start_flag" text not null default('X'),
    "end_date" date null,
    "end_flag" text not null default('X'),
    "start_year" int null,
    "end_year" int null,
    "has_multiple_parties" bool not null default(FALSE)
);


insert into person_party (
    "person_party_id",
    "person_id",
    "source_id",
    "party_id",
    "start_date",
    "start_flag",
    "end_date",
    "end_flag",
    "start_year",
    "end_year"
) 
  with affiliation as (
        select
            "person_id",
            "party",
            pa."start",
            pa."start_flag",
            pa."end",
            pa."end_flag",
            cast(substr(cast(pa."start" as text), 1, 4) as integer) as "start_year",
            cast(substr(cast(pa."end" as text), 1, 4) as integer)   as "end_year"
        from _party_affiliation pa
    )
        select 0, 'unknown', 1, 0, null, 'X', null, 'X', null, null
        union all
        select
            row_number() over (order by a."person_id" asc),
            a."person_id", 2 as "source_id",
            p."party_id",
            a."start",
            a."start_flag",
            a."end",
            a."end_flag",
            a."start_year",
            a."end_year"
        from affiliation a
        join persons_of_interest using ("person_id")
        join swedeb_parties sp
          on sp."swerik_party" = a."party"
        join party p on p."party" = sp."party"
        ;

with persons_with_many_partys as (
    select "person_id"
    from person_party
    group by "person_id"
    having count(distinct "party_id") > 1
)
    update persons_of_interest set has_multiple_parties = TRUE
        where "person_id" in (select "person_id" from persons_with_many_partys);

with persons_with_many_partys as (
    select "person_id"
    from person_party
    group by "person_id"
    having count(distinct "party_id") > 1
)
    update person_party set has_multiple_parties = TRUE
        where "person_id" in (select "person_id" from persons_with_many_partys);

/* Update "party_id" for persons having only one party */
with persons_with_single_party as (
    select "person_id", max("party_id") as "party_id"
    from person_party
    group by "person_id"
    having count(distinct "party_id") = 1
)
    insert into persons_of_interest ("person_id", "party_id")
        select "person_id", persons_with_single_party."party_id"
        from persons_of_interest
        join persons_with_single_party using ("person_id")
        where persons_of_interest."has_multiple_parties" = FALSE
    on conflict("person_id") do update 
        set "party_id"=excluded."party_id";

