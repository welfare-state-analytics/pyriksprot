drop table if exists years;
create table years (
    year integer primary key
);
drop table if exists gender;

with recursive series(value) as (
    values(1867)
    union all
    select value+1 from series
    where value<2023
) insert into years (year) select value from series;
create table gender (
    gender_id integer primary key,
    gender varchar not null
);

insert into gender values (0, 'unknown'), (1, 'man'), (2, 'woman');
drop table if exists district;

create table district (
    district_id integer primary key,
    district varchar not null
);

insert into district values (0, 'unknown');
insert into district (district)
    select distinct district
    from _member_of_parliament
    where district is not null
    order by district;
drop table if exists location;
create table location (
    location_id integer primary key,
    location_name varchar not null
);
insert into location values (0, 'unknown');
insert into location (location_name)
    select distinct location
    from _location_specifier
    where location is not null
    order by location;

drop table if exists government;
create table government (
    government_id integer primary key,
    government varchar not null,
    start_date date not null,
    end_date date null
);

insert into government (government, start_date, end_date)
    select government, [start], [end]
    from _government
    order by [start];

/* Party table: records are added in 05_person_party.sql */
drop table if exists party;

create table if not exists party (
    party_id integer primary key,
    party varchar not null unique,
    party_abbrev varchar not null unique,
    party_color varchar default('#3f1105')
);

with party_data(party_id, party, party_abbrev, party_color) as (
    values
        (0, 'unknown', '?', '#000000'),
        (1, 'Other', 'X', '#333333'),
        (2, 'Centerpartiet', 'C', '#009933'),
        (3, 'Kristdemokraterna', 'KD', '#000077'),
        (4, 'Socialistiska partiet', 'SP', '#FF112d'),
        (5, 'Liberalerna', 'L', '#006AB3'),
        (6, 'Miljöpartiet', 'MP', '#83CF39'),
        (7, 'Moderaterna', 'M', '#52BDEC'),
        (8, 'Ny demokrati', 'NYD', '#007700'),
        (9, 'Socialdemokraterna', 'S', '#E8112d'),
        (10, 'Sverigedemokraterna', 'SD', '#DDDD00'),
        (11, 'Vänsterpartiet', 'V', '#DA291C'),
        (12, 'Frisinnad', 'FRIS', '#226AB3'),
        (13, 'Arbetarpartiet kommunisterna', 'APK', '#DA291C')
)
    insert into party(party_id, party, party_abbrev, party_color)
        select party_id, party, party_abbrev, party_color
        from party_data
;
