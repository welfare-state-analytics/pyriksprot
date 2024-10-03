/* Distinct years */

drop table if exists years;

create table years (
    "year" integer primary key
);

insert into years ("year")
    with recursive series(value) as (
        values(1867)
        union all
        select value+1 from series
        where value<2030
    )
        select value
        from series;

/* Gender */

drop table if exists gender;

create table gender (
    "gender_id" integer primary key,
    "gender" varchar not null,
    "gender_abbrev" varchar not null,
    "gender_source" varchar not null
);
    
insert into gender ("gender_id", "gender", "gender_abbrev", "gender_source")
    values (0, 'Okänt', '?', 'unknown'),
           (1, 'Man', 'M', 'man'),
           (2, 'Kvinna', 'K', "woman");

/* District */

drop table if exists district;

create table district (
    "district_id" integer primary key,
    "district" varchar not null
);

insert into district values (0, 'unknown');

insert into district ("district_id", "district")
    with unique_districts as (
        select distinct "district"
        from _member_of_parliament
        where "district" is not null
    )
        select row_number() over (order by "district" asc), "district"
        from unique_districts;

/* Location */

drop table if exists location;

create table location (
    "location_id" serial primary key,
    "location_name" varchar not null
);

insert into location values (0, 'unknown');

insert into location ("location_id", "location_name")
    with unique_locations as (
        select distinct "location"
        from _location_specifier
        where "location" is not null
    )
        select row_number() over (order by "location" asc), "location"
        from unique_locations;

/* Government */

drop table if exists government;

create table government (
    "government_id" integer primary key,
    "government" varchar not null,
    "start_date" date not null,
    "end_date" date null
);

insert into government ("government_id", "government", "start_date", "end_date")
    select row_number() over (order by "start" asc), "government", cast("start" as date), cast("end" as date)
    from _government
    order by "start";

/* Party: records are added in 05_person_party.sql */

drop table if exists party;

create table if not exists party (
    "party_id" integer primary key,
    "party" varchar not null unique,
    "party_abbrev" varchar not null unique,
    "party_color" varchar default('#3f1105')
);

insert into party("party_id", "party", "party_abbrev", "party_color")
    with party_data("party_id", "party", "party_abbrev", "party_color") as (
        values
            (0, 'Okänt', '?', '#000000'),
            (1, 'Partilös', 'X', '#333333'),
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
            select "party_id", "party", "party_abbrev", "party_color"
            from party_data
;

/*
Create a single table out of:
  - members of parliament
  - ministers
  - speakers
*/
drop table if exists minister_government;
drop table if exists terms_of_office;
drop table if exists sub_office_type;
drop table if exists office_type;
drop table if exists chamber;

/* Chambers */

create table chamber (
    chamber_id integer primary key,
    chamber text
);

insert into chamber (chamber_id, chamber)
    values
        (0, 'unknown'),
        (1, 'Första kammaren'),
        (2, 'Andra kammaren'),
        (3, 'Sveriges riksdag')
;

/* Office types */
create table office_type (
    office_type_id integer primary key,
    office text,
    role text
);

insert into office_type (office_type_id, office)
    values
        (0, 'unknown'),
        (1, 'Ledamot'),
        (2, 'Minister'),
        (3, 'Talman')
;

/*
sub_office_type: division of office_type (e.g. minister types, speaker types)
*/
create table sub_office_type (
    "sub_office_type_id" integer primary key not null,
    "office_type_id" integer not null references office_type(office_type_id),
    "description" text not null,
    "chamber_id" integer null references chamber(chamber_id),
    "identifier" text null
);

insert into sub_office_type (sub_office_type_id, office_type_id, "description", chamber_id, identifier)
	with sot(sub_office_type_id, office_type_id, "description", chamber_id, identifier) as (
	    values
	        (0, 0, 'unknown', null, null),
	        (1, 1, 'Ledamot av första kammaren', 1, 'förstakammarledamot'),
	        (2, 1, 'Ledamot av andra kammaren', 2, 'andrakammarledamot'),
	        (3, 1, 'ledamot av Sveriges riksdag', 3, 'ledamot av Sveriges riksdag'),
	        (4, 2, 'minister (ospecificerad)', null, null),
	        (5, 3, 'talman (ospecificerad)', null, null)
	)
		select sub_office_type_id, office_type_id, "description", chamber_id, identifier
		from sot;
	
insert into sub_office_type (sub_office_type_id, office_type_id, "description", chamber_id, identifier)
	with mnstr as (
	    select distinct role, (select max(sub_office_type_id) from sub_office_type) as max_id
	    from _minister
	)
		select row_number() over (order by role) + max_id, 2, role, null, role
		from mnstr
	    order by role;

insert into sub_office_type (sub_office_type_id, office_type_id, "description", chamber_id, identifier)
	with spkr as (
	    select distinct role, (select max(sub_office_type_id) from sub_office_type) as max_id
	    from _speaker
	)
	    select row_number() over (order by role) + max_id, 3, role,
	        case
	            when role like '%första kammaren%' then 1
	            when role like '%andra kammaren%' then 2
	            else 3
	        end,
	        role
	    from spkr;
