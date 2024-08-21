/*
Create a single table out of:
  - members of parliament
  - ministers
  - speakers
*/
drop table if exists minister_government;
drop table if exists chamber;
drop table if exists office_type;
drop table if exists sub_office_type;
drop table if exists terms_of_office;
/* Create chambers code table */
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
    [sub_office_type_id] integer primary key not null,
    [office_type_id] integer not null references office_type(office_type_id),
    [description] text not null,
    [chamber_id] integer null references chamber(chamber_id),
    [identifier] text null
);
insert into sub_office_type (sub_office_type_id, office_type_id, [description], chamber_id, identifier)
    values
        (0, 0, 'unknown', null, null);

insert into sub_office_type (office_type_id, [description], chamber_id, identifier)
    values
        (1, 'Ledamot av första kammaren', 1, 'förstakammarledamot'),
        (1, 'Ledamot av andra kammaren', 2, 'andrakammarledamot'),
        (1, 'ledamot av Sveriges riksdag', 3, 'ledamot av Sveriges riksdag'),
        (2, 'minister (ospecificerad)', null, null),
        (3, 'talman (ospecificerad)', null, null);

insert into sub_office_type (office_type_id, [description], identifier)
    select distinct 2, role, role
    from _minister
    order by role;

insert into sub_office_type (office_type_id, [description], chamber_id, identifier)
    select distinct 3, role,
        case
            when role like '%första kammaren%' then 1
            when role like '%andra kammaren%' then 2
            else 3
        end,
        role
    from _speaker
    order by role;

create table terms_of_office (
   [terms_of_office_id] integer primary key,
   [person_id] varchar not null references persons_of_interest(person_id),
   [office_type_id] integer not null default(0) references office_type(office_type_id),
   [sub_office_type_id] integer not null default(0) references sub_office_type(sub_office_type_id),
   [district_id] integer null references district(district_id),
   [start_date] date null,
   [start_flag] text not null default('X'),
   [end_date] date null,
   [end_flag] text not null default('X'),
   [start_year] integer null,
   [end_year] integer null,
   [government_id] integer null
);

insert into terms_of_office (
        person_id, office_type_id, sub_office_type_id, district_id,
        [start_date], [start_flag], [end_date], [end_flag], [start_year], [end_year]
    )
    select  _member_of_parliament.person_id,
            1,
            coalesce(sub_office_type.sub_office_type_id, 0),
            coalesce(district.district_id, 0),
            _member_of_parliament.[start],
            _member_of_parliament.[start_flag],
            _member_of_parliament.[end],
            _member_of_parliament.[end_flag],
            cast(substr(_member_of_parliament.[start], 1, 4) as integer) as [start_year],
            cast(substr(_member_of_parliament.[end], 1, 4) as integer) as [end_year]
    from _member_of_parliament
    join persons_of_interest using (person_id)
    left join district on district.district = _member_of_parliament.district
    left join sub_office_type on sub_office_type.identifier = _member_of_parliament.role;
--insert into terms_of_office (person_id, office_type_id, sub_office_type_id, district_id, start_year, end_year)
insert into terms_of_office (
        [person_id], [office_type_id], [sub_office_type_id],
        [start_date], [start_flag], [end_date], [end_flag], [start_year], [end_year], government_id
    )
    select  _minister.person_id,
            2,
            coalesce(sub_office_type.sub_office_type_id, 0),
            _minister.[start],
            _minister.[start_flag],
            _minister.[end],
            _minister.[end_flag],
            cast(substr(_minister.[start], 1, 4) as integer) as start_year,
            cast(substr(_minister.[end], 1, 4) as integer) as end_year,
            government.government_id
    from _minister
    join government
      on government.government = _minister.government
    join persons_of_interest using (person_id)
    left join sub_office_type
      on sub_office_type.office_type_id = 2
     and sub_office_type.identifier = _minister.role;

insert into terms_of_office (
        person_id, office_type_id, sub_office_type_id,
        [start_date], [start_flag], [end_date], [end_flag], [start_year], [end_year]
    )
    select  _speaker.person_id,
            3,
            coalesce(sub_office_type.sub_office_type_id, 0),
            _speaker.[start],
            _speaker.[start_flag],
            _speaker.[end],
            _speaker.[end_flag],
            cast(substr(_speaker.[start], 1, 4) as integer) as start_year,
            cast(substr(_speaker.[end], 1, 4) as integer) as end_year
    from _speaker
    join persons_of_interest using (person_id)
    left join sub_office_type
      on sub_office_type.office_type_id = 3
     and sub_office_type.identifier = _speaker.role;

create table minister_government (
    minister_government_id integer primary key,
    terms_of_office_id int not null references terms_of_office(terms_of_office_id),
    government_id integer not null references government(government_id)
);
insert into minister_government (terms_of_office_id, government_id)
    select terms_of_office_id, government_id
    from terms_of_office
    where office_type_id = 2;
