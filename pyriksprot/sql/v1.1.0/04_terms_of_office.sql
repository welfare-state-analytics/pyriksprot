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

/* Terms of office */

create table terms_of_office (
   "terms_of_office_id" integer primary key,
   "person_id" varchar not null references persons_of_interest(person_id),
   "office_type_id" integer not null default(0) references office_type(office_type_id),
   "sub_office_type_id" integer not null default(0) references sub_office_type(sub_office_type_id),
   "district_id" integer null references district(district_id),
   "start_date" date null,
   "start_flag" text not null default('X'),
   "end_date" date null,
   "end_flag" text not null default('X'),
   "start_year" integer null,
   "end_year" integer null,
   "government_id" integer null
);

insert into terms_of_office (
        "terms_of_office_id",
		"person_id",
        "office_type_id",
        "sub_office_type_id",
        "district_id",
        "start_date",
        "start_flag",
        "end_date",
        "end_flag",
        "start_year",
        "end_year",
        "government_id"
    )
    
    select
        row_number() over (order by "person_id" asc), 
		"person_id",
        "office_type_id",
        "sub_office_type_id",
        "district_id",
        "start_date",
        "start_flag",
        "end_date",
        "end_flag",
        "start_year",
        "end_year",
        "government_id"
    from (
        
        select _member_of_parliament."person_id" 					as "person_id",
                1 													as "office_type_id",
                coalesce(sub_office_type.sub_office_type_id, 0) 	as "sub_office_type_id",
                coalesce(district.district_id, 0) 					as "district_id",
                _member_of_parliament."start"                       as "start_date",
                _member_of_parliament."start_flag"                  as "start_flag",
                _member_of_parliament."end"                         as "end_date",
                _member_of_parliament."end_flag"                    as "end_flag",
                cast(substr(cast(_member_of_parliament."start" as text), 1, 4) as integer) as "start_year",
                cast(substr(cast(_member_of_parliament."end" as text), 1, 4) as integer)   as "end_year",
                null as "government_id"
        from _member_of_parliament
        join persons_of_interest using ("person_id")
        left join district on district."district" = _member_of_parliament."district"
        left join sub_office_type on sub_office_type."identifier" = _member_of_parliament."role"
        
        union
        
        select  _minister."person_id",
                2,
                coalesce(sub_office_type."sub_office_type_id", 0),
                null,
                _minister."start",
                _minister."start_flag",
                _minister."end",
                _minister."end_flag",
                cast(substr(cast(_minister."start" as text), 1, 4) as integer) ,
                cast(substr(cast(_minister."end" as text), 1, 4) as integer),
                government."government_id"
        from _minister
        join government using ("government")
        join persons_of_interest using (person_id)
        left join sub_office_type
          on sub_office_type."office_type_id" = 2
         and sub_office_type."identifier" = _minister.role
        
        union
        
        select  _speaker."person_id",
                3,
                coalesce(sub_office_type."sub_office_type_id", 0),
                null,
                _speaker."start",
                _speaker."start_flag",
                _speaker."end",
                _speaker."end_flag",
                cast(substr(cast(_speaker."start" as text), 1, 4) as integer),
                cast(substr(cast(_speaker."end" as text), 1, 4) as integer),
                null
        from _speaker
        join persons_of_interest using ("person_id")
        left join sub_office_type
          on sub_office_type."office_type_id" = 3
         and sub_office_type."identifier" = _speaker."role"
        
    ) as b;


/* Minister government */

create table minister_government (
    "minister_government_id" integer primary key,
    "terms_of_office_id" int not null references terms_of_office(terms_of_office_id),
    "government_id" integer not null references government(government_id)
);

insert into minister_government ("minister_government_id", "terms_of_office_id", "government_id")
    select row_number() over (order by "government_id", "terms_of_office_id" asc), "terms_of_office_id", "government_id"
    from terms_of_office
    where "office_type_id" = 2;

