
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

