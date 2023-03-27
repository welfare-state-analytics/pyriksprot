-- drop table unknowns;
alter table unknowns rename to _unknowns;

-- alter table terms_of_office drop column _government_id;

/* Drop column the long way */

/*
pragma foreign_keys=off;

begin transaction;

drop table if exists _terms_of_office;
alter table terms_of_office rename to _terms_of_office;

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
   [end_year] integer null
);

insert into terms_of_office (
    [terms_of_office_id], [person_id], [office_type_id], [sub_office_type_id], [district_id], [start_date], [start_flag], [end_date], [end_flag] [start_year], [end_year]
)
  select [terms_of_office_id], [person_id], [office_type_id], [sub_office_type_id], [district_id], [start_date], [start_flag], [end_date], [end_flag] [start_year], [end_year]
  from _terms_of_office;

commit;
*/

pragma foreign_keys=on;