
/* Persons actually occuring in the corpus */
drop view if exists person_name;
drop table if exists person_location;
drop table if exists persons_of_interest;
create view person_name as
    select person_id, coalesce(
           max(case when primary_name = 1 then _name.name else null end),
           max(name)) as name
    from _name
    group by person_id;
create table persons_of_interest (
   [person_id] varchar primary key not null,
   [gender_id] integer not null default(0) references gender(gender_id),
   -- Only set for person having a single party:
   [party_id] integer null references party(party_id),
   [name] varchar not null default(''),
   [year_of_birth] integer null,
   [year_of_death] integer null,
   [has_multiple_parties] bool not null default(FALSE)
);

delete from persons_of_interest;

insert into persons_of_interest (person_id)
    select distinct person_id
    from utterances;

insert into persons_of_interest (person_id, gender_id, year_of_birth, year_of_death)
    select  persons_of_interest.person_id,
            gender.gender_id,
            cast(substr(_person.[born], 1, 4) as integer) as year_of_birth,
            cast(substr(_person.[dead], 1, 4) as integer) as year_of_death
    from persons_of_interest
    join _person using (person_id)
    join gender using (gender)
      on conflict(person_id) do update
        set gender_id=excluded.gender_id, year_of_birth=excluded.year_of_birth, year_of_death=excluded.year_of_death;

insert into persons_of_interest (person_id, [name])
    select persons_of_interest.person_id, person_name.name
    from persons_of_interest
    join person_name using (person_id)
    on conflict(person_id) do update set name=excluded.[name];

create table person_location (
   [person_location_id] integer primary key,
   [person_id] integer not null default(0) references persons_of_interest(person_id),
   [location_id] varchar not null references [location](location_id)
);

insert into person_location (person_id, location_id)
    select person_id, location_id
    from persons_of_interest
    join _location_specifier using (person_id)
    join [location] on [location].location_name =  _location_specifier.[location]
