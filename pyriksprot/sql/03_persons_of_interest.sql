/* Persons actually occuring in the corpus */

drop view if exists person_name;
drop table if exists person_location;
drop table if exists persons_of_interest;

create view person_name as
    select "person_id",
            coalesce(
                max(case when "primary_name" = 1 then _name."name" else null end),
                max("name")
            ) as "name"
    from _name
    group by "person_id";

create table persons_of_interest (
   "person_id" varchar primary key not null,
   "gender_id" integer not null default(0) references gender("gender_id"),
   -- Only set for person having a single party:
   "party_id" integer null references party("party_id"),
   "name" varchar not null default(''),
   "year_of_birth" integer null,
   "year_of_death" integer null,
   "has_multiple_parties" bool not null default(FALSE),
   "wiki_id" text not null default('unknown')
);

delete from persons_of_interest;

insert into persons_of_interest ("person_id")
    select distinct "person_id"
    from utterances;

insert into persons_of_interest("person_id", "gender_id")
    select persons_of_interest."person_id", gender."gender_id"
    from persons_of_interest
    join _person using ("person_id")
    join gender on _person."gender" = gender."gender_source"
	  on conflict("person_id") do update
        set "gender_id" = excluded."gender_id";

insert into persons_of_interest("person_id", "year_of_birth", "year_of_death")
    select persons_of_interest."person_id",
           cast(substr(cast(_person."born" as text), 1, 4) as integer) as "year_of_birth",
           cast(substr(cast(_person."dead" as text), 1, 4) as integer) as "year_of_death"
    from persons_of_interest
    join _person using ("person_id")
      on conflict("person_id") do update
        set "year_of_birth" = excluded."year_of_birth",
            "year_of_death" = excluded."year_of_death";

insert into persons_of_interest ("person_id", "name")
    select persons_of_interest."person_id", person_name."name"
    from persons_of_interest
    join person_name using ("person_id")
	    on conflict("person_id") do update
			set "name" = excluded."name";

insert into persons_of_interest ("person_id", "wiki_id")
    select persons_of_interest."person_id", _wiki_id."wiki_id"
    from persons_of_interest
    join _wiki_id using ("person_id")
	    on conflict("person_id") do update
			set "wiki_id" = excluded."wiki_id";

create table person_location (
   "person_location_id" integer primary key,
   "person_id" varchar not null references persons_of_interest("person_id"),
   "location_id" integer not null references "location"("location_id")
);

insert into person_location ("person_location_id", "person_id", "location_id")
    select row_number() over (order by person_id asc), "person_id", "location_id"
    from persons_of_interest
    join _location_specifier using ("person_id")
    join location
	  on location."location_name" = _location_specifier."location"
