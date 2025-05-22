
insert into _person (person_id, born, dead, gender, riksdagen_id) values ('unknown', null, null, 'unknown', null);

insert into _name values ('unknown', 'Okänd', 1);

update _name set "name" = replace("name", '&', '&amp') where "name" like '%&%';
update _name set "name" = replace("name", '<', '&lt') where "name" like '%<%';
update _name set "name" = replace("name", '>', '&gt') where "name" like '%>%';
update _name set "name" = replace("name", '"', '&quot') where "name" like '%"%';
update _name set "name" = replace("name", '''', '&apos') where "name" like '%''%';
