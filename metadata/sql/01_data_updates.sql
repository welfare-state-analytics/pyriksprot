
/* Add missing Governments */

insert into _government (government, start, end)
  values
    ('De Geer d.y.', '1920-10-27', '1921-01-23'),
    ('von Sydow', '1923-01-23', '1921-10-13'),
    ('Trygger', '1923-04-19', '1924-10-24'),
    ('Sandler', '1925-01-24', '1926-06-07'),
    ('Ekman I', '1926-06-07', '1928-10-02'),
    ('Lindman II', '1928-10-02', '1930-06-07'),
    ('Ekman II', '1930-06-07', '1932-08-06'),
    ('Hamrin', '1932-08-06', '1932-09-24'),
    ('Hansson I', '1932-09-24', '1936-06-19'),
    ('Pehrsson-Bramstorp', '1936-06-19', '1936-09-28'),
    ('Regeringen Erlander II', '1951-10-01', '1957-10-31'),
    ('Regeringen Erlander III', '1957-10-31', '1969-10-14');

insert into _person values ('unknown', null, null, 'unknown', null, null, null);

insert into _name values ('unknown', 'unknown', 1);

update _party_affiliation set party = 'Miljöpartiet' where person_id = 'Q21731075';
