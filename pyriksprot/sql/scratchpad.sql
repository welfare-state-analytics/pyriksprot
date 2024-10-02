
with reference_constraint_errors(table_name, person_id) as (
	select '_references_map', person_id from _references_map where person_id not in (select person_id from _person) UNION
	select '_speaker', person_id from _speaker where person_id not in (select person_id from _person) UNION
	select '_chair_mp', person_id from _chair_mp where person_id not in (select person_id from _person) UNION
	select '_described_by_source', person_id from _described_by_source where person_id not in (select person_id from _person) UNION
	select '_explicit_no_party', person_id from _explicit_no_party where person_id not in (select person_id from _person) UNION
	select '_external_identifiers', person_id from _external_identifiers where person_id not in (select person_id from _person) UNION
	select '_location_specifier', person_id from _location_specifier where person_id not in (select person_id from _person) UNION
	select '_member_of_parliament', person_id from _member_of_parliament where person_id not in (select person_id from _person) UNION
	select '_minister', person_id from _minister where person_id not in (select person_id from _person) UNION
	select '_name', person_id from _name where person_id not in (select person_id from _person) UNION
	select '_party_affiliation', person_id from _party_affiliation where person_id not in (select person_id from _person) UNION
	select '_place_of_birth', person_id from _place_of_birth where person_id not in (select person_id from _person) UNION
	select '_place_of_death', person_id from _place_of_death where person_id not in (select person_id from _person) UNION
	select '_portraits', person_id from _portraits where person_id not in (select person_id from _person) UNION
	select '_wiki_id', person_id from _wiki_id where person_id not in (select person_id from _person) UNION
	select '_twitter', person_id from _twitter where person_id not in (select person_id from _person)
) select table_name, string_agg('''' || person_id || '''', ', '), count(*)
  from reference_constraint_errors
  group by table_name
