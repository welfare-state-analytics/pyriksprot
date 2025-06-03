-- FIXME: #88 incorporate this into the main generate database script
-- select 'drop table if exists "' || name || '";'
--   from sqlite_master
--   where type = 'table' and name like '\_%' escape '\';

drop table if exists "_chairs";
drop table if exists "_government";
drop table if exists "_party_abbreviation";
drop table if exists "_person";
drop table if exists "_riksdag_year";
drop table if exists "_chair_mp";
drop table if exists "_described_by_source";
drop table if exists "_explicit_no_party";
drop table if exists "_external_identifiers";
drop table if exists "_location_specifier";
drop table if exists "_member_of_parliament";
drop table if exists "_minister";
drop table if exists "_name";
drop table if exists "_party_affiliation";
drop table if exists "_place_of_birth";
drop table if exists "_place_of_death";
drop table if exists "_portraits";
drop table if exists "_references_map";
drop table if exists "_speaker";
drop table if exists "_twitter";
drop table if exists "_wiki_id";

---drop table utterances;
---drop table protocols;
---drop table person_yearly_party;

---drop table unknown_utterance_gender;
---drop table unknown_utterance_party;

