
-- drop table if exists unknown_speaker_note_party;
-- create table unknown_speaker_note_party (
--     speaker_note_id varchar primary key,
--     party_id int not null
-- );
-- insert into unknown_speaker_note_party (speaker_note_id, party_id)
--     select [uuid], party_id
--     from unknowns
--     join _party_abbreviation pa using (party)
--     join party on party.party_abbrev = pa.abbreviation;
create index idx_utterances_speaker_note_id on utterances(speaker_note_id);

drop table if exists unknown_utterance_party;
create table unknown_utterance_party (
    u_id varchar, -- primary key,
    party_id int not null
);
/* NOTE: unknowns CSV has (from 0.4.1) dupes `uuid` records
    hence the grouping below.
    None of the dupes has ambigous party_id.
*/
with unknown_speaker_note_party (speaker_note_id, party_id) as (
    select [uuid], max(party_id) as party_id
    from unknowns
    join _party_abbreviation pa using (party)
    join party on party.party_abbrev = pa.abbreviation
    group by [uuid]
)
    insert into unknown_utterance_party(u_id, party_id)
        select u_id, party_id
        from utterances
        join unknown_speaker_note_party using (speaker_note_id);
drop table if exists unknown_utterance_gender;
create table unknown_utterance_gender (
    u_id varchar primary key,
    gender_id int not null
);
with unknown_speaker_note_gender (speaker_note_id, gender_id) as (
    select [uuid], max(gender_id) as gender_id
    from unknowns
    join gender using (gender)
    group by [uuid]
)
    insert into unknown_utterance_gender(u_id, gender_id)
        select u_id, gender_id
        from utterances
        join unknown_speaker_note_gender using (speaker_note_id);
