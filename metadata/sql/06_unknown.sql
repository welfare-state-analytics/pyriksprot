
-- drop table if exists unknown_speaker_note_party;
-- create table unknown_speaker_note_party (
--     speaker_hash varchar primary key,
--     party_id int not null
-- );
-- insert into unknown_speaker_note_party (speaker_hash, party_id)
--     select [hash], party_id
--     from unknowns
--     join _party_abbreviation pa using (party)
--     join party on party.party_abbrev = pa.abbreviation;
create index idx_utterances_speaker_hash on utterances(speaker_hash);

drop table if exists unknown_utterance_party;
create table unknown_utterance_party (
    u_id varchar primary key,
    party_id int not null
);
with unknown_speaker_note_party (speaker_hash, party_id) as (
    select [hash], party_id
    from unknowns
    join _party_abbreviation pa using (party)
    join party on party.party_abbrev = pa.abbreviation
)
    insert into unknown_utterance_party(u_id, party_id)
        select u_id, party_id
        from utterances
        join unknown_speaker_note_party using (speaker_hash);
drop table if exists unknown_utterance_gender;
create table unknown_utterance_gender (
    u_id varchar primary key,
    gender_id int not null
);
with unknown_speaker_note_gender (speaker_hash, gender_id) as (
    select [hash], gender_id
    from unknowns
    join gender using (gender)
)
    insert into unknown_utterance_gender(u_id, gender_id)
        select u_id, gender_id
        from utterances
        join unknown_speaker_note_gender using (speaker_hash);
