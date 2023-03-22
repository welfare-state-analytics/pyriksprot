
-- insert into _person values ('unknown', null, null, 'unknown', null, null, null);

-- insert into _name values ('unknown', 'unknown', 1);

update _party_abbreviation
    set abbreviation = 'FRIS'
        where abbreviation = 'independent';

drop table if exists _party_abbreviation_jj;

create table if not exists _party_abbreviation_jj (
    party text not null unique,
    abbreviation text not null,
    ocr_correction text not null
);

/*
    2023-03-20: all correnctions seems to be fixed in source except "partilös" -> "X"
*/

-- update _party_affiliation set party = 'Miljöpartiet' where person_id = 'Q21731075';
 with _party_abbreviation_jj_data(party, abbreviation, ocr_correction) as (
        values
            ('Bondeförbundet', 'C', 'Korrigeringar av J. Jarlbrink'),
            ('Centerpartiet', 'C', 'Korrigeringar av J. Jarlbrink'),
            ('Feministiskt initiativ', 'X', 'Korrigeringar av J. Jarlbrink'),
            ('Folkpartiet', 'L', 'Korrigeringar av J. Jarlbrink'),
            ('Första kammarens nationella parti', 'M', 'Korrigeringar av J. Jarlbrink'),
            ('fris', 'FRIS', 'Korrigeringar av J. Jarlbrink'),
            ('Frisinnade folkpartiet', 'FRIS', 'Korrigeringar av J. Jarlbrink'),
            ('Frisinnade försvarsvänner', 'L', 'Korrigeringar av J. Jarlbrink'),
            ('Frisinnade landsföreningen', 'FRIS', 'Korrigeringar av J. Jarlbrink'),
            ('Högerns riksdagsgrupp', 'M', 'Korrigeringar av J. Jarlbrink'),
            ('Högerpartiet', 'M', 'Korrigeringar av J. Jarlbrink'),
            ('högervilde', 'X', 'Korrigeringar av J. Jarlbrink'),
            ('Jordbrukarnas fria grupp', 'C', 'Korrigeringar av J. Jarlbrink'),
            ('Kilbomspartiet', 'SP', 'Korrigeringar av J. Jarlbrink'),
            ('Klimatalliansen', 'V', 'Korrigeringar av J. Jarlbrink'),
            ('kommunistisk vilde', 'X', 'Korrigeringar av J. Jarlbrink'),
            ('Kommunistiska partiet', 'SP', 'Korrigeringar av J. Jarlbrink'),
            ('Kommunistiska partiets riksdagsgrupp', 'V', 'Korrigeringar av J. Jarlbrink'),
            ('kommunistiskt parti', 'V', 'Korrigeringar av J. Jarlbrink'),
            ('Kommunistpartiet', 'V', 'Korrigeringar av J. Jarlbrink'),
            ('Kristdemokrater i Svenska kyrkan', 'KD', 'Korrigeringar av J. Jarlbrink'),
            ('Kristdemokraterna', 'KD', 'Korrigeringar av J. Jarlbrink'),
            ('Lantmanna- och borgarepartiet inom andrakammaren', 'M', 'Korrigeringar av J. Jarlbrink'),
            ('Liberala riksdagspartiet', 'L', 'Korrigeringar av J. Jarlbrink'),
            ('Liberala samlingspartiet', 'L', 'Korrigeringar av J. Jarlbrink'),
            ('Liberalerna', 'L', 'Korrigeringar av J. Jarlbrink'),
            ('Miljöpartiet', 'MP', 'Korrigeringar av J. Jarlbrink'),
            ('mod vilde', 'X', 'Korrigeringar av J. Jarlbrink'),
            ('Moderaterna', 'M', 'Korrigeringar av J. Jarlbrink'),
            ('Ny demokrati', 'NYD', 'Korrigeringar av J. Jarlbrink'),
            ('Partiet Vändpunkt', 'MP', 'Korrigeringar av J. Jarlbrink'),
            ('partilös', 'X', 'Korrigeringar av J. Jarlbrink'),
            ('Socialdemokraterna', 'S', 'Korrigeringar av J. Jarlbrink'),
            ('Socialdemokratiska vänstergruppen', 'V', 'Korrigeringar av J. Jarlbrink'),
            ('Socialistiska partiet', 'SP', 'Korrigeringar av J. Jarlbrink'),
            ('Sverges kommunistiska parti', 'V', 'Korrigeringar av J. Jarlbrink'),
            ('Sverigedemokraterna', 'SD', 'Korrigeringar av J. Jarlbrink'),
            ('Sveriges liberala parti', 'L', 'Korrigeringar av J. Jarlbrink'),
            ('Sveriges nationella förbund', 'X', 'Korrigeringar av J. Jarlbrink'),
            ('unknown_missing', '?', 'Korrigeringar av J. Jarlbrink'),
            ('unknown', '?', 'Korrigeringar av J. Jarlbrink'),
            ('Vänsterpartiet', 'V', 'Korrigeringar av J. Jarlbrink'),
            ('vänstersocialistisk vilde', 'X', 'Korrigeringar av J. Jarlbrink'),
            ('vilde', 'X', 'Korrigeringar av J. Jarlbrink')
    )
        insert into _party_abbreviation_jj (party, abbreviation, ocr_correction)
            select party, abbreviation, ocr_correction
            from _party_abbreviation_jj_data;


insert into _party_abbreviation (party, abbreviation, ocr_correction)
    select party, abbreviation, ocr_correction
    from _party_abbreviation_jj
    where TRUE
        on conflict(party) do update set abbreviation = excluded.abbreviation, ocr_correction=excluded.ocr_correction
;
