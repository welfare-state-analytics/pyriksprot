
-- insert into _person values ('unknown', null, null, 'unknown', null, null, null);

-- insert into _name values ('unknown', 'unknown', 1);

-- update _party_affiliation set party = 'Miljöpartiet' where person_id = 'Q21731075';
insert into _party_abbreviation (party, abbreviation, ocr_correction)
    with manual_corrections_by_jj(party, abbreviation, ocr_correction) as (
        select *
        from (values
            ('Socialdemokraterna', 'S', 'Korrigeringar av J. Jarlbrink'),
            ('Moderaterna', 'M', 'Korrigeringar av J. Jarlbrink'),
            ('Liberalerna', 'L', 'Korrigeringar av J. Jarlbrink'),
            ('Centerpartiet', 'C', 'Korrigeringar av J. Jarlbrink'),
            ('Vänsterpartiet', 'V', 'Korrigeringar av J. Jarlbrink'),
            ('unknown', '?', 'Korrigeringar av J. Jarlbrink'),
            ('Miljöpartiet', 'MP', 'Korrigeringar av J. Jarlbrink'),
            ('Kristdemokraterna', 'KD', 'Korrigeringar av J. Jarlbrink'),
            ('Folkpartiet', 'L', 'Korrigeringar av J. Jarlbrink'),
            ('Sverigedemokraterna', 'SD', 'Korrigeringar av J. Jarlbrink'),
            ('Bondeförbundet', 'C', 'Korrigeringar av J. Jarlbrink'),
            ('Högerpartiet', 'M', 'Korrigeringar av J. Jarlbrink'),
            ('Lantmanna- och borgarepartiet inom andrakammaren', 'M', 'Korrigeringar av J. Jarlbrink'),
            ('Första kammarens nationella parti', 'M', 'Korrigeringar av J. Jarlbrink'),
            ('Frisinnade folkpartiet', 'FRIS', 'Korrigeringar av J. Jarlbrink'),
            ('unknown_missing', '?', 'Korrigeringar av J. Jarlbrink'),
            ('Ny demokrati', 'NYD', 'Korrigeringar av J. Jarlbrink'),
            ('Liberala samlingspartiet', 'L', 'Korrigeringar av J. Jarlbrink'),
            ('Högerns riksdagsgrupp', 'M', 'Korrigeringar av J. Jarlbrink'),
            ('Liberala riksdagspartiet', 'L', 'Korrigeringar av J. Jarlbrink'),
            ('partilös', 'X', 'Korrigeringar av J. Jarlbrink'),
            ('Klimatalliansen', 'V', 'Korrigeringar av J. Jarlbrink'),
            ('Kommunistiska partiets riksdagsgrupp', 'V', 'Korrigeringar av J. Jarlbrink'),
            ('Partiet Vändpunkt', 'MP', 'Korrigeringar av J. Jarlbrink'),
            ('Kilbomspartiet', 'SP', 'Korrigeringar av J. Jarlbrink'),
            ('Socialdemokratiska vänstergruppen', 'V', 'Korrigeringar av J. Jarlbrink'),
            ('Socialistiska partiet', 'SP', 'Korrigeringar av J. Jarlbrink'),
            ('partilös', 'X', 'Korrigeringar av J. Jarlbrink'),
            ('Frisinnade landsföreningen', 'FRIS', 'Korrigeringar av J. Jarlbrink'),
            ('vänstersocialistisk vilde', 'X', 'Korrigeringar av J. Jarlbrink'),
            ('Kristdemokrater i Svenska kyrkan', 'KD', 'Korrigeringar av J. Jarlbrink'),
            ('vilde', 'X', 'Korrigeringar av J. Jarlbrink'),
            ('Jordbrukarnas fria grupp', 'C', 'Korrigeringar av J. Jarlbrink'),
            ('högervilde', 'X', 'Korrigeringar av J. Jarlbrink'),
            ('kommunistiskt parti', 'V', 'Korrigeringar av J. Jarlbrink'),
            ('Kommunistpartiet', 'V', 'Korrigeringar av J. Jarlbrink'),
            ('Kommunistiska partiet', 'SP', 'Korrigeringar av J. Jarlbrink'),
            ('Feministiskt initiativ', 'X', 'Korrigeringar av J. Jarlbrink'),
            ('mod vilde', 'X', 'Korrigeringar av J. Jarlbrink'),
            ('Sveriges nationella förbund', 'X', 'Korrigeringar av J. Jarlbrink'),
            ('kommunistisk vilde', 'X', 'Korrigeringar av J. Jarlbrink'),
            ('Sverges kommunistiska parti', 'V', 'Korrigeringar av J. Jarlbrink'),
            ('Sveriges liberala parti', 'L', 'Korrigeringar av J. Jarlbrink'),
            ('Frisinnade försvarsvänner', 'L', 'Korrigeringar av J. Jarlbrink'),
            ('fris', 'FRIS', '2'))
    ) select *
    from manual_corrections_by_jj
    where party not in (
        select party from _party_abbreviation
    )
