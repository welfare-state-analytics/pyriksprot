from pyriksprot.workflows import tag


def test_to_csv():
    tagged_documents: list[tag.TaggedDocument] = [
        {
            'lemma': ['hej', '!', 'detta', 'vara', 'en', 'test', '!'],
            'num_tokens': 7,
            'num_words': 7,
            'pos': ['IN', 'MID', 'PN', 'VB', 'DT', 'NN', 'MAD'],
            'token': ['Hej', '!', 'Detta', 'är', 'ett', 'test', '!'],
            'xpos': [
                'IN',
                'MID',
                'VB.PRS.AKT',
                'DT.NEU.SIN.IND',
                'NN.NEU.SIN.IND.NOM',
                'MAD',
            ],
        }
    ]

    tagged_csv_str: str = tag.ITagger.to_csv(tagged_documents[0])

    assert (
        tagged_csv_str == "token\tlemma\tpos\n"
        "Hej\thej\tIN\n"
        "!\t!\tMID\n"
        "Detta\tdetta\tPN\n"
        "är\tvara\tVB\n"
        "ett\ten\tDT\n"
        "test\ttest\tNN\n"
        "!\t!\tMAD"
    )
