import os
from os.path import join as jj
from typing import List
from uuid import uuid4

import pytest

import pyriksprot


@pytest.mark.parametrize('storage_format', ['json', 'csv'])
def test_store_protocols(storage_format: str):
    protocol: pyriksprot.Protocol = pyriksprot.Protocol(
        name='prot-1958-fake',
        date='1958',
        utterances=[
            pyriksprot.Utterance(
                u_id='i-1',
                n='c01',
                who='A',
                prev_id=None,
                next_id='i-2',
                paragraphs=['Hej! Detta är en mening.'],
                annotation="token\tpos\tlemma\nA\ta\tNN",
                delimiter='\n',
            )
        ],
    )

    output_filename: str = jj("tests", "output", f"{str(uuid4())}.zip")

    pyriksprot.store_protocol(
        output_filename,
        protocol=protocol,
        storage_format=storage_format,
        checksum='apa',
    )

    assert os.path.isfile(output_filename)

    metadata: dict = pyriksprot.load_metadata(output_filename)

    assert metadata is not None

    loaded_protocol: pyriksprot.Protocol = pyriksprot.load_protocol(output_filename)

    assert protocol.name == loaded_protocol.name
    assert protocol.date == loaded_protocol.date
    assert [u.__dict__ for u in protocol.utterances] == [u.__dict__ for u in loaded_protocol.utterances]

    # os.unlink(output_filename)


def test_to_csv():

    tagged_documents: List[pyriksprot.TaggedDocument] = [
        {
            'lemma': ['hej', '!', 'detta', 'vara', 'en', 'test', '!'],
            'num_tokens': 7,
            'num_words': 7,
            'pos': ['IN', 'MID', 'PN', 'VB', 'DT', 'NN', 'MAD'],
            'token': ['Hej', '!', 'Detta', 'är', 'ett', 'test', '!'],
            'xpos': [
                'IN',
                'MID',
                'PN.NEU.SIN.DEF.SUB+OBJ',
                'VB.PRS.AKT',
                'DT.NEU.SIN.IND',
                'NN.NEU.SIN.IND.NOM',
                'MAD',
            ],
        }
    ]

    tagged_csv_str: str = pyriksprot.ITagger.to_csv(tagged_documents[0])

    assert (
        tagged_csv_str == "token\tlemma\tpos\txpos\n"
        "Hej\thej\tIN\tIN\n"
        "!\t!\tMID\tMID\n"
        "Detta\tdetta\tPN\tPN.NEU.SIN.DEF.SUB+OBJ\n"
        "är\tvara\tVB\tVB.PRS.AKT\n"
        "ett\ten\tDT\tDT.NEU.SIN.IND\n"
        "test\ttest\tNN\tNN.NEU.SIN.IND.NOM\n"
        "!\t!\tMAD\tMAD"
    )
