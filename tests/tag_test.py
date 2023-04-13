import os
from os.path import join as jj
from unittest.mock import Mock
from uuid import uuid4

import pyriksprot

from .utility import RIKSPROT_PARLACLARIN_FAKE_FOLDER


def test_tag_protocol_xml():
    def tag(text: str, preprocess: bool):  # pylint: disable=unused-argument
        return [
            dict(
                token=['Olle', 'är', 'snäll', '.'],
                lemma=['Olle', 'vara', 'snäll', '.'],
                pos=['PM', 'VB', 'ADJ', 'MAD'],
                xpos=['PM', 'VB', 'ADJ', 'MAD'],
                num_tokens=3,
                num_words=3,
            )
        ]

    tagger: pyriksprot.ITagger = Mock(
        spec=pyriksprot.ITagger, tag=tag, to_csv=pyriksprot.ITagger.to_csv, preprocess=lambda x: x
    )

    input_filename: str = jj(RIKSPROT_PARLACLARIN_FAKE_FOLDER, "prot-1958-fake.xml")
    output_filename: str = jj("tests", "output", f"{str(uuid4())}.zip")

    pyriksprot.tag_protocol_xml(input_filename=input_filename, output_filename=output_filename, tagger=tagger)

    assert os.path.isfile(output_filename)

    os.unlink(output_filename)
