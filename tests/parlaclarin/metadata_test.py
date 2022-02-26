import os
import uuid

from pyriksprot import metadata as md

jj = os.path.join


def test_to_folder():

    target_folder: str = f'./tests/output/{str(uuid.uuid4())[:6]}'

    md.download_to_folder(tag="v0.4.0", folder=target_folder, force=True)

    assert all(os.path.isfile(jj(target_folder, f"{basename}.csv")) for basename in md.RIKSPROT_METADATA_TABLES)


def test_collect_utterance_whos():
    corpus_folder: str = "./tests/test_data/source/parlaclarin/v0.4.0"
    df = md.generate_utterance_index(corpus_folder)
    assert df is not None
