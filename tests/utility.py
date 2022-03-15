import os
import shutil
from os.path import isdir, isfile
from os.path import join as jj
from typing import List

from dotenv import load_dotenv
from loguru import logger

from pyriksprot import interface
from pyriksprot import metadata as md
from pyriksprot.corpus.parlaclarin import ProtocolMapper
from pyriksprot.utility import download_protocols, replace_extension

load_dotenv()


PARLACLARIN_BRANCH = os.environ["CORPUS_REPOSITORY_TAG"]
PARLACLARIN_SOURCE_TAG = os.environ["CORPUS_REPOSITORY_TAG"]
PARLACLARIN_SOURCE_FOLDER = jj("tests/test_data/source/parlaclarin", PARLACLARIN_SOURCE_TAG)
PARLACLARIN_SOURCE_PATTERN = jj(PARLACLARIN_SOURCE_FOLDER, "**/prot-*.xml")
PARLACLARIN_FAKE_FOLDER = 'tests/test_data/source/parlaclarin/fake'

TAGGED_SOURCE_FOLDER = jj("tests/test_data/source/tagged_frames", PARLACLARIN_SOURCE_TAG)
TAGGED_METADATA_DATABASE_NAME = jj(TAGGED_SOURCE_FOLDER, "riksprot_metadata.db")
TAGGED_SOURCE_PATTERN = jj(TAGGED_SOURCE_FOLDER, "prot-*.zip")

TEST_DOCUMENTS = [
    "prot-1933--fk--5",
    "prot-1955--ak--22",
    "prot-197879--14",
    "prot-199596--35",
    'prot-199192--127',
    'prot-199192--21',
]


def ensure_test_corpora_exist(force: bool = False):
    if force or not sample_xml_corpus_exists():
        download_protocols(
            protocols=TEST_DOCUMENTS,
            target_folder=jj(PARLACLARIN_SOURCE_FOLDER, "protocols"),
            create_subfolder=True,
            tag=PARLACLARIN_SOURCE_TAG,
        )

    if force or not sample_metadata_exists():
        """Create just a subset of the data"""
        md.subset_to_folder(
            parser=ProtocolMapper,
            source_folder=PARLACLARIN_SOURCE_FOLDER,
            source_metadata="metadata/data",
            target_folder=jj(PARLACLARIN_SOURCE_FOLDER, "metadata"),
        )

    if force or not isdir(TAGGED_SOURCE_FOLDER):
        try:
            setup_sample_tagged_frames_corpus(
                protocols=TEST_DOCUMENTS,
                source_folder=os.environ["PARLACLARIN_TAGGED_FOLDER"],
                target_folder=TAGGED_SOURCE_FOLDER,
            )
        except Exception as ex:
            logger.warning(ex)


def sample_xml_corpus_exists():
    return all(isfile(jj(PARLACLARIN_SOURCE_FOLDER, "protocols", x.split('-')[1], f"{x}.xml")) for x in TEST_DOCUMENTS)


def sample_metadata_exists():
    return all(isfile(jj(PARLACLARIN_SOURCE_FOLDER, "metadata", f"{x}.csv")) for x in md.RIKSPROT_METADATA_TABLES)


def sample_tagged_corpus_exists():
    return all(isfile(jj(TAGGED_SOURCE_FOLDER, "protocols", f"{x}.zip")) for x in TEST_DOCUMENTS)


def setup_sample_tagged_frames_corpus(
    *,
    source_folder: str,
    protocols: List[str] = None,
    target_folder: str = TAGGED_SOURCE_FOLDER,
) -> None:

    protocols = protocols or TEST_DOCUMENTS

    shutil.rmtree(target_folder, ignore_errors=True)
    os.makedirs(target_folder, exist_ok=True)

    for name in protocols:
        filename: str = replace_extension(name, 'zip')
        subfolder: str = filename.split('-')[1]
        source_filename: str = jj(source_folder, subfolder, filename)
        if not isfile(source_filename):
            logger.warning(f"test data: test file {name} not found")
            continue
        shutil.copy(src=source_filename, dst=jj(target_folder, filename))

    """Create metadata from test corpus"""
    md.create_database(
        TAGGED_METADATA_DATABASE_NAME,
        branch=None,
        folder=jj(PARLACLARIN_SOURCE_FOLDER, "metadata"),
        force=True,
    )
    create_subset_metadata_to_folder()


def create_subset_metadata_to_folder():
    md.subset_to_folder(
        ProtocolMapper,
        source_folder=PARLACLARIN_SOURCE_FOLDER,
        source_metadata="metadata/data",
        target_folder=jj(PARLACLARIN_SOURCE_FOLDER, "metadata"),
    )
    md.create_database(
        database_filename=TAGGED_METADATA_DATABASE_NAME,
        branch=None,
        folder=jj(PARLACLARIN_SOURCE_FOLDER, "metadata"),
        force=True,
    )
    md.generate_utterance_index(
        ProtocolMapper,
        corpus_folder=PARLACLARIN_SOURCE_FOLDER,
        target_folder=jj(PARLACLARIN_SOURCE_FOLDER, "metadata"),
    )
    md.load_utterance_index(
        database_filename=TAGGED_METADATA_DATABASE_NAME,
        source_folder=jj(PARLACLARIN_SOURCE_FOLDER, "metadata"),
    )
    md.load_scripts(
        database_filename=TAGGED_METADATA_DATABASE_NAME,
        script_folder="./metadata/sql",
    )


TAGGED_CSV_STR = (
    "token\tlemma\tpos\txpos\n"
    "Hej\thej\tIN\tIN\n"
    "!\t!\tMID\tMID\n"
    "Detta\tdetta\tPN\tPN.NEU.SIN.DEF.SUB+OBJ\n"
    "är\tvara\tVB\tVB.PRS.AKT\n"
    "ett\ten\tDT\tDT.NEU.SIN.IND\n"
    "test\ttest\tNN\tNN.NEU.SIN.IND.NOM\n"
    "!\t!\tMAD\tMAD\n"
    "'\t\tMAD\tMAD\n"
    '"\t\tMAD\tMAD'
)

UTTERANCES_DICTS = [
    {
        'u_id': 'i-1',
        'n': 'c01',
        'who': 'A',
        'prev_id': None,
        'next_id': 'i-2',
        'paragraphs': 'Hej! Detta är en mening.',
        'annotation': TAGGED_CSV_STR,
        'page_number': '',
        'speaker_hash': '',
        'checksum': '107d28f2f90d3ccc',
    },
    {
        'u_id': 'i-2',
        'n': 'c02',
        'who': 'A',
        'prev_id': 'i-1',
        'next_id': None,
        'paragraphs': 'Jag heter Ove.@#@Vad heter du?',
        'annotation': TAGGED_CSV_STR,
        'page_number': '',
        'speaker_hash': '',
        'checksum': '9c3ee2212f9db2eb',
    },
    {
        'u_id': 'i-3',
        'n': 'c03',
        'who': 'B',
        'prev_id': None,
        'next_id': None,
        'paragraphs': 'Jag heter Adam.',
        'annotation': TAGGED_CSV_STR,
        'page_number': '',
        'speaker_hash': '',
        'checksum': '8a2880190e158a8a',
    },
    {
        'u_id': 'i-4',
        'n': 'c03',
        'who': 'B',
        'prev_id': None,
        'next_id': None,
        'paragraphs': 'Ove är dum.',
        'annotation': TAGGED_CSV_STR,
        'page_number': '',
        'speaker_hash': '',
        'checksum': '13ed9d8bf4098390',
    },
    {
        'u_id': 'i-5',
        'n': 'c09',
        'who': 'A',
        'prev_id': None,
        'next_id': None,
        'annotation': 'token\tlemma\tpos\txpos\nHej\thej\tIN\tIN\n!\t!\tMID\tMID\nDetta\tdetta\tPN\tPN.NEU.SIN.DEF.SUB+OBJ\när\tvara\tVB\tVB.PRS.AKT\nett\ten\tDT\tDT.NEU.SIN.IND\ntest\ttest\tNN\tNN.NEU.SIN.IND.NOM\n!\t!\tMAD\tMAD\n\'\t\tMAD\tMAD\n"\t\tMAD\tMAD',
        'paragraphs': 'Adam är dum.',
        'page_number': '',
        'speaker_hash': '',
        'checksum': 'a2f0635f8991d206',
    },
]


def create_utterances() -> List[interface.Utterance]:
    return [
        interface.Utterance(
            u_id='i-1',
            n='c01',
            who='A',
            prev_id=None,
            next_id='i-2',
            paragraphs=['Hej! Detta är en mening.'],
            delimiter='\n',
            annotation=TAGGED_CSV_STR,
        ),
        interface.Utterance(
            u_id='i-2',
            n='c02',
            who='A',
            prev_id='i-1',
            next_id=None,
            paragraphs=['Jag heter Ove.', 'Vad heter du?'],
            delimiter='\n',
            annotation=TAGGED_CSV_STR,
        ),
        interface.Utterance(
            u_id='i-3',
            n='c03',
            who='B',
            prev_id=None,
            next_id=None,
            paragraphs=['Jag heter Adam.'],
            delimiter='\n',
            annotation=TAGGED_CSV_STR,
        ),
        interface.Utterance(
            u_id='i-4',
            n='c03',
            who='B',
            prev_id=None,
            next_id=None,
            paragraphs=['Ove är dum.'],
            delimiter='\n',
            annotation=TAGGED_CSV_STR,
        ),
        interface.Utterance(
            u_id='i-5',
            n='c09',
            who='A',
            prev_id=None,
            next_id=None,
            paragraphs=['Adam är dum.'],
            delimiter='\n',
            annotation=TAGGED_CSV_STR,
        ),
    ]
