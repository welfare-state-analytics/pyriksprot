import os
import shutil
from os.path import join as jj
from typing import List

from dotenv import load_dotenv
from loguru import logger

from pyriksprot import interface
from pyriksprot.utility import download_url, replace_extension

load_dotenv()

PARLACLARIN_SOURCE_TAG = os.environ["CORPUS_REPOSITORY_TAG"]
PARLACLARIN_SOURCE_FOLDER = 'tests/test_data/source/parlaclarin'
PARLACLARIN_SOURCE_PATTERN = f'{PARLACLARIN_SOURCE_FOLDER}/**/prot-*.xml'
PARLACLARIN_FAKE_FOLDER = 'tests/test_data/source/fake'

TAGGED_SOURCE_FOLDER = 'tests/test_data/source/tagged_frames'
TAGGED_SOURCE_PATTERN = f'{TAGGED_SOURCE_FOLDER}/prot-*.zip'

TEST_DOCUMENTS = [
    "prot-1933--fk--5",
    "prot-1955--ak--22",
    "prot-197879--14",
    "prot-199596--35",
    'prot-199192--127',
    'prot-199192--21',
]

METADATA_FILENAMES = [
    'members_of_parliament.csv',
    'members_of_parliament_sk.csv',
    'ministers.csv',
    'talman.csv',
    'suppleants.csv',
    'party_mapping.json',
]


def _metadata_url(*, filename: str, tag: str) -> str:
    return f'https://raw.githubusercontent.com/welfare-state-analytics/riksdagen-corpus/{tag}/corpus/{filename}'


def _protocol_uri(filename: str, sub_folder: str, tag: str) -> str:
    return f"https://github.com/welfare-state-analytics/riksdagen-corpus/raw/{tag}/corpus/{sub_folder}/{filename}"


def _download_parliamentary_protocols(
    protocols: List[str], target_folder: str, create_subfolder: bool, tag: str
) -> None:
    os.makedirs(target_folder, exist_ok=True)
    for filename in protocols:
        sub_folder: str = filename.split('-')[1]
        filename: str = replace_extension(filename, 'xml')
        download_url(
            url=_protocol_uri(filename=filename, sub_folder=sub_folder, tag=tag),
            target_folder=target_folder if not create_subfolder else jj(target_folder, sub_folder),
            filename=filename,
        )


def _download_parliamentary_metadata(target_folder: str, tag: str = PARLACLARIN_SOURCE_TAG):
    for filename in METADATA_FILENAMES:
        url: str = _metadata_url(filename=filename, tag=tag)
        download_url(url=url, target_folder=target_folder, filename=filename)


def setup_parlaclarin_test_corpus(
    *, protocols: List[str] = None, target_folder: str = PARLACLARIN_SOURCE_FOLDER, tag: str = PARLACLARIN_SOURCE_TAG
) -> None:

    if protocols is None:
        protocols = TEST_DOCUMENTS

    shutil.rmtree(target_folder, ignore_errors=True)
    os.makedirs(target_folder, exist_ok=True)

    logger.info("downloading parliamentary metadata")
    _download_parliamentary_metadata(target_folder, tag)

    logger.info(f"downloading test protocols: {', '.join(protocols)}")
    _download_parliamentary_protocols(protocols, target_folder, create_subfolder=True, tag=tag)


def setup_tagged_frames_test_corpus(
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
        if not os.path.isfile(source_filename):
            logger.warning(f"test data: test file {name} not found")
            continue
        shutil.copy(src=source_filename, dst=jj(target_folder, filename))

    for filename in METADATA_FILENAMES:
        shutil.copy(src=jj(PARLACLARIN_SOURCE_FOLDER, filename), dst=jj(target_folder, filename))


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
