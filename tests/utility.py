import os
import shutil
from os.path import join as jj
from typing import List

from loguru import logger

from pyriksprot import interface
from pyriksprot.utility import download_url

PARLACLARIN_SOURCE_BRANCH = "main"
PARLACLARIN_SOURCE_FOLDER = 'tests/test_data/source/parlaclarin'
PARLACLARIN_SOURCE_PATTERN = f'{PARLACLARIN_SOURCE_FOLDER}/**/prot-*.xml'
PARLACLARIN_FAKE_FOLDER = 'tests/test_data/source/fake'

MEMBERS_FILENAME = f'{PARLACLARIN_SOURCE_FOLDER}/members_of_parliament.csv'
MINISTERS_FILENAME = f'{PARLACLARIN_SOURCE_FOLDER}/ministers.csv'
SPEAKERS_FILENAME = f'{PARLACLARIN_SOURCE_FOLDER}/talman.csv'

TAGGED_SOURCE_FOLDER = 'tests/test_data/source/tagged_frames'
TAGGED_SOURCE_PATTERN = f'{TAGGED_SOURCE_FOLDER}/prot-*.zip'

DEFAULT_TEST_DOCUMENT_NAMES = [
    "prot-1933--fk--5",
    "prot-1955--ak--22",
    "prot-197879--14",
    "prot-199596--35",
    'prot-199192--127.xml',
    'prot-199192--21.xml',
]


def github_source_url(branch: str = PARLACLARIN_SOURCE_BRANCH):
    return f"https://github.com/welfare-state-analytics/riksdagen-corpus/raw/{branch}/corpus"


def download_metadata_url(*, filename: str, branch: str = PARLACLARIN_SOURCE_BRANCH) -> str:
    return f'https://raw.githubusercontent.com/welfare-state-analytics/riksdagen-corpus/{branch}/corpus/{filename}'


def download_parliamentary_protocols(
    protocols: List[str], target_folder: str, create_subfolder: bool = True, branch: str = PARLACLARIN_SOURCE_BRANCH
) -> None:
    os.makedirs(target_folder, exist_ok=True)
    for filename in protocols:
        if not filename.endswith('.xml'):
            filename = f"{filename}.xml"
        subfolder: str = filename.split('-')[1]
        url: str = f'{github_source_url(branch)}/{subfolder}/{filename}'
        target_subfolder = target_folder if not create_subfolder else jj(target_folder, subfolder)
        download_url(url=url, target_folder=target_subfolder, filename=filename)


def download_parliamentary_metadata(target_folder: str, branch: str = PARLACLARIN_SOURCE_BRANCH):
    for filename in ['members_of_parliament.csv', 'ministers.csv', 'talman.csv']:
        url: str = download_metadata_url(filename=filename, branch=branch)
        download_url(url=url, target_folder=target_folder, filename=filename)


def create_parlaclarin_corpus(
    protocols: List[str] = None, target_folder: str = PARLACLARIN_SOURCE_FOLDER, branch: str = PARLACLARIN_SOURCE_BRANCH
) -> None:

    if protocols is None:
        protocols = DEFAULT_TEST_DOCUMENT_NAMES

    shutil.rmtree(target_folder, ignore_errors=True)
    os.makedirs(target_folder, exist_ok=True)

    logger.info("downloading parliamentary metadata")
    download_parliamentary_metadata(target_folder, branch)

    logger.info(f"downloading test protocols: {', '.join(protocols)}")
    download_parliamentary_protocols(protocols, target_folder, create_subfolder=True, branch=branch)


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
