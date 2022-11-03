import os
import shutil
from os.path import isfile
from os.path import join as jj
from typing import List

from dotenv import load_dotenv
from loguru import logger

from pyriksprot import dispatch, interface
from pyriksprot import metadata as md
from pyriksprot import workflows
from pyriksprot.corpus.parlaclarin import ProtocolMapper
from pyriksprot.utility import download_protocols, replace_extension

load_dotenv()


RIKSPROT_REPOSITORY_TAG = os.environ["RIKSPROT_REPOSITORY_TAG"]
ROOT_FOLDER = jj("tests/test_data/source/", RIKSPROT_REPOSITORY_TAG)

RIKSPROT_PARLACLARIN_FOLDER = jj(ROOT_FOLDER, "parlaclarin")
RIKSPROT_PARLACLARIN_METADATA_FOLDER = jj(RIKSPROT_PARLACLARIN_FOLDER, "metadata")
RIKSPROT_PARLACLARIN_PATTERN = jj(RIKSPROT_PARLACLARIN_FOLDER, "**/prot-*.xml")
RIKSPROT_PARLACLARIN_FAKE_FOLDER = 'tests/test_data/source/fake'

TAGGED_SOURCE_FOLDER = jj(ROOT_FOLDER, "tagged_frames")
TAGGED_SOURCE_PATTERN = jj(TAGGED_SOURCE_FOLDER, "prot-*.zip")
TAGGED_SPEECH_FOLDER = jj(ROOT_FOLDER, "tagged_frames_speeches.feather")

SAMPLE_METADATA_DATABASE_NAME = jj(ROOT_FOLDER, "riksprot_metadata.db")

TEST_DOCUMENTS = [
    "prot-1933--fk--5",
    "prot-1955--ak--22",
    "prot-197879--14",
    "prot-199596--35",
    'prot-199192--127',
    'prot-199192--21',
]


def sample_parlaclarin_corpus_exists():
    return all(
        isfile(jj(RIKSPROT_PARLACLARIN_FOLDER, "protocols", x.split('-')[1], f"{x}.xml")) for x in TEST_DOCUMENTS
    )


def sample_metadata_exists():
    return all(isfile(jj(RIKSPROT_PARLACLARIN_FOLDER, "metadata", f"{x}.csv")) for x in md.RIKSPROT_METADATA_TABLES)


def sample_tagged_frames_corpus_exists():
    return all(isfile(jj(TAGGED_SOURCE_FOLDER, f"{x}.zip")) for x in TEST_DOCUMENTS)


def sample_tagged_speech_corpus_exists():
    return all(isfile(jj(TAGGED_SPEECH_FOLDER, f"{x}.zip")) for x in TEST_DOCUMENTS)


def ensure_test_corpora_exist(force: bool = False):

    if force or not sample_parlaclarin_corpus_exists():
        create_sample_parlaclarin_corpus()

    if force or not sample_metadata_exists():
        create_sample_metadata()

    if force or not sample_tagged_frames_corpus_exists():
        create_sample_tagged_frames_corpus()

    if force or not sample_tagged_speech_corpus_exists:
        create_sample_speech_corpus()


def create_sample_parlaclarin_corpus():
    # FIXME Read from local folder instead or prevent "main" tag
    download_protocols(
        protocols=TEST_DOCUMENTS,
        target_folder=jj(RIKSPROT_PARLACLARIN_FOLDER, "protocols"),
        create_subfolder=True,
        tag=RIKSPROT_REPOSITORY_TAG,
    )


def create_sample_metadata():
    """Subset metadata to folder"""
    md.subset_to_folder(
        ProtocolMapper,
        source_folder=RIKSPROT_PARLACLARIN_FOLDER,
        source_metadata=jj("metadata/data", RIKSPROT_REPOSITORY_TAG),
        target_folder=RIKSPROT_PARLACLARIN_METADATA_FOLDER,
    )
    """Create metadata database"""
    md.create_database(
        database_filename=SAMPLE_METADATA_DATABASE_NAME,
        tag=None,
        folder=RIKSPROT_PARLACLARIN_METADATA_FOLDER,
        force=True,
    )
    md.generate_corpus_indexes(
        ProtocolMapper,
        corpus_folder=RIKSPROT_PARLACLARIN_FOLDER,
        target_folder=RIKSPROT_PARLACLARIN_METADATA_FOLDER,
    )
    md.load_corpus_indexes(
        database_filename=SAMPLE_METADATA_DATABASE_NAME,
        source_folder=RIKSPROT_PARLACLARIN_METADATA_FOLDER,
    )
    md.load_scripts(
        database_filename=SAMPLE_METADATA_DATABASE_NAME,
        script_folder="./metadata/sql",
    )


def create_sample_tagged_frames_corpus() -> None:

    protocols: str = TEST_DOCUMENTS
    source_folder: str = os.environ["TEST_RIKSPROT_TAGGED_FOLDER"]
    target_folder: str = TAGGED_SOURCE_FOLDER

    logger.info("Creating sample tagged frames corpus")
    logger.info(f"  source: {source_folder}")
    logger.info(f"  target: {target_folder}")

    protocols = protocols or TEST_DOCUMENTS

    shutil.rmtree(target_folder, ignore_errors=True)
    os.makedirs(target_folder, exist_ok=True)

    for name in protocols:

        filename: str = replace_extension(name, 'zip')
        subfolder: str = filename.split('-')[1]
        source_filename: str = jj(source_folder, subfolder, filename)
        target_filename: str = jj(target_folder, filename)

        if not isfile(source_filename):
            logger.warning(f"test data: test file {name} not found")
            continue
        shutil.copy(src=source_filename, dst=target_filename)
        logger.info(f"  copied: {source_filename} to {jj(target_folder, filename)}")


def create_sample_speech_corpus():

    # target_type: str, merge_strategy: to_speech.MergeStrategyType, compress_type: str):
    target_type: str = 'single-id-tagged-frame-per-group'
    merge_strategy: str = 'chain'
    compress_types: list[str] = ['csv', 'feather']

    logger.info("Creating sample speech tagged ID frame corpus")
    logger.info(f"    source: {TAGGED_SOURCE_FOLDER}")
    logger.info(f"  metadata: {SAMPLE_METADATA_DATABASE_NAME}")

    for compress_type in compress_types:

        target_name: str = jj(
            "tests/test_data/source/", RIKSPROT_REPOSITORY_TAG, f"tagged_frames_speeches.{compress_type}"
        )

        logger.info(f"    target: {target_name}")

        fixed_opts: dict = dict(
            source_folder=TAGGED_SOURCE_FOLDER,
            metadata_filename=SAMPLE_METADATA_DATABASE_NAME,
            segment_level=interface.SegmentLevel.Speech,
            temporal_key=interface.TemporalKey.NONE,
            content_type=interface.ContentType.TaggedFrame,
            multiproc_keep_order=None,
            multiproc_processes=None,
            multiproc_chunksize=100,
            segment_skip_size=1,
            years=None,
            group_keys=('who',),
            force=True,
            skip_lemma=False,
            skip_text=True,
            skip_puncts=True,
            skip_stopwords=True,
            lowercase=True,
            progress=False,
        )
        workflows.extract_corpus_tags(
            **fixed_opts,
            target_name=target_name,
            target_type=target_type,
            compress_type=dispatch.CompressType(compress_type),
            merge_strategy=merge_strategy,
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
        'who': 'A',
        'prev_id': None,
        'next_id': 'i-2',
        'paragraphs': 'Hej! Detta är en mening.',
        'annotation': TAGGED_CSV_STR,
        'page_number': '',
        'speaker_note_id': 'a1',
        'checksum': '107d28f2f90d3ccc',
    },
    {
        'u_id': 'i-2',
        'who': 'A',
        'prev_id': 'i-1',
        'next_id': None,
        'paragraphs': 'Jag heter Ove.@#@Vad heter du?',
        'annotation': TAGGED_CSV_STR,
        'page_number': '',
        'speaker_note_id': 'a1',
        'checksum': '9c3ee2212f9db2eb',
    },
    {
        'u_id': 'i-3',
        'who': 'B',
        'prev_id': None,
        'next_id': None,
        'paragraphs': 'Jag heter Adam.',
        'annotation': TAGGED_CSV_STR,
        'page_number': '',
        'speaker_note_id': 'b1',
        'checksum': '8a2880190e158a8a',
    },
    {
        'u_id': 'i-4',
        'who': 'B',
        'prev_id': None,
        'next_id': None,
        'paragraphs': 'Ove är dum.',
        'annotation': TAGGED_CSV_STR,
        'page_number': '',
        'speaker_note_id': 'b2',
        'checksum': '13ed9d8bf4098390',
    },
    {
        'u_id': 'i-5',
        'who': 'A',
        'prev_id': None,
        'next_id': None,
        'annotation': 'token\tlemma\tpos\txpos\nHej\thej\tIN\tIN\n!\t!\tMID\tMID\nDetta\tdetta\tPN\tPN.NEU.SIN.DEF.SUB+OBJ\när\tvara\tVB\tVB.PRS.AKT\nett\ten\tDT\tDT.NEU.SIN.IND\ntest\ttest\tNN\tNN.NEU.SIN.IND.NOM\n!\t!\tMAD\tMAD\n\'\t\tMAD\tMAD\n"\t\tMAD\tMAD',
        'paragraphs': 'Adam är dum.',
        'page_number': '',
        'speaker_note_id': 'a2',
        'checksum': 'a2f0635f8991d206',
    },
]


def create_utterances() -> List[interface.Utterance]:
    return [
        interface.Utterance(
            u_id='i-1',
            who='A',
            speaker_note_id="a1",
            prev_id=None,
            next_id='i-2',
            paragraphs=['Hej! Detta är en mening.'],
            delimiter='\n',
            annotation=TAGGED_CSV_STR,
        ),
        interface.Utterance(
            u_id='i-2',
            who='A',
            speaker_note_id="a1",
            prev_id='i-1',
            next_id=None,
            paragraphs=['Jag heter Ove.', 'Vad heter du?'],
            delimiter='\n',
            annotation=TAGGED_CSV_STR,
        ),
        interface.Utterance(
            u_id='i-3',
            who='B',
            speaker_note_id="b1",
            prev_id=None,
            next_id=None,
            paragraphs=['Jag heter Adam.'],
            delimiter='\n',
            annotation=TAGGED_CSV_STR,
        ),
        interface.Utterance(
            u_id='i-4',
            who='B',
            speaker_note_id="b2",
            prev_id=None,
            next_id=None,
            paragraphs=['Ove är dum.'],
            delimiter='\n',
            annotation=TAGGED_CSV_STR,
        ),
        interface.Utterance(
            u_id='i-5',
            who='A',
            speaker_note_id="a2",
            prev_id=None,
            next_id=None,
            paragraphs=['Adam är dum.'],
            delimiter='\n',
            annotation=TAGGED_CSV_STR,
        ),
    ]
