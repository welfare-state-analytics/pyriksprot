from __future__ import annotations

import os
from typing import Sequence

from loguru import logger

from pyriksprot.corpus.iterate import ProtocolSegment
from pyriksprot.corpus.parlaclarin import iterate

from .. import dehyphenation
from .. import dispatch as sd
from .. import interface
from .. import metadata as md
from .. import preprocess as pp
from ..corpus import corpus_index

# pylint: disable=too-many-arguments

jj = os.path.join


def extract_speech_texts(
    source_folder: str = None,
    metadata_filename: str = None,
    target_name: str = None,
    subfolder_key: interface.TemporalKey = None,
    naming_keys: Sequence[interface.GroupingKey] = None,
    merge_strategy: str = "chain",
    years: str = None,
    skip_size: int = 1,
    multiproc_keep_order: str = None,
    multiproc_processes: int = 1,
    multiproc_chunksize: int = 100,
    dedent: bool = True,
    dehyphen: bool = False,
    dehyphen_folder: str = '.',
    compress_type: sd.CompressType = sd.CompressType.Zip,
    **_,
) -> None:
    """Special case of `extract_text.extract_corpus_text` for speech extraction only (no merge).

    Adds ability to organize files according to `subfolder_key`.
    Adds values of attributes `naming_keys` to each filename.
    The temporal key, and naming keys are used for subfolders (temporal key) and naming of result file.
    Sub-folder key kan be any of None, 'Year', 'Lustrum', 'Decade' or custom year periods
    - 'Year', 'Lustrum', 'Decade' or custom year periods given as comma separated string

    Args:
        source_folder (str, optional): Corpus source folder. Defaults to None.
        metadata_filename (str, optional): Metadata filename. Defaults to None.
        target_name (str, optional): Target name. Defaults to None.
        subfolder_key (str, optional): Sub-folder key used in store. Defaults to None.
        naming_keys (Sequence[str], optional): Naming keys. Defaults to None.
        merge_strategy (str, optional): Speech merge strategy. Defaults to `chain`.
        years (str, optional): Years filter. Defaults to None.
        skip_size (int, optional): Speech text length skip size. Defaults to 1.
        multiproc_keep_order (str, optional): Force correct iterate yield order when multiprocessing. Defaults to None.
        multiproc_processes (int, optional): Number of processes during iterate. Defaults to 1.
        multiproc_chunksize (int, optional): Chunksize to use per process during iterate. Defaults to 100.
        dedent (bool, optional): Dedent text. Defaults to True.
        dehyphen (bool, optional): Dehyphen text. Defaults to False.
        dehyphen_folder (str, optional): Path to model data (used by dedent/dehyphen). Defaults to '.'.
        compress_type (CompressType, optional): Target file compression type. Defaults to CompressType.zip.
    """
    source_index: corpus_index.CorpusSourceIndex = corpus_index.CorpusSourceIndex.load(
        source_folder=source_folder, source_pattern='**/prot-*.xml', years=years
    )
    lookups: md.Codecs = md.Codecs().load(metadata_filename)

    dehypenator: dehyphenation.SwedishDehyphenator = (
        dehyphenation.SwedishDehyphenator(data_folder=dehyphen_folder, word_frequencies=None) if dehyphen else None
    )

    if dehyphen:
        if not dehypenator.word_frequencies:
            raise ValueError("dehyphenation requires word frequencies (frequency file empty or not specified)")

    speaker_service: md.SpeakerInfoService = md.SpeakerInfoService(database_filename=metadata_filename)

    def preprocess(item: ProtocolSegment) -> None:
        if dedent:
            item.data = pp.dedent(item.data)

        if dehypenator:
            item.data = dehypenator.dehyphen_text(item.data)  # pylint: disable=not-callable

        item.speaker_info = speaker_service.get_speaker_info(u_id=item.u_id, person_id=item.who, year=item.year)

    segments: iterate.XmlUntangleSegmentIterator = iterate.XmlUntangleSegmentIterator(
        filenames=source_index.paths,
        segment_level=interface.SegmentLevel.Speech,
        segment_skip_size=skip_size,
        merge_strategy=merge_strategy,
        multiproc_keep_order=multiproc_keep_order,
        multiproc_processes=multiproc_processes,
        multiproc_chunksize=multiproc_chunksize,
        preprocess=preprocess,
    )

    with sd.SortedSpeechesInZipDispatcher(
        target_name, compress_type=compress_type, subfolder_key=subfolder_key, naming_keys=naming_keys, lookups=lookups
    ) as dispatcher:
        for segment in segments:
            dispatcher.dispatch([segment])

    logger.info(f"Corpus stored in {target_name}.")
