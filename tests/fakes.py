import io
import os
from os.path import join as jj
from typing import Iterable, Literal

import numpy as np
import pandas as pd
from dotenv import load_dotenv

from pyriksprot import interface
from pyriksprot import utility as pu
from pyriksprot.corpus import iterate

load_dotenv()


RIKSPROT_REPOSITORY_TAG = os.environ["RIKSPROT_REPOSITORY_TAG"]
ROOT_FOLDER = jj("tests/test_data/source/", RIKSPROT_REPOSITORY_TAG)

RIKSPROT_PARLACLARIN_FOLDER = jj(ROOT_FOLDER, "parlaclarin")
RIKSPROT_PARLACLARIN_METADATA_FOLDER = jj(RIKSPROT_PARLACLARIN_FOLDER, "metadata")
RIKSPROT_PARLACLARIN_PATTERN = jj(RIKSPROT_PARLACLARIN_FOLDER, "**/prot-*.xml")
RIKSPROT_PARLACLARIN_FAKE_FOLDER = f'tests/test_data/fakes/{RIKSPROT_REPOSITORY_TAG}'

TAGGED_SOURCE_FOLDER = jj(ROOT_FOLDER, "tagged_frames")
TAGGED_SOURCE_PATTERN = jj(TAGGED_SOURCE_FOLDER, "prot-*.zip")
TAGGED_SPEECH_FOLDER = jj(ROOT_FOLDER, "tagged_frames_speeches.feather")

SAMPLE_METADATA_DATABASE_NAME = jj(ROOT_FOLDER, "riksprot_metadata.db")


def load_sample_utterances(filename: str) -> list[interface.Utterance]:
    utterance_data: list[dict] = load_sample_utterance_data(filename)
    return [interface.Utterance(**data) for data in utterance_data]


def load_segment_stream(filename: str, level: interface.SegmentLevel) -> Iterable[iterate.ProtocolSegment]:
    utterances: list[interface.Utterance] = load_sample_utterances(filename)
    protocol_name: str = pu.strip_path_and_extension(filename)
    year: str = int(protocol_name.split('-')[1])

    if len(utterances) == 0:
        return

    if level == interface.SegmentLevel.Utterance:
        """Return each utterance"""
        for i, u in enumerate(utterances):
            yield iterate.ProtocolSegment(
                content_type=interface.ContentType.Text,
                data=u.text,
                id=u.u_id,
                n_tokens=0,
                n_utterances=1,
                name=f'{protocol_name}_{i+1:03}',
                page_number=u.page_number,
                protocol_name=protocol_name,
                segment_level=level,
                u_id=u.u_id,
                who=u.who,
                year=year,
            )

    if level == interface.SegmentLevel.Protocol:
        """Return a single merged utterance"""
        yield iterate.ProtocolSegment(
            content_type=interface.ContentType.Text,
            data='\n'.join(u.text for u in utterances),
            id=protocol_name,
            n_tokens=0,
            n_utterances=len(utterances),
            name=protocol_name,
            page_number=0,
            protocol_name=protocol_name,
            segment_level=level,
            speaker_info=None,
            speaker_note_id=None,
            u_id=None,
            who=None,
            year=year,
        )

    if level in (interface.SegmentLevel.Who, interface.SegmentLevel.Speech):
        """Return grouping as specified in "fake-segments.csv" file"""
        data_frame: pd.DataFrame = load_sample_segments_dataframe(filename, level=level)
        groups: list[str, dict] = data_frame.groupby('segment_id', sort=False).agg(list).reset_index().to_dict('records')

        for i, group in enumerate(groups):
            segment_id: str = group.get("segment_id")
            u_ids: str = group.get("u_id")
            segment_utterences: list[interface.Utterance] = [u for u in utterances if u.u_id in u_ids]

            yield iterate.ProtocolSegment(
                content_type=interface.ContentType.Text,
                data='\n'.join(u.text for u in segment_utterences),
                id=segment_id,
                n_tokens=0,
                n_utterances=len(segment_utterences),
                name=f'{protocol_name}_{i+1:03}',
                page_number=segment_utterences[0].page_number,
                protocol_name=protocol_name,
                segment_level=level,
                speaker_info=None,
                speaker_note_id=None,
                speech_index=i,
                u_id=segment_utterences[0].u_id,
                who=segment_utterences[0].who,
                year=year,
            )


def load_speech_stream(filename: str, strategy: str) -> Iterable[interface.Speech]:
    utterances: list[interface.Utterance] = load_sample_utterances(filename)
    protocol_name: str = pu.strip_path_and_extension(filename)
    year: str = int(protocol_name.split('-')[1])

    if len(utterances) == 0:
        return

    data_frame: pd.DataFrame = load_sample_speech_dataframe(filename, strategy=strategy)
    groups: list[str, dict] = data_frame.groupby('speech_id', sort=False).agg(list).reset_index().to_dict('records')

    for i, group in enumerate(groups):
        # speech_id: str = group.get("speech_id")
        u_ids: str = group.get("u_id")
        speech_utterences: list[interface.Utterance] = [u for u in utterances if u.u_id in u_ids]

        yield interface.Speech(
            protocol_name=protocol_name,
            document_name=f'{protocol_name}_{(i + 1):03}',
            speech_id=speech_utterences[0].u_id,
            who=speech_utterences[0].who,
            page_number=speech_utterences[0].page_number,
            speech_date=str(year),
            speech_index=i + 1,
            utterances=speech_utterences,
        )


def _sample_filename(filename: str, suffix: str, extension: str = "csv") -> str:
    if filename.endswith(f'{suffix}.{extension}'):
        return filename
    return pu.replace_extension(pu.path_add_suffix(filename, f'-{suffix}'), f'.{extension}')


def sample_tagged_filename(filename: str) -> str:
    return _sample_filename(filename, "tagged")


def sample_utterance_filename(filename: str) -> str:
    return _sample_filename(filename, "utterances")


def sample_segments_filename(filename: str) -> str:
    return _sample_filename(filename, "segments")


def sample_speeches_filename(filename: str) -> str:
    return _sample_filename(filename, "merge-speeches")


def load_sample_utterance_data(filename: str) -> list[dict]:
    tagged_merged: pd.DataFrame = load_sample_tagged_data(filename)
    utterances: pd.DataFrame = pd.read_csv(
        sample_utterance_filename(filename), sep=";", quotechar='"', na_values=''
    ).replace({np.nan: None})
    utterances = utterances.merge(tagged_merged, left_on='u_id', right_index=True, how='left')
    utterances['checksum'] = utterances.paragraphs.apply(interface.UtteranceHelper.compute_paragraph_checksum)
    data: list[dict] = utterances.replace({np.nan: None}).to_dict('record')
    return data


def load_sample_tagged_data(filename: str) -> pd.DataFrame:
    tagged: pd.DataFrame = pd.read_csv(sample_tagged_filename(filename), sep=";", quotechar='"', na_values='')
    tagged['annotated'] = tagged.token + '\t' + tagged.lemma + '\t' + tagged.pos
    tagged_merged = tagged[['u_id', 'annotated']].groupby(['u_id'], sort=False).agg('\n'.join)
    tagged_merged['annotated'] = 'token\tlemma\tpos\n' + tagged_merged.annotated
    return tagged_merged.replace({np.nan: None})


def load_sample_tagged_dataframe(filename: str) -> pd.DataFrame:
    """Loads fake file and returns df[[u_id, token, lemma, pos]]"""
    data: pd.DataFrame = load_sample_tagged_data(filename)
    tagged_frame: pd.DataFrame = sample_tagged_to_merged_dataframe(data)
    return tagged_frame


def load_sample_segments_dataframe(filename: str, level: interface.SegmentLevel) -> pd.DataFrame:
    """Loads fake file and returns df[[segment_id, u_id]] filtered by level"""
    return _load_sample_utterance_groupings(
        filename,
        group=level.name,
        suffix='segments',
        group_name='level',
        group_id_name='segment_id',
    )


def load_sample_speech_dataframe(filename: str, strategy: str) -> pd.DataFrame:
    return _load_sample_utterance_groupings(
        filename,
        group=strategy,
        suffix='merge-speeches',
        group_name='strategy',
        group_id_name='speech_id',
    )


def _load_sample_utterance_groupings(
    filename: str, group: str, suffix: str, group_name: str, group_id_name: str
) -> pd.DataFrame:
    """Loads utterance groups from fake file and returns df[[group_id_name, u_id]] for given group type"""
    data_frame: pd.DataFrame = pd.read_csv(
        _sample_filename(filename, suffix), sep=";", quotechar='"', na_values=''
    ).replace({np.nan: ''})
    data_frame = data_frame[data_frame[group_name] == group][[group_id_name, 'u_id']]
    return data_frame


def sample_tagged_to_utterance_csv_dict(tagged_data: pd.DataFrame) -> dict:
    return tagged_data.to_dict('dict')['annotated']


def sample_tagged_to_dataframes(tagged_data: pd.DataFrame | dict) -> dict:
    """Converts { u_id: tsv } or df[[u_id, csv]] to { u_id: pd.DataFrame}"""
    if not isinstance(tagged_data, dict):
        tagged_data: dict = sample_tagged_to_utterance_csv_dict(tagged_data)
    return {u_id: pd.read_csv(io.StringIO(data), sep='\t') for u_id, data in tagged_data.items()}


def sample_tagged_to_merged_dataframe(tagged_data: pd.DataFrame | dict) -> dict:
    """Converts { u_id: tsv } or df[[u_id, csv]] to df[[u_id, token, lemma, pos]]"""
    tagged_frames: dict[str, pd.DataFrame] = sample_tagged_to_dataframes(tagged_data)
    if len(tagged_frames) == 0:
        return pd.DataFrame(data=[], columns=['u_id', 'token', 'lemma', 'pos'], dtype=str)
    for u_id, df in tagged_frames.items():
        df['u_id'] = u_id
    tagged_frame: pd.DataFrame = pd.concat(tagged_frames)
    return tagged_frame[['u_id', 'token', 'lemma', 'pos']]


def sample_compute_expected_counts(filename: str, kind: Literal['token', 'lemma', 'pos'], lowercase: bool):
    try:
        data: pd.DataFrame = load_sample_tagged_dataframe(filename)
        if lowercase:
            data[kind] = data[kind].str.lower()
        return data.groupby(kind, sort=False)['u_id'].count().to_dict()
    except:  # pylint: disable=bare-except
        return {}


def load_expected_stream(
    level: interface.SegmentLevel.Protocol, document_names: list[str]
) -> list[iterate.ProtocolSegment]:
    return pu.flatten(
        load_segment_stream(jj(RIKSPROT_PARLACLARIN_FAKE_FOLDER, f"{d}.xml"), level) for d in document_names
    )


def load_expected_speeches(strategy: str, document_name: str) -> list[iterate.ProtocolSegment]:
    return load_speech_stream(jj(RIKSPROT_PARLACLARIN_FAKE_FOLDER, f"{document_name}.xml"), strategy)
