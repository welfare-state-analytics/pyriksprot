from __future__ import annotations

import os

import pandas as pd

from pyriksprot import interface
from pyriksprot import to_speech as ts
from pyriksprot.corpus import tagged as tagged_corpus

from ..utility import strip_path_and_extension

# pylint: disable=redefined-outer-name

jj = os.path.join


def extract_speech_ids_by_strategy(filename: str, target_folder: str = "") -> None:
    """Extracts utterance ids for testing purposes"""
    protocol_name: interface.Protocol = strip_path_and_extension(filename)
    protocol: interface.Protocol = tagged_corpus.load_protocol(filename=filename)

    utterances: pd.DataFrame = pd.DataFrame(
        data=[(x.u_id, x.who, x.next_id, x.prev_id, x.speaker_note_id) for x in protocol.utterances],
        columns=['u_id', 'who', 'next_id', 'prev_id', 'speaker_note_id'],
    )
    for merge_strategy in [
        'who_sequence',
        'who_speaker_note_id_sequence',
        'speaker_note_id_sequence',
        'chain',
        'chain_consecutive_unknowns',
    ]:
        merger: ts.IMergeStrategy = ts.MergerFactory.get(merge_strategy)

        items: list[list[interface.Utterance]] = merger.group(protocol.utterances)

        speech_ids = []
        for i, item in enumerate(items):
            speech_ids.extend(len(item) * [i])

        utterances[merge_strategy] = speech_ids

    utterances.to_excel(jj(target_folder, f"utterances_{protocol_name}.xlsx"))
