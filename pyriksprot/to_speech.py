from __future__ import annotations

import abc
import inspect
from collections import defaultdict
from enum import Enum
from itertools import groupby
from typing import TYPE_CHECKING, Type

from loguru import logger

from .interface import Speech

if TYPE_CHECKING:
    from .interface import Protocol, Utterance

# pylint: disable=too-many-arguments, no-member


class MergeStrategyType(str, Enum):
    who = 'who'
    who_sequence = 'who_sequence'
    chain = 'chain'
    chain_consecutive_unknowns = 'chain_consecutive_unknowns'
    who_speaker_note_id_sequence = 'who_speaker_note_id_sequence'
    speaker_note_id_sequence = 'speaker_note_id_sequence'
    undefined = 'undefined'


def to_speeches(
    *, protocol: Protocol, merge_strategy: MergeStrategyType | Type[IMergeStrategy], skip_size: int = 1
) -> list[Speech]:
    if not protocol.utterances:
        return []

    merger: IMergeStrategy = MergerFactory.get(merge_strategy)

    speeches: list[Speech] = [
        Speech(
            protocol_name=protocol.name,
            document_name=f'{protocol.name}_{(i + 1):03}',
            speech_id=utterances[0].u_id,
            who=utterances[0].who,
            page_number=utterances[0].page_number,
            speech_date=protocol.date,
            speech_index=i + 1,
            utterances=utterances,
        )
        for i, utterances in enumerate(merger.cluster(utterances=protocol.utterances))
    ]

    if skip_size > 0:
        speeches = [s for s in speeches if len(s.text or "") >= skip_size]

    return speeches


class IMergeStrategy(abc.ABC):
    """Abstract strategy for merging a protocol into speeches"""

    def cluster(self, utterances: Protocol) -> list[list[Utterance]]:
        return [utterances]


class MergeByWho(IMergeStrategy):
    """Merge all uterrances for a unique `who` into a single speech"""

    def cluster(self, utterances: Protocol) -> list[list[Utterance]]:
        """Create a speech for each unique `who`. Return list of Speech."""
        data = defaultdict(list)
        for u in utterances or []:
            data[u.who].append(u)
        return [data[who] for who in data]


class MergeByWhoSequence(IMergeStrategy):
    """Merge sequences with same `who` into a speech"""

    def cluster(self, utterances: list[Utterance]) -> list[list[Utterance]]:
        groups: list[list[Utterance]] = [list(g) for _, g in groupby(utterances or [], key=lambda x: x.who)]
        return groups


class MergeBySpeakerNoteIdSequence(IMergeStrategy):
    """Merge sequences with same `speaker_note_id` into a speech"""

    def cluster(self, utterances: list[Utterance]) -> list[list[Utterance]]:
        groups: list[list[Utterance]] = [list(g) for _, g in groupby(utterances or [], key=lambda x: x.speaker_note_id)]
        return groups


class MergeByWhoSpeakerNoteIdSequence(IMergeStrategy):
    """Merge sequences with same `who` & 'speaker_note_id' into a speech"""

    def cluster(self, utterances: list[Utterance]) -> list[list[Utterance]]:
        groups: list[list[Utterance]] = [
            list(g) for _, g in groupby(utterances or [], key=lambda x: f"{x.who}_{x.speaker_note_id}")
        ]
        return groups


class MergeByChain(IMergeStrategy):
    def __init__(self, merge_consecutive_unknowns: bool = False):
        self.merge_consecutive_unknowns = merge_consecutive_unknowns

    def cluster(self, utterances: list[Utterance]) -> list[list[Utterance]]:
        groups: list[list[Utterance]] = MergeByChain.group_utterances_by_chain(
            utterances, self.merge_consecutive_unknowns
        )
        return groups

    @staticmethod
    def group_utterances_by_chain(
        utterances: list[Utterance], merge_consecutive_unknowns: bool
    ) -> list[list[Utterance]]:
        """Group utterances based on prev/next pointers. Return list of lists."""
        speeches: list[list[Utterance]] = []
        speech: list[Utterance] = []
        is_start_of_speech: bool = None

        for _, u in enumerate(utterances or []):
            """Rules:
            no prev & no next => utterance is entire speech (i.e. is start and end of speech)
            no prev & next    => utterance is start of speech
            prev & next       => utterance is continuation of speech
            prev & no next    => utterance is end of speech
            """

            is_part_of_chain: bool = bool(u.prev_id) or bool(u.next_id)

            is_unknown_continuation: bool = (
                not is_part_of_chain
                and (
                    bool(speech)
                    and u.who == "unknown" == speech[-1].who
                    and u.speaker_note_id == speech[-1].speaker_note_id
                )
                and merge_consecutive_unknowns
            )

            is_start_of_speech: bool = not bool(u.prev_id) and not is_unknown_continuation

            if is_start_of_speech:
                if bool(speech) and bool(speech[-1].next_id):
                    logger.warning(
                        f"logic error: {u.u_id} is start of speech but previous utterance has a next attribute"
                    )

                speech = [u]
                speeches.append(speech)

            else:
                if bool(speech):
                    if bool(u.prev_id) and speech[-1].u_id != u.prev_id:
                        logger.warning(f"u[{u.u_id}]: current prev_id differs from first u.u_id '{speech[0].u_id}'")

                    if speech[0].who != u.who:
                        raise ValueError(f"u[{u.u_id}]: multiple who ids in current speech '{speech[0].who}'")

                if not bool(speech) and bool(u.prev_id):
                    logger.warning(f"logic error: {u.u_id} has prev attribute but no previous utterance")

                speech.append(u)

        return speeches


class MergeByChainAndConsecutiveUnknowns(MergeByChain):
    def __init__(self):
        super().__init__(True)


class UndefinedMerge(IMergeStrategy):
    def cluster(self, utterances: list[Utterance]) -> list[list[Utterance]]:
        raise ValueError("undefined merge strategy encountered")

    def merge(self, protocol: Protocol) -> list[Speech]:
        raise ValueError("undefined merge strategy encountered")


class MergerFactory:
    strategies: dict[MergeStrategyType, IMergeStrategy] = {
        'who': MergeByWho(),
        'who_sequence': MergeByWhoSequence(),
        'who_speaker_note_id_sequence': MergeByWhoSpeakerNoteIdSequence(),
        'speaker_note_id_sequence': MergeBySpeakerNoteIdSequence(),
        'chain': MergeByChain(False),
        'chain_consecutive_unknowns': MergeByChain(True),
        'undefined': UndefinedMerge(),
    }

    @staticmethod
    def get(strategy: str | Type[IMergeStrategy]) -> IMergeStrategy:
        return (
            strategy()
            if inspect.isclass(strategy) and issubclass(strategy, IMergeStrategy)
            else MergerFactory.strategies.get(strategy)
            if strategy in MergerFactory.strategies
            else MergerFactory.strategies.get('undefined')
        )
