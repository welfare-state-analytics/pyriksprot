from __future__ import annotations

import abc
from collections import defaultdict
from enum import Enum
from itertools import groupby

from loguru import logger

from .interface import Protocol, Speech, Utterance

# pylint: disable=too-many-arguments, no-member


class MergeSpeechStrategyType(str, Enum):
    who = 'who'
    who_sequence = 'who_sequence'
    chain = 'chain'
    who_speaker_hash_sequence = 'who_speaker_hash_sequence'
    speaker_hash_sequence = 'speaker_hash_sequence'
    undefined = 'undefined'


def to_speeches(
    *, protocol: Protocol, merge_strategy: MergeSpeechStrategyType, segment_skip_size: int = 1, **_
) -> list[Speech]:
    """Convert utterances into speeches using specified strategy. Return list."""
    speeches: list[Speech] = SpeechMergerFactory.get(merge_strategy).speeches(
        protocol, segment_skip_size=segment_skip_size
    )
    return speeches


def group_utterances_by_chain(utterances: list[Utterance]) -> list[list[Utterance]]:
    """Split utterances based on prev/next pointers. Return list of lists."""
    speeches: list[list[Utterance]] = []
    speech: list[Utterance] = []
    start_of_speech: bool = None

    for _, u in enumerate(utterances or []):
        """Rules:
        - attrib `next` --> start of new speech with consecutive utterances
        - attrib `prev` --> utterance is belongs to same speech as previous utterance
        - if neither `prev` or `next` are set, then utterance is the entire speech
        - attribs `prev` and `next` are never both set
        """

        if bool(u.prev_id) and bool(u.next_id):
            raise ValueError(f"logic error: {u.u_id} has both prev/next attrbutes set")

        is_part_of_chain: bool = bool(u.prev_id) or bool(u.next_id)
        is_unknown_continuation: bool = (
            bool(speech) and u.who == "unknown" == speech[-1].who and u.speaker_hash == speech[-1].speaker_hash
        )

        start_of_speech: bool = (
            True
            if bool(u.next_id)
            else not is_unknown_continuation
            if not is_part_of_chain
            else not bool(speech) and bool(u.prev_id)
        )

        if start_of_speech:

            if bool(u.prev_id) and not bool(speech):
                logger.warning(f"logic error: {u.u_id} has prev attribute but no previous utterance")

            speech = [u]
            speeches.append(speech)

        else:

            if bool(speech):

                if bool(u.prev_id) and speech[0].u_id != u.prev_id:
                    logger.warning(f"u[{u.u_id}]: current prev_id differs from first u.u_id '{speech[0].u_id}'")

                if speech[0].who != u.who:
                    raise ValueError(f"u[{u.u_id}]: multiple who ids in current speech '{speech[0].who}'")

            speech.append(u)

    return speeches


def merge_utterances_by_speaker_hash(utterances: list[Utterance]) -> list[list[Utterance]]:
    """Split utterances based on prev/next pointers. Return list of lists."""
    speeches: list[list[Utterance]] = [list(g) for _, g in groupby(utterances or [], key=lambda x: x.speaker_hash)]
    return speeches


class IMergeSpeechStrategy(abc.ABC):
    """Abstract strategy for merging a protocol into speeches"""

    def create(self, protocol: Protocol, utterances: list[Utterance] = None, speech_index: int = 0) -> Speech:
        """Create a new speech entity."""

        if not utterances:
            utterances = protocol.utterances

        return Speech(
            protocol_name=protocol.name,
            document_name=f'{protocol.name}_{speech_index:03}',
            speech_id=utterances[0].u_id,
            who=utterances[0].who,
            page_number=utterances[0].page_number,
            speech_date=protocol.date,
            speech_index=speech_index,
            utterances=utterances,
        )

    def speeches(self, protocol: Protocol, segment_skip_size: int = 1) -> list[Speech]:
        speeches: list[Speech] = self.merge(protocol=protocol)
        if segment_skip_size > 0:
            speeches = [s for s in speeches if len(s.text or "") >= segment_skip_size]
        return speeches

    def split(self, utterances: Protocol) -> list[list[Utterance]]:  # pylint: disable=unused-argument
        return []

    def merge(self, protocol: Protocol) -> list[Speech]:
        """Create a speech for each consecutive sequence with the same `who`. Return list of Speech."""
        if not protocol.utterances:
            return []
        return [
            self.create(protocol, utterances=utterances, speech_index=i + 1)
            for i, utterances in enumerate(self.split(protocol.utterances))
        ]


class MergeSpeechByWho(IMergeSpeechStrategy):
    """Merge all uterrances for a unique `who` into a single speech """

    def split(self, utterances: Protocol) -> list[list[Utterance]]:
        """Create a speech for each unique `who`. Return list of Speech."""
        data = defaultdict(list)
        for u in utterances or []:
            data[u.who].append(u)
        return [data[who] for who in data]


class MergeSpeechByWhoSequence(IMergeSpeechStrategy):
    """Merge sequences with same `who` into a speech """

    def split(self, utterances: list[Utterance]) -> list[list[Utterance]]:
        groups: list[list[Utterance]] = [list(g) for _, g in groupby(utterances or [], key=lambda x: x.who)]
        return groups


class MergeSpeechBySpeakerHashSequence(IMergeSpeechStrategy):
    """Merge sequences with same `who` into a speech """

    def split(self, utterances: list[Utterance]) -> list[list[Utterance]]:
        groups: list[list[Utterance]] = [list(g) for _, g in groupby(utterances or [], key=lambda x: x.speaker_hash)]
        return groups


class MergeSpeechByWhoSpeakerHashSequence(IMergeSpeechStrategy):
    """Merge sequences with same `who` into a speech """

    def split(self, utterances: list[Utterance]) -> list[list[Utterance]]:
        groups: list[list[Utterance]] = [
            list(g) for _, g in groupby(utterances or [], key=lambda x: f"{x.who}_{x.speaker_hash}")
        ]
        return groups


class MergeSpeechByChain(IMergeSpeechStrategy):
    def split(self, utterances: list[Utterance]) -> list[list[Utterance]]:
        groups: list[list[Utterance]] = group_utterances_by_chain(utterances)
        return groups


class UndefinedMergeSpeech(IMergeSpeechStrategy):
    def split(self, utterances: list[Utterance]) -> list[list[Utterance]]:
        raise ValueError("undefined merge strategy encountered")

    def merge(self, protocol: Protocol) -> list[Speech]:
        raise ValueError("undefined merge strategy encountered")


class SpeechMergerFactory:

    strategies: dict[MergeSpeechStrategyType, IMergeSpeechStrategy] = {
        'who': MergeSpeechByWho(),
        'who_sequence': MergeSpeechByWhoSequence(),
        'who_speaker_hash_sequence': MergeSpeechByWhoSpeakerHashSequence(),
        'speaker_hash_sequence': MergeSpeechBySpeakerHashSequence(),
        'chain': MergeSpeechByChain(),
        'undefined': UndefinedMergeSpeech(),
    }

    @staticmethod
    def get(strategy: str) -> IMergeSpeechStrategy:
        return (
            SpeechMergerFactory.strategies.get(strategy)
            if strategy in SpeechMergerFactory.strategies
            else SpeechMergerFactory.strategies.get('undefined')
        )
