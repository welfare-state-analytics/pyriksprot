from __future__ import annotations

import hashlib
from typing import Callable

from .. import metadata as md
from .. import utility
from ..corpus import corpus_index, iterate

def hashcoder_with_no_grouping_keys(item: iterate.ProtocolSegment, **_) -> tuple[dict, str, str]:
    return ({}, item.name, hashlib.md5(item.name.encode('utf-8')).hexdigest())


def create_grouping_hashcoder(
    grouping_keys: list[str],
) -> Callable[[iterate.ProtocolSegment, corpus_index.CorpusSourceItem], str]:
    """Create a hashcode function for given grouping keys"""

    grouping_keys: set[str] = set(grouping_keys)

    if not grouping_keys:
        """No grouping apart from temporal key """
        return hashcoder_with_no_grouping_keys

    speaker_keys, item_keys, corpus_index_keys = utility.split_properties_by_dataclass(
        grouping_keys, md.SpeakerInfo, iterate.ProtocolSegment, corpus_index.CorpusSourceItem
    )

    def hashcoder(item: iterate.ProtocolSegment, source_item: corpus_index.CorpusSourceItem) -> tuple[dict, str, str]:
        """Compute hash for item, speaker and source item. Return values, hash string and hash code"""
        assert isinstance(source_item, corpus_index.CorpusSourceItem)
        try:
            speaker_data: dict = (
                {attr: str(getattr(item.speaker_info, attr)) for attr in speaker_keys} if speaker_keys else {}
            )
        except AttributeError as ex:
            raise ValueError(
                f"Grouping hashcoder: failed on retrieving key values from item.speaker_info. {ex}"
            ) from ex

        parts: dict[str, str | int] = {
            **speaker_data,
            **{attr: str(getattr(source_item, attr)) for attr in corpus_index_keys},
            **{attr: str(getattr(item, attr)) for attr in item_keys},
        }
        hashcode_str = utility.slugify('_'.join(x.lower().replace(' ', '_') for x in parts.values()))

        return (parts, hashcode_str, hashlib.md5(hashcode_str.encode('utf-8')).hexdigest())

    return hashcoder if grouping_keys else hashcoder_with_no_grouping_keys
