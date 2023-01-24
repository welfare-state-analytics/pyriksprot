from __future__ import annotations

import hashlib
from os.path import splitext
from typing import Callable

from .. import metadata as md
from .. import utility
from ..corpus import corpus_index, iterate
from ..interface import TemporalKey

# pylint: disable=unbalanced-tuple-unpacking


def hashcoder_with_no_grouping_keys(item: iterate.ProtocolSegment, **_) -> tuple[dict, str, str]:
    return ({}, item.name, hashlib.md5(item.name.encode('utf-8')).hexdigest())


def create_grouping_hashcoder(
    grouping_keys: list[str],
) -> Callable[[iterate.ProtocolSegment, corpus_index.ICorpusSourceItem], str]:
    """Create a hashcode function for given grouping keys"""

    grouping_keys: set[str] = set(grouping_keys)

    if not grouping_keys:
        """No grouping apart from temporal key """
        return hashcoder_with_no_grouping_keys

    speaker_keys, item_keys, corpus_index_keys = utility.split_properties_by_dataclass(
        grouping_keys, md.SpeakerInfo, iterate.ProtocolSegment, corpus_index.ICorpusSourceItem
    )

    def hashcoder(item: iterate.ProtocolSegment, source_item: corpus_index.ICorpusSourceItem) -> tuple[dict, str, str]:
        """Compute hash for item, speaker and source item. Return values, hash string and hash code"""
        assert issubclass(type(source_item), corpus_index.ICorpusSourceItem)
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


def truncate_year_to_category(year: int, temporal_key: TemporalKey) -> int:
    """truncates year to closest lustrum or decade."""
    if temporal_key == TemporalKey.Decade:
        return year - year % 10
    if temporal_key == TemporalKey.Lustrum:
        return year - year % 5
    return year


def to_temporal_category(temporal_key: str | TemporalKey | dict, year: int, default_value: str) -> str:

    if isinstance(temporal_key, (TemporalKey, str, type(None))):

        if temporal_key == TemporalKey.Year:
            return str(year)

        if temporal_key == TemporalKey.Lustrum:
            low_year: int = year - (year % 5)
            return f"{low_year}-{low_year+4}"

        if temporal_key == TemporalKey.Decade:
            low_year: int = year - (year % 10)
            return f"{low_year}-{low_year+9}"

        return default_value

    if isinstance(temporal_key, dict):
        """custom periods as a dict {'category-name': (from_year,to_year), ...}"""
        for k, v in temporal_key:
            if v[0] <= year <= v[1]:
                return k

    raise ValueError(f"temporal period failed for {default_value}")


def decode_protocol_segment_filename(lookups: md.Codecs, speech: iterate.ProtocolSegment, naming_keys: list[str]):

    basename, extension = splitext(speech.filename)

    suffix: str = ""

    for key in naming_keys:
        if hasattr(speech, key):
            key_value: int = getattr(speech, key)
        elif hasattr(speech.speaker_info, key):
            key_value: int = getattr(speech.speaker_info, key)
        else:
            raise ValueError(f"attribute {key} not found")

        key_value_label: str = lookups.lookup_name(key, key_value, "unknown")

        suffix = f"{suffix}_{key_value_label}"

    if speech.speaker_info is not None:
        suffix += f"_{speech.speaker_info.name[:80]}_{speech.speaker_info.person_id}"

    suffix = utility.slugify(suffix.lower(), True)

    filename: str = f"{basename}_{suffix}{extension}"
    return filename
