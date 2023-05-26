from __future__ import annotations

import hashlib
from os.path import splitext
from typing import Any, Callable

from .. import metadata as md
from .. import utility
from ..corpus import corpus_index, iterate
from ..interface import TemporalKey

# pylint: disable=unbalanced-tuple-unpacking


def hashcoder_with_no_grouping_keys(item: iterate.ProtocolSegment, **_) -> tuple[dict, str, str]:
    return ({}, item.name, hashlib.md5(item.name.encode('utf-8')).hexdigest())


def take(d: dict, properties: set[str]) -> dict:
    return {k: v for k, v in (d.items() or {}) if k in properties}


def probe(x: Any, properties: set[str]) -> dict:
    return {k: getattr(x, k) for k in properties if hasattr(x, k)}


def create_grouping_hashcoder(
    grouping_keys: list[str],
) -> Callable[[iterate.ProtocolSegment, corpus_index.ICorpusSourceItem], str]:
    """Create a hashcode function for given grouping keys"""

    grouping_keys: set[str] = set(grouping_keys)

    if not grouping_keys:
        """No grouping apart from temporal key"""
        return hashcoder_with_no_grouping_keys

    def hashcoder(item: iterate.ProtocolSegment, source_item: corpus_index.ICorpusSourceItem) -> tuple[dict, str, str]:
        """Compute hash for item, speaker and source item. Return values, hash string and hash code"""
        assert issubclass(type(source_item), corpus_index.ICorpusSourceItem)
        parts: dict[str, str | int] = {
            # **take(item.speaker_info.asdict(), grouping_keys),
            # **take(source_item.to_dict(), grouping_keys),
            # **take(item.to_dict(), grouping_keys),
            **(probe(item.speaker_info.term_of_office, grouping_keys) if item.speaker_info else {}),
            **probe(item.speaker_info, grouping_keys),
            **probe(source_item, grouping_keys),
            **probe(item, grouping_keys),
        }
        missing: set[str] = grouping_keys - set(parts.keys())
        if len(missing) > 0:
            raise ValueError(f"unknown keys: {', '.join(list(missing))}")

        hashcode_str = utility.slugify('_'.join(str(x).lower().replace(' ', '_') for x in parts.values()))

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


# def _split_properties_by_dataclass(properties: set[str], *cls_list: tuple[Type, ...]) -> tuple[set[str], ...]:

#     properties = set(properties)

#     key_sets: list[set[str]] = []

#     for cls in cls_list:

#         if not is_dataclass(cls):
#             raise ValueError(f"{cls.__name__} is not a dataclass")

#         key_set: set[str] = properties.intersection({f.name for f in fields(cls)})

#         properties -= key_set

#         key_sets.append(key_set)

#     if properties:
#         raise ValueError(f"split_properties_by_dataclass: {','.join(properties)} not found.")

#     return tuple(key_sets)
