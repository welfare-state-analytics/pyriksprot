from __future__ import annotations

import abc
import bz2
import gzip
import hashlib
import lzma
import os
import zipfile
from dataclasses import dataclass, field, fields
from functools import reduce
from typing import Any, Callable, Iterable, List, Literal, Mapping, Sequence, Set, Tuple, Type

import pandas as pd
from loguru import logger

from . import iterators
from .convert import dedent as dedent_text
from .interface import IterateLevel, ProtocolIterItem
from .member import ParliamentaryMember, ParliamentaryMemberIndex
from .source import SourceIndex, SourceIndexItem
from .utility import slugify

TemporalKey = Literal[None, 'year', 'decade', 'lustrum', 'custom']
GroupingKey = Literal[None, 'speaker', 'speech', 'party', 'gender']

# pylint: disable=too-many-arguments


def create_grouping_hashcoder(
    grouping_keys: Sequence[str],
) -> Callable[[ProtocolIterItem, ParliamentaryMember, SourceIndexItem], str]:

    grouping_keys: Set[str] = set(grouping_keys)

    member_keys: Set[str] = grouping_keys.intersection({f.name for f in fields(ParliamentaryMember)})
    index_keys: Set[str] = grouping_keys.intersection({f.name for f in fields(SourceIndexItem)})
    item_keys: Set[str] = grouping_keys.intersection({f.name for f in fields(ProtocolIterItem)})

    def hashcoder(item: ProtocolIterItem, member: ParliamentaryMember, source_item: SourceIndexItem) -> Tuple[str, str]:

        parts: Mapping[str, str | int] = {}

        for attr in member_keys:
            parts[attr] = str(getattr(member, attr, "unknown") or attr)

        for attr in index_keys:
            parts[attr] = str(getattr(source_item, attr, "unknown") or attr)

        for attr in item_keys:
            parts[attr] = str(getattr(item, attr, "unknown") or attr)

        hashcode_str = slugify('_'.join(x.lower().replace(' ', '_') for x in parts.values()))

        return (parts, hashcode_str, hashlib.md5(hashcode_str.encode('utf-8')).hexdigest())

    return hashcoder


class AggregateSlotClosed(Exception):
    ...


@dataclass
class AggregateIterItem:

    temporal_key: str
    name: str
    # who: str
    id: str
    page_number: str
    grouping_keys: Sequence[str]
    grouping_values: Mapping[str, str | int]

    texts: List[str] = field(default_factory=list)

    """Groups keys values, as a comma separated string"""
    key_values: str = field(init=False, default='')

    def __post_init__(self):
        self.key_values: str = '\t'.join(self.grouping_values[k] for k in self.grouping_keys)

    @property
    def text(self):
        return '\n'.join(self.texts)

    def add(self, item: ProtocolIterItem):
        self.texts.append(item.text)

    def __repr__(self) -> str:
        return (
            f"{self.temporal_key}"
            f"\t{self.name}"
            # f\t"{self.who or ''}"
            # f\t"{self.id or ''}"
            f"\t{self.page_number or ''}"
            f"\t{self.key_values}"
            f"\t{self.n_size}"
        )

    @property
    def n_size(self):
        return len(self.text)

    def to_dict(self):
        return {
            'period': self.temporal_key,
            'document_name': self.name,
            'filename': f'{self.name}.txt',
            'n_tokens': 0,
            **self.grouping_values,
        }


class TextAggregator:
    """Aggregate ProtocolIterItem based on time and grouping keys

    Time aggregation is always performed before group aggregation.
    This aggregator assumes that data is sorted by the temporal key.

    The temporal key can be None, 'year', 'lustrum', 'decade' or 'custom'.
    Temporal value is a tuple (from-year, to-year) where

    NOTE! if multiprocessing, then all items with same temporal values
    must be distributed to same process!

    """

    def __init__(
        self,
        source_index: SourceIndex,
        member_index: ParliamentaryMemberIndex,
        temporal_key: TemporalKey,
        grouping_keys: Sequence[GroupingKey],
    ):
        self.source_index: SourceIndex = source_index
        self.member_index: ParliamentaryMemberIndex = member_index
        self.temporal_key: TemporalKey = temporal_key
        self.custom_temporal_specification = None
        self.grouping_keys: Sequence[GroupingKey] = grouping_keys
        self.grouping_hashcoder = create_grouping_hashcoder(grouping_keys)

    def aggregate(self, iterator: iterators.IProtocolTextIterator) -> Iterable[AggregateIterItem]:

        """Current temporal value """
        current_temporal_hashcode: str = None
        current_aggregate: Mapping[str, AggregateIterItem] = {}
        grouping_keys: Set[str] = set(self.grouping_keys)

        if iterator.level is None:
            raise ValueError("protocol iter level cannot be None")

        if len(grouping_keys or []) == 0:
            raise ValueError("no grouping key specified")

        if 'who' in grouping_keys and iterator.level == 'protocol':
            raise ValueError("group by `who` not possible at protocol level.")

        for item in iterator:

            source_item: SourceIndexItem = self.source_index[item.name]

            if source_item is None:
                raise ValueError(f"source item not found: {item.name}")

            temporal_hashcode: str = source_item.temporal_hashcode(self.temporal_key)
            member: ParliamentaryMember = None if item.who is None else self.member_index[item.who]

            if current_temporal_hashcode != temporal_hashcode:
                logger.info(f"aggregating {temporal_hashcode}")
                """Yield previous aggregates"""
                for x in current_aggregate.values():
                    yield x

                current_aggregate, current_temporal_hashcode = {}, temporal_hashcode

            grouping_values, group_str, group_hashcode = self.grouping_hashcoder(item, member, source_item)

            if group_hashcode not in current_aggregate:

                current_aggregate[group_hashcode] = AggregateIterItem(
                    temporal_key=temporal_hashcode,
                    grouping_keys=self.grouping_keys,
                    grouping_values=grouping_values,
                    # who=member.id if 'who' in grouping_keys else None,
                    id=group_hashcode,
                    name=group_str,
                    page_number=0,
                    texts=[],
                )

            current_aggregate[group_hashcode].add(item)

        """Yield last aggregates"""
        for x in current_aggregate.values():
            yield x


def dehyphen_text(text: str) -> str:
    return text


def compose(*fns: Sequence[Callable[[str], str]]) -> Callable[[str], str]:
    """Create a composed function from a list of function. Return function."""
    if len(fns) == 0:
        return None
    return reduce(lambda f, g: lambda *args: f(g(*args)), fns)


def make_header(grouping_keys: Sequence[str]) -> str:
    header: str = "period\tname\ttid\t" + '\t'.join(v for v in grouping_keys)
    return header


class IDispatcher(abc.ABC):
    def __init__(self, target, mode):
        self.target = target
        self.document_data: List[dict] = []
        self.document_id: int = 0
        self.mode = mode

    def __enter__(self) -> IDispatcher:
        self.open_target(self.target)
        return self

    def __exit__(self, _type, _value, _traceback):  # pylint: disable=unused-argument
        self.close_target()
        return True

    @abc.abstractmethod
    def open_target(self, target: Any) -> None:
        ...

    def close_target(self) -> None:
        self.dispatch_index()

    @abc.abstractmethod
    def dispatch_index(self) -> None:
        ...

    @abc.abstractmethod
    def dispatch(self, item: AggregateIterItem) -> None:
        index_item: dict = item.to_dict()
        index_item['document_id'] = self.document_id
        self.document_id += 1
        self.document_data.append(index_item)

    @staticmethod
    def get_cls(mode: str) -> Type[IDispatcher]:
        if mode.startswith('zip'):
            return ZipFileDispatcher
        return FolderDispatcher


class FolderDispatcher(IDispatcher):
    def open_target(self, target: Any) -> None:
        os.makedirs(target, exist_ok=True)

    def dispatch(self, item: AggregateIterItem) -> None:
        super().dispatch(item)
        self.store(f'{item.temporal_key}_{item.name}.txt', item.text)

    def dispatch_index(self) -> None:
        csv_str: str = (
            pd.DataFrame(self.document_data).set_index('document_name', drop='false').rename_axis('').to_csv(sep='\t')
        )
        self.store('document_index.csv', csv_str)

    def store(self, filename: str, text: str) -> None:
        path: str = os.path.join(self.target, f"{filename}")
        store_text(filename=path, text=text, mode=self.mode)


def store_text(filename: str, text: str, mode: str) -> None:

    modules = {'gzip': (gzip, 'gz'), 'bz2': (bz2, 'bz2'), 'lzma': (lzma, 'xz')}

    if mode in modules:
        module, extension = modules[mode]
        with module.open(f"{filename}.{extension}", 'wb') as fp:
            fp.write(text.encode('utf-8'))

    if mode == 'plain':
        with open(filename, 'w', encoding='utf-8') as fp:
            fp.write(text)

    raise ValueError(f"unknown mode {mode}")


class ZipFileDispatcher(IDispatcher):
    def __init__(self, target, mode):
        self.zup: zipfile.ZipFile = None
        super().__init__(target, mode)

    def open_target(self, target: Any) -> None:
        if os.path.isdir(target):
            raise ValueError("zip mode: target must be name of zip file (not folder)")
        self.zup = zipfile.ZipFile(  # pylint: disable=consider-using-with
            self.target, mode="w", compression=zipfile.ZIP_DEFLATED
        )

    def close_target(self) -> None:
        super().close_target()
        self.zup.close()

    def dispatch(self, item: AggregateIterItem) -> None:
        super().dispatch(item)
        self.zup.writestr(f'{item.temporal_key}_{item.name}.txt', item.text)

    def dispatch_index(self) -> None:
        csv_str: str = (
            pd.DataFrame(self.document_data).set_index('document_name', drop='false').rename_axis('').to_csv(sep='\t')
        )
        self.zup.writestr('document_index.csv', csv_str)


class S3Dispatcher:
    ...


def extract_corpus_text(
    source_folder: str = None,
    target: str = None,
    target_mode: str = None,
    level: IterateLevel = None,
    dedent: bool = True,
    dehyphen: bool = False,
    keep_order: str = None,
    skip_size: int = 1,
    processes: int = 1,
    years: str = None,
    temporal_key: str = None,
    group_keys: Sequence[str] = None,
    **_,
) -> None:
    """Group extracted protocol blocks by `temporal_key` and attribute `group_keys`.

    Temporal key kan be any of None, 'year', 'lustrum', 'decade' or custom year periods
    - 'year', 'lustrum', 'decade' or custom year periods given as comma separated string

    """
    print(locals())
    source_index: SourceIndex = SourceIndex.load(source_folder=source_folder, years=years)
    member_index: ParliamentaryMemberIndex = ParliamentaryMemberIndex(f'{source_folder}/members_of_parliament.csv')

    preprocessor: Callable[[str], str] = compose(
        ([dedent_text] if dedent else []) + ([dehyphen_text] if dehyphen else [])
    )

    texts: iterators.IProtocolTextIterator = iterators.XmlProtocolTextIterator(
        filenames=source_index.paths,
        level=level,
        skip_size=skip_size,
        processes=processes,
        ordered=keep_order,
        preprocessor=preprocessor,
    )

    aggregator: TextAggregator = TextAggregator(
        source_index=source_index,
        member_index=member_index,
        temporal_key=temporal_key,
        grouping_keys=group_keys,
    )

    with IDispatcher.get_cls(target_mode)(target, target_mode) as dispatcher:
        for item in aggregator.aggregate(texts):
            dispatcher.dispatch(item)

    print(f"Corpus stored in {target}.")
