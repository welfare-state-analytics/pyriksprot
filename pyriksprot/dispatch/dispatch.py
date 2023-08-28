from __future__ import annotations

import abc
import os
import string
import sys
import zipfile
from collections import defaultdict
from enum import Enum
from io import StringIO
from typing import Any, Literal, Type

import numpy as np
import pandas as pd
from loguru import logger

from pyriksprot.corpus.iterate import ProtocolSegment
from pyriksprot.dispatch.item import DispatchItem
from pyriksprot.dispatch.utility import decode_protocol_segment_filename, to_temporal_category
from pyriksprot.foss.pos_tags import PoS_Tag_Scheme, PoS_TAGS_SCHEMES
from pyriksprot.foss.sparv_tokenize import SegmenterRepository
from pyriksprot.foss.stopwords import STOPWORDS
from pyriksprot.metadata import Codecs

from .. import utility
from ..interface import IDispatchItem, SegmentLevel

jj = os.path.join

TargetTypeKey = Literal[
    'files-in-zip',
    'single-tagged-frame-per-group',
    'single-id-tagged-frame-per-group',
    'single-id-tagged-frame',
    'checkpoint-per-group',
    'files-in-folder',
    'one-hot-sparse',
    'sorted-speeches-in-zip',
]

PERSON_ATTRIBUTES = {'sub_office_type_id', 'office_type_id', 'protocol_name', 'gender_id', 'party_id'}


class CompressType(str, Enum):
    Plain = 'csv'
    Zip = 'zip'
    Gzip = 'gzip'
    Bz2 = 'bz2'
    Lzma = 'lzma'
    Feather = 'feather'

    def to_zipfile_compression(self):
        if self.value == "csv":
            return zipfile.ZIP_STORED
        if self.value == "bz2":
            return zipfile.ZIP_BZIP2
        if self.value == "lzma":
            return zipfile.ZIP_LZMA
        return zipfile.ZIP_DEFLATED

    @classmethod
    def values(cls) -> list[str]:
        return [e.value for e in cls]


class IDispatcher(abc.ABC):
    name: str = 'parent'

    def __init__(self, *, target_name: str, compress_type: CompressType, lookups: Codecs, **kwargs):
        """Dispatches text blocks to a target_name zink.

        Args:
            target_name (str): Target filename or folder.
            compress_type ([str]): Target compress format.
        """
        self.target_name: str = target_name
        self.document_data: list[dict] = []
        self.document_id: int = 0
        self.compress_type: CompressType = compress_type
        self.kwargs: dict = kwargs
        self.lowercase: bool = kwargs.get('lowercase', False)
        self.skip_stopwords: bool = kwargs.get('skip_stopwords', False)
        self.lookups: Codecs = lookups

    def __enter__(self) -> IDispatcher:
        self.open_target(self.target_name)
        return self

    def __exit__(self, _type, _value, _traceback):  # pylint: disable=unused-argument
        """If the suite was exited due to an exception, and the return value from the __exit__() method was false,
        the exception is reraised. If the return value was true, the exception is suppressed, and execution
        continues with the statement following the with statement."""
        self.close_target()
        return False

    @abc.abstractmethod
    def open_target(self, target_name: Any) -> None:
        """Open zink."""

    def close_target(self) -> None:
        """Close zink."""
        self.dispatch_index()

    def dispatch_index(self) -> None:
        """Dispatch an index of dispatched documents."""

    def dispatch(self, dispatch_items: list[IDispatchItem]) -> None:
        for item in dispatch_items:
            self._dispatch_index_item(item)
            self._dispatch_item(item)

    def _reset_index(self) -> None:
        self.document_data = []
        self.document_id: int = 0

    def _dispatch_index_item(self, item: IDispatchItem) -> None:
        """Default one document per group"""
        self.document_data.append({**item.to_dict(), **{'document_id': self.document_id}})
        self.document_id += 1

    @abc.abstractmethod
    def _dispatch_item(self, item: IDispatchItem) -> None:
        ...

    def document_index(self) -> pd.DataFrame:
        document_index: pd.DataFrame = pd.DataFrame(self.document_data)
        return document_index

    def document_index_str(self) -> str:
        csv_str: str = self.document_index().to_csv(sep='\t')
        return csv_str

    def store(self, filename: str, data: str | pd.DataFrame) -> None:
        """Store text to file."""

        if not os.path.split(filename)[0]:
            filename = jj(self.target_name, f"{filename}")

        if isinstance(data, pd.DataFrame):
            if self.compress_type == 'feather':
                data.to_feather(utility.replace_extension(filename, 'feather'))
                return

            data = data.to_csv(sep='\t')

        if isinstance(data, str):
            utility.store_str(filename=filename, text=data, compress_type=self.compress_type.value)

    @staticmethod
    def dispatchers() -> list[Type]:
        return utility.find_subclasses(sys.modules[__name__], IDispatcher)

    @staticmethod
    def dispatcher(key: str) -> Type[IDispatcher]:
        """Return dispatcher class for `key`."""
        dispatchers: list[Type[IDispatcher]] = IDispatcher.dispatchers()
        for dispatcher in dispatchers:
            if dispatcher.name == key:
                return dispatcher
        logger.warning(f"unknown dispatcher {key}: falling back to FolderDispatcher ")
        return FilesInFolderDispatcher

    @staticmethod
    def dispatcher_keys() -> list[str]:
        return [d.name for d in IDispatcher.dispatchers()]

    def to_lower(self, text: str) -> str:
        # FIXME: PoS tags gets lowercased???
        return text.lower() if self.lowercase else text

    # FIXME: Consolidate this code!

    def get_filename(self, item: IDispatchItem) -> str:
        if isinstance(item, DispatchItem):
            return self.decoded_filename(item)
        return item.filename

    def decoded_group_name(self, item: DispatchItem) -> str:
        group_name: str = ""
        for key, key_id in item.group_values.items():
            try:
                value_name: str = self.lookups.lookup_name(key, int(key_id), None)
                group_name = (
                    f"{group_name}_{key_id}"
                    if value_name is None
                    else f"{group_name}_unknown"
                    if value_name == "?"
                    else f"{group_name}_{value_name}"
                )
            except Exception as _:
                group_name = f"{group_name}_{key_id}"
        return group_name

    def decoded_document_name(self, item: DispatchItem) -> str:
        group_name: str = self.decoded_group_name(item)
        if group_name != "":
            group_name = f"_{group_name}"
        return f'{item.group_temporal_value}{self.decoded_group_name(item)}'

    def decoded_filename(self, item: DispatchItem) -> str:
        return f'{self.decoded_document_name(item=item)}.{item.extension}'


class FilesInFolderDispatcher(IDispatcher):
    """Dispatch text to filesystem as single files (optionally compressed)"""

    def __init__(self, target_name: str, compress_type: CompressType, lookups: Codecs, **kwargs):
        super().__init__(target_name=target_name, compress_type=compress_type, lookups=lookups, **kwargs)

    name: str = 'files-in-folder'

    def open_target(self, target_name: Any) -> None:
        os.makedirs(target_name, exist_ok=True)

    def _dispatch_item(self, item: IDispatchItem) -> None:
        self.store(self.get_filename(item), self.to_lower(item.text))

    def dispatch_index(self) -> None:
        """Write index of documents to disk."""
        self.store('document_index.csv', self.document_index_str())


class FilesInZipDispatcher(IDispatcher):
    """Dispatch text to a single zip file."""

    name: str = 'files-in-zip'

    def __init__(self, target_name: str, compress_type: CompressType, lookups: Codecs, **kwargs):
        self.zup: zipfile.ZipFile = None
        super().__init__(target_name=target_name, compress_type=compress_type, lookups=lookups, **kwargs)

    def open_target(self, target_name: Any) -> None:
        """Create and open a new zip file."""
        if os.path.isdir(target_name):
            raise ValueError("zip mode: target_name must be name of zip file (not folder)")
        self.zup = zipfile.ZipFile(  # pylint: disable=consider-using-with
            self.target_name, mode="w", compression=self.compress_type.to_zipfile_compression()
        )

    def close_target(self) -> None:
        """Close the zip file."""
        super().close_target()
        self.zup.close()

    def dispatch_index(self) -> None:
        """Write index of documents to zip file."""
        if len(self.document_data) == 0:
            return
        csv_str: str = self.document_index_str()
        self.zup.writestr('document_index.csv', csv_str)

    def _dispatch_item(self, item: IDispatchItem) -> None:
        self.zup.writestr(self.get_filename(item), self.to_lower(item.text))


class SortedSpeechesInZipDispatcher(FilesInZipDispatcher):
    """Dispatch speeches to files in a single zip file."""

    name: str = 'sorted-speeches-in-zip'

    def __init__(
        self,
        target_name: str,
        compress_type: CompressType,
        lookups: Codecs,
        subfolder_key: str = None,
        naming_keys: list[str] = None,
        **kwargs,
    ):
        super().__init__(target_name=target_name, compress_type=compress_type, lookups=lookups, **kwargs)
        self.subfolder_key: str = subfolder_key
        self.naming_keys: list[str] = naming_keys

    def to_speech_segment(self, item: IDispatchItem) -> ProtocolSegment:
        if item.segment_level != SegmentLevel.Speech:
            raise ValueError(f"{type(self).__name__}: requires speech segment level")

        if issubclass(type(item), ProtocolSegment):
            return item

        if hasattr(item, "protocol_segments"):
            segments: list[ProtocolSegment] = getattr(item, "protocol_segments")
            if len(segments) > 0:
                return segments[0]

        raise ValueError(f"{type(self).__name__}: item has no protocol segments")

    def _dispatch_index_item(self, item: IDispatchItem) -> None:
        tokens: list[str] = SegmenterRepository.default_tokenize(item.text)
        tokens = [t for t in tokens if len(t) > 1 or t not in string.punctuation]
        item.n_tokens = len(tokens)
        return super()._dispatch_index_item(item)

    def _dispatch_item(self, item: IDispatchItem) -> None:
        speech: ProtocolSegment = self.to_speech_segment(item)

        subfolder: str = to_temporal_category(self.subfolder_key, speech.year, speech.protocol_name)

        filename: str = jj(subfolder or "", decode_protocol_segment_filename(self.lookups, speech, self.naming_keys))

        self.zup.writestr(filename, self.to_lower(speech.text))


class CheckpointPerGroupDispatcher(IDispatcher):
    """Store as sequence of zipped CSV files (stream of Checkpoint)."""

    def __init__(self, target_name: str, compress_type: CompressType, lookups: Codecs, **kwargs):
        super().__init__(target_name=target_name, compress_type=compress_type, lookups=lookups, **kwargs)

    name: str = 'checkpoint-per-group'

    def open_target(self, target_name: Any) -> None:
        return

    def close_target(self) -> None:
        return

    def _dispatch_item(self, item: IDispatchItem) -> None:
        return

    def dispatch_index(self) -> None:
        return

    def dispatch(self, dispatch_items: list[IDispatchItem]) -> None:
        self._reset_index()

        if len(dispatch_items) == 0:
            return

        checkpoint_name: str = f'{dispatch_items[0].group_temporal_value}.zip'
        subfolder: str = dispatch_items[0].group_temporal_value.split('-')[1]
        path: str = jj(self.target_name, subfolder)

        os.makedirs(path, exist_ok=True)

        with zipfile.ZipFile(
            jj(path, checkpoint_name), mode="w", compression=self.compress_type.to_zipfile_compression()
        ) as fp:
            for item in dispatch_items:
                fp.writestr(item.filename, self.to_lower(item.text))
                self._dispatch_index_item(item)

            if len(self.document_data) > 0:
                fp.writestr('document_index.csv', self.document_index_str())
                self._reset_index()


class TaggedFramePerGroupDispatcher(FilesInFolderDispatcher):
    """Store merged group items in a single tagged frame.
    NOTE! This dispatcher is ONLY valid for Speech level segments.
    """

    def __init__(self, target_name: str, compress_type: CompressType, lookups: Codecs, **kwargs):
        super().__init__(target_name=target_name, compress_type=compress_type, lookups=lookups, **kwargs)
        self.skip_text: bool = kwargs.get('skip_text', False)
        self.skip_lemma: bool = kwargs.get('skip_lemma', False)
        self.skip_puncts: bool = kwargs.get('skip_puncts', False)

    name: str = 'single-tagged-frame-per-group'

    def _dispatch_item(self, item: IDispatchItem) -> None:
        return

    def _dispatch_index_item(self, item: IDispatchItem) -> None:
        item: DispatchItem = item

        if item.segment_level != SegmentLevel.Speech:
            raise ValueError(f"TaggedFramePerGroupDispatcher: expected Speech, found {item.segment_level}")

        if item.group_values == {}:
            """Speech level segments and no grouping => Add all speech metadata to index"""
            if len(item.protocol_segments) > 1:
                raise ValueError(
                    f"TaggedFramePerGroupDispatcher: expected exacly one Speech, found {len(item.protocol_segments)}"
                )
            speech_data: dict = item.protocol_segments[0].to_dict()
            item_data: dict = {**{'document_id': self.document_id}, **item.to_dict(), **speech_data}
        else:
            item_data: dict = {**{'document_id': self.document_id}, **item.to_dict()}
            speech_data: dict = item.protocol_segments[0].to_dict()
            if 'who' in item.group_values:
                """If `who` in grouping attributes => add person attribute from first speech"""
                for key in PERSON_ATTRIBUTES:
                    if key in speech_data:
                        item_data[key] = speech_data[key]
            if 'n_tokens' in speech_data:
                n_tokens: int = 0
                for sd in item.protocol_segments:
                    n_tokens += sd.n_tokens
                item_data['n_tokens'] = n_tokens

            # FIXME #39 Protocol segements must be merged into one Document index item.
            # for speech_segment in item.protocol_segments:
            #     self.document_data.append(
            #         {**{'document_id': self.document_id}, **item.to_dict(), **speech_segment.to_dict()}
            #     )
            #     self.document_id += 1

        self.document_data.append(item_data)
        self.document_id += 1

    def dispatch(self, dispatch_items: list[IDispatchItem]) -> None:
        if len(dispatch_items) == 0:
            return

        tagged_frames: list[pd.DataFrame] = []
        for item in dispatch_items:
            tagged_frame: pd.DataFrame = self.create_tagged_frame(item)
            tagged_frames.append(tagged_frame)
            item.n_tokens = len(tagged_frame)
            self._dispatch_index_item(item)

        if len(tagged_frames) > 0:
            total_frame: pd.DataFrame = pd.concat(tagged_frames, ignore_index=True)
            self.flush(total_frame, dispatch_items)

    def flush(self, tagged_frame: pd.DataFrame, dispatch_items: list[IDispatchItem]):
        temporal_value: str = dispatch_items[0].group_temporal_value
        subfolder: str = temporal_value.split('-')[1] if '-' in temporal_value else temporal_value
        path: str = jj(self.target_name, subfolder)
        os.makedirs(path, exist_ok=True)
        target_name: str = jj(path, f'{temporal_value}.csv')
        self.store(filename=target_name, data=tagged_frame)

    def create_tagged_frame(self, item: IDispatchItem) -> pd.DataFrame:
        pads: set = {'MID', 'MAD', 'PAD'}

        tagged_frame: pd.DataFrame = pd.read_csv(StringIO(item.text), sep='\t', quoting=3, dtype=str)
        tagged_frame['document_id'] = self.document_id

        if self.lowercase:
            tagged_frame["token"] = tagged_frame["token"].str.lower()
            tagged_frame["lemma"] = tagged_frame["lemma"].str.lower()

        drop_columns: list[str] = []

        if 'xpos' in tagged_frame.columns:
            drop_columns.append('xpos')

        if self.skip_stopwords and self.skip_puncts:
            tagged_frame = tagged_frame[
                ~(tagged_frame["token"].str.lower().isin(STOPWORDS) | tagged_frame["pos"].isin(pads))
            ]
        else:
            if self.skip_stopwords:
                tagged_frame = tagged_frame[~tagged_frame["token"].str.lower().isin(STOPWORDS)]
            if self.skip_puncts:
                tagged_frame = tagged_frame[~tagged_frame["pos"].isin(pads)]

        if self.skip_text:
            drop_columns.append('token')
        elif self.lowercase:
            tagged_frame['token'] = tagged_frame['token'].str.lower()

        if self.skip_lemma:
            drop_columns.append('lemma')
        elif self.lowercase:
            tagged_frame['lemma'] = tagged_frame['lemma'].str.lower().fillna('')
            assert not tagged_frame.lemma.isna().any(), "YOU SHALL UPDATE LEMMA FROM TEXT"

        tagged_frame = tagged_frame.drop(columns=drop_columns)

        return tagged_frame

    def dispatch_index(self) -> None:
        """Write index of documents to disk."""

        if len(self.document_data) == 0:
            return

        document_index: pd.DataFrame = self.document_index()

        for column_name in [
            'Unnamed: 0',
            'period',
            'protocol_name',
        ]:
            if column_name in document_index.columns:
                document_index.drop(columns=column_name, inplace=True)

        document_index = trim_data_frame_typs(document_index)
        # document_index['year'] = trim_series_type(document_index.year)
        # document_index['n_tokens'] = trim_series_type(document_index.n_tokens)
        # document_index['document_id'] = trim_series_type(document_index.document_id)

        self.store(filename=jj(self.target_name, 'document_index.csv'), data=document_index)


class IdTaggedFramePerGroupDispatcher(TaggedFramePerGroupDispatcher):
    """Store merged group items in a single tagged frame.
    NOTE! This dispatcher is ONLY valid for Speech level segments.
    """

    name: str = 'single-id-tagged-frame-per-group'

    def __init__(self, target_name: str, compress_type: CompressType, lookups: Codecs, **kwargs):
        super().__init__(target_name=target_name, compress_type=compress_type, lookups=lookups, **kwargs)
        self.token2id: defaultdict = defaultdict()
        self.tfs: defaultdict = defaultdict()
        self.token2id.default_factory = self.token2id.__len__
        self.pos_schema: PoS_Tag_Scheme = PoS_TAGS_SCHEMES.SUC

    def create_tagged_frame(self, item: IDispatchItem) -> pd.DataFrame:
        try:
            tagged_frame: pd.DataFrame = super().create_tagged_frame(item)
            fg = lambda t: self.token2id[t]  # pylint: disable=unnecessary-lambda-assignment
            pg = self.pos_schema.pos_to_id.get  # pylint: disable=unnecessary-lambda-assignment

            if not self.skip_text:
                tagged_frame['token_id'] = tagged_frame.token.apply(fg)

            if not self.skip_lemma:
                tagged_frame['lemma_id'] = tagged_frame.lemma.apply(fg)

            tagged_frame['pos_id'] = tagged_frame.pos.apply(pg).fillna(0).astype(np.int8)
            tagged_frame.drop(columns=['lemma', 'token', 'pos'], inplace=True, errors='ignore')
            return tagged_frame
        except Exception as ex:
            logger.error(f"create_tagged_frame: {ex}")
            logger.error(f" filename: {item.filename}")

            raise ex

    def dispatch_index(self) -> None:
        super().dispatch_index()
        self.dispatch_vocabulary()

    def dispatch_vocabulary(self) -> None:
        vocabulary: pd.DataFrame = pd.DataFrame(
            data={
                'token': self.token2id.keys(),
                'token_id': self.token2id.values(),
            }
        )
        self.store(filename=jj(self.target_name, 'token2id.csv'), data=vocabulary)


class SingleIdTaggedFrameDispatcher(IdTaggedFramePerGroupDispatcher):
    """Store merged group items in a single, global tagged frame."""

    name: str = 'single-id-tagged-frame'
    corpus_name: str = "corpus.feather"

    def __init__(self, target_name: str, compress_type: CompressType, lookups: Codecs, **kwargs):
        super().__init__(target_name=target_name, compress_type=compress_type, lookups=lookups, **kwargs)
        self.tagged_frames: list[pd.DataFrame] = []

    def flush(self, tagged_frame: pd.DataFrame, dispatch_items: list[IDispatchItem]):
        self.tagged_frames.append(tagged_frame)

    def close_target(self) -> None:
        super().close_target()
        total_frame: pd.DataFrame = pd.concat(self.tagged_frames, ignore_index=True)
        self.store(filename=self.corpus_name, data=total_frame)


def trim_series_type(series: pd.Series) -> pd.Series:
    max_value: int = series.max()
    for np_type in [np.int8, np.int16, np.int32]:
        if max_value < np.iinfo(np_type).max:
            return series.astype(np_type)
    return series


def trim_data_frame_typs(frame: pd.DataFrame, columns: list[str] = None) -> pd.DataFrame:
    columns = columns if columns else frame.columns
    for column in columns:
        if not column in frame.columns:
            continue
        if not np.issubdtype(frame[column].dtype, np.integer):
            continue
        frame[column] = trim_series_type(frame[column])
    return frame
