from __future__ import annotations

import abc
import os
import sys
import zipfile
from collections import defaultdict
from enum import Enum
from io import StringIO
from typing import Any, List, Literal, Type, Union

import numpy as np
import pandas as pd
from loguru import logger

from pyriksprot.foss.pos_tags import PoS_Tag_Scheme, PoS_TAGS_SCHEMES
from pyriksprot.foss.stopwords import STOPWORDS

from . import interface, merge, utility

DispatchItem = Union[merge.MergedSegmentGroup, interface.ProtocolSegment]

jj = os.path.join
# pylint: disable=no-member
TargetTypeKey = Literal[
    'files-in-zip',
    'single-tagged-frame-per-group',
    'single-id-tagged-frame-per-group',
    'checkpoint-per-group',
    'files-in-folder',
]


class CompressType(str, Enum):
    Plain = 'plain'
    Zip = 'zip'
    Gzip = 'gzip'
    Bz2 = 'bz2'
    Lzma = 'lzma'
    Feather = 'feather'

    def to_zipfile_compression(self):
        if self.value == "plain":
            return zipfile.ZIP_STORED
        if self.value == "bz2":
            return zipfile.ZIP_BZIP2
        if self.value == "lzma":
            return zipfile.ZIP_LZMA
        return zipfile.ZIP_DEFLATED

    @classmethod
    def values(cls) -> List[str]:
        return [e.value for e in cls]


class IDispatcher(abc.ABC):

    name: str = 'parent'

    def __init__(self, *, target_name: str, compress_type: CompressType, **kwargs):
        """Dispatches text blocks to a target_name zink.

        Args:
            target_name (str): Target filename or folder.
            compress_type ([str]): Target compress format.
        """
        self.target_name: str = target_name
        self.document_data: List[dict] = []
        self.document_id: int = 0
        self.compress_type: CompressType = compress_type
        self.kwargs: dict = kwargs
        self.lowercase: bool = kwargs.get('lowercase', False)
        self.skip_stopwords: bool = kwargs.get('skip_stopwords', False)

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
        ...

    def close_target(self) -> None:
        """Close zink."""
        self.dispatch_index()

    def dispatch_index(self) -> None:
        """Dispatch an index of dispatched documents."""
        ...

    def dispatch(self, dispatch_items: List[DispatchItem]) -> None:
        for item in dispatch_items:
            self._dispatch_index_item(item)
            self._dispatch_item(item)

    def _reset_index(self) -> None:
        self.document_data = []
        self.document_id: int = 0

    def _dispatch_index_item(self, item: DispatchItem) -> None:
        self.document_data.append({**item.to_dict(), **{'document_id': self.document_id}})
        self.document_id += 1

    @abc.abstractmethod
    def _dispatch_item(self, item: merge.MergedSegmentGroup) -> None:
        ...

    def document_index(self) -> pd.DataFrame:
        document_index: pd.DataFrame = pd.DataFrame(self.document_data)
        return document_index

    def document_index_str(self) -> str:
        csv_str: str = self.document_index().to_csv(sep='\t')
        return csv_str

    def store(self, filename: str, data: str | pd.DataFrame) -> None:
        """Store text to file."""

        path: str = jj(self.target_name, f"{filename}")

        if isinstance(data, pd.DataFrame):

            if self.compress_type == 'feather':
                data.to_feather(utility.replace_extension(filename, 'feather'))
                return

            data = data.to_csv(sep='\t')

        if isinstance(data, str):
            utility.store_str(filename=path, text=data, compress_type=self.compress_type.value)

    @staticmethod
    def dispatchers() -> List[Type]:
        return utility.find_subclasses(sys.modules[__name__], IDispatcher)

    @staticmethod
    def dispatcher(key: str) -> Type[IDispatcher]:
        """Return dispatcher class for `key`."""
        dispatchers: List[Type[IDispatcher]] = IDispatcher.dispatchers()
        for dispatcher in dispatchers:
            if dispatcher.name == key:
                return dispatcher
        logger.warning(f"unknown dispatcher {key}: falling back to FolderDispatcher ")
        return FilesInFolderDispatcher

    @staticmethod
    def dispatcher_keys() -> List[str]:
        return [d.name for d in IDispatcher.dispatchers()]


class FilesInFolderDispatcher(IDispatcher):
    """Dispatch text to filesystem as single files (optionally compressed)"""

    def __init__(self, target_name: str, compress_type: CompressType, **kwargs):
        super().__init__(target_name=target_name, compress_type=compress_type, **kwargs)

    name: str = 'files-in-folder'

    def open_target(self, target_name: Any) -> None:
        os.makedirs(target_name, exist_ok=True)

    def _dispatch_item(self, item: merge.MergedSegmentGroup) -> None:
        filename: str = f'{item.temporal_key}_{item.name}.{item.extension}'
        # FIXME: POS is made lowercase!
        self.store(filename, item.data.lower() if self.lowercase else item.data)

    def dispatch_index(self) -> None:
        """Write index of documents to disk."""
        self.store('document_index.csv', self.document_index_str())


class FilesInZipDispatcher(IDispatcher):
    """Dispatch text to a single zip file."""

    name: str = 'files-in-zip'

    def __init__(self, target_name: str, compress_type: CompressType, **kwargs):
        self.zup: zipfile.ZipFile = None
        super().__init__(target_name=target_name, compress_type=compress_type, **kwargs)

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

    def _dispatch_item(self, item: DispatchItem) -> None:
        # FIXME: POS is made lowercase!
        self.zup.writestr(item.filename, item.data.lower() if self.lowercase else item.data)


class CheckpointPerGroupDispatcher(IDispatcher):
    """Store as sequence of zipped CSV files (stream of Checkpoint)."""

    def __init__(self, target_name: str, compress_type: CompressType, **kwargs):
        super().__init__(target_name=target_name, compress_type=compress_type, **kwargs)

    name: str = 'checkpoint-per-group'

    def open_target(self, target_name: Any) -> None:
        return

    def close_target(self) -> None:
        return

    def _dispatch_item(self, item: DispatchItem) -> None:
        return

    def dispatch_index(self) -> None:
        return

    def dispatch(self, dispatch_items: List[DispatchItem]) -> None:

        self._reset_index()

        if len(dispatch_items) == 0:
            return

        checkpoint_name: str = f'{dispatch_items[0].temporal_key}.zip'
        sub_folder: str = dispatch_items[0].temporal_key.split('-')[1]
        path: str = jj(self.target_name, sub_folder)

        os.makedirs(path, exist_ok=True)

        with zipfile.ZipFile(
            jj(path, checkpoint_name), mode="w", compression=self.compress_type.to_zipfile_compression()
        ) as fp:

            for item in dispatch_items:
                # FIXME: POS is made lowercase!
                fp.writestr(item.filename, item.data.lower() if self.lowercase else item.data)
                self._dispatch_index_item(item)

            if len(self.document_data) > 0:
                fp.writestr('document_index.csv', self.document_index_str())
                self._reset_index()


class SingleTaggedFrameDispatcher(FilesInFolderDispatcher):
    """Store merged group items in a single tagged frame.

    NOTE! This dispatcher is ONLY tested for Speech level segments.

    """

    def __init__(self, target_name: str, compress_type: CompressType, **kwargs):
        super().__init__(target_name=target_name, compress_type=compress_type, **kwargs)
        self.skip_text: bool = kwargs.get('skip_text', False)
        self.skip_lemma: bool = kwargs.get('skip_lemma', False)
        self.skip_puncts: bool = kwargs.get('skip_puncts', False)

    name: str = 'single-tagged-frame-per-group'

    def _dispatch_item(self, item: DispatchItem) -> None:
        return

    def dispatch(self, dispatch_items: List[DispatchItem]) -> None:

        if len(dispatch_items) == 0:
            return

        first_item: DispatchItem = dispatch_items[0]

        sub_folder: str = first_item.temporal_key.split('-')[1]
        path: str = jj(self.target_name, sub_folder)
        target_name: str = jj(path, f'{first_item.temporal_key}.csv')

        # if first_item.grouping_keys:
        #     raise ValueError('FeatherDispatcher currently only valid for intra-protocol dispatch segments')

        # FIXME: Add guard for temporal key not in in year/decade/lustrum/custom
        os.makedirs(path, exist_ok=True)

        tagged_frames: List[pd.DataFrame] = []
        for item in dispatch_items:

            tagged_frame: pd.DataFrame = self.create_tagged_frame(item)

            tagged_frames.append(tagged_frame)
            item.n_tokens = len(tagged_frame)
            self._dispatch_index_item(item)

        total_frame: pd.DataFrame = pd.concat(tagged_frames, ignore_index=True)

        self.store(filename=target_name, data=total_frame)

    def create_tagged_frame(self, item: DispatchItem) -> pd.DataFrame:

        pads: set = {'MID', 'MAD', 'PAD'}
        tagged_frame: pd.DataFrame = pd.read_csv(StringIO(item.data), sep='\t', quoting=3, dtype=str)
        tagged_frame['document_id'] = self.document_id

        drop_columns: List[str] = []

        if 'xpos' in tagged_frame.columns:
            drop_columns.append('xpos')

        if self.skip_stopwords and self.skip_puncts:
            tagged_frame = tagged_frame[~(tagged_frame.token.str.lower().isin(STOPWORDS) | tagged_frame.pos.isin(pads))]
        else:
            if self.skip_stopwords:
                tagged_frame = tagged_frame[~tagged_frame.token.str.lower().isin(STOPWORDS)]
            if self.skip_puncts:
                tagged_frame = tagged_frame[~tagged_frame.pos.isin(pads)]

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

        for column_name in ['Unnamed: 0', 'period']:
            if column_name in document_index.columns:
                document_index.drop(columns=column_name, inplace=True)

        document_index['year'] = trim_series_type(document_index.year)
        document_index['n_tokens'] = trim_series_type(document_index.n_tokens)
        document_index['document_id'] = trim_series_type(document_index.document_id)

        self.store(filename=jj(self.target_name, 'document_index.csv'), data=document_index)


class SingleIdTaggedFrameDispatcher(SingleTaggedFrameDispatcher):
    """Store merged group items in a single tagged frame.
    NOTE! This dispatcher is ONLY tested for Speech level segments.
    """

    name: str = 'single-id-tagged-frame-per-group'

    def __init__(self, target_name: str, compress_type: CompressType, **kwargs):
        super().__init__(target_name=target_name, compress_type=compress_type, **kwargs)
        self.token2id: defaultdict = defaultdict()
        self.token2id.default_factory = self.token2id.__len__
        self.pos_schema: PoS_Tag_Scheme = PoS_TAGS_SCHEMES.SUC

    def create_tagged_frame(self, item: DispatchItem) -> pd.DataFrame:
        tagged_frame: pd.DataFrame = super().create_tagged_frame(item)
        fg = lambda t: self.token2id[t]
        pg = self.pos_schema.pos_to_id.get

        if not self.skip_text:
            tagged_frame['token_id'] = tagged_frame.token.apply(fg)

        if not self.skip_lemma:
            tagged_frame['lemma_id'] = tagged_frame.lemma.apply(fg)

        tagged_frame['pos_id'] = tagged_frame.pos.apply(pg).astype(np.int8)
        tagged_frame.drop(columns=['lemma', 'token', 'pos'], inplace=True, errors='ignore')
        return tagged_frame

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


def trim_series_type(series: pd.Series) -> pd.Series:
    max_value: int = series.max()
    for np_type in [np.int16, np.int32]:
        if max_value < np.iinfo(np_type).max:
            return series.astype(np_type)
    return series


class S3Dispatcher:
    name: str = 'S3'
