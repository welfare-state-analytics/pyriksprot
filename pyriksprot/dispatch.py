from __future__ import annotations

import abc
import os
import sys
import zipfile
from enum import Enum
from types import ModuleType
from typing import Any, List, Type, Union

import pandas as pd

from . import interface, merge, utility

# TargetType = Literal['plain', 'zip', 'checkpoint', 'gzip', 'bz2', 'lzma']
DispatchItem = Union[merge.MergedSegmentGroup, interface.ProtocolSegment]

jj = os.path.join


class TargetType(str, Enum):
    Plain = 'plain'
    Zip = 'zip'
    Checkpoint = 'checkpoint'
    Gzip = 'gzip'
    Bz2 = 'bz2'
    Lzma = 'lzma'


class IDispatcher(abc.ABC):

    name: str = 'parent'

    def __init__(self, target_name: str, target_type: TargetType, **kwargs):
        """Dispatches text blocks to a target_name zink.

        Args:
            target_name (str): Target filename or folder.
            target_type ([str]): Target store format.
        """
        self.target_name: str = target_name
        self.document_data: List[dict] = []
        self.document_id: int = 0
        self.target_type: TargetType = target_type
        self.kwargs: dict = kwargs

    def __enter__(self) -> IDispatcher:
        self.open_target(self.target_name)
        return self

    def __exit__(self, _type, _value, _traceback):  # pylint: disable=unused-argument
        self.close_target()
        return True

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

    @staticmethod
    def get_cls(target_type: TargetType) -> Type[IDispatcher]:
        """Return dispatcher class for `target_type`."""
        if target_type == 'zip':
            return ZipFileDispatcher
        if target_type == 'checkpoint':
            return CheckpointDispatcher
        return FolderDispatcher

    def document_index_str(self) -> str:
        csv_str: str = (
            pd.DataFrame(self.document_data).set_index('document_name', drop=False).rename_axis('').to_csv(sep='\t')
        )
        return csv_str

    @staticmethod
    def dispatchers() -> List[ModuleType]:
        return utility.find_subclasses(sys.modules[__name__], IDispatcher)


class FolderDispatcher(IDispatcher):
    """Dispatch text to filesystem as single files (optionally compressed)"""

    name: str = 'plain'

    def open_target(self, target_name: Any) -> None:
        os.makedirs(target_name, exist_ok=True)

    def _dispatch_item(self, item: merge.MergedSegmentGroup) -> None:
        filename: str = f'{item.temporal_key}_{item.name}.{item.extension}'
        self.store(filename, item.data)

    def dispatch_index(self) -> None:
        """Write index of documents to disk."""
        self.store('document_index.csv', self.document_index_str())

    def store(self, filename: str, text: str) -> None:
        """Store text to file."""
        path: str = jj(self.target_name, f"{filename}")
        utility.store_to_compressed_file(filename=path, text=text, target_type=self.target_type.value)


# class FeatherDispatcher(IDispatcher):
#     """Dispatch feather to filesystem as single files (optionally compressed)"""
#     name: str = 'feather'

#     def open_target(self, target_name: Any) -> None:
#         os.makedirs(target_name, exist_ok=True)

#     def _dispatch_item(self, item: merge.MergedSegmentGroup) -> None:
#         filename: str = f'{item.temporal_key}_{item.name}.{item.extension}'
#         self.store(filename, item.data)

#     def dispatch_index(self) -> None:
#         """Write index of documents to disk."""
#         self.store('document_index.csv', self.document_index_str())

#     def store(self, filename: str, text: str) -> None:
#         """Store text to file."""
#         path: str = os.path.join(self.target_name, f"{filename}")
#         utility.store_to_compressed_file(filename=path, text=text, target_type=self.target_type.value)


class ZipFileDispatcher(IDispatcher):
    """Dispatch text to a single zip file."""

    name: str = 'zip'

    def __init__(self, target_name: str, target_type: TargetType, **kwargs):
        self.zup: zipfile.ZipFile = None
        super().__init__(target_name, target_type, **kwargs)

    def open_target(self, target_name: Any) -> None:
        """Create and open a new zip file."""
        if os.path.isdir(target_name):
            raise ValueError("zip mode: target_name must be name of zip file (not folder)")
        self.zup = zipfile.ZipFile(  # pylint: disable=consider-using-with
            self.target_name, mode="w", compression=zipfile.ZIP_DEFLATED
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
        self.zup.writestr(item.filename, item.data)


class CheckpointDispatcher(IDispatcher):
    """Store as sequence of zipped CSV files (stream of Checkpoint)."""

    name: str = 'checkpoint'

    def open_target(self, target_name: Any) -> None:
        return

    def close_target(self) -> None:
        return

    def _dispatch_item(self, item: DispatchItem) -> None:
        return

    def dispatch_index(self) -> None:
        return

    def dispatch(self, dispatch_items: List[DispatchItem]) -> None:
        """Item is a Mapping in this case."""
        self._reset_index()
        if len(dispatch_items) == 0:
            return
        # FIXME: Only allowed for `protocol` level
        # NOTE: temporal_key is protocol name at `protocol` level
        checkpoint_name: str = f'{dispatch_items[0].temporal_key}.zip'
        sub_folder: str = dispatch_items[0].temporal_key.split('-')[1]
        path: str = jj(self.target_name, sub_folder)
        compression: int = self.kwargs.get('compression', zipfile.ZIP_LZMA)

        os.makedirs(path, exist_ok=True)

        self._reset_index()

        with zipfile.ZipFile(jj(path, checkpoint_name), mode="w", compression=compression) as fp:

            for item in dispatch_items:
                fp.writestr(item.filename, item.data)
                self._dispatch_index_item(item)

            if len(self.document_data) == 0:
                fp.writestr('document_index.csv', self.document_index_str())
                self._reset_index()


# class SingleTaggedFrameDispatcher(IDispatcher):
#     """Store as sequence of zipped CSV files (stream of Checkpoint)."""

#     def open_target(self, target_name: Any) -> None:
#         ...

#     def dispatch(self, group: Mapping[str, MergedSegmentGroup]) -> None:
#         """Item is a Mapping in this case."""
#         self.document_data = []
#         self.document_id: int = 0
#         with zipfile.ZipFile(self.target_name, mode="w", compression=zipfile.ZIP_DEFLATED) as zup:
#             for value in group.values():
#                 super().dispatch(value)
#                 zup.writestr(f'{group.temporal_key}_{group.name}.txt', group.data)
#             csv_str: str = self.document_index_str()
#             zup.writestr('document_index.csv', csv_str)


class S3Dispatcher:
    name: str = 'S3'
