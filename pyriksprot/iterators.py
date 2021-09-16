from __future__ import annotations

import abc
from multiprocessing import get_context
from typing import TYPE_CHECKING, List, Tuple

from .parse import ProtocolMapper, XmlIterParseProtocol, XmlUntangleProtocol

if TYPE_CHECKING:
    from .interface import IterateLevel


class IProtocolTextIterator(abc.ABC):
    ...

    def __init__(
        self,
        *,
        filenames: List[str],
        skip_size: int = 1,
        level: IterateLevel = 'protocol',
        processes: int = None,
        chunksize: int = 100,
        ordered: bool = False,
        merge_strategy: str = 'n',
    ):
        self.filenames: List[str] = filenames
        self.iterator = None
        self.skip_size: int = skip_size
        self.level: IterateLevel = level
        self.processes: int = processes or 1
        self.chunksize: int = chunksize
        self.ordered: int = ordered
        self.merge_strategy: str = merge_strategy

    def __iter__(self):
        self.iterator = self.create_iterator()
        return self

    def __next__(self):
        return next(self.iterator)

    def create_iterator(self):

        if self.processes > 1:
            args: List[Tuple[str, str, int]] = [(name, self.level, self.skip_size) for name in self.filenames]
            with get_context("spawn").Pool(processes=self.processes) as executor:
                imap = executor.imap if self.ordered else executor.imap_unordered
                futures = self.map_futures(imap=imap, args=args)
                for payload in futures:
                    for item in payload:
                        yield item
        else:
            for filename in self.filenames:
                for item in self.load(filename=filename):
                    yield item

    @abc.abstractmethod
    def load(self, filename: str) -> List[Tuple[str, str, str, str]]:
        ...

    @abc.abstractmethod
    def map_futures(self, imap, args):
        ...


def multiprocessing_xml_load(args) -> List[Tuple[str, str, str, str, str]]:
    """Load protocol from XML. Aggregate text to `level`. Return (name, speaker, id, text)."""
    return XmlUntangleProtocol(data=args[0], skip_size=args[2]).to_text(level=args[1])


class XmlProtocolTextIterator(IProtocolTextIterator):
    """Iterate ParlaClarin XML files using `untangle` wrapper."""

    def load(self, filename: str) -> List[Tuple[str, str, str, str, str]]:
        """Load protocol from XML. Aggregate text to `level`. Return (name, speaker, id, text)."""
        return XmlUntangleProtocol(data=filename, skip_size=self.skip_size).to_text(level=self.level)

    def map_futures(self, imap, args: List[Tuple[str, str, int]]):
        return imap(multiprocessing_xml_load, args)


def multiprocessing_load(args):
    return ProtocolMapper.to_protocol(data=args[0]).to_text(args[1], skip_size=args[2])


class ProtocolTextIterator(IProtocolTextIterator):
    """Reads xml files and returns a stream of (name, who, id, text)"""

    def load(self, filename: str) -> List[Tuple[str, str, int]]:
        return ProtocolMapper.to_protocol(data=filename, skip_size=self.skip_size).to_text(
            self.level, skip_size=self.skip_size
        )

    def map_futures(self, imap, args):
        return imap(multiprocessing_load, args, chunksize=self.chunksize)


def multiprocessing_xml_iter_load(args) -> List[Tuple[str, str, str, str, str]]:
    """Load protocol from XML. Aggregate text to `level`. Return (name, speaker, id, text)."""
    return XmlIterParseProtocol(data=args[0], skip_size=args[2]).to_text(level=args[1])


class XmlIterProtocolTextIterator(IProtocolTextIterator):
    """Reads xml files and returns a stream of (name, who, id, text, page_number).
    Uses SAX streaming"""

    def load(self, filename: str) -> List[Tuple[str, str, int]]:
        return XmlIterParseProtocol(data=filename, skip_size=self.skip_size).to_text(level=self.level)

    def map_futures(self, imap, args):
        return imap(multiprocessing_xml_iter_load, args, chunksize=self.chunksize)
