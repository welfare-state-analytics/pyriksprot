from __future__ import annotations

from typing import Iterable, List, Tuple

from pyriksprot.utility import deprecated

from ..interface import ProtocolSegment, ProtocolSegmentIterator
from .parse import ProtocolMapper, XmlIterParseProtocol, XmlUntangleProtocol


def multiprocessing_xml_load(args) -> Iterable[ProtocolSegment]:
    """Load protocol from XML. Aggregate text to `segment_level`. Return (name, who, id, text)."""
    return XmlUntangleProtocol(data=args[0], segment_skip_size=args[3]).to_text(segment_level=args[2])


class XmlUntangleSegmentIterator(ProtocolSegmentIterator):
    """Iterate ParlaClarin XML files using `untangle` wrapper."""

    def load(self, filename: str) -> Iterable[ProtocolSegment]:
        """Load protocol from XML. Aggregate text to `segment_level`. Return sequence of ProtocolSegment."""
        return XmlUntangleProtocol(data=filename, segment_skip_size=self.segment_skip_size).to_text(
            segment_level=self.segment_level
        )

    def map_futures(self, imap, args: List[Tuple[str, str, int]]):
        return imap(multiprocessing_xml_load, args)


@deprecated
def multiprocessing_load(args):
    return ProtocolMapper.to_protocol(data=args[0]).to_segments(
        content_type=args[1], segment_level=args[2], segment_skip_size=args[3]
    )


# @deprecated
class XmlProtocolSegmentIterator(ProtocolSegmentIterator):
    """Reads xml files using Protocol entity and returns a stream of `ProtocolSegment`"""

    def load(self, filename: str) -> List[ProtocolSegment]:
        return ProtocolMapper.to_protocol(data=filename, segment_skip_size=self.segment_skip_size).to_segments(
            content_type=self.content_type,
            segment_level=self.segment_level,
            segment_skip_size=self.segment_skip_size,
        )

    def map_futures(self, imap, args):
        return imap(multiprocessing_load, args, chunksize=self.multiproc_chunksize)


@deprecated
def multiprocessing_xml_iter_load(args) -> Iterable[ProtocolSegment]:
    """Load protocol from XML. Aggregate text to `segment_level`. Return (name, who, id, text)."""
    return XmlIterParseProtocol(data=args[0], segment_skip_size=args[3]).to_text(segment_level=args[2])


# @deprecated
class XmlSaxSegmentIterator(ProtocolSegmentIterator):
    """Reads xml files and returns a stream of (name, who, id, text, page_number).
    Uses SAX streaming"""

    def load(self, filename: str) -> List[ProtocolSegment]:
        return XmlIterParseProtocol(data=filename, segment_skip_size=self.segment_skip_size).to_text(
            segment_level=self.segment_level
        )

    def map_futures(self, imap, args):
        return imap(multiprocessing_xml_iter_load, args, chunksize=self.multiproc_chunksize)
