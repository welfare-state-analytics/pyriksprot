from __future__ import annotations

from typing import Iterable, List, Tuple

from pyriksprot import segment
from pyriksprot.interface import ContentType
from pyriksprot.utility import deprecated

from .parse import ProtocolMapper, XmlIterParseProtocol, XmlUntangleProtocol


def multiprocessing_xml_load(args) -> Iterable[segment.ProtocolSegment]:
    """Load protocol from XML. Aggregate text to `segment_level`. Return (name, who, id, text)."""
    return segment.to_segments(
        content_type=ContentType.Text,
        protocol=XmlUntangleProtocol(data=args[0], segment_skip_size=args[3]),
        segment_level=args[2],
        merge_strategy=args[4],
        segment_skip_size=args[3],
    )


class XmlUntangleSegmentIterator(segment.ProtocolSegmentIterator):
    """Iterate ParlaClarin XML files using `untangle` wrapper."""

    def load(self, filename: str) -> Iterable[segment.ProtocolSegment]:
        """Load protocol from XML. Aggregate text to `segment_level`. Return sequence of segment.ProtocolSegment."""
        return segment.to_segments(
            content_type=ContentType.Text,
            protocol=XmlUntangleProtocol(data=filename, segment_skip_size=self.segment_skip_size),
            segment_level=self.segment_level,
            merge_strategy=self.merge_strategy,
            segment_skip_size=self.segment_skip_size,
        )

    def map_futures(self, imap, args: List[Tuple[str, str, int]]):
        return imap(multiprocessing_xml_load, args)


@deprecated
def multiprocessing_load(args):
    return segment.to_segments(
        protocol=ProtocolMapper.to_protocol(data=args[0]),
        content_type=args[1],
        segment_level=args[2],
        segment_skip_size=args[3],
        merge_strategy=args[4],
    )


# @deprecated
class XmlProtocolSegmentIterator(segment.ProtocolSegmentIterator):
    """Reads xml files using Protocol entity and returns a stream of `segment.ProtocolSegment`"""

    def load(self, filename: str) -> List[segment.ProtocolSegment]:
        return segment.to_segments(
            protocol=ProtocolMapper.to_protocol(data=filename, segment_skip_size=self.segment_skip_size),
            content_type=self.content_type,
            segment_level=self.segment_level,
            segment_skip_size=self.segment_skip_size,
            merge_strategy=self.merge_strategy,
        )

    def map_futures(self, imap, args):
        return imap(multiprocessing_load, args, chunksize=self.multiproc_chunksize)


@deprecated
def multiprocessing_xml_iter_load(args) -> Iterable[segment.ProtocolSegment]:
    """Load protocol from XML. Aggregate text to `segment_level`. Return (name, who, id, text)."""
    return segment.to_segments(
        content_type=ContentType.Text,
        protocol=XmlIterParseProtocol(data=args[0], segment_skip_size=args[3]),
        segment_level=args[2],
        merge_strategy=args[4],
        segment_skip_size=args[3],
    )


# @deprecated
class XmlSaxSegmentIterator(segment.ProtocolSegmentIterator):
    """Reads xml files and returns a stream of (name, who, id, text, page_number).
    Uses SAX streaming"""

    def load(self, filename: str) -> List[segment.ProtocolSegment]:
        return segment.to_segments(
            content_type=ContentType.Text,
            protocol=XmlIterParseProtocol(data=filename, segment_skip_size=self.segment_skip_size),
            segment_level=self.segment_level,
            merge_strategy=self.merge_strategy,
            segment_skip_size=self.segment_skip_size,
        )

    def map_futures(self, imap, args):
        return imap(multiprocessing_xml_iter_load, args, chunksize=self.multiproc_chunksize)
