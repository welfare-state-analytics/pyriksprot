from __future__ import annotations

from typing import Iterable, List, Tuple

from pyriksprot.corpus import iterate
from pyriksprot.interface import ContentType
from pyriksprot.utility import deprecated

from .parse import ProtocolMapper


def multiprocessing_xml_load(args) -> Iterable[iterate.ProtocolSegment]:
    """Load protocol from XML. Aggregate text to `segment_level`. Return (name, who, id, text)."""
    return iterate.to_segments(
        content_type=ContentType.Text,
        protocol=ProtocolMapper.parse(filename=args[0]),
        segment_level=args[2],
        merge_strategy=args[4],
        segment_skip_size=args[3],
        which_year=args[5],
    )


class XmlUntangleSegmentIterator(iterate.ProtocolSegmentIterator):
    """Iterate ParlaClarin XML files using `untangle` wrapper."""

    def load(self, filename: str) -> Iterable[iterate.ProtocolSegment]:
        """Load protocol from XML. Aggregate text to `segment_level`. Return sequence of segment.ProtocolSegment."""
        return iterate.to_segments(
            content_type=ContentType.Text,
            protocol=ProtocolMapper.parse(filename=filename),
            segment_level=self.segment_level,
            merge_strategy=self.merge_strategy,
            segment_skip_size=self.segment_skip_size,
            which_year=self.which_year,
        )

    def map_futures(self, imap, args: List[Tuple[str, str, int]]):
        return imap(multiprocessing_xml_load, args)


@deprecated
def multiprocessing_load(args):
    return iterate.to_segments(
        protocol=ProtocolMapper.parse(filename=args[0]),
        content_type=args[1],
        segment_level=args[2],
        segment_skip_size=args[3],
        merge_strategy=args[4],
        which_year=args[5],
    )


# @deprecated
class XmlProtocolSegmentIterator(iterate.ProtocolSegmentIterator):
    """Reads xml files using Protocol entity and returns a stream of `segment.ProtocolSegment`"""

    def load(self, filename: str) -> List[iterate.ProtocolSegment]:
        return iterate.to_segments(
            protocol=ProtocolMapper.parse(filename=filename),
            content_type=self.content_type,
            segment_level=self.segment_level,
            segment_skip_size=self.segment_skip_size,
            merge_strategy=self.merge_strategy,
            which_year=self.which_year,
        )

    def map_futures(self, imap, args):
        return imap(multiprocessing_load, args, chunksize=self.multiproc_chunksize)
