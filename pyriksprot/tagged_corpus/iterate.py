from __future__ import annotations

from typing import Iterable, List, Tuple

from ..interface import ProtocolSegment, ProtocolSegmentIterator
from . import persist


def multiprocessing_load(args) -> Iterable[ProtocolSegment]:
    return persist.load_protocol(filename=args[0]).to_segments(
        content_type=args[1], segment_level=args[2], segment_skip_size=args[3]
    )


class ProtocolIterator(ProtocolSegmentIterator):
    """Reads xml files using Protocol entity and returns a stream of `ProtocolSegment`"""

    def load(self, filename: str) -> List[Tuple[str, str, int]]:
        return persist.load_protocol(filename=filename).to_segments(
            content_type=self.content_type,
            segment_level=self.segment_level,
            segment_skip_size=self.segment_skip_size,
        )

    def map_futures(self, imap, args):
        return imap(multiprocessing_load, args, chunksize=self.multiproc_chunksize)
