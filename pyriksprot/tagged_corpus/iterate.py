from __future__ import annotations

from typing import Iterable, List, Tuple

import pandas as pd

from ..interface import Protocol, ProtocolSegment, ProtocolSegmentIterator
from . import persist


def multiprocessing_load(args) -> Iterable[ProtocolSegment]:

    protocol: Protocol = persist.load_protocol(filename=args[0])
    return (
        []
        if protocol is None
        else protocol.to_segments(content_type=args[1], segment_level=args[2], segment_skip_size=args[3])
    )


class ProtocolIterator(ProtocolSegmentIterator):
    """Reads xml files using Protocol entity and returns a stream of `ProtocolSegment`"""

    def load(self, filename: str) -> List[Tuple[str, str, int]]:

        protocol: Protocol = persist.load_protocol(filename=filename)

        return (
            []
            if protocol is None
            else protocol.to_segments(
                content_type=self.content_type,
                segment_level=self.segment_level,
                segment_skip_size=self.segment_skip_size,
            )
        )

    def map_futures(self, imap, args):
        return imap(multiprocessing_load, args, chunksize=self.multiproc_chunksize)

    def to_segment_index(self) -> pd.DataFrame:
        """Iterate and return a meta data index over segments."""
        segment_index: pd.DataFrame = pd.DataFrame([x.to_dict() for x in self])
        return segment_index
