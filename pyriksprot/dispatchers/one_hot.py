from __future__ import annotations

from typing import List

import pandas as pd

from ..dispatch import DispatchItem, IDispatcher

# pylint: disable=useless-super-delegation


class OneHotDispatcher(IDispatcher):

    name: str = 'one-hot-sparse'

    def __init__(self, *, target_name: str, **kwargs):
        super().__init__(target_name=target_name, compress_type=None, **kwargs)

    def dispatch(self, dispatch_items: List[DispatchItem]) -> None:
        return super().dispatch(dispatch_items)

    def dispatch_index(self) -> None:
        return super().dispatch_index()

    def store(self, filename: str, data: str | pd.DataFrame) -> None:
        return super().store(filename, data)
