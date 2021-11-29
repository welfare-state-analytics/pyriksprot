import uuid
from typing import Mapping

from pyriksprot import dispatch, merge


def test_folder_dispatch():

    dispatcher: dispatch.FolderDispatcher = dispatch.FolderDispatcher(
        target_name=f'./tests/output/{uuid.uuid1()}',
        target_type=dispatch.TargetType.Zip,
    )
    group: Mapping[str, merge.MergedSegmentGroup]
    dispatcher.dispatch(group)

    raise NotImplementedError()
