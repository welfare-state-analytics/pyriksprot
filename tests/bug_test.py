from pyriksprot.configuration.inject import ConfigStore
from pyriksprot.corpus.corpus_index import CorpusSourceIndex


def test_xml_source_index() -> CorpusSourceIndex:
    source_folder: str = ConfigStore.config().get("corpus.folder")
    items: CorpusSourceIndex =  CorpusSourceIndex.load(
        source_folder=source_folder,
        source_pattern='**/prot-*-*.xml',
        skip_empty=False,
    )
    return items
