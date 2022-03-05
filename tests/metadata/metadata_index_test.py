from pyriksprot import metadata as md


def test_load_code_maps():
    database_filename: str = "./metadata/riksprot_metadata.v0.4.0.db"
    code_maps: md.MetaDataCodeMaps = md.MetaDataCodeMaps.load(database_filename, persons=None)

    assert code_maps is not None


def test_load():

    database_filename: str = "./metadata/riksprot_metadata.v0.4.0.db"
    metadata_index: md.PersonIndex = md.PersonIndex.load(database_filename)

    assert metadata_index is not None
