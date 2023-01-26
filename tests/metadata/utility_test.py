from pyriksprot.metadata import verify


def test_verify_metadata_filenames():
    verify.verify_metadata_files("./metadata/data/v0.5.0")


def test_verify_metadata_columns():
    verify.verify_metadata_columns("v0.5.0")
