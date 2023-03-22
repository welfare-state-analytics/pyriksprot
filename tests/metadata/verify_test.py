from pyriksprot.metadata import verify
from ..utility import RIKSPROT_REPOSITORY_TAG

SAMPLE_METADATA = f"./tests/test_data/source/{RIKSPROT_REPOSITORY_TAG}/parlaclarin/metadata"

def test_config_conforms_to_folder_pecification():
    verify.ConfigConformsToFolderSpecification(folder=SAMPLE_METADATA).is_satisfied()


def test_config_conforms_to_tags_pecification():
    verify.ConfigConformsToTagSpecification(tag=RIKSPROT_REPOSITORY_TAG).is_satisfied()


def test_tags_conform_specification():
    verify.TagsConformSpecification(tag1="v0.5.0",tag2=RIKSPROT_REPOSITORY_TAG).is_satisfied()
