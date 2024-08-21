from pyriksprot.metadata import verify

from ..utility import RIKSPROT_REPOSITORY_TAG

SAMPLE_METADATA = f"./tests/test_data/source/{RIKSPROT_REPOSITORY_TAG}/parlaclarin/metadata"


def test_config_conforms_to_folder_pecification():
    verify.ConfigConformsToFolderSpecification(tag=RIKSPROT_REPOSITORY_TAG, folder=SAMPLE_METADATA).is_satisfied()


def test_config_conforms_to_tags_pecification():
    verify.ConfigConformsToTagSpecification(tag=RIKSPROT_REPOSITORY_TAG).is_satisfied()


def test_tags_conform_specification():
    verify.TagsConformSpecification(tag1="v0.5.0", tag2=RIKSPROT_REPOSITORY_TAG).is_satisfied()


def collapse_consecutive_integers(numbers: list[int]) -> list[tuple[int, int] | int]:
    result = []
    current_sequence = []

    for num in numbers:
        if not current_sequence or num == current_sequence[-1] + 1:
            current_sequence.append(num)
        else:
            if len(current_sequence) > 1:
                result.append((current_sequence[0], current_sequence[-1]))
            else:
                result.extend(current_sequence)
            current_sequence = [num]

    if len(current_sequence) > 1:
        result.append((current_sequence[0], current_sequence[-1]))
    else:
        result.extend(current_sequence)

    return result


# def test_overlap():
#     import pandas as pd

#     overlaps = pd.read_csv('party-overlap.csv', sep=';')
#     overlap_person = overlaps.groupby(['name', 'id']).agg(list)
#     overlap_person['overlap'] = overlap_person.year.apply(collapse_consecutive_integers).apply(
#         lambda p: ', '.join(map(lambda x: f'{x[0]}-{x[-1]}' if isinstance(x, tuple) else str(x), p))
#     )

#     pass
