from pyriksprot.configuration import ConfigValue
from pyriksprot.metadata import verify


def test_config_conforms_to_folder_specification():
    tag: str = ConfigValue("version").resolve()
    folder: str = ConfigValue("metadata:folder").resolve()
    verify.ConfigConformsToFolderSpecification(tag=tag, folder=folder).is_satisfied()


def test_config_conforms_to_tags_specification():
    tag: str = ConfigValue("version").resolve()
    github: str = ConfigValue("metadata.github").resolve()
    verify.ConfigConformsToTagSpecification(
        user=github.get("user"), repository=github.get("repository"), path=github.get("path"), tag=tag
    ).is_satisfied()


def test_tags_conform_specification():
    github: str = ConfigValue("metadata.github").resolve()
    tag: str = ConfigValue("version").resolve()
    verify.TagsConformSpecification(
        user=github.get("user"), repository=github.get("repository"), path=github.get("path"), tag1="v0.5.0", tag2=tag
    ).is_satisfied()


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
