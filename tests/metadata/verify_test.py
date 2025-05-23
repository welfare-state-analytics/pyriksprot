from pyriksprot.configuration import ConfigValue
from pyriksprot.metadata import verify


def test_config_conforms_to_folder_specification():
    tag: str = ConfigValue("metadata.version").resolve()
    folder: str = ConfigValue("metadata:folder").resolve()
    verify.ConfigConformsToFolderSpecification(tag=tag, folder=folder).is_satisfied()


def test_config_conforms_to_tags_specification():
    tag: str = ConfigValue("metadata.version").resolve()
    user: str = ConfigValue("metadata.github.user").resolve()
    repository: str = ConfigValue("metadata.github.repository").resolve()
    path: str = ConfigValue("metadata.github.path").resolve()
    verify.ConfigConformsToTagSpecification(user=user, repository=repository, path=path, tag=tag).is_satisfied()


def test_tags_conform_specification():
    tag: str = ConfigValue("metadata.version").resolve()
    user: str = ConfigValue("metadata.github.user").resolve()
    repository: str = ConfigValue("metadata.github.repository").resolve()
    path: str = ConfigValue("metadata.github.path").resolve()

    verify.TagsConformSpecification(user=user, repository=repository, path=path, tag1=tag, tag2=tag).is_satisfied()


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
