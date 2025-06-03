"""
Prints tag & commit info (SHA & tag) for a Git repository.

"""

import click

from pyriksprot.configuration import ConfigStore, ConfigValue
from pyriksprot.gitchen import GitInfo
from pyriksprot.utility import write_yaml

# pylint: disable=too-many-arguments, unused-argument


@click.command()
@click.argument('config_file', type=str)
@click.argument('folder', type=str)
@click.option('--tag', type=str, help='Print info for specified tag (default HEAD).', default=None)
@click.option('--organisation', type=str, help='Github organisation.', default=None)
@click.option('--repository', type=str, help='Github repository.', default=None)
@click.option(
    '--key',
    type=click.Choice(["tag", "ref", "sha", "sha8", "tag_url", "commit_url"], case_sensitive=False),
    help='Prints value for given key',
    default=None,
)
def main(
    config_file: str,
    folder: str = None,
    tag: str = None,
    organisation: str = None,
    repository: str = None,
    key: str = None,
):
    ConfigStore.configure_context(source=config_file)

    tag = tag or ConfigValue("corpus.version").value
    organisation = organisation or ConfigValue("corpus.github.user").value
    repository = repository or ConfigValue("corpus.github.repository").value

    data: dict = GitInfo(organisation=organisation, repository=repository, source_folder=folder).tag_info(
        source='workdir', tag=tag
    )
    if key:
        print(data.get(key, ""))
    else:
        write_yaml(data=data, file="-")


if __name__ == "__main__":
    main()  # type: ignore # pylint: disable=no-value-for-parameter
