import warnings

import click
from loguru import logger
from pyriksprot import configuration
from pyriksprot.gitchen import VersionSpecification
from pyriksprot.workflows.tag import ITagger, TaggerProvider, tag_protocols
# from pyriksprot_tagger.utility import VersionSpecification, check_cuda


@click.command()
@click.argument('config_filename')
@click.argument('source_folder')
@click.argument('target_folder')
@click.option('--force', is_flag=True, default=False, help='Force if exists')
@click.option('--skip-version-check', is_flag=True, default=False, help='Skip version check')
@click.option('--recursive', is_flag=True, default=True, help='Recurse subfolders')
@click.option('--pattern', type=str, default="**/prot-*-*.xml", help='Recurse subfolders')
def main(
    config_filename: str,
    source_folder: str,
    target_folder: str,
    force: bool = False,
    skip_version_check: bool = False,
    recursive: bool = True,
    pattern: str = "**/prot-*-*.xml",
) -> None:
    tagit(
        config_filename=config_filename,
        source_folder=source_folder,
        target_folder=target_folder,
        force=force,
        check_version=not skip_version_check,
        recursive=recursive,
        pattern=pattern,
    )


def tagit(
    config_filename: str,
    source_folder: str,
    target_folder: str,
    force: bool = False,
    recursive: bool = True,
    pattern: str | None = None,
    check_version: bool = True,
):
    # check_cuda()

    configuration.configure_context(
        source=config_filename,
        context="default",
        env_prefix="PYRIKSPROT",
    )
    if (
        check_version
        and VersionSpecification.is_satisfied(source_folder, configuration.ConfigValue("corpus.version").value) is False
    ):  # None is ok!
        raise ValueError(
            f"Version {configuration.ConfigValue('corpus.version').value} differs from current tag {source_folder}"
        )

    tagger: ITagger = TaggerProvider.tagger_factory().create()

    tag_protocols(
        tagger=tagger,
        source_folder=source_folder,
        target_folder=target_folder,
        force=force,
        recursive=recursive,
        pattern=pattern,
    )

    logger.info("workflow ended")


if __name__ == "__main__":

    with warnings.catch_warnings():
        warnings.simplefilter('ignore', FutureWarning)

    main()  # type: ignore # pylint: disable=no-value-for-parameter
