# import click

# from pyriksprot.purgatory import member


# @click.command()
# @click.argument('source-folder', type=click.STRING)
# @click.argument('target-name', type=click.STRING)
# def main(source_folder: str, target_name: str) -> None:
#     """Compile and store index of parliamentary persons found in source folder.

#     Args:
#         source_folder (str): Source folder where riksprot metadata is stored
#         target_name (str): Target CSV filename
#     """
#     member_index: member.ParliamentaryMemberIndex = member.ParliamentaryMemberIndex(source=source_folder, tag=None)

#     member_index.store(target_name)


# if __name__ == "__main__":

#     main()  # pylint: disable=no-value-for-parameter