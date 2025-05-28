import shutil
import zipfile
from os import listdir, rename
from os.path import basename, isdir, join, splitext

from loguru import logger


def rename_tagged_filename(zip_path: str):

    protocol_name: str = splitext(basename(zip_path))[0]

    try:
        with zipfile.ZipFile(zip_path, 'r') as fp:
            faulty_name: str = [name for name in fp.namelist() if name.startswith("prot")][0]
    except Exception:  # noqa
        logger.warning(f"skipping {protocol_name}, is it empty?")
        return

    if faulty_name == f"{protocol_name}.json":
        logger.info(f"File '{faulty_name}' already has the correct name.")
        return

    temp_dir: str = f"/tmp/{protocol_name}"

    try:

        with zipfile.ZipFile(zip_path, 'r') as fp:
            fp.extractall(temp_dir)

        rename(join(temp_dir, faulty_name), join(temp_dir, f"{protocol_name}.json"))

        with zipfile.ZipFile(zip_path, 'w', compression=zipfile.ZIP_DEFLATED) as fp:
            for file in listdir(temp_dir):
                fp.write(join(temp_dir, file), file)

        logger.info(f"Renamed '{faulty_name}' to '{protocol_name}.json' in the zip file.")

    finally:
        shutil.rmtree(temp_dir)


def main():
    folder: str = "/data/riksdagen_corpus_data/v1.4.1/tagged_frames"
    for subfolder in listdir(folder):
        tagged_folder: str = join(folder, subfolder)
        if not isdir(tagged_folder):
            continue
        if not subfolder.isdigit():
            continue
        for file in listdir(tagged_folder):
            if file.endswith(".zip"):
                rename_tagged_filename(join(tagged_folder, file))
                # print(f"rename_tagged_filename({join(tagged_folder, file)})")


if __name__ == "__main__":
    main()
