# import sqlite3
# import xml.etree.ElementTree as ET
# from pathlib import Path

# import pandas as pd
# import pytest
# import tqdm

# from pyriksprot import to_speech
# from pyriksprot.configuration.inject import ConfigStore
# from pyriksprot.corpus.corpus_index import CorpusSourceIndex
# from pyriksprot.corpus.parlaclarin.parse import ProtocolMapper
# from pyriksprot.corpus.utility import load_chamber_indexes, ls_corpus_folder
# from pyriksprot.metadata.corpus_index_factory import CorpusIndexFactory, CorpusScanner
# from pyriksprot.metadata.schema import MetadataSchema


# # pylint: disable=import-outside-toplevel


# def xml_has_u_tag(filename: str) -> bool:
#     """
#     Return True if the XML file contains a <u> element, otherwise False.
#     """
#     tree = ET.parse(filename)
#     root = tree.getroot()

#     # Check for <u> with or without namespace
#     for elem in root.iter():
#         if elem.tag == "u" or elem.tag.endswith("}u"):
#             return True

#     return False


# def xml_has_u_tag_fast(filename: str) -> bool:
#     """
#     Return True if the XML file contains a <u> element.
#     Uses streaming parsing for performance.
#     """
#     for _, elem in ET.iterparse(filename, events=("start",)):
#         tag: str = elem.tag
#         if tag == "u" or tag.endswith("}u"):
#             return True
#         # Free memory as we go
#         elem.clear()
#     return False


# def extract_filenames(xml_filename: str) -> list[str]:
#     """
#     Parse a TEI corpus XML file and return a list of filenames
#     from xi:include/@href elements.
#     """
#     tree = ET.parse(xml_filename)
#     root = tree.getroot()

#     namespaces = {"xi": "http://www.w3.org/2001/XInclude"}

#     return [elem.attrib["href"] for elem in root.findall(".//xi:include", namespaces) if "href" in elem.attrib]


# def find_using_rglob(root: str | Path, pattern: str) -> list[Path]:
#     root = Path(root)
#     return [p for p in root.rglob(pattern) if p.is_file()]


# def list_using_ls_corpus_folder(folder: str, pattern: str) -> list[str]:
#     return [Path(item).stem for item in ls_corpus_folder(folder=folder, pattern=pattern)]


# def list_using_index_files(folder: str) -> list[str]:
#     return [
#         Path(f).stem
#         for index_file in ['prot-ek.xml', 'prot-fk.xml', 'prot-ak.xml']
#         for f in extract_filenames(Path(folder) / index_file)
#     ]


# def list_using_rglob(folder: str, pattern: str) -> list[str]:
#     return [Path(item).stem for item in find_using_rglob(folder, pattern=pattern)]


# def list_files_without_speeches_using_rglob(folder: str, pattern: str) -> list[str]:
#     return [
#         Path(item).stem
#         for item in tqdm.tqdm(find_using_rglob(folder, pattern=pattern), total=17938)
#         if not xml_has_u_tag_fast(item)
#     ]


# def get_db_protocols(metadata_filename) -> set[str]:
#     conn = sqlite3.connect(metadata_filename)
#     cursor = conn.cursor()
#     cursor.execute("select distinct protocol_name from protocols;")
#     protocols = {row[0] for row in cursor.fetchall()}
#     conn.close()
#     return protocols


# def get_speech_protocols(filename: str) -> set[str]:
#     speech_index: pd.DataFrame = pd.read_feather(filename)
#     return set(speech_index['protocol_name'].tolist())


# # ------- Test cases -------


# @pytest.mark.integration
# def test_xml_source_index():
#     source_folder: str = ConfigStore.config().get("corpus.folder")
#     items: CorpusSourceIndex = CorpusSourceIndex.load(
#         source_folder=source_folder,
#         source_pattern='**/prot-*-*.xml',
#         skip_empty=False,
#     )
#     assert items is not None


# def test_test_data_ls_source_files():

#     expected_documents: set[str] = set(
#         Path('tests/test_data/test_documents.txt').read_text(encoding="utf-8").splitlines()
#     )

#     source_folder: str = ConfigStore.config().get("corpus.folder")
#     items: list[str] = ls_corpus_folder(
#         folder=source_folder,
#         pattern='**/prot-*-*.xml',
#     )
#     assert items is not None
#     assert set(Path(item).stem for item in items) == expected_documents


# def test_global_ls_source_files():

#     swedeb_folder: str = "/data/swedeb/v1.4.1/riksdagen-records/"
#     swedeb_filenames: dict[str, list[str]] = {
#         "ls_corpus_folder": list_using_ls_corpus_folder(folder=swedeb_folder, pattern='**/prot-*-*.xml'),
#         "files_in_index": list_using_index_files(folder=swedeb_folder),
#         "list_using_rglob": list_using_rglob(folder=swedeb_folder, pattern='**/prot-*-*.xml'),
#     }

#     print("----------------- Swedeb ----------------")
#     print("")
#     print(f"               Source folder: {swedeb_folder}")
#     print(f"              Files in index: {len(swedeb_filenames['files_in_index'])}")
#     print(f"Files using ls_corpus_folder: {len(swedeb_filenames['ls_corpus_folder'])}")
#     print(f"      Files using Path.rglob: {len(swedeb_filenames['list_using_rglob'])}")
#     print("")

#     westac_folder = "/data/riksdagen_corpus_data/riksdagen-records/data/"
#     westac_filenames: dict[str, list[str]] = {
#         "ls_corpus_folder": list_using_ls_corpus_folder(folder=westac_folder, pattern='**/prot-*-*.xml'),
#         "files_in_index": list_using_index_files(folder=westac_folder),
#         "list_using_rglob": list_using_rglob(folder=westac_folder, pattern='**/prot-*-*.xml'),
#     }
#     print("----------------- Westac ----------------")
#     print("")
#     print(f"               Source folder: {westac_folder}")
#     print(f"              Files in index: {len(westac_filenames['files_in_index'])}")
#     print(f"Files using ls_corpus_folder: {len(westac_filenames['ls_corpus_folder'])}")
#     print(f"      Files using Path.rglob: {len(westac_filenames['list_using_rglob'])}")
#     print("")

#     vrt_folder: str = "/data/swedeb/v1.4.1/tagged_frames/"
#     vrt_filenames = list_using_rglob(vrt_folder, pattern='**/*.zip')

#     print("------------------- VRT -----------------")
#     print("")
#     print(f"               VRT folder: {vrt_folder}")
#     print(f"            VRT zip files: {len(vrt_filenames)}")

#     # Manually verified counts:
#     # ┌ [12:25:35] [roger@berra] [~/.../pyriksprot/tmp] [missing-files-bug *]
#     # └─> [.venv] λ find /data/swedeb/v1.4.1/riksdagen-records -name "prot*.xml" | wc
#     # 17941   17941 1185051
#     # 17941 includes three indx files, so 17938 unique protocol files.

#     # find files in metadata
#     print("---------------- METADATA----------------")

#     metadata_filename: str = "/data/swedeb/metadata/riksprot_metadata.v1.1.3.db"
#     unique_protocols_in_metadata: set[str] = get_db_protocols(metadata_filename)

#     speech_index_filename: str = "/data/swedeb/v1.4.1/speech-index.feather"
#     unique_protocols_in_speech_index: set[str] = get_speech_protocols(speech_index_filename)

#     print("")
#     print(f"      Unique protocols in metadata: {len(unique_protocols_in_metadata)}")
#     print(f"  Unique protocols in speech index: {len(unique_protocols_in_speech_index)}")
#     print("")

#     empty_files_on_disk: list[str] = list_files_without_speeches_using_rglob(
#         "/data/swedeb/v1.4.1/riksdagen-records/", pattern='**/prot-*-*.xml'
#     )
#     print(f"           Empty protocols on disk: {len(empty_files_on_disk)}")

#     missing_files = list(
#         set(swedeb_filenames['ls_corpus_folder']) - set(empty_files_on_disk) - unique_protocols_in_speech_index
#     )
#     pd.DataFrame({'filename': missing_files}).to_csv('missing_files.csv')


# def test_generate_corpus_index():

#     corpus_folder: str = ConfigStore.config().get("global.corpus.folder")

#     parser = ProtocolMapper

#     schema = MetadataSchema("v1.1.3")
#     index_service: CorpusIndexFactory = CorpusIndexFactory(parser, schema=schema)
#     index_service.generate(corpus_folder=corpus_folder, target_folder="tmp/scan_result")


# def test_scan():

#     corpus_folder: str = ConfigStore.config().get("global.corpus.folder")
#     parser = ProtocolMapper
#     filenames: list[str] = ls_corpus_folder(corpus_folder)
#     chambers: dict[str, set[str]] = load_chamber_indexes(corpus_folder)
#     service: CorpusScanner = CorpusScanner(parser)
#     scan_result: CorpusScanner.ScanResult = service.scan(filenames, chambers)
#     # data = service.to_dataframes(scan_result)

#     assert scan_result is not None

#     data: dict[str, pd.DataFrame] = service.to_dataframes(scan_result)


# def test_generate_speech_index():
#     vrt_folder: str = "/data/swedeb/v1.4.1/tagged_frames/"
#     source_pattern: str = '**/prot-*.zip'

#     source_index: CorpusSourceIndex = CorpusSourceIndex.load(
#         source_folder=vrt_folder, source_pattern=source_pattern, years=None
#     )

#     assert source_index is not None

#     print("---------------- CorpusSourceIndex (VRT) ----------------")
#     print(f" Source folder: {vrt_folder}")
#     print(f"Source pattern: {source_pattern}")
#     print(f"   # source files: {len(source_index.filenames)}")
#     print("")

#     from pyriksprot import interface
#     from pyriksprot import metadata as md
#     from pyriksprot.corpus import iterate, tagged

#     # source_folder: str
#     metadata_filename: str = "/data/swedeb/metadata/riksprot_metadata.v1.1.3.db"

#     # target_name: str = 'speech_index.csv.gz'
#     segment_level: interface.SegmentLevel = interface.SegmentLevel.Speech
#     segment_skip_size: int = 1
#     # years: str | None = None
#     multiproc_keep_order: str | None = None
#     multiproc_processes: int = 1
#     multiproc_chunksize: int = 100
#     merge_strategy: to_speech.MergeStrategyType = 'chain'
#     source_pattern: str = '**/prot-*.zip'

#     speaker_service: md.SpeakerInfoService = md.SpeakerInfoService(database_filename=metadata_filename)

#     def get_speaker(item: iterate.ProtocolSegment) -> None:
#         item.speaker_info = speaker_service.get_speaker_info(u_id=item.u_id, person_id=item.who, year=item.year)

#     preprocess = get_speaker if segment_level not in ('protocol', None) else None
#     # Test one of the missing files

#     filenames = ["/data/swedeb/v1.4.1/tagged_frames/1966/prot-1966--ak--028.zip"]

#     speeches_iterator: iterate.ProtocolSegmentIterator = tagged.ProtocolIterator(
#         filenames=filenames,  #  source_index.paths,
#         content_type=interface.ContentType.TaggedFrame,
#         segment_level=segment_level,
#         segment_skip_size=segment_skip_size,
#         multiproc_keep_order=multiproc_keep_order,
#         multiproc_processes=multiproc_processes,
#         multiproc_chunksize=multiproc_chunksize,
#         merge_strategy=merge_strategy,
#         preprocess=preprocess,
#     )
#     speeches = [ x for x in speeches_iterator]

#     # Bug revealed that this file was missing speeches
#     assert len(speeches) > 0

#     # Cause of bug; the file stored in the VRT zip has a name that does not match the expected pattern
#     # 'prot-1966--ak--028.xml' vs 'prot-1966-höst-ak--028.json'
#     # The loading function should be robust to this situation, or at least log a warning or raise an exception.
#     # The reason for the mismatch is that the filename used  in the VRT Zip originate from the source XML file,
#     # which in this case was named differently than the expected pattern
#     # which in this case differs from the (basename) of the XML file used to create the VRT.
#     #
