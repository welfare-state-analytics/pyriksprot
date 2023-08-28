# from pyriksprot import interface
# from pyriksprot.dispatch.dispatch import CompressType
# from pyriksprot.workflows import extract_tags

# def test_bug():
#     arguments: dict = {
#         'source_folder': 'data/swedeb-data/decade-1960/v0.9.0/tagged_frames',
#         'metadata_filename': 'data/swedeb-data/decade-1960/v0.9.0/riksprot_metadata.db',
#         'target_name': 'tests/output',
#         'target_type': 'single-id-tagged-frame-per-group',
#         'content_type': 'tagged_frame',
#         'merge_strategy': 'chain',
#         'compress_type': CompressType.Feather,
#         'multiproc_processes': None,
#         'skip_lemma': False,
#         'skip_text': False,
#         'skip_puncts': False,
#         'skip_stopwords': False,
#         'lowercase': True,
#         'force': True,
#     }
#     extract_tags.extract_corpus_tags(
#         **{
#             **arguments,
#             **dict(
#                 segment_level=interface.SegmentLevel.Speech,
#                 segment_skip_size=1,
#                 years=None,
#                 temporal_key=None,
#                 group_keys=None,
#                 multiproc_keep_order=False,
#                 multiproc_chunksize=10,
#             ),
#         },
#         # source_pattern='**/prot-1968--fk--28.zip',
#         source_pattern='**/prot-196*.zip',
#     )
#     # riksprot2speech --options-filename opts/tagged-speeches/tagged_frames_speeches_text.feather.yml --force v0.9.0/tagged_frames v0.9.0/riksprot_metadata.db v0.9.0/speeches/tagged_frames_speeches_text.feather
