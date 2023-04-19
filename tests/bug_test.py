# from pyriksprot import interface
# from pyriksprot.workflows import extract_tags


# def test_bug():
#     arguments: dict = {
#         'source_folder': 'data/swedeb-data/dataset-01/v0.6.0/tagged_frames',
#         'metadata_filename': 'data/swedeb-data/dataset-01/v0.6.0/riksprot_metadata.db',
#         'target_name': 'data/swedeb-data/dataset-01/v0.6.0/speeches/tagged_frames_speeches_lemma.feather',
#         'target_type': 'single-id-tagged-frame-per-group',
#         'content_type': 'tagged_frame',
#         'merge_strategy': 'chain',
#         'compress_type': 'lzma',
#         'multiproc_processes': None,
#         'skip_lemma': False,
#         'skip_text': False,
#         'skip_puncts': False,
#         'skip_stopwords': False,
#         'lowercase': True,
#         'force': True,
#     }
#     # 'options_filename': 'data/swedeb-data/dataset-01/opts/tagged-speeches/tagged_frames_speeches_lemma.feather.yml'
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
#         }
#     )
