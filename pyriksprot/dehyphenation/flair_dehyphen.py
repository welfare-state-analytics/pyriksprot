# import re
# from typing import List

# import flair
# from dehyphen import FlairScorer, text_to_format

# ExplodedText = List[List[List[str]]]


# class FlairDehyphenService:
#     """Wrapper class for pd3.dehyphen that uses Flair embeddings"""

#     def __init__(self, lang='sv', cache_root='/data/flair-embedding-models'):

#         if cache_root:
#             flair.cache_root = cache_root

#         self.scorer = FlairScorer(lang=lang)

#     def dehyphen_text(self, text: str, merge_paragraphs=True) -> str:

#         paragraphs = self.explode(text)

#         if len(paragraphs) == 0:
#             return text

#         paragraphs = self.scorer.dehyphen(paragraphs)

#         if merge_paragraphs:
#             paragraphs = self.merge(paragraphs)

#         dehyphened_text = self.join(paragraphs)
#         return dehyphened_text

#     def join(self, dehyphened_words: ExplodedText) -> str:
#         return '\n\n'.join(['\n'.join([' '.join(s).strip() for s in p]) for p in dehyphened_words])

#     def explode(self, text: str) -> ExplodedText:
#         text = re.sub(r'(\s*\n){3,}', '\n\n', text.strip())
#         data = text_to_format(text)
#         return data

#     def merge(self, paragraphs: ExplodedText) -> ExplodedText:

#         if len(paragraphs) < 2:
#             return paragraphs

#         merged_paragraphs = [paragraphs[0]]

#         for i in range(1, len(paragraphs)):
#             merger = self.scorer.is_split_paragraph(merged_paragraphs[-1], paragraphs[i])
#             if merger:
#                 merged_paragraphs[-1] = merger
#             else:
#                 merged_paragraphs.append(paragraphs[i])

#         return merged_paragraphs
