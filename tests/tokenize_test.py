from pyriksprot.foss import sparv_tokenize as st


def test_better_sentence_tokenizer():
    doc_str: str = "Jag heter Ove. Vad heter du?"
    tokenizer: st.BetterSentenceTokenizer = st.BetterSentenceTokenizer.create()
    assert tokenizer is not None

    sentences: list[str] = list(tokenizer.tokenize(doc_str))
    assert len(sentences) == 2

    sentences: list[str] = list(tokenizer.tokenize("Jag heter Ove"))
    assert len(sentences) == 1

    sentences: list[str] = list(tokenizer.tokenize("Jag"))
    assert len(sentences) == 1

    sentences: list[str] = list(tokenizer.tokenize("1 Vad heter du?"))
    assert len(sentences) == 1

    sentences: list[str] = list(tokenizer.tokenize(""))
    assert len(sentences) == 0

    spans: list[tuple[int, int]] = list(tokenizer.span_tokenize(doc_str))
    assert len(spans) == 2
    assert spans == [(0, 14), (15, 28)]

    spans: list[tuple[int, int]] = list(tokenizer.span_tokenize(""))
    assert len(spans) == 0

    spans: list[tuple[int, int]] = list(tokenizer.tokenize2(doc_str))
    assert len(spans) == 2

    assert spans == [("Jag heter Ove.", 0, 14), ("Vad heter du?", 15, 28)]


def test_better_word_tokenizer():
    doc_str: str = "Jag heter Ove. Vad heter du?"
    tokenizer: st.BetterWordTokenizer = st.BetterWordTokenizer.create()
    assert tokenizer is not None

    tokens: list[str] = list(tokenizer.tokenize(doc_str))

    assert tokens == ["Jag", "heter", "Ove", ".", "Vad", "heter", "du", "?"]

    tokens: list[str] = list(tokenizer.tokenize(""))
    assert not tokens

    token_spans: list[tuple[int, int]] = list(tokenizer.span_tokenize(doc_str))
    tokens = [doc_str[x:y] for x, y in token_spans]
    assert tokens == ["Jag", "heter", "Ove", ".", "Vad", "heter", "du", "?"]

    token_spans: list[tuple[str, int, int]] = list(tokenizer.tokenize2(doc_str))

    tokens = [doc_str[x:y] for _, x, y in token_spans]
    assert tokens == ["Jag", "heter", "Ove", ".", "Vad", "heter", "du", "?"]

    tokens = [w for w, _, _ in token_spans]
    assert tokens == ["Jag", "heter", "Ove", ".", "Vad", "heter", "du", "?"]


def test_better_combined_tokenizer():
    doc_str: str = "Jag heter Ove. Vad heter du?"
    sentences_str: list[str] = ["Jag heter Ove.", "Vad heter du?"]

    tokenizer: st.BetterWordSentenceTokenizer = st.BetterWordSentenceTokenizer.create(sentenize=False)
    assert tokenizer is not None

    tokens: list[str] = list(tokenizer.tokenize(doc_str))

    assert tokens == ["Jag", "heter", "Ove", ".", "Vad", "heter", "du", "?"]

    token_spans: list[tuple[int, int]] = list(tokenizer.span_tokenize(doc_str))
    tokens = [doc_str[x:y] for x, y in token_spans]
    assert tokens == ["Jag", "heter", "Ove", ".", "Vad", "heter", "du", "?"]

    token_spans: list[tuple[int, int]] = list(tokenizer.tokenize2(doc_str))
    tokens = [doc_str[x:y] for _, x, y in token_spans]
    assert tokens == ["Jag", "heter", "Ove", ".", "Vad", "heter", "du", "?"]

    tokens = [w for w, _, _ in token_spans]
    assert tokens == ["Jag", "heter", "Ove", ".", "Vad", "heter", "du", "?"]

    tokenizer: st.BetterWordSentenceTokenizer = st.BetterWordSentenceTokenizer.create(sentenize=True)
    tokens: list[list[str]] = [list(s) for s in tokenizer.tokenize(doc_str)]

    assert tokens == [["Jag", "heter", "Ove", "."], ["Vad", "heter", "du", "?"]]

    token_spans: list[list[tuple[int, int]]] = [list(s) for s in tokenizer.span_tokenize(doc_str)]
    tokens = [[sentences_str[i][x:y] for x, y in s] for i, s in enumerate(token_spans)]
    assert tokens == [["Jag", "heter", "Ove", "."], ["Vad", "heter", "du", "?"]]

    token_spans: list[list[tuple[int, int]]] = [list(s) for s in tokenizer.tokenize2(doc_str)]
    tokens = [[sentences_str[i][x:y] for _, x, y in s] for i, s in enumerate(token_spans)]
    assert tokens == [["Jag", "heter", "Ove", "."], ["Vad", "heter", "du", "?"]]

    tokens = [[w for w, _, _ in s] for s in token_spans]
    assert tokens == [["Jag", "heter", "Ove", "."], ["Vad", "heter", "du", "?"]]
