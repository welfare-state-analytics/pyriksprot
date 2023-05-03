from pyriksprot.foss.sparv_tokenize import BetterSentenceTokenizer, BetterWordTokenizer


def test_better_tokenizer():
    tokenizer: BetterWordTokenizer = BetterWordTokenizer.create()

    data = tokenizer.tokenize("Hej! Vad heter du?")

    assert list(data) == ["Hej", "!", "Vad", "heter", "du", "?"]

    assert not list(tokenizer.tokenize(""))


def test_better_sentencer():
    tokenizer: BetterSentenceTokenizer = BetterSentenceTokenizer.create()

    data = tokenizer.span_tokenize("Hej! Vad heter du?")
    assert list(data) == [(0, 4), (5, 18)]

    data = tokenizer.tokenize("Hej! Vad heter du?")
    assert list(data) == ["Hej!", "Vad heter du?"]

    assert not list(tokenizer.tokenize(""))
