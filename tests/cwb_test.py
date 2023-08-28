from ccc import Corpus


def test_to_cwb():
    pass


def test_ccc():
    # corpora: Corpora = Corpora(registry_path="/usr/local/share/cwb/registry/")
    corpus = Corpus(corpus_name="RIKSPROT_V060_TEST", registry_path="/usr/local/share/cwb/registry/")
    dump = corpus.query(
        '[lemma="Sverige"]',
        context_left=5,
        context_right=5,
    )
    df = dump.concordance()
    assert df is not None
