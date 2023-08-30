from ccc import Corpus
import pandas as pd


def test_to_cwb():
    pass


def test_ccc():
    # corpora: Corpora = Corpora(registry_path="/usr/local/share/cwb/registry/")
    corpus: Corpus = Corpus(corpus_name="RIKSPROT_V090_TEST", registry_path="/usr/local/share/cwb/registry/")


    subcorpus = corpus.query(
        '[lemma="information"]',
        context_left=5,
        context_right=5,
    )

    data: pd.DataFrame = subcorpus.concordance()
    assert data is not None

    data = subcorpus.collocates()
    assert data is not None
