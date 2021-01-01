"""Testing for tidying and preprocessing text data."""

from interlocutor.nlp import preprocessing


def test_preprocess_texts():
    """
    Texts are transformed appropriately i.e. stop words and punctuation are removed, then words are lemmatised and
    lowercased.
    """

    input_texts = [
        'This is a text which contains some stop words',
        'This text Contains some UPPERCASING ',
        'This text contains some high-profile punctuation! ...',
        'This text contains multiple spaces    in      it which need removing',
        'This text contains some words that require lemmatisation as we are playing with the processor class',
        'This text contains some contractions shan\'t it. Alicia\'s thinking so too',
    ]

    expected_output = [
        'text contain stop word',
        'text contains uppercasing',
        'text contain high profile punctuation',
        'text contain multiple space need remove',
        'text contain word require lemmatisation play processor class',
        'text contain contraction shall alicia think',
    ]

    preprocessor = preprocessing.BagOfWordsPreprocessor()

    assert preprocessor.preprocess_texts(input_texts) == expected_output
