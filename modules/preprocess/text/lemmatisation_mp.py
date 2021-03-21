import nltk


def get_wordnet_pos(word):
    """Map POS tag to first character lemmatizer.lemmatize() accepts"""
    tag = nltk.pos_tag([word])[0][1][0].upper()
    tag_dict = {
        "J": nltk.corpus.wordnet.ADJ,
        "N": nltk.corpus.wordnet.NOUN,
        "V": nltk.corpus.wordnet.VERB,
        "R": nltk.corpus.wordnet.ADV
    }
    return tag_dict.get(tag, nltk.corpus.wordnet.NOUN)


def run_lemmatize(string):
    """Lemmatize a sentence with the appropriate POS tag.
        'J' = ADJ, 'N' = NOUN, 'V' = VERB, 'R' = ADV"""
    lemmatizer = nltk.stem.WordNetLemmatizer()
    return " ".join(
        [lemmatizer.lemmatize(word, get_wordnet_pos(word))
         for word in nltk.word_tokenize(string)]
    )


def process(*args):
    data = args[0]
    tmp = data[data.notnull()]
    tmp = tmp.apply(run_lemmatize)
    data.loc[tmp.index] = tmp
    return data
