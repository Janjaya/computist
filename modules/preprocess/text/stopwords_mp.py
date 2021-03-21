def run_stopwords(string="", stopwords=[]):
    return " ".join(
        [word for word in string.split(" ") if word not in stopwords]
    )


def process(*args):
    data = args[0]
    stopwords = args[1]
    return data.apply(run_stopwords, args=(stopwords,))
