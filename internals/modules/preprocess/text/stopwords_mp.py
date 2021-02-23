from multiprocessing import current_process


def run_stopwords(string='', stopwords=[]):
    return ' '.join([word for word in string.split(' ') if word not in stopwords])


def process(*args):
    if current_process().name == 'MainProcess':
        return args[0].apply(run_stopwords, args=(args[1],))
    else:
        return args[1].apply(run_stopwords, args=(args[0],))
