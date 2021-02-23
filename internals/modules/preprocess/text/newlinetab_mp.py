from multiprocessing import current_process


def process(*args):
    if current_process().name == 'MainProcess':
        data = args[0].str.replace(r'\n|\t|\r', r' ')
    else:
        data = args[0].str.replace(r'\n|\t|\r', r' ')
    # remove extra whitespace
    data = data.str.replace(r'\s{2,}', r' ')
    return data
