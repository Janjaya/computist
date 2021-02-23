from multiprocessing import current_process


def process(*args):
    # remove BBcode tags
    if current_process().name == 'MainProcess':
        data = args[0]
        tags = args[1] + args[2]
    else:
        data = args[2]
        tags = args[0] + args[1]
    data = data.str.replace(r'|'.join(tags), r' ')
    # remove extra whitespace
    data = data.str.replace(r'\s{2,}', r' ')
    return data
