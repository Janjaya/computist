from multiprocessing import current_process


def process(*args):
    if current_process().name == 'MainProcess':
        return args[0].str.lower()
    else:
        return args[0].str.lower()
