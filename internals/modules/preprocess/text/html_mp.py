from multiprocessing import current_process


def process(*args):
    if current_process().name == "MainProcess":
        data = args[0]
    else:
        data = args[0]
    # remove HTML tags (incl. its contents)
    data = data.str.replace(r"<[^>]*>", r" ")
    # remove HTML entities (e.g. "&nbsp;")
    data = data.str.replace(r"&[^\s]*;", r" ")
    # remove extra whitespace
    data = data.str.replace(r"\s{2,}", r" ")
    return data
