def process(*args):
    data = args[0]
    data = data.str.replace(r"\n|\t|\r", r" ")
    # remove extra whitespace
    data = data.str.replace(r"\s{2,}", r" ")
    return data
