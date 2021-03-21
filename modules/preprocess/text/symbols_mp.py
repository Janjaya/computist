def process(*args):
    data = args[0]
    data = data.str.replace(r"[^a-z0-9 ]", r" ")
    # remove extra whitespace
    data = data.str.replace(r"\s{2,}", r" ")
    return data
