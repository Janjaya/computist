def process(*args):
    data = args[0]
    # remove HTML tags (incl. its contents)
    data = data.str.replace(r"<[^>]*>", r" ")
    # remove HTML entities (e.g. "&nbsp;")
    data = data.str.replace(r"&[^\s]*;", r" ")
    # remove extra whitespace
    data = data.str.replace(r"\s{2,}", r" ")
    return data
