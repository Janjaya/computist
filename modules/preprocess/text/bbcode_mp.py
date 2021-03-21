def process(*args):
    data = args[0]
    tags = args[1]
    # remove BBcode tags
    data = data.str.replace(r"|".join(tags), r" ")
    # remove extra whitespace
    data = data.str.replace(r"\s{2,}", r" ")
    return data
