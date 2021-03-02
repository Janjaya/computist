from multiprocessing import current_process


def process(*args):
    if current_process().name == "MainProcess":
        data = args[0]
        patterns = args[1]
    else:
        data = args[1]
        patterns = args[0]
    # remove URLs
    data = data.str.replace(r"http[s]?:\/\/(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+", r" ")
    # remove e-mail address:password combination
    data = data.str.replace(r"[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+:[a-zA-Z0-9]+", r" ")
    # remove e-mails
    data = data.str.replace(r"[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+", r" ")
    # remove emojies (typically emoticons like :hype:, :fiesta:, etc.)
    data = data.str.replace(r"|".join(patterns), r" ")
    # remove extra whitespace
    data = data.str.replace(r"\s{2,}", r" ")
    return data
