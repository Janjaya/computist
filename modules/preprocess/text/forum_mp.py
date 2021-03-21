def process(*args):
    data = args[0]
    patterns = args[1]
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
