from internals.modules.preprocess.text.lemmatisation_mp import run_lemmatize
from multiprocessing import current_process
import pandas as pd
import re
import nltk


def extract_repeating_words(string):
    # TODO: Make a description of this function.
    pattern = re.compile(r"(([a-z]{2,4})\2{1,})")
    matches = pattern.finditer(string)
    words = []
    words_pos = []
    for match in matches:
        break_flag = False
        start, end = match.span()
        for wp in words_pos:
            if wp[0] <= start < wp[1] and wp[0] < end <= wp[1]:
                break_flag = True
                break
        if break_flag is not True:
            for i in range(start, -1, -1):
                if string[i] == " " or i == 0:
                    if i == 0:
                        start = i
                    else:
                        start = i + 1
                    break
            for i in range(end, len(string), 1):
                if string[i] == " " or i == len(string) - 1:
                    if i == len(string) - 1:
                        end = i + 1
                    else:
                        end = i
                    break
            words.append(re.sub(r"([a-z])\1{1,}", r"\1\1", string[start:end]))
            words_pos.append((start, end))
    return words


def run_repeating(string):
    pattern = re.compile(r"([a-z]*([a-z])\2{1,}[a-z]*)")
    repeating_characters = re.findall(pattern, string)
    repeating_characters = [word for (word, char) in repeating_characters]
    rstring = " ".join([word for word in string.split(" ")
                       if word not in repeating_characters])
    repeating_words = extract_repeating_words(rstring)
    return repeating_characters + repeating_words


def run_replace_shorten_word(string, replace):
    whitespace = nltk.tokenize.WhitespaceTokenizer()
    string = whitespace.tokenize(string)
    for i in range(len(string)):
        if string[i] in replace:
            string[i] = replace[string[i]]
    return " ".join(string)


def processe(*args):
    if current_process().name == "MainProcess":
        return args[0].apply(run_repeating)
    else:
        return args[1].apply(run_repeating)


def processr(*args):
    if current_process().name == "MainProcess":
        replace_original = pd.read_csv(args[1], sep=";", index_col="index")
        replace_short = pd.read_csv(args[2], sep=";", index_col="index")
        additional_words = args[3]
        data = args[0]
    else:
        replace_original = pd.read_csv(args[0], sep=";", index_col="index")
        replace_short = pd.read_csv(args[1], sep=";", index_col="index")
        additional_words = args[2]
        data = args[3]
    replace_original["changed"] = False
    replace_short = replace_short[~(replace_short["replace"].isnull())]
    # word[1][0] = short, word[1][1] = size, word[1][2] = replace
    for word in replace_short.iterrows():
        replace_original.loc[
            replace_original.short == word[1][0], "changed"] = True
        replace_original.loc[
            replace_original.short == word[1][0], "short"] = word[1][2]
    # Add words that is not any duplicates/repetition
    for word in additional_words:
        replace_original.loc[len(replace_original)] = [word["word"], word["size"], word["short"], word["changed"]]
    replace_original = replace_original[replace_original["changed"] is True].drop(columns="changed").set_index("word").to_dict()["short"]
    for item in replace_original:
        replace_original[item] = run_lemmatize(replace_original[item])
    data = data.apply(run_replace_shorten_word, args=(replace_original,))
    return data
