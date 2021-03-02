from internals.modules.preprocess.text.repeating_mp import processe, processr
from internals.mixins.processes import ProcessingMixin
from internals.core.module import BaseModule
import re
import os
import enchant
import numpy as np


class Module(BaseModule, ProcessingMixin):
    meta = {
        "name": "Repeating words and characters",
        "author": "Jan William Johnsen (@frozenbeer)",
        "version": "1.0",
        "description": "Extract and replace repeating words and characters in specified column(s).",
        "options": (
            ("column", "", True, "column(s) to use, separate with comma"),
            ("type", "e", True, "extract (e) or replace (r)"),
            ("sep", ";", True, "CSV separator")
        ),
    }

    def extract(self, column):
        df_repeating = processe(self.dataframe[column])
        df_repeating = df_repeating.to_frame("word").explode("word").dropna().reset_index(drop=True).groupby("word").size().to_frame(name="size").reset_index()
        df_repeating["short"] = df_repeating["word"].apply(run_shorten_word)
        df_repeating = df_repeating.sort_values(by="size", ascending=False).reset_index(drop=True)
        outfile = os.path.join(self.workspace,
                               "repeating_original.csv")
        df_repeating.to_csv(outfile,
                            sep=self.options["sep"],
                            index_label="index")

        # write short values to file and manually go through top 1000
        # words to find their replacement
        df_repeating_groups = df_repeating.groupby("short").sum().reset_index().sort_values(by="size", ascending=False).reset_index(drop=True)
        df_repeating_groups = remove_english_words(df_repeating_groups)
        df_repeating_groups.loc[:, "replace"] = ""
        if self._global_options["verbosity"] >= 1:
            self.show_statistics(column, df_repeating_groups)
        self.output("Written repeating words and characters to files:")
        self.output(f"{outfile.split(os.path.sep)[-1]}")
        outfile = os.path.join(self.workspace, "repeating_short.csv")
        self.output(f"{outfile.split(os.path.sep)[-1]}")
        df_repeating_groups.to_csv(outfile, sep=self.options["sep"], index_label="index")
        self.output("Edit SHORT file to find replacement words.")

    def replace(self, column):
        additional_words = [
            {"word": "ty", "size": np.NaN, "short": "thanks", "changed": True},
            {"word": "thx", "size": np.NaN, "short": "thanks", "changed": True},
            {"word": "bro", "size": np.NaN, "short": "brother", "changed": True},
            {"word": "tyy", "size": np.NaN, "short": "thanks", "changed": True},
            {"word": "tyyy", "size": np.NaN, "short": "thanks", "changed": True},
            {"word": "tyty", "size": np.NaN, "short": "thanks", "changed": True},
            {"word": "wp", "size": np.NaN, "short": "well played", "changed": True},
            {"word": "thnx", "size": np.NaN, "short": "thanks", "changed": True},
            {"word": "gj", "size": np.NaN, "short": "good job", "changed": True},
            {"word": "tyvm", "size": np.NaN, "short": "thank you very much", "changed": True},
            {"word": "info", "size": np.NaN, "short": "information", "changed": True},
            {"word": "gracias", "size": np.NaN, "short": "thanks", "changed": True},
            {"word": "tnx", "size": np.NaN, "short": "thanks", "changed": True},
            {"word": "tx", "size": np.NaN, "short": "thanks", "changed": True},
            {"word": "thnks", "size": np.NaN, "short": "thanks", "changed": True},
            {"word": "tks", "size": np.NaN, "short": "thanks", "changed": True},
            {"word": "wub", "size": np.NaN, "short": "love", "changed": True},
            {"word": "bln", "size": np.NaN, "short": "ball licking noob", "changed": True},
            {"word": "merci", "size": np.NaN, "short": "thanks", "changed": True},
            {"word": "wtf", "size": np.NaN, "short": "what the fuck", "changed": True},
            {"word": "omg", "size": np.NaN, "short": "oh my god", "changed": True},
            {"word": "pls", "size": np.NaN, "short": "please", "changed": True},
            {"word": "thk", "size": np.NaN, "short": "try hard kid", "changed": True},
            {"word": "lvl", "size": np.NaN, "short": "level", "changed": True},
            {"word": "plz", "size": np.NaN, "short": "please", "changed": True},
            {"word": "bud", "size": np.NaN, "short": "buddy", "changed": True},
            {"word": "awsome", "size": np.NaN, "short": "awesome", "changed": True},
            {"word": "matey", "size": np.NaN, "short": "mate", "changed": True},
            {"word": "brah", "size": np.NaN, "short": "brother", "changed": True},
            # {"word": "", "size": np.NaN, "short": "", "changed": True},
            # {"word": "", "size": np.NaN, "short": "", "changed": True},
            # {"word": "lol", "size": np.NaN, "short": "haha", "changed": True},
        ]
        original = os.path.join(self.workspace, "repeating_original.csv")
        short = os.path.join(self.workspace, "repeating_short.csv")
        self.processes(processr, self.dataframe[column], original, short, additional_words)

    def module_run(self):
        columns = []
        if "," in self.options["column"]:
            columns = self.options["column"].replace(" ", "").split(",")
        else:
            columns = [self.options["column"]]
        for column in columns:
            if self.options["type"] == "e":
                self.extract(column)
            elif self.options["type"] == "r":
                self.replace(column)

    def show_statistics(self, column, df_repeating_groups):
        # FIX check statistics after removing english words
        # print out summary of the repeating words and repeating_characters
        total = df_repeating_groups["size"].sum()
        groups_top = df_repeating_groups[:1000]["size"].sum()
        df_temp = self.dataframe[column].str.lower().str.split()
        df_total = df_temp.str.len().sum()
        self.alert("Statistics are incorrect!")
        self.output("Statistics:")
        self.output("\tTotal repeating words/characters: "
                    f"{total}.")
        self.output("\tTop 1000 repeating words/characters: "
                    f"{groups_top}.")
        self.output("\tPercentage of the top 1000 in repeating: "
                    f"{(groups_top/total) * 100}.")
        self.output(f"\tTotal words in dataset: {df_total}.")
        self.output("\tPercentage of the top 1000 in total (all) words: "
                    f"{(total/df_total) * 100}.")


def remove_english_words(df_repeating_groups):
    d = enchant.Dict("en_US")
    english = df_repeating_groups["short"].dropna().apply(lambda x: d.check(x)).to_frame("english")
    df_repeating_groups = df_repeating_groups[df_repeating_groups.index.isin(english[english["english"] is not True].index)]
    return df_repeating_groups


def run_shorten_word(string):
    shorten = re.sub(r"([a-z])\1{1,}", r"\1\1", string)
    return re.sub(r"(([a-z]{2,4})\2{1,})", r"\2\2", shorten)
