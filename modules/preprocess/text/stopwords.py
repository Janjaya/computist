from core.module import BaseModule
from mixins.processes import ProcessingMixin
from modules.preprocess.text.stopwords_mp import process
import pandas as pd
import os


class Module(BaseModule, ProcessingMixin):
    meta = {
        "name": "Stopword removal",
        "author": "Jan William Johnsen (@frozenbeer)",
        "version": "1.1",
        "description": "Remove stopwords from specified column(s).",
        "options": (
            ("column", "", True, "column(s) to use, separate with comma"),
            ("stopwords", "", False, "additional stopwords"),
        ),
    }

    def module_run(self):
        df_sw = pd.read_json(
            os.path.join(self.data_path, "stopwords", "stopwords.json")
        )
        if "," in self.options["stopwords"]:
            additional_words = self.options["stopwords"].\
                replace(" ", "").split(",")
        else:
            additional_words = [self.options["stopwords"]]
        additional_words = [{"word": word} for word in additional_words]
        df_sw = df_sw.append(pd.DataFrame(additional_words), ignore_index=True)
        stopwords = df_sw["word"].to_list()
        columns = []
        if "," in self.options["column"]:
            columns = self.options["column"].replace(" ", "").split(",")
        else:
            columns = [self.options["column"]]
        for column in columns:
            self.dataframe[column] = self.processes(
                process, self.dataframe[column], stopwords
            )
