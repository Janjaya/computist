from internals.modules.preprocess.text.stopwords_mp import process
from internals.core.module import BaseModule
import pandas as pd
import os


class Module(BaseModule):
    meta = {
        "name": "Lemmatisation",
        "author": "Jan William Johnsen (@frozenbeer)",
        "version": "1.0",
        "description": "Lemmatise specified column(s).",
        "options": (
            ("column", "", True, "column(s) to lemmatise, separate with comma"),
            # TODO: Manually add extra stopwords to remove.
        ),
    }

    def module_run(self):
        df_stopwords = pd.read_json(os.path.join(self.data_path, "stopwords", "stopwords.json"))
        stopwords = df_stopwords["word"].to_list()
        columns = []
        if "," in self.options["column"]:
            columns = self.options["column"].replace(" ", "").split(",")
        else:
            columns = [self.options["column"]]
        for column in columns:
            self.dataframe[column] = process(self.dataframe[column], stopwords)
