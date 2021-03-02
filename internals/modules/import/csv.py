from internals.core.module import BaseModule
import pandas as pd
import os


class Module(BaseModule):
    meta = {
        "name": "Import from CSV file",
        "author": "Jan William Johnsen (@frozenbeer)",
        "version": "1.0",
        "description": "Import a dataset from a Comma Separated Values (CSV) "
                       "file.",
        "options": (
            ("file", "", True, "path to CSV file"),
            ("sep", ",", True, "delimiter to use"),
            ("engine", "c", True, "parser engine to use {'c', 'python'}")
        ),
    }

    def module_run(self):
        self.dataframe = pd.read_csv(self.options["file"], sep=self.options["sep"], engine=self.options["engine"])
        filename = self.options["file"].split(os.sep)[-1]
        self.save_dataframe()
        self.output(f"Imported '{filename}'.")
