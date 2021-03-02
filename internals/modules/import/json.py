from internals.core.module import BaseModule
import pandas as pd
import os


class Module(BaseModule):
    meta = {
        "name": "Import from JSON file",
        "author": "Jan William Johnsen (@frozenbeer)",
        "version": "1.0",
        "description": "Import a dataset from a JSON file.",
        "options": (
            ("file", "", True, "path to JSON file"),
        ),
    }

    def module_run(self):
        self.dataframe = pd.read_json(self.options["file"])
        filename = self.options["file"].split(os.sep)[-1]
        self.save_dataframe()
        self.output(f"Imported '{filename}'.")
