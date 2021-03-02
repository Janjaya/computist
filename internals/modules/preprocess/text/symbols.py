from internals.core.module import BaseModule
from internals.modules.preprocess.text.symbols_mp import process


class Module(BaseModule):
    meta = {
        "name": "Remove symbols and numbers",
        "author": "Jan William Johnsen (@frozenbeer)",
        "version": "1.0",
        "description": "Remove symbols and numbers in specified column(s).",
        "options": (
            ("column", "", True, "column(s) to use, separate with comma"),
        ),
    }

    def module_run(self):
        columns = []
        if "," in self.options["column"]:
            columns = self.options["column"].replace(" ", "").split(",")
        else:
            columns = [self.options["column"]]
        for column in columns:
            self.dataframe[column] = process(self.dataframe[column])
