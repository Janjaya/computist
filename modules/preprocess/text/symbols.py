from core.module import BaseModule
from mixins.processes import ProcessingMixin
from modules.preprocess.text.symbols_mp import process


class Module(BaseModule, ProcessingMixin):
    meta = {
        "name": "Symbols removal",
        "author": "Jan William Johnsen (@frozenbeer)",
        "version": "1.1",
        "description": "Remove symbols in specified column(s).",
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
            self.dataframe[column] = self.processes(
                process, self.dataframe[column]
            )
