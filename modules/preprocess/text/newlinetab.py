from core.module import BaseModule
from mixins.processes import ProcessingMixin
from modules.preprocess.text.newlinetab_mp import process


class Module(BaseModule, ProcessingMixin):
    meta = {
        "name": "Newline and tabular characters removal",
        "author": "Jan William Johnsen (@frozenbeer)",
        "version": "1.1",
        "description": ("Remove newline and tabular characters from specified "
                        + "column(s)."),
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
