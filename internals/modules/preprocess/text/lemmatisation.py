from internals.modules.preprocess.text.lemmatisation_mp import process
from internals.mixins.processes import ProcessingMixin
from internals.core.module import BaseModule


class Module(BaseModule, ProcessingMixin):
    meta = {
        "name": "Lemmatisation",
        "author": "Jan William Johnsen (@frozenbeer)",
        "version": "1.0",
        "description": "Lemmatise specified column(s).",
        "options": (
            ("column", "", True, "column(s) to lemmatise, separate with comma"),
        ),
    }

    def module_run(self):
        columns = []
        if "," in self.options["column"]:
            columns = self.options["column"].replace(" ", "").split(",")
        else:
            columns = [self.options["column"]]
        for column in columns:
            self.dataframe[column] = self.processes(process, self.dataframe[column])
