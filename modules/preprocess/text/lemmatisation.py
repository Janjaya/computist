from core.module import BaseModule
from mixins.processes import ProcessingMixin
from modules.preprocess.text.lemmatisation_mp import process


class Module(BaseModule, ProcessingMixin):
    meta = {
        "name": "Lemmatisation",
        "author": "Jan William Johnsen (@frozenbeer)",
        "version": "1.2",
        "description": "Lemmatise specified column(s).",
        "options": (
            ("column", "", True, ("column(s) to lemmatise, "
                                  + "separate with comma")),
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
