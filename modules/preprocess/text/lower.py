from core.module import BaseModule
from mixins.processes import ProcessingMixin
from modules.preprocess.text.lower_mp import process


class Module(BaseModule, ProcessingMixin):
    meta = {
        "name": "Lowercase",
        "author": "Jan William Johnsen (@frozenbeer)",
        "version": "1.1",
        "description": "Set specified column(s) to lowercase.",
        "options": (
            ("column", "", True, ("column(s) to lowercase, "
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
            self.dataframe[column] = process(self.dataframe[column])
