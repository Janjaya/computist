from internals.modules.preprocess.text.forum_mp import process
from internals.mixins.processes import ProcessingMixin
from internals.core.module import BaseModule


class Module(BaseModule, ProcessingMixin):
    meta = {
        "name": "Remove forum spesific text",
        "author": "Jan William Johnsen (@frozenbeer)",
        "version": "1.0",
        "description": "Removes URLs, 'e-mail:password'-combinations, e-mail addresses and emojies.",
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
            patterns = [
                r":\)", r";\)", r":P", r":D", r":\(", r":@", r":mellow:",
                r":huh:", r"\^_\^", r":o", r"B\)", r"-_-", r"&lt;_&lt;",
                r":wub:", r":S", r":wacko:", r":blink:", r":ph34r:", r"&lt;3",
                r":ezy:", r":pogchamp:", r":comfy:", r":pupper:", r":wut:",
                r":thinking:", r":pepelove:", r":pepehappy:", r":jodus:",
                r":pepolove:", r":PepeSanta:", r":pepeokay:", r":enjoy:",
                r":pepesad:"
            ]
            self.dataframe[column] = self.processes(process, self.dataframe[column], patterns)
