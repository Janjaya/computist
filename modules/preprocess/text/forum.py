from core.module import BaseModule
from mixins.processes import ProcessingMixin
from modules.preprocess.text.forum_mp import process


class Module(BaseModule, ProcessingMixin):
    meta = {
        "name": "Forum-spesific text removal",
        "author": "Jan William Johnsen (@frozenbeer)",
        "version": "1.1",
        "description": ("Removes URLs, 'e-mail:password'-combinations, "
                        + "e-mail addresses and emojies."),
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
            # use to find other patterns: tmp = df[].str.findall(r"\:\w+?\:")
            # filter out empty with tmp[tmp.map(len) > 0]
            # patterns are emojies/icons found on forum boards
            patterns = [
                r":\)", r";\)", r":P", r":D", r":\(", r":@", r":d", r"-_-",
                r":mellow:", r":huh:", r"\^_\^", r":o", r"B\)",  r"&lt;_&lt;",
                r":wub:", r":S", r":wacko:", r":blink:", r":ph34r:", r"&lt;3",
                r":ezy:", r":pogchamp:", r":comfy:", r":pupper:", r":wut:",
                r":thinking:", r":pepelove:", r":pepehappy:", r":jodus:",
                r":pepolove:", r":PepeSanta:", r":pepeokay:", r":enjoy:",
                r":pepesad:", r":feelsgood:", r":fiesta:", r":kappa:",
                r":uuh:", r":pepo:", r":monkas:", r":kek:", r":pepe:",
                r":smart:", r":fine:", r":heart:", r":feelsbadman:", r":jew:",
                r":email:", r":handsup:", r":pedo:", r":fine:", r":pepi:",
                r":\?\?:",
            ]
            self.dataframe[column] = self.processes(
                process, self.dataframe[column], patterns
            )
