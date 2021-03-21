from core.module import BaseModule
from mixins.processes import ProcessingMixin
from modules.preprocess.text.bbcode_mp import process
import pandas as pd
import os


class Module(BaseModule, ProcessingMixin):
    meta = {
        "name": "BBcode removal",
        "author": "Jan William Johnsen (@frozenbeer)",
        "version": "1.1",
        "description": "Removes BBcode from text.",
        "options": (
            ("column", "", True, "column(s) to use, separate with comma"),
            ("bbcodes", "", False, ("path to JSON file with BBcode tags "
                                    + "(if empty use default)"))
        ),
    }

    def module_run(self):
        # uses regex ".+?" to remove in a non-greedy way.
        patterns = [
            r"\[quote.+?\[\/quote\]?", r"\[url=?.+?\[\/url\]",
            r"\[img=?.?\].+?\[\/img\]", r"\[email\]", r"\[\/email\]",
            r"\[size=.+?\]", r"\[\/size\]", r"\[align=.+?\]", r"\[\/align\]",
            r"\[color.+?\]", r"\[\/color\]", r"\[list.+?\]", r"\[\/list\]",
            r"\[hide\]", r"\[\/hide\]", r"\[b\]", r"\[\/b\]", r"\[i\]",
            r"\[\/i\]", r"\[u\]", r"\[\/u\]", r"\[s\]", r"\[\/s\]",
            r"\[font.+?\]", r"\[\/font\]", r"\[spoiler\]", r"\[\/spoiler\]",
            r"\[code\]", r"\[\/code\]", r"\[skype\]", r"\[\/skype\]",
            r"\[php\]", r"\[\/php\]", r"\[video=?.*?\]", r"\[\/video\]",
            r"\[hr\]", r"\[member=?.+?\]", r"\[background=?.+?\]",
            r"\[\/background\]", r"\[center\]", r"\[\/center\]", r"\[\*\]",
        ]
        columns = []
        if not self.options["bbcodes"]:
            df_bbcodes = pd.read_json(
                os.path.join(self.data_path, "stopwords", "bbcodes.json")
            )
        else:
            df_bbcodes = pd.read_json(self.options["bbcodes"])
        tags = df_bbcodes["word"].to_list()
        if "," in self.options["column"]:
            columns = self.options["column"].replace(" ", "").split(",")
        else:
            columns = [self.options["column"]]
        for column in columns:
            self.dataframe[column] = self.processes(
                process, self.dataframe[column], tags + patterns
            )
