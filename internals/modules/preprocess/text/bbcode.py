from internals.modules.preprocess.text.bbcode_mp import process
from internals.core.module import BaseModule
import pandas as pd
import os


class Module(BaseModule):
    meta = {
        'name': 'Remove BBcode',
        'author': 'Jan William Johnsen (@frozenbeer)',
        'version': '1.0',
        'description': 'Removes BBcode from text.',
        'files': [],
        'options': (
            ('column', '', True, 'column(s) to use, separate with comma'),
            ('path', '', False, 'filepath to list of BBcode tags (if empty use default)')
        ),
    }

    def module_run(self):
        # uses regex '.+?' to remove in a non-greedy way.
        patterns = [
            r'\[quote.+?\[\/quote\]?', r'\[url=?.+?\[\/url\]',
            r'\[img=?.?\].+?\[\/img\]', r'\[email\]', r'\[\/email\]',
            r'\[size=.+?\]', r'\[\/size\]', r'\[align=.+?\]', r'\[\/align\]',
            r'\[color.+?\]', r'\[\/color\]', r'\[list.+?\]', r'\[\/list\]',
            r'\[hide\]', r'\[\/hide\]', r'\[b\]', r'\[\/b\]', r'\[i\]',
            r'\[\/i\]', r'\[u\]', r'\[\/u\]', r'\[s\]', r'\[\/s\]',
            r'\[font.+?\]', r'\[\/font\]', r'\[spoiler\]', r'\[\/spoiler\]',
            r'\[code\]', r'\[\/code\]', r'\[skype\]', r'\[\/skype\]',
            r'\[php\]', r'\[\/php\]', r'\[video=?.*?\]', r'\[\/video\]',
            r'\[hr\]', r'\[member=?.+?\]', r'\[background=?.+?\]',
            r'\[\/background\]', r'\[center\]', r'\[\/center\]', r'\[\*\]',
        ]
        columns = []
        if not self.options['path']:
            df_bbcodes = pd.read_json(os.path.join(self.data_path, 'stopwords', 'bbcodes.json'))
        else:
            df_bbcodes = pd.read_json(self.options['path'])
        tags = df_bbcodes['word'].to_list()
        if ',' in self.options['column']:
            columns = self.options['column'].replace(' ', '').split(',')
        else:
            columns = [self.options['column']]
        for column in columns:
            self.dataframe[column] = process(self.dataframe[column],
                                             tags, patterns)
