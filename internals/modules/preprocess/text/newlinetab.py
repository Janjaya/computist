from internals.core.module import BaseModule
from internals.modules.preprocess.text.newlinetab_mp import process


class Module(BaseModule):
    meta = {
        'name': 'Remove newline/tabular characters',
        'author': 'Jan William Johnsen (@frozenbeer)',
        'version': '1.0',
        'description': 'Remove newline and tabular characters from specified column(s).',
        'files': [],
        'options': (
            ('column', '', True, 'column(s) to use, separate with comma'),
        ),
    }

    def module_run(self):
        columns = []
        if ',' in self.options['column']:
            columns = self.options['column'].replace(' ', '').split(',')
        for column in columns:
            self.dataframe[column] = process(self.dataframe[column])
