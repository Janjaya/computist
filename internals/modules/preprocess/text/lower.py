from internals.core.module import BaseModule
from internals.modules.preprocess.text.lower_mp import process


class Module(BaseModule):
    meta = {
        'name': 'Lowercase',
        'author': 'Jan William Johnsen (@frozenbeer)',
        'version': '1.0',
        'description': 'Set specified column(s) to lowercase.',
        'files': [],
        'options': (
            ('column', '', True, 'column(s) to lowercase, separate with comma'),
        ),
    }

    def module_run(self):
        columns = []
        if ',' in self.options['column']:
            columns = self.options['column'].replace(' ', '').split(',')
        else:
            columns = [self.options['column']]
        for column in columns:
            self.dataframe[column] = process(self.dataframe[column])
