from internals.modules.preprocess.text.html_mp import process
from internals.core.module import BaseModule


class Module(BaseModule):
    meta = {
        'name': 'HTML tags & entities removal',
        'author': 'Jan William Johnsen (@frozenbeer)',
        'version': '1.0',
        'description': 'Remove HTML tags and entities in specified column(s).',
        'files': [],
        'options': (
            ('column', '', True, 'column(s) to use, separate with comma'),
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
