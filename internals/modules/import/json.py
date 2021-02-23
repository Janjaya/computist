from internals.core.module import BaseModule
import pandas as pd
import os


class Module(BaseModule):
    meta = {
        'name': 'Import from JSON file',
        'author': 'Jan William Johnsen (@frozenbeer)',
        'version': '1.0',
        'description': 'Import a dataset from a JSON file.',
        'files': [],
        'options': (
            ('filepath', '', True, 'filepath to JSON file'),
        ),
    }

    def module_run(self):
        self.dataframe = pd.read_json(self.options['filepath'])
        filename = self.options['filepath'].split(os.sep)[-1]
        self._do_df_save(None)
        self.output(f'Imported \'{filename}\'.')

        ''' TODO: Write dataframe to database.
        fields = ', '.join(list(map(lambda x: ' '.join(x), zip(df.columns, convert_dtype_to_sqlite3(df.dtypes.astype('str').to_list())))))
        self.output(f'CREATE TABLE IF NOT EXISTS dataframe ({fields}, notes TEXT, module TEXT)')
        if not self.query('SELECT name FROM sqlite_master WHERE type=\'table\' AND name=\'dataframe\''):
            self.query(f'CREATE TABLE IF NOT EXISTS dataframe ({fields}, notes TEXT, module TEXT)')
            for index, row in self.dataframe.iterrows():
                data = dict(row)
                rowcount = self.insert('dataframe', data.copy(), data.keys())
        else:
            self.error('Cannot import data because \'dataframe\' table already exists.')
        '''
