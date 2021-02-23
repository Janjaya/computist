from internals.core.module import BaseModule
import pandas as pd
import os


class Module(BaseModule):
    meta = {
        'name': 'Import from CSV file',
        'author': 'Jan William Johnsen (@frozenbeer)',
        'version': '1.0',
        'description': 'Import a dataset from a Comma Separated Values (CSV) file.',
        'files': [],
        'options': (
            ('filepath', '', True, 'filepath to CSV file'),
            ('sep', ',', True, 'delimiter to use'),
            ('engine', 'c', True, 'parser engine to use {‘c’, ‘python’}')
        ),
    }

    sqlite_keywords = ['abort', 'action', 'add', 'after', 'all', 'alter', 'always', 'analyze', 'and', 'as', 'asc', 'attach', 'autoincrement', 'before', 'begin', 'between', 'by', 'cascade', 'case', 'cast', 'check', 'collate', 'column', 'commit', 'conflict', 'constraint', 'create', 'cross', 'current', 'current_date', 'current_time', 'current_timestamp', 'database', 'default', 'deferrable', 'deferred', 'delete', 'desc', 'detach', 'distinct', 'do', 'drop', 'each', 'else', 'end', 'escape', 'except', 'exclude', 'exclusive', 'exists', 'explain', 'fail', 'filter', 'first', 'following', 'for', 'foreign', 'from', 'full', 'generated', 'glob', 'group', 'groups', 'having', 'if', 'ignore', 'immediate', 'in', 'index', 'indexed', 'initially', 'inner', 'insert', 'instead', 'intersect', 'into', 'is', 'isnull', 'join', 'key', 'last', 'left', 'like', 'limit', 'match', 'natural', 'no', 'not', 'nothing', 'notnull', 'null', 'nulls', 'of', 'offset', 'on', 'or', 'order', 'others', 'outer', 'over', 'partition', 'plan', 'pragma', 'preceding', 'primary', 'query', 'raise', 'range', 'recursive', 'references', 'regexp', 'reindex', 'release', 'rename', 'replace', 'restrict', 'right', 'rollback', 'row', 'rows', 'savepoint', 'select', 'set', 'table', 'temp', 'temporary', 'then', 'ties', 'to', 'transaction', 'trigger', 'unbounded', 'union', 'unique', 'update', 'using', 'vacuum', 'values', 'view', 'virtual', 'when', 'where', 'window', 'with', 'without']

    def check_column_name(self):
        pass

    def convert_dtype_to_sqlite3(self, list_dtype):
        # https://www.lucidchart.com/pages/database-diagram/database-design
        # CHAR - a specific length of text
        # VARCHAR - text of variable lengths
        # TEXT - large amounts of text
        # INT - positive or negative whole number
        # FLOAT, DOUBLE - can also store floating point numbers
        # BLOB - binary data
        conversion_table = {
            'int32': 'INTEGER',
            'int64': 'INTEGER',
            'float32': 'FLOAT',
            'float64': 'FLOAT',
            'bool': 'BOOL',
            'datetime64': 'DATE',
            'object': 'TEXT',
        }
        return [conversion_table.get(item, 'Invalid') for item in list_dtype]

    def module_run(self):
        self.dataframe = pd.read_csv(self.options['filepath'], sep=self.options['sep'], engine=self.options['engine'])
        filename = self.options['filepath'].split(os.sep)[-1]
        self.do_save(None)
        self.output(f'Imported \'{filename}\'.')
        #fields = ', '.join(list(map(lambda x: ' '.join(x), zip(self.dataframe.columns, self.convert_dtype_to_sqlite3(self.dataframe.dtypes.astype('str').to_list())))))
        #if not self.query('SELECT name FROM sqlite_master WHERE type=\'table\' AND name=\'dataframe\''):
        #    self.query(f'CREATE TABLE IF NOT EXISTS dataframe ({fields}, notes TEXT, module TEXT)')
        #    for index, row in self.dataframe.iterrows():
        #        data = dict(row)
        #        rowcount = self.insert('dataframe', data.copy(), data.keys())
        #else:
        #    self.error('Cannot import data because \'dataframe\' table already exists.')
