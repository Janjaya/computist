'''
Copyright (c) 2012-2021 Tim Tomes
Modifications Copyright (c) 2021 Jan William Johnsen
2021-02-23: Updated the framework to manage a Pandas dataframe and other
small fixes

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.
'''

from contextlib import closing
from datetime import datetime
import cmd
import codecs
import inspect
import json
import os
import re
import sqlite3
import platform
import subprocess
import sys
import traceback
import pandas as pd
import random
import string


# #============================================================================
# SUPPORT CLASSES
# #============================================================================


class FrameworkException(Exception):
    def __init__(self, message):
        Exception.__init__(self, message)


class Colors(object):
    if platform.system() == 'Windows':
        N = R = G = O = B = ''
    else:
        N = '\033[m'    # native
        R = '\033[31m'  # red
        G = '\033[32m'  # green
        O = '\033[33m'  # orange
        B = '\033[34m'  # blue


class Options(dict):
    def __init__(self, *args, **kwargs):
        self.required = {}
        self.description = {}
        super(Options, self).__init__(*args, **kwargs)

    def __getitem__(self, name):
        name = self.__keytransform__(name)
        return super(Options, self).__getitem__(name)

    def __setitem__(self, name, value):
        name = self.__keytransform__(name)
        value = self._autoconvert(value)
        super(Options, self).__setitem__(name, value)

    def __delitem__(self, name):
        name = self.__keytransform__(name)
        super(Options, self).__delitem__(name)
        if name in self.required:
            del self.required[name]
        if name in self.description:
            del self.description[name]

    def __keytransform__(self, key):
        return key.upper()

    def _boolify(self, value):
        # throws exception if value is not a string representation of a boolean
        return {'true': True, 'false': False}[value.lower()]

    def _autoconvert(self, value):
        if value in (None, True, False):
            return value
        elif (isinstance(value, str)) and value.lower() in ('none', "''",
                                                            '""'):
            return None
        orig = value
        for fn in (self._boolify, int, float):
            try:
                value = fn(value)
                break
            except ValueError:
                pass
            except KeyError:
                pass
            except AttributeError:
                pass
        if type(value) is int and '.' in str(orig):
            return float(orig)
        return value

    def init_option(self, name, value=None, required=False, description=''):
        name = self.__keytransform__(name)
        self[name] = value
        self.required[name] = required
        self.description[name] = description

    def serialize(self):
        options = []
        for key in self:
            option = {}
            option['name'] = key
            option['value'] = self[key]
            option['required'] = self.required[key]
            option['description'] = self.description[key]
            options.append(option)
        return options


# #============================================================================
# FRAMEWORK CLASS
# #============================================================================


class Framework(cmd.Cmd):
    prompt = '>>>'
    # mode flags
    _script = 0
    _load = 0
    _mode = 0
    # framework variables
    _global_options = Options()
    _loaded_modules = {}
    app_path = ''
    data_path = ''
    core_path = ''
    home_path = ''
    mod_path = ''
    spaces_path = ''
    workspace = ''
    cache_path = ''
    _record = None
    _spool = None
    _summary_counts = {}
    dataframe = None

    def __init__(self, params):
        cmd.Cmd.__init__(self)
        self._modulename = params
        self.ruler = '-'
        self.spacer = '  '
        self.time_format = '%Y-%m-%d %H:%M:%S'
        self.nohelp = f"{Colors.R}[!] No help on %s{Colors.N}"
        self.do_help.__func__.__doc__ = '''Displays this menu'''
        self.doc_header = 'Commands (type [help|?] <topic>):'
        self._exit = 0

    # ##=======================================================================
    # CMD OVERRIDE METHODS
    # ##=======================================================================

    def default(self, line):
        self.error(f"Invalid command: {line}")

    def emptyline(self):
        # disables running of last command when no command is given
        # return flag to tell interpreter to continue
        return 0

    def precmd(self, line):
        if Framework._load:
            print('\r', end='')
        if Framework._script:
            print(f"{line}")
        if Framework._record:
            recorder = codecs.open(Framework._record, 'ab', encoding='utf-8')
            recorder.write(f"{line}{os.linesep}")
            recorder.flush()
            recorder.close()
        if Framework._spool:
            Framework._spool.write(f"{self.prompt}{line}{os.linesep}")
            Framework._spool.flush()
        return line

    def onecmd(self, line):
        cmd, arg, line = self.parseline(line)
        if not line:
            return self.emptyline()
        if line == 'EOF':
            # reset stdin for raw_input
            sys.stdin = sys.__stdin__
            Framework._script = 0
            Framework._load = 0
            return
        if cmd is None:
            return self.default(line)
        self.lastcmd = line
        if cmd == '':
            return self.default(line)
        else:
            try:
                func = getattr(self, 'do_' + cmd)
            except AttributeError:
                return self.default(line)
            try:
                return func(arg)
            except Exception:
                self.print_exception()

    # make help menu more attractive
    def print_topics(self, header, cmds, cmdlen, maxcol):
        if cmds:
            self.stdout.write(f"{header}{os.linesep}")
            if self.ruler:
                self.stdout.write(f"{self.ruler * len(header)}{os.linesep}")
            for _cmd in cmds:
                self.stdout.write(f"{_cmd.ljust(15)} "
                                  f"{getattr(self, 'do_' + _cmd).__doc__}"
                                  f"{os.linesep}")
            self.stdout.write(os.linesep)

    # ##=======================================================================
    # SUPPORT METHODS
    # ##=======================================================================

    def to_unicode_str(self, obj, encoding='utf-8'):
        # converts non-stringish types to unicode
        if type(obj) not in (str, bytes):
            obj = str(obj)
        obj = self.to_unicode(obj, encoding)
        return obj

    # FIX Gives on Windows UnicodeDecodeError: 'utf-8' codec can't decode byte
    # 0xff in position x: invalid start byte
    def to_unicode(self, obj, encoding='utf-8'):
        # converts bytes to unicode
        if isinstance(obj, bytes):
            obj = obj.decode(encoding)
        return obj

    def is_hash(self, hashstr):
        hashdict = [
            {'pattern': r'^[a-fA-F0-9]{32}$', 'type': 'MD5'},
            {'pattern': r'^[a-fA-F0-9]{16}$', 'type': 'MySQL'},
            {'pattern': r'^\*[a-fA-F0-9]{40}$', 'type': 'MySQL5'},
            {'pattern': r'^[a-fA-F0-9]{40}$', 'type': 'SHA1'},
            {'pattern': r'^[a-fA-F0-9]{56}$', 'type': 'SHA224'},
            {'pattern': r'^[a-fA-F0-9]{64}$', 'type': 'SHA256'},
            {'pattern': r'^[a-fA-F0-9]{96}$', 'type': 'SHA384'},
            {'pattern': r'^[a-fA-F0-9]{128}$', 'type': 'SHA512'},
            {'pattern': r'^\$[PH]{1}\$.{31}$', 'type': 'phpass'},
            {'pattern': r'^\$2[ya]?\$.{56}$', 'type': 'bcrypt'},
        ]
        for hashitem in hashdict:
            if re.match(hashitem['pattern'], hashstr):
                return hashitem['type']
        return False

    def get_random_str(self, length):
        return ''.join(random.choice(string.ascii_lowercase)
                       for i in range(length))

    def _is_writeable(self, filename):
        try:
            fp = open(filename, 'a')
            fp.close()
            return True
        except IOError:
            return False

    def _parse_rowids(self, rowids):
        xploded = []
        rowids = [x.strip() for x in rowids.split(',')]
        for rowid in rowids:
            try:
                if '-' in rowid:
                    start = int(rowid.split('-')[0].strip())
                    end = int(rowid.split('-')[-1].strip())
                    xploded += range(start, end+1)
                else:
                    xploded.append(int(rowid))
            except ValueError:
                continue
        return sorted(list(set(xploded)))

    # ##=======================================================================
    # OUTPUT METHODS
    # ##=======================================================================

    def print_exception(self, line=''):
        stack_list = [x.strip() for x in
                      traceback.format_exc().strip().splitlines()]
        message = stack_list[-1].split(':', 1)[-1].strip()
        if self._global_options['verbosity'] == 0:
            return
        elif self._global_options['verbosity'] == 1:
            line = ' '.join([x for x in [message, line] if x])
            self.error(line)
        elif self._global_options['verbosity'] == 2:
            print(f"{Colors.R}{'-'*60}")
            traceback.print_exc()
            print(f"{'-'*60}{Colors.N}")

    def error(self, line):
        '''Formats and presents errors.'''
        if not re.search('[.,;!?]$', line):
            line += '.'
        line = line[:1].upper() + line[1:]
        print(f"{Colors.R}[!] {line}{Colors.N}")

    def output(self, line):
        '''Formats and presents normal output.'''
        print(f"{Colors.B}[*]{Colors.N} {line}")

    def alert(self, line):
        '''Formats and presents important output.'''
        print(f"{Colors.G}[*]{Colors.N} {line}")

    def verbose(self, line):
        '''Formats and presents output if in verbose mode.'''
        if self._global_options['verbosity'] >= 1:
            self.output(line)

    def debug(self, line):
        '''Formats and presents output if in debug mode (very verbose).'''
        if self._global_options['verbosity'] >= 2:
            self.output(line)

    def heading(self, line, level=1):
        '''Formats and presents styled header text.'''
        line = line
        print('')
        if level == 0:
            print(self.ruler*len(line))
            print(line.upper())
            print(self.ruler*len(line))
        if level == 1:
            print(f"{self.spacer}{line.title()}")
            print(f"{self.spacer}{self.ruler*len(line)}")

    def table(self, data, header=[], title=''):
        '''Accepts a list of rows and outputs a table.'''
        tdata = list(data)
        if header:
            tdata.insert(0, header)
        if len(set([len(x) for x in tdata])) > 1:
            raise FrameworkException('Row lengths not consistent.')
        lens = []
        cols = len(tdata[0])
        # create a list of max widths for each column
        for i in range(0, cols):
            lens.append(len(max([self.to_unicode_str(x[i])
                                 if x[i] is not None
                                 else ''
                                 for x in tdata], key=len)))
        # calculate dynamic widths based on the title
        title_len = len(title)
        tdata_len = sum(lens) + (3*(cols-1))
        diff = title_len - tdata_len
        if diff > 0:
            diff_per = diff / cols
            lens = [x+diff_per for x in lens]
            diff_mod = diff % cols
            for x in range(0, diff_mod):
                lens[x] += 1
        # build ascii table
        if len(tdata) > 0:
            separator_str = f"{self.spacer}+-{'%s---'*(cols-1)}%s-+"
            separator_sub = tuple(['-'*x for x in lens])
            separator = separator_str % separator_sub
            data_str = f"{self.spacer}| {'%s | '*(cols-1)}%s |"
            # top of ascii table
            print('')
            print(separator)
            # ascii table data
            if title:
                print(f"{self.spacer}| {title.center(tdata_len)} |")
                print(separator)
            if header:
                rdata = tdata.pop(0)
                data_sub = tuple([rdata[i].center(lens[i])
                                  for i in range(0, cols)])
                print(data_str % data_sub)
                print(separator)
            for rdata in tdata:
                data_sub = tuple([self.to_unicode_str(rdata[i]).ljust(lens[i])
                                  if rdata[i] is not None
                                  else ''.ljust(lens[i])
                                  for i in range(0, cols)])
                print(data_str % data_sub)
            # bottom of ascii table
            print(separator)
            print('')

    # ##=======================================================================
    # DATABASE METHODS
    # ##=======================================================================

    def query(self, *args, **kwargs):
        path = os.path.join(self.workspace, 'data.db')
        return self._query(path, *args, **kwargs)

    def _query(self, path, query, values=(), include_header=False):
        '''Queries the database and returns the results as a list.'''
        self.debug(f"DATABASE => {path}")
        self.debug(f"QUERY => {query}")
        with sqlite3.connect(path) as conn:
            with closing(conn.cursor()) as cur:
                if values:
                    self.debug(f"VALUES => {repr(values)}")
                    cur.execute(query, values)
                else:
                    cur.execute(query)
                # a rowcount of -1 typically refers to a select statement
                if cur.rowcount == -1:
                    rows = []
                    if include_header:
                        rows.append(tuple([x[0] for x in cur.description]))
                    rows.extend(cur.fetchall())
                    results = rows
                # a rowcount of 1 == success and 0 == failure
                else:
                    conn.commit()
                    results = cur.rowcount
                return results

    def get_columns(self, table):
        return [(x[1], x[2]) for x in self.query(
            f"PRAGMA table_info('{table}')")]

    def get_tables(self):
        return [x[0] for x in self.query(
            "SELECT name FROM sqlite_master WHERE type='table'") if x[0] not in
            ['dashboard', 'snapshots']]

    # ##=======================================================================
    # INSERT METHODS
    # ##=======================================================================

    def _display(self, data, rowcount):
        display = self.alert if rowcount else self.verbose
        for key in sorted(data.keys()):
            display(f"{key.title()}: {data[key]}")
        display(self.ruler*50)

    def insert_snapshot(self, snapshot=None, notes=None, mute=False):
        '''Adds a snapshot to the database'''
        data = dict(
            snapshot=f"snapshot_{snapshot}",
            date=datetime.strptime(
                str(snapshot), '%Y%m%d%H%M%S').strftime(self.time_format),
            notes=notes
        )
        rowcount = self.insert('snapshots', data.copy(), data.keys())
        if not mute:
            self._display(data, rowcount)
        return rowcount

    def insert(self, table, data, unique_columns=[]):
        '''Inserts items into database and returns the affected row count.
        table          : the table to insert the data into
        data           : the information to insert into the database table in
                         the form of a dictionary where the keys are the column
                         names and the values are the column values
        unique_columns : a list of column names that should be used to
                         determine if the information being inserted is
                         unique'''
        # set module to the calling module unless the do_add command was used
        if '_do_db_insert' in [x[3] for x in inspect.stack()]:
            data['module'] = 'user_defined'
        else:
            data['module'] = self._modulename.split(os.path.sep)[-1]
        # sanitize the inputs to remove NoneTypes, blank strings, and zeros
        columns = [x for x in data.keys() if data[x]]
        # make sure that module is not seen as a unique column
        unique_columns = [x for x in unique_columns if x in columns and
                          x != 'module']
        # exit if there is nothing left to insert
        if not columns:
            return 0
        # convert any type to unicode (str) for external processing
        for column in columns:
            data[column] = self.to_unicode_str(data[column])

        # build the insert query
        columns_str = '`, `'.join(columns)
        placeholder_str = ', '.join('?'*len(columns))
        unique_columns_str = ' and '.join([f"`{column}`=?" for column in
                                          unique_columns])
        if not unique_columns:
            query = f"INSERT INTO `{table}` (`{columns_str}`) VALUES ({placeholder_str})"
        else:
            query = f"INSERT INTO `{table}` (`{columns_str}`) SELECT {placeholder_str} WHERE NOT EXISTS(SELECT * FROM `{table}` WHERE {unique_columns_str})"
        values = tuple([data[column] for column in columns] + [data[column] for column in unique_columns])

        # query the database
        rowcount = self.query(query, values)

        # increment summary tracker
        if table not in self._summary_counts:
            self._summary_counts[table] = {'count': 0, 'new': 0}
        self._summary_counts[table]['new'] += rowcount
        self._summary_counts[table]['count'] += 1

        return rowcount

    # ##=======================================================================
    # DATAFRAME METHODS
    # ##=======================================================================

    def _save_dataframe(self):
        path = os.path.join(self.workspace, 'data.json')
        self.dataframe.to_json(path)

    def _load_dataframe(self):
        path = os.path.join(self.workspace, 'data.json')
        if os.path.isfile(path):
            print('Loading data. This can take a while...', end='\r')
            self.dataframe = pd.read_json(path)
            print(' ' * 40)

    # ##=======================================================================
    # OPTIONS METHODS
    # ##=======================================================================

    def register_option(self, name, value, required, description):
        self.options.init_option(name=name, value=value, required=required, description=description)
        # needs to be optimized rather than ran on every register
        self._load_config()

    def _validate_options(self):
        for option in self.options:
            # if value type is bool or int, then we know the options is set
            if not type(self.options[option]) in [bool, int]:
                if self.options.required[option] is True and not self.options[option]:
                    raise FrameworkException(f"Value required for the '{option}' option.")
        return

    def _list_options(self, options=None):
        '''Lists options'''
        if options is None:
            options = self.options
        if options:
            pattern = f"{self.spacer}%s  %s  %s  %s"
            key_len = len(max(options, key=len))
            if key_len < 4:
                key_len = 4
            val_len = len(max([self.to_unicode_str(options[x]) for x in options], key=len))
            if val_len < 13:
                val_len = 13
            print('')
            print(pattern % ('Name'.ljust(key_len), 'Current Value'.ljust(val_len), 'Required', 'Description'))
            print(pattern % (self.ruler*key_len, (self.ruler*13).ljust(val_len), self.ruler*8, self.ruler*11))
            for key in sorted(options):
                value = options[key] if options[key] is not None else ''
                reqd = 'no' if options.required[key] is False else 'yes'
                desc = options.description[key]
                print(pattern % (key.ljust(key_len), self.to_unicode_str(value).ljust(val_len), self.to_unicode_str(reqd).ljust(8), desc))
            print('')
        else:
            print('')
            print(f"{self.spacer}No options available for this module.")
            print('')

    def _load_config(self):
        config_path = os.path.join(self.workspace, 'config.dat')
        # don't bother loading if a config file doesn't exist
        if os.path.exists(config_path):
            # retrieve saved config data
            with open(config_path) as config_file:
                try:
                    config_data = json.loads(config_file.read())
                except ValueError:
                    # file is corrupt, nothing to load, exit gracefully
                    pass
                else:
                    # set option values
                    for key in self.options:
                        try:
                            self.options[key] = config_data[self._modulename][key]
                        except KeyError:
                            # invalid key, contnue to load valid keys
                            continue

    def _save_config(self, name, module=None, options=None):
        config_path = os.path.join(self.workspace, 'config.dat')
        # create a config file if one doesn't exist
        open(config_path, 'a').close()
        # retrieve saved config data
        with open(config_path) as config_file:
            try:
                config_data = json.loads(config_file.read())
            except ValueError:
                # file is empty or corrupt, nothing to load
                config_data = {}
        # override implicit defaults if specified
        module = module or self._modulename
        options = options or self.options
        # create a container for the current module
        if module not in config_data:
            config_data[module] = {}
        # set the new option value in the config
        config_data[module][name] = options[name]
        # remove the option if it has been unset
        if config_data[module][name] is None:
            del config_data[module][name]
        # remove the module container if it is empty
        if not config_data[module]:
            del config_data[module]
        # write the new config data to the config file
        with open(config_path, 'w') as config_file:
            json.dump(config_data, config_file, indent=4)

    # ##=======================================================================
    # MODULES METHODS
    # ##=======================================================================

    def _match_modules(self, params):
        # return an exact match
        if params in Framework._loaded_modules:
            return [params]
        # use the provided name as a keyword search and return the results
        return [x for x in Framework._loaded_modules if params in x]

    def _list_modules(self, modules):
        if modules:
            last_category = ''
            for module in sorted(modules):
                category = module.split(os.path.sep)[0]
                if category != last_category:
                    # print header
                    last_category = category
                    self.heading(last_category)
                # print module
                print(f"{self.spacer*2}{module}")
        else:
            print('')
            self.alert('No modules enabled/installed.')
        print('')

    # ##=======================================================================
    # SHOW METHODS
    # ##=======================================================================

    def _get_show_names(self):
        # Any method beginning with "show_" will be parsed
        # and added as a subcommand for the show command.
        prefix = 'show_'
        return [x[len(prefix):] for x in self.get_names() if x.startswith(prefix)]

    def show_snapshots(self):
        snapshots = self.query("SELECT * FROM `snapshots`")
        if snapshots:
            self.table([list(s) for s in snapshots],
                       header=['Snapshots', 'Dates', 'Notes', 'Modules'])
        else:
            self.output('This workspace has no snapshots.')

    # ##=======================================================================
    # COMMAND METHODS
    # ##=======================================================================

    def _parse_subcommands(self, command):
        subcommands = []
        for method in dir(self):
            if f"_do_{command}_" in method:
                subcommands.append(method.split('_')[-1])
        return subcommands

    def _parse_params(self, params):
        params = params.split()
        arg = ''
        if params:
            arg = params.pop(0)
        params = ' '.join(params)
        return arg, params

    def do_exit(self, params):
        '''Exits the framework'''
        self._exit = 1
        return True

    def do_back(self, params):
        '''Exits the current context'''
        return True

    def do_options(self, params):
        '''Manages the current context options'''
        if not params:
            self.help_options()
            return
        arg, params = self._parse_params(params)
        if arg in self._parse_subcommands('options'):
            return getattr(self, '_do_options_'+arg)(params)
        else:
            self.help_options()

    def _do_options_list(self, params):
        '''Shows the current context options'''
        self._list_options()

    def _do_options_set(self, params):
        '''Sets a current context option'''
        option, value = self._parse_params(params)
        if not (option and value):
            self._help_options_set()
            return
        name = option.upper()
        if name in self.options:
            self.options[name] = value
            print(f"{name} => {value}")
            self._save_config(name)
        else:
            self.error('Invalid option name.')

    def _do_options_unset(self, params):
        '''Unsets a current context option'''
        option, value = self._parse_params(params)
        if not option:
            self._help_options_unset()
            return
        name = option.upper()
        if name in self.options:
            self._do_options_set(' '.join([name, 'None']))
        else:
            self.error('Invalid option name.')

    def do_modules(self, params):
        '''Interfaces with installed modules'''
        if not params:
            self.help_modules()
            return
        arg, params = self._parse_params(params)
        if arg in self._parse_subcommands('modules'):
            return getattr(self, '_do_modules_'+arg)(params)
        else:
            self.help_modules()

    def _do_modules_search(self, params):
        '''Searches installed modules'''
        modules = [x for x in Framework._loaded_modules]
        if params:
            self.output(f"Searching installed modules for '{params}'...")
            modules = [x for x in Framework._loaded_modules if re.search(params, x)]
        if modules:
            self._list_modules(modules)
        else:
            self.error('No modules found.')
            self._help_modules_search()

    def _do_modules_load(self, params):
        '''Searches installed modules'''
        raise NotImplementedError

    def do_show(self, params):
        '''Shows various framework items'''
        if not params:
            self.help_show()
            return
        arg, params = self._parse_params(params)
        if arg in self._get_show_names():
            getattr(self, 'show_' + arg)()
        elif arg in self.get_tables():
            self.do_db(f"query SELECT ROWID, * FROM `{arg}`")
        else:
            self.help_show()

    def do_db(self, params):
        '''Interfaces with the workspace's database'''
        if not params:
            self.help_db()
            return
        arg, params = self._parse_params(params)
        if arg in self._parse_subcommands('db'):
            return getattr(self, '_do_db_'+arg)(params)
        else:
            self.help_db()

    def _do_db_notes(self, params):
        '''Adds notes to rows in the database'''
        table, params = self._parse_params(params)
        if not table:
            self._help_db_notes()
            return
        if table in self.get_tables():
            # get rowid and note from parameters
            if params:
                arg, note = self._parse_params(params)
                rowids = self._parse_rowids(arg)
            # get rowid and note from interactive input
            else:
                try:
                    # prompt user for data
                    params = input('rowid(s) (INT): ')
                    rowids = self._parse_rowids(params)
                    note = input('note (TXT): ')
                except KeyboardInterrupt:
                    print('')
                    return
                finally:
                    # ensure proper output for resource scripts
                    if Framework._script:
                        print(f"{params}")
            # delete record(s) from the database
            count = 0
            for rowid in rowids:
                count += self.query(f"UPDATE `{table}` SET notes=? WHERE ROWID IS ?", (note, rowid))
            self.output(f"{count} rows affected.")
        else:
            self.output('Invalid table name.')

    def _do_db_insert(self, params):
        '''Inserts a row into the database'''
        table, params = self._parse_params(params)
        if not table:
            self._help_db_insert()
            return
        if table in self.get_tables():
            # validate insert_* method for table
            if not hasattr(self, 'insert_' + table):
                self.error('Cannot add records to dynamically created tables.')
                return
            columns = [x for x in self.get_columns(table) if x[0] != 'module']
            # sanitize column names to avoid conflicts with builtins in insert_* method
            sanitize_column = lambda x: '_'+x if x in ['hash', 'type'] else x
            record = {}
            # build record from parameters
            if params:
                # parse params into values by delim
                values = params.split('~')
                # validate parsed value input
                if len(columns) == len(values):
                    # assign each value to a column
                    for i in range(0,len(columns)):
                        record[sanitize_column(columns[i][0])] = values[i]
                else:
                    self.error('Columns and values length mismatch.')
                    return
            # build record from interactive input
            else:
                for column in columns:
                    try:
                        # prompt user for data
                        value = input(f"{column[0]} ({column[1]}): ")
                        record[sanitize_column(column[0])] = value
                    except KeyboardInterrupt:
                        print('')
                        return
                    finally:
                        # ensure proper output for resource scripts
                        if Framework._script:
                            print(f"{value}")
            # add record to the database
            func = getattr(self, 'insert_' + table)
            count = func(mute=True, **record)
            self.output(f"{count} rows affected.")
        else:
            self.output('Invalid table name.')

    def _do_db_delete(self, params):
        '''Deletes a row from the database'''
        table, params = self._parse_params(params)
        if not table:
            self._help_db_delete()
            return
        if table in self.get_tables():
            # get rowid from parameters
            if params:
                rowids = self._parse_rowids(params)
            # get rowid from interactive input
            else:
                try:
                    # prompt user for data
                    params = input('rowid(s) (INT): ')
                    rowids = self._parse_rowids(params)
                except KeyboardInterrupt:
                    print('')
                    return
                finally:
                    # ensure proper output for resource scripts
                    if Framework._script:
                        print(f"{params}")
            # delete record(s) from the database
            count = 0
            for rowid in rowids:
                count += self.query(f"DELETE FROM `{table}` WHERE ROWID IS ?", (rowid,))
            self.output(f"{count} rows affected.")
        else:
            self.output('Invalid table name.')

    def _do_db_query(self, params):
        '''Queries the database with custom SQL'''
        if not params:
            self._help_db_query()
            return
        try:
            results = self.query(params, include_header=True)
        except sqlite3.OperationalError as e:
            self.error(f"Invalid query. {type(e).__name__} {e}")
            return
        if type(results) == list:
            header = results.pop(0)
            if not results:
                self.output('No data returned.')
            else:
                self.table(results, header=header)
                self.output(f"{len(results)} rows returned")
        else:
            self.output(f"{results} rows affected.")

    def _do_db_schema(self, params):
        '''Displays the database schema'''
        tables = self.get_tables()
        for table in tables:
            columns = self.get_columns(table)
            self.table(columns, title=table)

    def do_script(self, params):
        '''Records and executes command scripts'''
        if not params:
            self.help_script()
            return
        arg, params = self._parse_params(params)
        if arg in self._parse_subcommands('script'):
            return getattr(self, '_do_script_'+arg)(params)
        else:
            self.help_script()

    def _do_script_record(self, params):
        '''Records commands in a script file'''
        if not Framework._record:
            filename, params = self._parse_params(params)
            if not filename:
                self._help_script_record()
                return
            if not self._is_writeable(filename):
                self.output(f"Cannot record commands to '{filename}'.")
            else:
                Framework._record = filename
                self.output(f"Recording commands to '{Framework._record}'.")
        else:
            self.output('Recording is already started.')

    def _do_script_stop(self, params):
        '''Stops command recording'''
        if Framework._record:
            self.output(f"Recording stopped. Commands saved to '{Framework._record}'.")
            Framework._record = None
        else:
            self.output('Recording is already stopped.')

    def _do_script_status(self, params):
        '''Provides the status of command recording'''
        status = 'started' if Framework._record else 'stopped'
        self.output(f"Command recording is {status}.")

    def _do_script_execute(self, params):
        '''Executes commands from a script file'''
        if not params:
            self._help_script_execute()
            return
        if os.path.exists(params):
            # works even when called before computist.start due
            # to stdin waiting for the iteractive prompt
            sys.stdin = open(params)
            Framework._script = 1
        else:
            self.error(f"Script file '{params}' not found.")

    def do_spool(self, params):
        '''Spools output to a file'''
        if not params:
            self.help_spool()
            return
        arg, params = self._parse_params(params)
        if arg in self._parse_subcommands('spool'):
            return getattr(self, '_do_spool_'+arg)(params)
        else:
            self.help_spool()

    def _do_spool_start(self, params):
        '''Starts output spooling'''
        if not Framework._spool:
            filename, params = self._parse_params(params)
            if not filename:
                self._help_spool_start()
                return
            if not self._is_writeable(filename):
                self.output(f"Cannot spool output to '{filename}'.")
            else:
                Framework._spool = codecs.open(filename, 'ab', encoding='utf-8')
                self.output(f"Spooling output to '{Framework._spool.name}'.")
        else:
            self.output('Spooling is already started.')

    def _do_spool_stop(self, params):
        '''Stops output spooling'''
        if Framework._spool:
            self.output(f"Spooling stopped. Output saved to '{Framework._spool.name}'.")
            Framework._spool = None
        else:
            self.output('Spooling is already stopped.')

    def _do_spool_status(self, params):
        '''Provides the status of output spooling'''
        status = 'started' if Framework._spool else 'stopped'
        self.output(f"Output spooling is {status}.")

    def do_shell(self, params):
        '''Executes shell commands'''
        if not params:
            self.help_shell()
            return
        proc = subprocess.Popen(params, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, stdin=subprocess.PIPE)
        self.output(f"Command: {params}")
        stdout = proc.stdout.read()
        stderr = proc.stderr.read()
        if stdout:
            print(f"{Colors.O}{self.to_unicode(stdout)}{Colors.N}", end='')
        if stderr:
            print(f"{Colors.R}{self.to_unicode(stderr)}{Colors.N}", end='')

    def do_dashboard(self, params):
        '''Displays a summary of activity'''
        rows = self.query('SELECT * FROM dashboard ORDER BY 1')
        if rows:
            # display activity table
            tdata = []
            for row in rows:
                tdata.append(row)
            self.table(tdata, header=['Module', 'Runs'], title='Activity Summary')
            # display summary results table
            tables = self.get_tables()
            tdata = []
            for table in tables:
                count = self.query(f"SELECT COUNT(*) FROM `{table}`")[0][0]
                tdata.append([table.title(), count])
            self.table(tdata, header=['Category', 'Quantity'], title='Results Summary')
        else:
            self.output('This workspace has no record of activity.')

    def do_df(self, params):
        '''Interfaces with the workspace's dataframe'''
        if not params:
            self.help_df()
            return
        arg, params = self._parse_params(params)
        if arg in self._parse_subcommands('df'):
            return getattr(self, '_do_df_'+arg)(params)
        else:
            self.help_df()

    def _do_df_head(self, params):
        '''Shows dataframe head'''
        if self.dataframe is None:
            self.output('This workspace has no dataframe.')
            return
        if not params:
            self.output(f'{os.linesep}{str(self.dataframe)}')
            return
        params = ''.join(params.split()).split(',')
        self.output(f'{os.linesep}{str(self.dataframe[params])}')

    def _do_df_columns(self, params):
        '''Shows dataframe columns'''
        if self.dataframe is None:
            self.output('This workspace has no dataframe.')
            return
        tdata = [[column] for column in self.dataframe.columns.to_list()]
        self.table(tdata, header=['Columns'])
        return

    def _do_df_save(self, params):
        '''Save dataframe'''
        if self.dataframe is None:
            self.output('This workspace has no dataframe.')
            return
        self._save_dataframe()

    # ##=======================================================================
    # HELP METHODS
    # ##=======================================================================

    def help_options(self):
        print(getattr(self, 'do_options').__doc__)
        print(f"{os.linesep}Usage: options <{'|'.join(self._parse_subcommands('options'))}> [...]{os.linesep}")

    def _help_options_set(self):
        print(getattr(self, '_do_options_set').__doc__)
        print(f"{os.linesep}Usage: options set <option> <value>{os.linesep}")

    def _help_options_unset(self):
        print(getattr(self, '_do_options_unset').__doc__)
        print(f"{os.linesep}Usage: options unset <option>{os.linesep}")

    def help_modules(self):
        print(getattr(self, 'do_modules').__doc__)
        print(f"{os.linesep}Usage: modules <{'|'.join(self._parse_subcommands('modules'))}> [...]{os.linesep}")

    def _help_modules_search(self):
        print(getattr(self, '_do_modules_search').__doc__)
        print(f"{os.linesep}Usage: modules search [<regex>]{os.linesep}")

    def _help_modules_load(self):
        print(getattr(self, '_do_modules_load').__doc__)
        print(f"{os.linesep}Usage: modules load <path>{os.linesep}")

    def help_show(self):
        options = sorted(self._get_show_names() + self.get_tables())
        print(getattr(self, 'do_show').__doc__)
        print(f"{os.linesep}Usage: show <{'|'.join(options)}>{os.linesep}")

    def help_db(self):
        print(getattr(self, 'do_db').__doc__)
        print(f"{os.linesep}Usage: db <{'|'.join(self._parse_subcommands('db'))}> [...]{os.linesep}")

    def _help_db_notes(self):
        print(getattr(self, '_do_db_notes').__doc__)
        print(f"{os.linesep}Usage: db note <table> [<rowid(s)> <note>]{os.linesep}")
        print(f"rowid(s) => ',' delimited values or '-' delimited ranges representing rowids{os.linesep}")

    def _help_db_insert(self):
        print(getattr(self, '_do_db_insert').__doc__)
        print(f"{os.linesep}Usage: db insert <table> [<values>]{os.linesep}")
        print(f"values => '~' delimited string representing column values (exclude rowid, module){os.linesep}")

    def _help_db_delete(self):
        print(getattr(self, '_do_db_delete').__doc__)
        print(f"{os.linesep}Usage: db delete <table> [<rowid(s)>]{os.linesep}")
        print(f"rowid(s) => ',' delimited values or '-' delimited ranges representing rowids{os.linesep}")

    def _help_db_query(self):
        print(getattr(self, '_do_db_query').__doc__)
        print(f"{os.linesep}Usage: db query <sql>{os.linesep}")

    def help_script(self):
        print(getattr(self, 'do_script').__doc__)
        print(f"{os.linesep}Usage: script <{'|'.join(self._parse_subcommands('script'))}> [...]{os.linesep}")

    def _help_script_record(self):
        print(getattr(self, '_do_script_record').__doc__)
        print(f"{os.linesep}Usage: script record <filename>{os.linesep}")

    def _help_script_execute(self):
        print(getattr(self, '_do_script_execute').__doc__)
        print(f"{os.linesep}Usage: script execute <filename>{os.linesep}")

    def help_spool(self):
        print(getattr(self, 'do_spool').__doc__)
        print(f"{os.linesep}Usage: spool <{'|'.join(self._parse_subcommands('spool'))}> [...]{os.linesep}")

    def _help_spool_start(self):
        print(getattr(self, '_do_spool_start').__doc__)
        print(f"{os.linesep}Usage: spool start <filename>{os.linesep}")

    def help_shell(self):
        print(getattr(self, 'do_shell').__doc__)
        print(f"{os.linesep}Usage: [shell|!] <command>{os.linesep}")

    def help_df(self):
        print(getattr(self, 'do_df').__doc__)
        print(f"{os.linesep}Usage: df <{'|'.join(self._parse_subcommands('df'))}> [...]{os.linesep}")

    def _help_df_head(self):
        print(getattr(self, '_do_df_head').__doc__)
        print(f"{os.linesep}Usage: head [<column(s)>]{os.linesep}")
        print(f"columns(s) => ',' delimited values{os.linesep}")

    def _help_df_columns(self):
        print(getattr(self, '_do_df_columns').__doc__)
        print(f"{os.linesep}Usage: columns {os.linesep}")

    def _help_df_save(self):
        print(getattr(self, '_do_df_save').__doc__)
        print(f"{os.linesep}Usage: save{os.linesep}")

    # ##=======================================================================
    # COMPLETE METHODS
    # ##=======================================================================

    def complete_options(self, text, line, *ignored):
        arg, params = self._parse_params(line.split(' ', 1)[1])
        subs = self._parse_subcommands('options')
        if arg in subs:
            return getattr(self, '_complete_options_'+arg)(text, params)
        return [sub for sub in subs if sub.startswith(text)]

    def _complete_options_list(self, text, *ignored):
        return []

    def _complete_options_set(self, text, *ignored):
        return [x for x in self.options if x.startswith(text.upper())]
    _complete_options_unset = _complete_options_set

    def complete_modules(self, text, line, *ignored):
        arg, params = self._parse_params(line.split(' ', 1)[1])
        subs = self._parse_subcommands('modules')
        if arg in subs:
            return getattr(self, '_complete_modules_'+arg)(text, params)
        return [sub for sub in subs if sub.startswith(text)]

    def _complete_modules_search(self, text, *ignored):
        return []

    def _complete_modules_load(self, text, *ignored):
        return [x for x in Framework._loaded_modules if x.startswith(text)]

    def complete_show(self, text, line, *ignored):
        options = sorted(self._get_show_names() + self.get_tables())
        return [x for x in options if x.startswith(text)]

    def complete_db(self, text, line, *ignored):
        arg, params = self._parse_params(line.split(' ', 1)[1])
        subs = self._parse_subcommands('db')
        if arg in subs:
            return getattr(self, '_complete_db_'+arg)(text, params)
        return [sub for sub in subs if sub.startswith(text)]

    def _complete_db_insert(self, text, *ignored):
        return [x for x in sorted(self.get_tables()) if x.startswith(text)]
    _complete_db_notes = _complete_db_delete = _complete_db_insert

    def _complete_db_query(self, text, *ignored):
        return []
    _complete_db_schema = _complete_db_query

    def complete_script(self, text, line, *ignored):
        arg, params = self._parse_params(line.split(' ', 1)[1])
        subs = self._parse_subcommands('script')
        if arg in subs:
            return getattr(self, '_complete_script_'+arg)(text, params)
        return [sub for sub in subs if sub.startswith(text)]

    def _complete_script_record(self, text, *ignored):
        return []
    _complete_script_execute = _complete_script_status = _complete_script_stop = _complete_script_record

    def complete_spool(self, text, line, *ignored):
        arg, params = self._parse_params(line.split(' ', 1)[1])
        subs = self._parse_subcommands('spool')
        if arg in subs:
            return getattr(self, '_complete_spool_'+arg)(text, params)
        return [sub for sub in subs if sub.startswith(text)]

    def _complete_spool_start(self, text, *ignored):
        return []
    _complete_spool_status = _complete_spool_stop = _complete_spool_start

    def complete_df(self, text, line, *ignored):
        arg, params = self._parse_params(line.split(' ', 1)[1])
        subs = self._parse_subcommands('df')
        if arg in subs:
            return getattr(self, '_complete_df_'+arg)(text, params)
        return [sub for sub in subs if sub.startswith(text)]
