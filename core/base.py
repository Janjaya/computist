"""
Copyright (c) 2012-2021 Tim Tomes
Modifications Copyright (c) 2021 Jan William Johnsen
2021-02-23: Updated to handle in-memory Pandas dataframe between modules and
snapshots now copies the data stored on disk.
2021-02-28: 1) Improved snapshot functions by using Subversion (SVN)
repositories. This is more efficient than file copying (leads to data
duplication), but keep the snapshot copy function for anyone without SVN.
SVN was chosen because of the xdelta algorithm which compute differences
between strings of bytes (and not strings of characters). 2) Added function to
check for updates in the Github repository.
2021-03-07: The imp module is deprecated since version 3.4 in favor of
importlib.

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
"""

__author__ = "Jan William Johnsen (@frozenbeer)"

from datetime import datetime
from pathlib import Path
from threading import Lock
from importlib.machinery import SourceFileLoader
from importlib import util
import os
import re
import shutil
import sys
import builtins
import platform
if util.find_spec('svn'):
    import svn.remote
    import svn.admin
# import framework libs
from core import framework
from core.constants import BANNER

__version__ = ""

# set the __version__ variable based on the VERSION file
exec(open(os.path.join(
    Path(os.path.abspath(__file__)).parents[1], "VERSION")
).read())

_print_lock = Lock()


def spool_print(*args, **kwargs):
    """
    Spooling system. Using stdout to spool causes tab complete issues.
    Override print function. Use a lock for thread safe console and spool
    output.
    """
    with _print_lock:
        if framework.Framework._spool:
            framework.Framework._spool.write(f"{args[0]}{os.linesep}")
            framework.Framework._spool.flush()
        # disable terminal output for server jobs
        if framework.Framework._mode == Mode.JOB:
            return
        # new print function must still use the old print function for backup
        builtins._print(*args, **kwargs)


# make a builtin backup of the original print function
builtins._print = print
# override the builtin print function with the new print function
builtins.print = spool_print


# #============================================================================
# BASE CLASS
# #============================================================================

class Computist(framework.Framework):

    def __init__(self, check=True):
        framework.Framework.__init__(self, "base")
        self._name = "computist"
        self._prompt_template = "{}[{}] > "
        self._base_prompt = self._prompt_template.format("", self._name)
        # set toggle flags
        self._check = check
        self._revisioning = True if sys.modules.get('svn.admin') else False
        # set path variables
        self.app_path = framework.Framework.app_path = sys.path[0]
        self.core_path = framework.Framework.core_path = os.path.join(
            self.app_path, "core"
        )
        self.home_path = framework.Framework.home_path = os.path.join(
            os.path.expanduser("~"), ".computist"
        )
        self.mod_path = framework.Framework.mod_path = os.path.join(
            self.app_path, "modules"
        )
        self.data_path = framework.Framework.data_path = os.path.join(
            self.app_path, "data"
        )
        self.spaces_path = framework.Framework.spaces_path = os.path.join(
            self.home_path, "workspaces"
        )
        self.cache_path = framework.Framework.cache_path
        self.repository_path = framework.Framework.repository_path
        self.dataframe = framework.Framework.dataframe

    def start(self, mode, workspace="default"):
        # initialize framework components
        self._mode = framework.Framework._mode = mode
        self._init_global_options()
        self._init_home()
        self._init_workspace(workspace)
        self._check_version()
        if self._mode == Mode.CON:
            self._print_banner()
            self.cmdloop()

    # ##=======================================================================
    # SUPPORT METHODS
    # ##=======================================================================

    def _init_global_options(self):
        self.options = self._global_options
        self.register_option(
            name="threads",
            value=10,
            required=True,
            description="number of threads"
        )
        self.register_option(
            name="processes",
            value=4,
            required=True,
            description="number of processes"
        )
        self.register_option(
            name="verbosity",
            value=1,
            required=True,
            description="verbosity level (0 = minimal, 1 = verbose, 2 = debug)"
        )
        self.register_option(
            name="timeout",
            value=10,
            required=True,
            description="socket timeout (seconds)"
        )
        self.register_option(
            name="user-agent",
            value=f"Computist/v{__version__.split('.')[0]}",
            required=True,
            description="user-agent string"
        )

    def _init_home(self):
        # initialize home folder
        if not os.path.exists(self.home_path):
            os.makedirs(self.home_path)

    def _check_version(self):
        if self._check:
            pattern = r"'(\d+\.\d+\.\d+[^']*)'"
            remote = 0
            local = 0
            try:
                remote = re.search(
                    pattern,
                    self.request(
                        "GET",
                        ("https://raw.githubusercontent.com/"
                         + "Janjaya/computist/main/VERSION")).text
                ).group(1)
                local = re.search(pattern, open('VERSION').read()).group(1)
            except Exception as e:
                self.error(f"Version check failed ({type(e).__name__}).")
                # self.print_exception()
            if remote != local:
                self.alert(
                    ("Your version of Computist does not match "
                     + "the latest release.")
                )
                self.alert("Please consider updating before further use.")
                self.output(f"Remote version:  {remote}")
                self.output(f"Local version:   {local}")
        else:
            self.alert('Version check disabled.')

    def _print_banner(self):
        banner = BANNER
        # banner_len = len(max(banner.split(os.linesep), key=len))
        author = "{0}".format(
            (f"{framework.Colors.O}[{self._name} v{__version__}, "
             + f"{__author__}]{framework.Colors.N}")
        )
        print(banner)
        print(author)
        print("")
        counts = [(len(self._loaded_category[x]), x)
                  for x in self._loaded_category]
        if counts:
            count_len = len(max([self.to_unicode_str(x[0])
                                 for x in counts], key=len))
            for count in sorted(counts, reverse=True):
                cnt = f"[{count[0]}]"
                print(
                    (f"{framework.Colors.B}{cnt.ljust(count_len+2)} "
                     + f"{count[1].title()} modules{framework.Colors.N}")
                )
        else:
            self.alert("No modules enabled/installed.")
        print("")

    # ##=======================================================================
    # WORKSPACE METHODS
    # ##=======================================================================

    def _init_workspace(self, workspace):
        if not workspace:
            return
        path = os.path.join(self.spaces_path, workspace)
        self.cache_path = os.path.join(self.spaces_path, workspace, "cache")
        self.workspace = framework.Framework.workspace = path
        self.dataframe = framework.Framework.dataframe = None
        if not os.path.exists(path):
            os.makedirs(path)
            os.makedirs(self.cache_path)
            self._create_db()
            self._create_repository()
        else:
            self._migrate_db()
        # set workspace prompt
        self.prompt = self._prompt_template.format(
            self._base_prompt[:-3], self.workspace.split(os.path.sep)[-1]
        )
        # load workspace configuration
        self._load_config()
        # reload modules after config to populate options
        self._load_modules()
        # load workspace data
        self.load_dataframe()
        return True

    def remove_workspace(self, workspace):
        # http://www.voidspace.org.uk/downloads/pathutils.py
        def errorRemoveReadonly(func, path, exc):
            # if the error is due to an access error (read only file)
            # it attempts to add write permission and then retries.
            import stat
            if not os.access(path, os.W_OK):
                # is the error an access error?
                os.chmod(path, stat.S_IWUSR)
                func(path)
            else:
                raise
        path = os.path.join(self.spaces_path, workspace)
        try:
            shutil.rmtree(path, onerror=errorRemoveReadonly)
        except OSError:
            return False
        if workspace == self.workspace.split(os.path.sep)[-1]:
            self._init_workspace("default")
        return True

    def _get_workspaces(self):
        workspaces = []
        path = self.spaces_path
        for name in os.listdir(path):
            if os.path.isdir(os.path.join(path, name)):
                workspaces.append(name)
        return workspaces

    def _get_snapshots(self):
        snapshots = self.query("SELECT SNAPSHOT FROM snapshots")
        return [snapshot[0] for snapshot in snapshots]

    def _db_version(self):
        return self.query("PRAGMA user_version")[0][0]

    def _create_db(self):
        self.query(
            ("CREATE TABLE IF NOT EXISTS dashboard (module TEXT PRIMARY KEY, "
             + "runs INT)")
        )
        self.query(
            ("CREATE TABLE IF NOT EXISTS snapshots (snapshot TEXT PRIMARY KEY,"
             + " date TEXT, revision TEXT, notes TEXT, module TEXT)")
        )
        self.query("PRAGMA user_version = 1")  # always latest DB version

    def _migrate_db(self):
        db_orig = self._db_version()
        if self._db_version() == 0:
            tmp = self.get_random_str(20)
            self.query(f"ALTER TABLE snapshots RENAME TO {tmp}")
            self.query(
                ("CREATE TABLE snapshots (snapshot TEXT PRIMARY KEY, "
                 + "date TEXT, revision TEXT, notes TEXT, module TEXT)")
            )
            self.query(
                ("INSERT INTO snapshots (snapshot, date, notes, module) "
                 + f"SELECT snapshot, date, notes, module FROM {tmp}")
            )
            self.query(f"DROP TABLE {tmp}")
            self.query("PRAGMA user_version = 1")
        if db_orig != self._db_version():
            self.alert(
                f"Database upgraded to version {self._db_version(self)}."
            )

    def _create_repository(self):
        if self._revisioning:
            self.repository_path = framework.Framework.repository_path = \
                os.path.join(self.workspace, "repository")
            # create SVN repository folder
            os.makedirs(self.repository_path)
            admin = svn.admin.Admin()
            admin.create(self.repository_path)
            # create local working copy of "remote" repository
            repository = svn.remote.RemoteClient(
                "file:///" + self.repository_path
            )
            repository.checkout(self.workspace)

    # ##=======================================================================
    # MODULE METHODS
    # ##=======================================================================

    def _load_modules(self):
        self._loaded_category = {}
        self._loaded_modules = framework.Framework._loaded_modules = {}
        # crawl the module directory and build the module tree
        for dirpath, dirnames, filenames in os.walk(self.mod_path,
                                                    followlinks=True):
            # remove hidden files and directories
            filenames = [f for f in filenames if not f[0] == "."]
            dirnames[:] = [d for d in dirnames if not d[0] == "."]
            if len(filenames) > 0:
                for filename in [f for f in filenames
                                 if f.endswith(".py") and not
                                 f.endswith("_mp.py")]:
                    self._load_module(dirpath, filename)

    def _load_module(self, dirpath, filename):
        mod_name = filename.split(".")[0]
        if platform.system() == "Windows":
            mod_category = re.search("modules\\\\(.*)", dirpath).group(1)
            mod_dispname = f"{os.path.sep}".join(
                re.split("modules", dirpath[2:])[-1][1:].split(
                    f" {os.path.sep} ") + [mod_name]
                )
        else:
            mod_dispname = f"{os.path.sep}".join(
                re.split(f"{os.path.sep}modules{os.path.sep}",
                         dirpath)[-1].split(f" {os.path.sep} ") + [mod_name])
            mod_category = re.search("modules([^/]*)", dirpath).group(1)
        mod_loadname = mod_dispname.replace(f"{os.path.sep}", "_")
        mod_loadpath = os.path.join(dirpath, filename)
        try:
            # import the module into memory
            SourceFileLoader(mod_loadname, mod_loadpath).load_module()
            __import__(mod_loadname)
            # add the module to the framework's loaded modules
            self._loaded_modules[mod_dispname] = sys.\
                modules[mod_loadname].Module(mod_dispname)
            self._categorize_module(mod_category, mod_dispname)
            # return indication of success to support module reload
            return True
        except ImportError as e:
            # notify the user of missing dependencies
            self.error(
                (f"Module '{mod_dispname}' disabled. "
                 + f"Dependency required: '{self.to_unicode_str(e)[16:]}'")
            )
        except Exception:
            # notify the user of errors
            self.print_exception()
            self.error(f"Module '{mod_dispname}' disabled.")
        # remove the module from the framework's loaded modules
        self._loaded_modules.pop(mod_dispname, None)
        self._categorize_module("disabled", mod_dispname)

    def _categorize_module(self, category, module):
        if category not in self._loaded_category:
            self._loaded_category[category] = []
        self._loaded_category[category].append(module)

    # ##=======================================================================
    # COMMAND METHODS
    # ##=======================================================================

    def do_workspaces(self, params):
        """Manages workspaces"""
        if not params:
            self.help_workspaces()
            return
        arg, params = self._parse_params(params)
        if arg in self._parse_subcommands("workspaces"):
            return getattr(self, "_do_workspaces_"+arg)(params)
        else:
            self.help_workspaces()

    def _do_workspaces_list(self, params):
        """Lists existing workspaces"""
        rows = []
        for workspace in self._get_workspaces():
            db_path = os.path.join(self.spaces_path, workspace, "data.db")
            modified = datetime.fromtimestamp(
                os.path.getmtime(db_path)
            ).strftime("%Y-%m-%d %H:%M:%S")
            rows.append((workspace, modified))
        rows.sort(key=lambda x: x[0])
        self.table(rows, header=["Workspaces", "Modified"])

    def _do_workspaces_create(self, params):
        """Creates a new workspace"""
        if not params:
            self._help_workspaces_create()
            return
        if not self._init_workspace(params):
            self.output(f"Unable to create '{params}' workspace.")

    def _do_workspaces_load(self, params):
        """Loads an existing workspace"""
        if not params:
            self._help_workspaces_load()
            return
        if params in self._get_workspaces():
            if not self._init_workspace(params):
                self.output(f"Unable to initialize '{params}' workspace.")
        else:
            self.output("Invalid workspace name.")

    def _do_workspaces_remove(self, params):
        """Removes an existing workspace"""
        if not params:
            self._help_workspaces_remove()
            return
        if not self.remove_workspace(params):
            self.output(f"Unable to remove '{params}' workspace.")

    def do_snapshots(self, params):
        """Manages workspace snapshots"""
        if not params:
            self.help_snapshots()
            return
        arg, params = self._parse_params(params)
        if arg in self._parse_subcommands("snapshots"):
            return getattr(self, "_do_snapshots_"+arg)(params)
        else:
            self.help_snapshots()

    def _do_snapshots_list(self, params):
        """Lists existing snapshots"""
        snapshots = self._get_snapshots()
        if snapshots:
            snapshots = self.query(
                "SELECT SNAPSHOT, DATE, NOTES FROM `snapshots`"
            )
            self.table(
                [list(s) for s in snapshots],
                header=["Snapshots", "Dates", "Notes"]
            )
        else:
            self.output("This workspace has no snapshots.")

    def _do_snapshots_take(self, params):
        """Takes a snapshot of the current environment"""
        ts = datetime.strftime(datetime.now(), "%Y%m%d%H%M%S")
        if self._revisioning:
            r = svn.local.LocalClient(self.workspace)
            r.commit(f"snapshot_{ts}")
            r.update()
            cr = r.info()["commit_revision"]
        else:
            # Simply copying data without a SVN repository leads to data
            # duplication and is less efficient for disk storage. Keep simple
            # copying for users without SVN installed. Hopefully they use it
            # for smaller datasets.
            cr = 0
            src = os.path.join(self.workspace, "data.json")
            dst = os.path.join(self.workspace, f"snapshot_{ts}.json")
            if os.path.exists(src):
                shutil.copyfile(src, dst)
        self.insert_snapshot(snapshot=f"{ts}", notes=params, revision=cr,
                             mute=True)
        self.output(f"Snapshot created: snapshot_{ts}")

    def _do_snapshots_load(self, params):
        """Loads an existing snapshot"""
        if not params:
            self._help_snapshots_load()
            return
        if params in self._get_snapshots():
            cr = self.query(
                ("SELECT revision FROM snapshots "
                 + f"WHERE snapshot LIKE '{params}'")
            )[0][0]
            if self._revisioning:
                r = svn.local.LocalClient(self.workspace)
                r.update(revision=cr)
            else:
                src = os.path.join(self.workspace, params + ".json")
                dst = os.path.join(self.workspace, "data.json")
                shutil.copyfile(src, dst)
            self.load_dataframe()
            self.output(f"Snapshot loaded: {params}")
        else:
            self.error(f"No snapshot named '{params}'.")

    def _do_snapshots_remove(self, params):
        """Removes a snapshot"""
        if not params:
            self._help_snapshots_remove()
            return
        if params in self._get_snapshots():
            if self._revisioning:
                self.alert("Cannot remove snapshot from repository.")
            else:
                path = os.path.join(self.workspace, params + ".json")
                if os.path.exists(path):
                    os.remove(path)
                self.query(
                    "DELETE FROM snapshots WHERE SNAPSHOT IS ?", (params,)
                )
            self.output(f"Snapshot removed: {params}")
        else:
            self.error(f"No snapshot named '{params}'.")

    def _do_modules_load(self, params):
        """Loads a module"""
        # validate global options before loading the module
        try:
            self._validate_options()
        except framework.FrameworkException as e:
            self.error(e)
            return
        if not params:
            self._help_modules_load()
            return
        # finds any modules that contain params
        modules = self._match_modules(params)
        # notify the user if none or multiple modules are found
        if len(modules) != 1:
            if not modules:
                self.error("Invalid module name.")
            else:
                self.output(f"Multiple modules match '{params}'.")
                self._list_modules(modules)
            return
        # load the module
        mod_dispname = modules[0]
        # loop to support reload logic
        while True:
            y = self._loaded_modules[mod_dispname]
            # send analytics information
            mod_loadpath = os.path.abspath(sys.modules[y.__module__].__file__)
            # return the loaded module if not in console mode
            if self._mode != Mode.CON:
                return y
            # begin a command loop
            y.prompt = self._prompt_template.format(
                self.prompt[:-3], mod_dispname.split(os.path.sep)[-1]
            )
            # store pointer to dataset
            y.dataframe = self.dataframe
            try:
                y.cmdloop()
            except KeyboardInterrupt:
                print("")
            # store new pointer to dataset
            self.dataframe = y.dataframe
            if y._exit == 1:
                return True
            if y._reload == 1:
                self.output("Reloading module...")
                # reload the module in memory
                is_loaded = self._load_module(
                    os.path.dirname(mod_loadpath),
                    os.path.basename(mod_loadpath))
                if is_loaded:
                    # reload the module in the framework
                    continue
                # shuffle category counts?
            break

    def _do_modules_reload(self, params):
        """Reloads installed modules"""
        self.output("Reloading modules...")
        self._load_modules()

    # ##=======================================================================
    # HELP METHODS
    # ##=======================================================================

    def help_workspaces(self):
        print(getattr(self, "do_workspaces").__doc__)
        print(f"{os.linesep}Usage: workspaces "
              f"<{'|'.join(self._parse_subcommands('workspaces'))}> "
              f"[...]{os.linesep}")

    def _help_workspaces_create(self):
        print(getattr(self, "_do_workspaces_create").__doc__)
        print(f"{os.linesep}Usage: workspace create <name>{os.linesep}")

    def _help_workspaces_load(self):
        print(getattr(self, "_do_workspaces_load").__doc__)
        print(f"{os.linesep}Usage: workspace load <name>{os.linesep}")

    def _help_workspaces_remove(self):
        print(getattr(self, "_do_workspaces_remove").__doc__)
        print(f"{os.linesep}Usage: workspace remove <name>{os.linesep}")

    def help_snapshots(self):
        print(getattr(self, "do_snapshots").__doc__)
        print(
            (f"{os.linesep}Usage: snapshots "
             + f"<{'|'.join(self._parse_subcommands('snapshots'))}> "
             + f"[...]{os.linesep}")
        )

    def _help_snapshots_load(self):
        print(getattr(self, "_do_snapshots_load").__doc__)
        print(f"{os.linesep}Usage: snapshots load <name>{os.linesep}")

    def _help_snapshots_remove(self):
        print(getattr(self, "_do_snapshots_remove").__doc__)
        print(f"{os.linesep}Usage: snapshots remove <name>{os.linesep}")

    # ##=======================================================================
    # COMPLETE METHODS
    # ##=======================================================================

    def complete_workspaces(self, text, line, *ignored):
        arg, params = self._parse_params(line.split(" ", 1)[1])
        subs = self._parse_subcommands("workspaces")
        if arg in subs:
            return getattr(self, "_complete_workspaces_"+arg)(text, params)
        return [sub for sub in subs if sub.startswith(text)]

    def _complete_workspaces_list(self, text, *ignored):
        return []
    _complete_workspaces_create = _complete_workspaces_list

    def _complete_workspaces_load(self, text, *ignored):
        return [x for x in self._get_workspaces() if x.startswith(text)]
    _complete_workspaces_remove = _complete_workspaces_load

    def complete_snapshots(self, text, line, *ignored):
        arg, params = self._parse_params(line.split(" ", 1)[1])
        subs = self._parse_subcommands("snapshots")
        if arg in subs:
            return getattr(self, "_complete_snapshots_"+arg)(text, params)
        return [sub for sub in subs if sub.startswith(text)]

    def _complete_snapshots_list(self, text, *ignored):
        return []
    _complete_snapshots_take = _complete_snapshots_list

    def _complete_snapshots_load(self, text, *ignored):
        return [x for x in self._get_snapshots() if x.startswith(text)]
    _complete_snapshots_remove = _complete_snapshots_load

    def _complete_modules_reload(self, text, *ignored):
        return []

# #============================================================================
# SUPPORT CLASSES
# #============================================================================


class Mode(object):
    """Contains constants that represent the state of the interpreter."""
    CON = 0  # console
    CLI = 1  # client
    WEB = 2  # web
    JOB = 3  # server

    def __init__(self):
        raise NotImplementedError("This class should never be instantiated.")
