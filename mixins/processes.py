"""
Copyright (c) 2021 Jan William Johnsen
2021-03-19: Simplified this code so modules no longer must sort *args in the
beginning of execution. It also has better exception handling. Added a progress
bar to indicate the processing progress for chunks.

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

import os
import sys
import time
import signal
import pandas as pd
import numpy as np
import multiprocessing as mp
from datetime import timedelta
from time import perf_counter
from importlib import util
if util.find_spec('tqdm'):
    from tqdm import tqdm

# https://github.com/ContinuumIO/anaconda-issues/issues/905
os.environ['FOR_DISABLE_CONSOLE_CTRL_HANDLER'] = '1'


class ProcessingMixin():

    def processes(self, func, iterable, *args):
        # disable multiprocessing in debug mode
        if self._global_options["verbosity"] >= 2:
            # call the process method in serial
            return func(iterable, *args)
        # begin multiprocessing code
        process_count = self._global_options["processes"]
        # split the data the queue from the user-defined iterable
        n = self.chunksize(process_count, len(iterable))
        data = [(func, chunk, *args) for chunk in np.array_split(iterable, n)]
        try:
            # launch the processes
            pool = mp.Pool(processes=process_count,
                           initializer=self.initialize)
            # chunksize=1 to prevent internal starmap_async chunking
            result = pool.starmap_async(self._process_wrapper, data,
                                        chunksize=1)
            pool.close()
            if "tqdm" in sys.modules:
                pbar = tqdm(leave=False, total=n)
                done = progress = 0
                # display progress bar
                while done < n:
                    progress = (n - result._number_left) - done
                    done += progress
                    pbar.update(progress)
                    time.sleep(0.1)
                pbar.close()
            pool.join()
        except KeyboardInterrupt:
            print("")
            self.error("Ok. Terminating processes...")
            pool.close()
            pool.terminate()
            raise
        result = result.get()
        if isinstance(result[0], pd.Series):
            result = pd.concat(result)
            result.sort_index(inplace=True)
        return result

    def compare_serial_parallel(self, func, iterable, *args):
        # time and execute serial function
        start = perf_counter()
        serial_results = func(iterable, *args)
        end = perf_counter()
        duration = timedelta(seconds=end-start)
        self.output(f"Serial executed in {duration}.")
        # time and execute parallel function
        start = perf_counter()
        parallel_results = self.processes(func, iterable, *args)
        end = perf_counter()
        duration = timedelta(seconds=end-start)
        self.output(f"Parallel executed in {duration}.")
        # compare results to check if equal
        equal = serial_results.equals(parallel_results)
        self.output(f"Serial and parallel result is equal: {equal}.")
        return serial_results

    @staticmethod
    def _process_wrapper(func, iterable, *args):
        return func(iterable, *args)

    @staticmethod
    def chunksize(n_workers, len_iterable, factor=8):
        """Calculate chunksize.
        Resembles mp.pool.Pool._map_async source code."""
        chunksize, extra = divmod(len_iterable, n_workers * factor)
        if extra:
            chunksize += 1
        return chunksize

    @staticmethod
    def initialize():
        signal.signal(signal.SIGINT, signal.SIG_IGN)
