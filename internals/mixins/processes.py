'''
Copyright (c) 2021 Jan William Johnsen

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

import multiprocessing
import pandas as pd
import numpy as np
from datetime import timedelta
from time import process_time
from functools import partial


class ProcessingMixin():
    def _processes_wrapper(self, function, data, *args):
        ''' Wrapper for the worker method defined in the module. Handles
        calling the actual worker, cleanly exiting upon interrupt, and passing
        exceptions back to the main process.'''
        process_count = self._global_options['processes']
        # split the data the queue from the user-defined iterable
        data = np.array_split(data, process_count)
        # launch the processes
        with multiprocessing.Pool(processes=process_count) as pool:
            results = pool.map_async(partial(function, *args), data)
            pool.close()
            pool.join()
        return pd.concat(results.get())

    def processes(self, function, data, *args):
        # disable multiprocessing in debug mode
        if self._global_options['verbosity'] >= 2:
            # call the process method in serial
            return function(data, *args)
        # begin multiprocessing code
        return self._processes_wrapper(function, data, *args)

    # alternative time display
    # m, s = divmod(seconds, 60)
    # h, m = divmod(m, 60)
    # print(f'{h:d}:{m:02d}:{s:02d}')
    def compare_serial_parallel(self, function, data, *args):
        # time and execute serial function
        start = process_time()
        serial_results = function(data, *args)
        end = process_time()
        duration = timedelta(seconds=end-start)
        self.output(f'Serial executed in {duration}.')

        # time and execute parallel function
        start = process_time()
        parallel_results = self._processes_wrapper(function, data, *args)
        end = process_time()
        duration = timedelta(seconds=end-start)
        self.output(f'Parallel executed in {duration}.')

        # compare results to check if equal
        equal = serial_results.equals(parallel_results)
        self.output(f'Serial and parallel data is equal: {equal}.')
        return serial_results
