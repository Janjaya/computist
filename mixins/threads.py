"""
Copyright (c) 2012-2021 Tim Tomes
Modifications Copyright (c) 2021 Jan William Johnsen

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

from queue import Queue, Empty
import threading
import time


class ThreadingMixin(object):
    def _thread_wrapper(self, *args):
        """ Wrapper for the worker method defined in the module. Handles
        calling the actual worker, cleanly exiting upon interrupt, and passing
        exceptions back to the main process."""
        thread_name = threading.current_thread().name
        self.debug(f"THREAD => {thread_name} started.")
        while not self.stopped.is_set():
            try:
                # use the get_nowait() method for retrieving a queued item to
                # prevent the thread from blocking when the queue is empty
                obj = self.q.get_nowait()
            except Empty:
                continue
            try:
                # launch the public module_thread method
                self.module_thread(obj, *args)
            except Exception:
                # handle exceptions local to the thread
                self.print_exception(
                    f"(thread={thread_name}, object={repr(obj)})"
                )
            finally:
                self.q.task_done()
        self.debug(f"THREAD => {thread_name} exited.")

    # sometimes a keyboardinterrupt causes a race condition between when
    # the self.q.task_done() call above and the self.q.empty() call below,
    # causing all the threads to hang. introducing the time.sleep(.7) call
    # below reduces the likelihood of encountering the race condition.

    def thread(self, *args):
        # disable threading in debug mode
        if self._global_options["verbosity"] >= 2:
            # call the thread method in serial for each input
            for item in args[0]:
                self.module_thread(item, *args[1:])
            return
        # begin threading code
        thread_count = self._global_options["threads"]
        self.stopped = threading.Event()
        self.exc_info = None
        self.q = Queue()
        # populate the queue from the user-defined iterable. should be done
        # before the threads start so they have something to process right away
        for item in args[0]:
            self.q.put(item)
        # launch the threads
        threads = []
        for i in range(thread_count):
            t = threading.Thread(target=self._thread_wrapper, args=args[1:])
            threads.append(t)
            t.setDaemon(True)
            t.start()
        # hack to catch keyboard interrupts
        try:
            while not self.q.empty():
                time.sleep(.7)
        except KeyboardInterrupt:
            self.error("Ok. Waiting for threads to exit...")
            # interrupt condition
            # set the event flag to trigger an exit for all threads
            self.stopped.set()
            # prevent the module from returning to the interpreter
            # until all threads have exited
            for t in threads:
                t.join()
            raise
        self.q.join()
        # normal condition
        # set the event flag to trigger an exit for all threads
        # the threads are no longer needed once all the data has been processed
        self.stopped.set()
