"""
Copyright (c) 2012-2021 Tim Tomes

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

from internals.core import base
from internals.core.web.db import Tasks
from rq import get_current_job
import traceback

# These tasks exist outside the web directory to avoid loading the entire
# application (which reloads the framework) on every task execution.

def run_module(workspace, module):

    results = {}
    try:
        # instantiate important objects
        job = get_current_job()
        computist = base.computist(check=False, analytics=False, marketplace=False)
        computist.start(base.Mode.JOB, workspace=workspace)
        tasks = Tasks(computist)
        # update the task's status
        tasks.update_task(job.get_id(), status=job.get_status())
        # execute the task
        module = computist._loaded_modules.get(module)
        module.run()
    except Exception as e:
        results["error"] = {
            "type": str(type(e)),
            "message": str(e),
            "traceback": traceback.format_exc(),
        }
    results["summary"] = module._summary_counts
    # update the task's status and results
    tasks.update_task(job.get_id(), status="finished", result=results)
    return results
