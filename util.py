"""
    This file contains some util definitions for 
    testrunner.  In particular
    - functions to help interpret the output of SMT solvers
    - functions to kill processes
"""

import logging
import os
import re
from enum import Enum
from datetime import datetime
import time
import psutil

from typing import (
    Callable,
    Iterable,
    Tuple,
    List,
    TypeVar,
    Union,
)
import itertools

T = TypeVar('T')

# Our scripts are commonly used for understanding output from SMT solvers
class Satisfiablity(str, Enum):
    SAT = "SAT"
    UNSAT = "UNSAT"
    UNKNOWN = "UNKNOWN"  # Actual result unknown
    UNSURE = "UNSURE"  # Could not find satisfiability


def satisfiability_of_output(output: str) -> Satisfiablity:
    # TODO liklely faster to do 'sat' in output.lower() or something
    if re.search('Unsat', output):
        return Satisfiablity.UNSAT
    elif re.search('Sat', output):
        return Satisfiablity.SAT
    elif re.search('Unknown', output):
        return Satisfiablity.UNKNOWN
    return Satisfiablity.UNSURE


def valid_smt(filename: Union[bytes, str]) -> bool:
    if os.fspath(filename).endswith('.smt2'):
        return True
    else:
        return False

def exclude(bad_values: List[str]) -> Callable[[Union[bytes, str]], bool]:
    def do_exclusion(dirname: Union[bytes, str]) -> bool:
        if type(dirname) == bytes:
            dirname = dirname.decode('utf8')
        pathname = os.path.basename(os.fspath(dirname))
        if pathname == '':
            pathname = os.path.basename(os.fspath(dirname[:-1]))
        return pathname not in bad_values
    return do_exclusion


def partition(pred: Callable[[T], bool], iterable: Iterable[T]) -> Tuple[List[T], List[T]]:
    "Use a predicate to partition entries into true entries and false entries"
    # partition(is_odd, range(10)) --> 1 3 5 7 9 and 0 2 4 6 8
    t1, t2 = itertools.tee(iterable)
    return list(filter(pred, t2)), list(itertools.filterfalse(pred, t1))


def now_string() -> str:
    """Returns the time in a %Y-%m-%d-%H-%M-%S formatted string"""
    now = datetime.now()
    return now.strftime("%Y-%m-%d-%H-%M-%S")


def setup_logging_default():
    logging.basicConfig(filename=f'log-{now_string()}.txt', level=logging.INFO)


def setup_logging_debug():
    logging.basicConfig(filename=f'log-{now_string()}.txt', level=logging.DEBUG)


def flatten(lst):
    """Flattens a list so that any elements that were lists are replaces by the elements of that list.
    ex: [1, [[2], 3]] -> [1, 2, 3]"""

    output = []
    for x in lst:
        if type(x) != list:
            output.append(x)
        else:
            output.extend(flatten(x))
    return output

# stuff about killing processes
# sometimes we want to kill all child processes and sometimes just Z3

def kill_child_processes():
    # kill all child processes
    current_process = psutil.Process()
    children = current_process.children(recursive=True)
    children.sort(key=lambda x: x.pid, reverse=True)  # NOTE: Why is this done? Can it be skipped?
    for c in children:
        logging.debug("Need to kill: " + str(c.pid) + " " + c.name() + "\n")
    for c in children:
        logging.debug("Trying to kill: " + str(c.pid) + " " + c.name() + "\n")
        kill_process(c, c.name())
    if children:
        wait_for_kill()

def wait_for_kill():
    # have to wait to make sure processes killed
    # When the time was set to 15 seconds, there's still a small possibility of Z3 process
    # hasn't been cleaned up yet. We increased it to 30 seconds to avoid that.
    time.sleep(30)  # seconds

# Kill only lingering Z3 processes
def kill_z3():
    for proc in psutil.process_iter():
        if 'z3' in proc.name().lower():
            # We found a z3 process
            logging.warn('Z3 is still running... killing')
            kill_process(proc,proc.name())
            wait_for_kill()

def check_process_running(process_name):
    """
    Check if there is any running process that contains the given name processName.
    from: https://thispointer.com/python-check-if-a-process-is-running-by-name-and-find-its-process-id-pid/
    """
    process_name = process_name.lower()
    # Iterate over the all the running process
    for proc in psutil.process_iter():
        try:
            # Check if process name contains the given name string.
            if process_name in proc.name().lower():
                logging.debug(f"process '{process_name}' exists: " + str(proc.pid))
                return True
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            pass
    return False

def kill_process(p, name):
    """Kills a process"""
    try:
        # occasionally the child process dies by itself and this then
        # causes an exception b/c child.pid does not exist
        # so the try/except is needed
        p.kill()
        # the following seems to have processlookup errors even though process
        # exists
        # os.killpg(p.pid, signal.SIGKILL)
        logging.info("killed: " + name + "\n")
    except ProcessLookupError:
        logging.warning("os.kill could not find: " + str(p.pid) + "\n")
        if check_process_running(name):
            logging.error(f'process {p.pid} was unable to be killed, but is still running!')
            exit(1)
    except Exception as ex:
        template = "An exception of type {0} occurred. Arguments:\n{1!r}"
        message = template.format(type(ex).__name__, ex.args)
        logging.exception(message + '\n' + "Did not kill: " + str(p.pid) + " " + name)


