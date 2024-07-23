"""
    This python script contains two important classes: TestRunner and its most commonly used subclass CSVTestRunner.  

    There is an example of how to use the class CSVTestRunner in example-simple.py .

    TestRunner repeatedly runs a single command with values substituted from a variety of options over the cross product of options.

    CSVTestRunner does the same, but allows the user to provide functions that interpret the output of each command into a line in a CSV output file.

    Here is an example of calling the CSVTestRunner:
    csvTestRunner = CSVTestRunner(
        command,  # command with placeholder for option values
        op,       # an option 
        numbers,  # an option 
        text,     # an option 
        csv_opt,  # an option 
        # keyword arguments 
        timeout=2000,  # in seconds
        output_file=outputfilepointer,  # csv output file
        string column names of outputs,
        fields_from_result = function to deal with timeouts -> returns an OptionDict,
        fields_from_timeout = function to deal with processing results -> returns an OptionDict
        ignore_fields = fields of input to not bother writing in CSV outputf
        )
    csvTestRunner.run()  # produces a CSV output file containing a line for each input 
                         # option combination plus values for the outputs of running 
                         # the command

    # This is the call to the CSVTestRunner to execute the runs
    with open(OUTPUT_FILE_NAME, 'w') as output_file:
        runner = tr.CSVTestRunner(
            COMMAND,  # command string with blanks to fill in
            compiler, # option
            model, # option
            timeout=TIMEOUT,
            output_file=output_file,
            fields_from_result=result_values, # how to interpret results of run
            fields_from_timeout=timeout_values,   # how to interpret timeouts
        )  
        runner.run(2)  # This will run every set of options twice

    The command is written as a string with python format string formatting.
    For example,     
        command = '{op} {number} {text} something else {model} -command {cmdnum}'
    with placeholders to substitute values for options named op, number, test, model, and cmdnum.

    Options for the values to substitute into the command are of the Option class.
    The testrunner initialization takes a variable number of option arguments (followed by keyword parameters).  Each option can be either:
    1) (string name of option, list of values of types str, int, float or bool)
    2) (string name, list of dictionary mapping strings to particular values as in 1)
    Item 2) above is for values that must be paired together (e.g., model + cmd#)
    Item 2) The string keys in the dictionary are treated as separate options, and should be the same for every element in the list
    Item 2) e.g [{'model': 'A', 'cmd#': 1}, {'model': 'B', 'cmd#': 2}] will run A on command 1, and B on 2, but NOT A on 2 or B on 1.
    Item 2) is often obtained from the row of a CSV file. e.g.,   
            csv_opt = CSVOption('csv_opt', 'test.csv')
    Values for an option can also be obtained form each line of a file using
            filename = FromFileOption('text', 'inputstrings.txt')
    If the options is to contain filenames accumulated across a directory, you can
    use the FilesOption, which collects names of files from a folder based on predicates.

"""


import csv
import os
import subprocess
import itertools
import functools
import logging
import sys
import typing
import shlex
from string import Formatter

import psutil
from tqdm.auto import tqdm

from typing import *
from time import monotonic as monotonic_timer
from .util import now_string, partition, setup_logging_debug

OptionInfo = Union[str, int, float, bool]
OptionValue = Union[OptionInfo, Dict[str, OptionInfo]]
"""A string, int, float or boolean value, or a dictionary containing multiple named string, int, float, or boolean values."""
# Option name to info
OptionDict = Dict[str, OptionValue]

CommandFunc = Callable[[OptionDict], List[str]]
# You should probably use a command function when your options contain a list
Command = Union[CommandFunc, str, List[str]]


class Option:
    """An option with one or more possible values to be iterated over when running tests."""
    # types of options are limited to str, int, float, bool (from OptionInfo above)

    def __init__(self, opt_name: str, option_values: List[OptionValue]):
        """`opt_name` is the name of this option, which can be referenced elsewhere.
        `option_values` are the option values to be iterated over (in order, should it matter). If the values are dictionaries, each value should likely contain the same keys.
        """
        self.opt_name = opt_name
        if type(option_values) != list:
            option_values = list(option_values)
        if len(option_values) == 0:
            raise ValueError("Cannot have no option values! In option " + self.opt_name)
        self.option_values: List[OptionInfo] = option_values

    def get_option_values(self) -> List[OptionValue]:
        """Returns a shallow copy of the option values"""
        return self.option_values.copy()

    def __iter__(self):
        return iter(self.option_values)

    def __len__(self):
        return len(self.option_values)

    @property
    def is_constant(self):
        return len(self.option_values) == 1

    def __str__(self):
        return f'<<{[x for x in self.get_option_values()]}>>'


class FromFileOption(Option):
    """Creates options where the value is each line of the given file."""

    def __init__(self, opt_name: str, filename):
        values = []
        with open(filename, 'r') as f:
            values = f.readlines()
        values = list(map(lambda x: x.strip(), values))
        super().__init__(opt_name, values)


class FilesOption(Option):
    """Creates an option where the value is every file in the given folder, optionally filtered."""

    def __init__(self, opt_name: str, folder_path: os.PathLike, recursive: bool = False,
                 folder_filter: Optional[Callable[[Union[bytes, str]], bool]] = None,
                 file_filter: Optional[Callable[[Union[bytes, str]], bool]] =None,
                 abs_path: bool = True,
                 ):
        """
        opt_name: the name of the option, which can be referenced elsewhere
        folder_path: the root folder to explore for files
        recursive: if true, look into subfolders for files
        folder_filter: A predicate on the folder name to filter folders
        file_filter: A predicate on the file name to filter files
        abs_path: True will return the absolute paths of the file, False will return paths relative to `folder_path`"""
        option_values = self._create_option_values(folder_path, recursive, folder_filter, file_filter, abs_path)
        super().__init__(opt_name, option_values)

    @staticmethod
    def _create_option_values(folder_path: os.PathLike, recursive: bool,
                              folder_filter: Optional[Callable[[Union[bytes, str]], bool]],
                              file_filter: Optional[Callable[[Union[bytes, str]], bool]],
                              abs_path: bool,
                              ) -> List[OptionInfo]:
        files_kept = []
        for root, dirs, files in os.walk(folder_path):  # https://docs.python.org/3.9/library/os.html#os.walk
            # Filter or keep files
            for file_name in files:
                file_name_from_root = os.path.join(root, file_name)
                if file_filter is None or file_filter(file_name_from_root):
                    if abs_path:
                        # This is the absolute path from the file system root
                        files_kept.append(os.path.abspath(file_name_from_root))
                    else:
                        files_kept.append(file_name_from_root)
                        
            # If we don't want to recurse into directories, skip them all
            if not recursive:
                dirs.clear()
            elif folder_filter is not None:
                #  Filter or keep folders for recursive calls
                dirs[:] = filter(lambda dir: folder_filter(os.path.join(root, dir)), dirs)
            
        return files_kept

class CSVOption(Option):
    """Reads in lines from a csv file.
    Creates an option for each column, and synchronizes values so each row is used as one command.
    
    If the csv file does not contain a header row, `all_headers` can be provided to label each column.
    If `kept_headers` is provided, only columns with headers in `kept_headers` are used.
    If `skip_initial_whitespace` is True, spaced immediately following the delimiter are ignored."""
    def __init__(self, opt_name: str, file_name: str,
                 all_headers: Optional[List[str]] = None,
                 kept_headers: Optional[List[str]] = None,
                 skip_initial_whitespace: bool = True):
        # Open the csv file
        with open(file_name, 'r') as csv_file:
            # Optionally label the columns
            if all_headers is None:
                reader = csv.DictReader(csv_file, skipinitialspace=skip_initial_whitespace)
            else:
                reader = csv.DictReader(csv_file, fieldnames=all_headers, skipinitialspace=skip_initial_whitespace)

            option_values = []
            for row in reader:
                # If we have kept headers, filter the row to only contain values in columns in kept_headers
                if kept_headers is not None:
                    # row = dict(filter(lambda k, v: k in kept_headers, row))
                    row = {key: row[key] for key in kept_headers}
                option_values.append(row)
        super().__init__(opt_name, option_values)




class TestRunner:
    """Abstract class to be overwritten. Runs commands for the cross product of each value in options."""
    # timeout is always a required value for a testrunner?
    def __init__(self, command: Command, *options: Option, timeout: int,
                 output_file: typing.TextIO = None):
        # Command should be formed for `Popen` but can have {kwarg} style formatting in place
        self.command = command
        # separate static and dynamic options
        static_options, self.dynamic_options = partition(lambda x: x.is_constant, options)
        # Flatten the static options
        self.static_option_values: OptionDict = self._flatten_options({opt.opt_name: opt.get_option_values()[0] for opt in static_options})
        # Add timeout to static options
        if 'timeout' not in self.static_option_values:
            self.static_option_values['timeout'] = timeout
        self.timeout = timeout
        self.output_file = output_file if output_file else sys.stdout

    @property
    def dynamic_option_names(self):
        """A list of the options containing more than one option value."""
        return list(map(lambda x: x.opt_name, self.dynamic_options))

    @staticmethod
    def format_command(command: Command, option_dict: OptionDict) -> List[str]:
        """Formats the command with the given options. If the command is a string, shlex is used to split it."""
        try:
            if type(command) == str:
                return shlex.split(command.format(**option_dict))
            elif type(command) == list:
                return list(map(lambda x: x.format(**option_dict), command))
            else:
                return command(option_dict)
        except KeyError as e:
            e.add_note("When formatting the command, an option was not found!")
            # Create a more verbose error message
            options_provided = sorted(list(option_dict.keys()))  # What options are passed to the command
            fieldnames = sorted([fname for _, fname, _, _ in Formatter().parse(command) if fname])  # What options does the command want
            e.add_note(f"The options provided are: {options_provided}.\nThe command expected: {fieldnames}.")
            in_fieldnames_only = [fname for fname in fieldnames if fname not in options_provided]
            if in_fieldnames_only:
                e.add_note(f"The following keys are missing: {in_fieldnames_only}")
            raise  # Re-raise the exception

    def get_num_commands_to_run(self):
        """Calculates the number of commands that will be run."""
        return functools.reduce(lambda x, y: x * len(y), self.dynamic_options, 1)

    def run(self, iterations: int = 1, skip: int = 0, retry_after_timeout=False):
        """
        Runs the given command over all the options `iterations` times.
        The first `skip` executions are skipped. `skip` counts command executions, not commands themselves.
        Therefore, if iterations is 2, skip=3 will skip the first command entirely and the second command will be run once.
        Calls `handle_result` and `handle_timeout` which can be overwritten to change behavior
        If `retry_after_timeout` is false, a command will not be run on the same set of option values
            for any additional iterations after it times out.
        """
        command_count = 0  # Counts each distinct command
        iteration_count = 0  # Counts each use of a command (each iteration)
        logging.info(f"Beginning test run at {now_string()}...")
        logging.debug(f"static options: {self.static_option_values}")
        dynamic_option_names = list(map(lambda x: x.opt_name, self.dynamic_options))  # Used to label the options each time they are generated

        num_commands_to_run = self.get_num_commands_to_run()  # How many commands will we be running
        num_command_calls = num_commands_to_run * iterations
        logging.info("Expecting %d commands, each run %d times. Total %d executions.",
                     num_commands_to_run, iterations, num_command_calls)
        try:
            # Get the next value in the cross product of the dynamic options and create a tqdm loading bar
            for dynamic_option_values in tqdm(itertools.product(*self.dynamic_options), desc="tests", total=num_commands_to_run, disable=self.output_file == sys.stdout):
                # Create a dictionary with option names -> option value for dynamic options then add static
                dynamic_option_values: Dict[str, OptionInfo] = dict(zip(dynamic_option_names, dynamic_option_values))
                dynamic_option_values = self._flatten_options(dynamic_option_values)  # Flatten any dicts of options
                option_values: OptionDict = dynamic_option_values.copy()
                option_values.update(self.static_option_values)  # Include the static options

                # Format the command
                formatted_command = self.format_command(self.command, option_values)

                command_count += 1
                logging.info(f"Command #{command_count}: {shlex.join(formatted_command)}")
                logging.debug('Dynamic values: ' + str(dynamic_option_values))
                for iteration_number in range(iterations):
                    iteration_count += 1
                    # Skip commands
                    if iteration_count <= skip:
                        continue
                    if skip and iteration_count == skip:
                        logging.info('Done skipping!')

                    try:
                        start = monotonic_timer()  # Start timing
                        result = subprocess.run(formatted_command, capture_output=True, text=True, timeout=self.timeout)  # Run the command
                        time_elapsed: float = monotonic_timer() - start  # End timing

                        logging.debug(f'Returned with code {result.returncode} in {time_elapsed:.4f} seconds')

                        # Handle a non-timeout run of the command
                        self.handle_result(option_values, result, time_elapsed)

                    except subprocess.TimeoutExpired as timeout_error:
                        logging.debug('Timed out')
                        self.handle_timeout(option_values, timeout_error)
                        # Break to the next command
                        if not retry_after_timeout:
                            break
                    # kill_child_processes()
        except:
            logging.critical("UNCAUGHT ERROR: Currently working on iteration count  #%d.", iteration_count,
                             exc_info=True)
            raise
        logging.info(f"Done at {now_string()}")

    @staticmethod
    def _flatten_options(option_values: OptionDict) -> OptionDict:
        flat = {}
        for key, value in option_values.items():
            if type(value) == dict:
                flat.update(value)
            else:
                flat[key] = value
        return flat

    def handle_timeout(self, options_values: OptionDict, timeout: subprocess.TimeoutExpired) -> None:
        """Absract method for what to do when a command times out."""
        pass

    def handle_result(self, option_values: OptionDict, result: subprocess.CompletedProcess, time_elapsed: float) -> None:
        """Abstract method for when a command returns without timing out."""
        pass

    def _filter_only_dynamic_options(self, options: OptionDict) -> OptionDict:
        """Collect only the dynamic options from an optiondict."""
        filtered_dict = {}
        for option in self.dynamic_options:
            option_name = option.opt_name
            filtered_dict[option_name] = options[option_name]
        return filtered_dict


class CSVTestRunner(TestRunner):
    """Runs the cross product of provided options on a command and records output in a csv file."""
    def __init__(self, command: Command, *options: Option, timeout: int,
                 output_file: typing.TextIO = None,
                 result_fields: Optional[List[str]] = [],
                 fields_from_timeout: Callable[[OptionDict, subprocess.TimeoutExpired], OptionDict],
                 fields_from_result: Callable[[OptionDict, subprocess.CompletedProcess, float], OptionDict],
                 ignore_fields: Optional[List[str]] = None,
                 write_header: bool = True,
                 ):
        """
        `command` is a `Command` with {} filter syntax. (e.g. "./runtest -n {repetitions} -f {function}")
        `result_fields` is a list of names of fields in the csv file that are determined by the result of the command.
            Essentially, your dependent variables.
        `fields_from_timeout` and `fields_from_result` are user-provided functions to fill in `result_fields`. 
            These functions must return a dictionary of fieldname -> value for each fieldname in `result_fields`.
            Both are provided with an `OptionDict` containing the options used in this execution of the command.
            `fields_from_timeout` is provided a `subprocess.TimeoutExpired` object as well.
            `fields_from_result` is provided a `subprocess.CompletedProcess` object and time elapsed in seconds as a `float`.
                Note that the `subprocess.run` command was passed `capture_output=True, text=True,`
        `ignore_fields` is an optional list of option names that will not be recorded in the csv file.
        If `write_header` is True the header to the csvfile will be written when not skipping the first line.
            If skipping the first line, `run` can be called with `force_write_header` to write the header anyway.
        """
        super().__init__(command, *options, timeout=timeout, output_file=output_file)
        self._result_fields: List[str] = result_fields
        # Default to including all fields
        if ignore_fields is None:
            ignore_fields = []
        # Specifically delve into csvoptions
        possible_option_names: List[str] = self._get_option_fieldnames(self.dynamic_options) + list(self.static_option_values)
        self._fields = list(filter(lambda x: x not in ignore_fields, possible_option_names)) + self._result_fields
        
        self.fields_from_timeout = fields_from_timeout
        self.fields_from_result = fields_from_result
        self.write_header = write_header
        
        self.csv_writer = csv.DictWriter(
            self.output_file,
            fieldnames=self._fields,
            extrasaction='ignore'  # We can put in the whole dict and just ignore the ones we don't want
        )
        
    @staticmethod
    def _get_option_fieldnames(options: List[Option]) -> List[str]:
        """Returns the names of all the fields from options.
        This includes splitting up CSV Options to get their dynamic options.
        Currently ignores dynamic check for csv options. If you have a column that is all the same that's your fault for now."""
        fieldnames = []
        for option in options:
            contains_multiple_options = type(option.option_values[0]) == dict
            if contains_multiple_options:
                headers = option.option_values[0].keys()
                fieldnames.extend(headers)
            else:
                fieldnames.append(option.opt_name)
        return fieldnames
                
    
    def run(self, iterations: int = 1, skip: int = 0, retry_after_timeout: bool = False, force_write_header: bool = False, force_skip_header=False):
        """Runs the command on the cross product of all options `iterations` times.
        The first `skip` executions are skipped. `skip` counts command executions, not commands themselves.
            Therefore, if iterations is 2, skip=3 will skip the first command entirely and the second command will be run once.
        The csv file's header will be written if `write_header` was True in the constructor and `skip==0` and `force_skip_header` is False.
            OR `force_write_header` is True. 
        If `retry_after_timeout` is false, a command will not be run on the same set of option values
            for any additional iterations after it times out."""
        if (self.write_header and skip == 0 and not force_skip_header) or force_write_header:
            self.csv_writer.writeheader()
        super().run(iterations, skip, retry_after_timeout=retry_after_timeout)
    
    def handle_timeout(self, options_values: OptionDict, timeout: subprocess.TimeoutExpired) -> None:
        """Gathers fields from the function provided in the constructor for timeouts.
        Then, writes the results to a new row in the csv file."""
        result_fields = self.fields_from_timeout(options_values, timeout)
        data = options_values.copy()
        data.update(result_fields)
        self.csv_writer.writerow(data)
        self.output_file.flush()
        
    def handle_result(self, option_values: OptionDict, result: subprocess.CompletedProcess, time_elapsed: float) -> None:
        """Gathers fields from the function provided in the constructor for non-timeout results.
        Then, writes the results to a new row in the csv file."""
        input_values = option_values.copy()
        result_fields = self.fields_from_result(input_values, result, time_elapsed)
        all_values = input_values
        all_values |= result_fields
        self.csv_writer.writerow(all_values)
        self.output_file.flush()
        
