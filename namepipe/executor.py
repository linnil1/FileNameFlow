import time
import pickle
import logging
import traceback
import subprocess
import importlib.util
import multiprocessing
from typing import Callable, Any
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
import __main__

from .path import NamePath


class BaseTaskExecutor:
    """
    Define the executor about how to run the task.

    This BaseTaskExecutor will run the task per name in the for loop.
    """

    def __init__(self):
        self._logger = logging.getLogger(__name__)

    def pre_task(self, input_name: NamePath) -> NamePath:
        """This method will be called before task. The input is the original path."""
        return input_name

    def run_tasks(self, names: list[NamePath], func: Callable) -> list[NamePath | str]:
        """Run the tasks by given matched names."""
        # self.logger.info(f"Run  func={self.func.__name__} {name=}")
        return_names = []
        for name in names:
            self._logger.info(f"Run func={func} output={name}")
            return_names.append(func(name))
        return return_names

    def post_task(self, output_name: NamePath) -> NamePath:
        """This method will be called after tasks. The input is the MERGED path."""
        return output_name


class ConcurrentTaskExecutor(BaseTaskExecutor):
    """
    Run the task per name concurrently

    Parameters:
      threads: Set how many threads to used. Left None if you want to use all
      mode: "process" or "thread"
    """

    def __init__(self, threads: int | None = None, mode: str = "process"):
        super().__init__()
        if threads:
            self.threads = threads
        else:
            self.threads = multiprocessing.cpu_count()
        self.mode = mode

    def run_tasks(self, names: list[NamePath], func: Callable) -> list[NamePath | str]:
        """
        Run the task concurrently by built-in
        ProcessPoolExecutor or ThreadPoolExecutor
        """
        exes = []
        if self.mode == "process":
            executor_func: Callable = ProcessPoolExecutor
        elif self.mode == "thread":
            executor_func = ThreadPoolExecutor
        else:
            raise NotImplementedError
        self._logger.info(f"Run tasks in {executor_func}")
        with executor_func(max_workers=self.threads) as executor:
            for name in names:
                exes.append(executor.submit(func, name))
            self._logger.info(f"Total {len(exes)} tasks submitted")
            return [exe.result() for exe in exes]


class StandaloneTaskExecutor(BaseTaskExecutor):
    """ A standalone way to execute the task """

    def __init__(self, threads: int | None = None, auto_cleanup: bool = True):
        super().__init__()
        if threads:
            self.threads = threads
        else:
            self.threads = multiprocessing.cpu_count()
        self.auto_cleanup = auto_cleanup

    @classmethod
    def import_func_in_main_namespace(cls, path: str) -> None:
        """
        Manually import __main__'s funtion.
        Which is the same as from __main__ import *

        Becuase the __main__ is different in standalone process and main process.
        """
        spec = importlib.util.spec_from_file_location("tmp", path)
        if not spec:
            return
        assert spec
        assert spec.loader
        module = importlib.util.module_from_spec(spec)
        assert module
        spec.loader.exec_module(module)
        for i in dir(module):
            if not i.startswith("__"):
                setattr(__main__, i, getattr(module, i))

    @classmethod
    def execute_standalone_function(cls, func: Callable, input_name: str) -> Any:
        """
        Run the function in standalone mode.

        If the function raise error,
        it will return the exception object.
        """
        try:
            return func(input_name)
        except BaseException as e:
            print(e)
            return BaseException(traceback.format_exc())

    @classmethod
    def run_standalone_task(cls, main_file_path: str, name: str) -> None:
        """ load object, execute function and dump the output """
        cls.import_func_in_main_namespace(main_file_path)
        with open(f"{name}.in", "rb") as f:
            func, input_name = pickle.load(f)
        out = cls.execute_standalone_function(func, input_name)
        with open(f"{name}.out", "wb") as f:
            pickle.dump(out, f, pickle.HIGHEST_PROTOCOL)

    def submit_standalone_task(self, name: str) -> subprocess.CompletedProcess:
        """ Define how to run the task in standalone python """
        cmd = ["python", "-c",
               "from namepipe import StandaloneTaskExecutor; "
               "StandaloneTaskExecutor.run_standalone_task("
               f"{repr(str(__main__.__file__))}, {repr(str(name))})"]
        # repr escaping is enough
        self._logger.debug(cmd)
        return subprocess.run(cmd, check=True)

    def setup_standalone_task(self, func: Callable, input_name: NamePath) -> str:
        """ Setup the data needed for standalone task (job_name.in). """
        name = "job_" + str(input_name).replace("/", "_")
        # create tmp input parameters and executor
        self._logger.debug(f"Write {name}.in")
        with open(f"{name}.in", "wb") as f:
            pickle.dump((func, input_name), f, pickle.HIGHEST_PROTOCOL)
        return name

    def run_tasks(self, names: list[NamePath], func: Callable) -> list[NamePath | str]:
        """
        Run each task in standalone process.
        The standalone processes
        will read the input object from `{job_name}.in` via pickle
        and pickle dump the output in `{job_name}.out`.

        This manager has three parts: setup, submit, wait and gathering.
        And here's something to mention:

        * The setup part will dump input objects in the file `{job_name}.in`.
        * If task's setup or task's submittion fails, it'll immediatly raise.
            Otherwise, it will raise error after all processes are done.
        * The job submittion is running in mutlithreads to avoid blocking.
        * The gathering part will wait until the `{job_name}.out` appear.
        """
        # setup
        job_names = [self.setup_standalone_task(func, name) for name in names]
        with ThreadPoolExecutor(max_workers=self.threads) as executor:
            # submit
            exes = [executor.submit(self.submit_standalone_task, name)
                    for name in job_names]
            self._logger.info(f"Submit {len(exes)} tasks")
            [exe.result() for exe in exes]
            self._logger.info(f"Submit {len(exes)} tasks done")

        # wait for .out
        # and read it
        output_name = []
        for name in job_names:
            output_file = f"{name}.out"
            while not Path(output_file).exists():
                self._logger.debug(f"Wait for {output_file}")
                time.sleep(3)
            self._logger.debug(f"Load {output_file}")
            with open(output_file, "rb") as f:
                output_name.append(pickle.load(f))
            if self.auto_cleanup:
                Path(f"{name}.in").unlink()
                Path(f"{name}.out").unlink()
        self._logger.info(f"{len(exes)} tasks done")

        # raise error and print the error message if return Error
        has_error = False
        for i in output_name:
            if isinstance(i, BaseException):
                self._logger.error(str(i))
                has_error = True
        if has_error:
            raise ValueError("Some tasks have error")
        return output_name
