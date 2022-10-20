from typing import Callable
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
import multiprocessing

from .path import NamePath


class BaseTaskExecutor:
    """
    Define the executor about how to run the task.

    This BaseTaskExecutor will run the task per name in the for loop.
    """

    def __init__(self):
        pass

    def pre_task(self, input_name: NamePath) -> NamePath:
        """This method will be called before task. The input is the original path."""
        return input_name

    def run_tasks(self, names: list[NamePath], func: Callable) -> list[NamePath | str]:
        """Run the tasks by given matched names."""
        # self.logger.info(f"Run  func={self.func.__name__} {name=}")
        return_names = []
        for name in names:
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
        """Run the task concurrently by built-in ProcessPoolExecutor or ThreadPoolExecutor"""
        exes = []
        if self.mode == "process":
            executor_func = ProcessPoolExecutor
        elif self.mode == "thread":
            executor_func = ThreadPoolExecutor
        else:
            raise NotImplementedError
        with executor_func(max_workers=self.threads) as executor:
            for name in names:
                exes.append(executor.submit(func, name))
            return [exe.result() for exe in exes]
