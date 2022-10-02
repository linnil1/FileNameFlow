from typing import Callable
import multiprocessing

from .path import NamePath
from .error import *
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor


class BaseTaskExecutor:
    def __init__(self):
        pass

    def pre_task(self, input_name: NamePath) -> NamePath:
        return input_name

    def run_tasks(
        self, names: list[NamePath], func: Callable, func_args: tuple, func_kwargs: dict
    ) -> list[NamePath | str]:
        # self.logger.info(f"Run  func={self.func.__name__} {name=}")
        return_names = []
        for name in names:
            return_names.append(func(name, *func_args, **func_kwargs))
        return return_names

    def post_task(self, output_name: NamePath) -> NamePath:
        return output_name


class ConcurrentTaskExecutor(BaseTaskExecutor):
    def __init__(self):
        self.threads = multiprocessing.cpu_count()

    def run_tasks(
        self, names: list[NamePath], func: Callable, func_args: tuple, func_kwargs: dict
    ) -> list[NamePath | str]:
        exes = []
        with ProcessPoolExecutor(max_workers=self.threads) as executor:
            for name in names:
                exes.append(executor.submit(func, name, *func_args, **func_kwargs))
            return [exe.result() for exe in exes]
