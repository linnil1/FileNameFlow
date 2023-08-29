from typing import Iterable, Callable, Union, Any
import logging

from dask.distributed import Client

from .path import FileNamePath
from .task import FileNameTaskFunc, FileNameTaskOutput


class FileNameBaseExecutor:
    """
    The FileNameExecutor is a base class for implementing custom task executors within the FileNameFlow framework.
    """

    _logger = logging.getLogger(__name__)

    def pre_task(self, path: FileNamePath) -> FileNamePath:
        """
        This method is called before executing a task.
        It receives the input FileNamePath and returns the modified FileNamePath.
        """
        return path

    def run_task(
        self,
        func: FileNameTaskFunc,
        paths: Iterable[FileNamePath],
    ) -> Iterable[FileNameTaskOutput]:
        """
        This method is responsible for running the task for each filename with FileNamePath class.
        It yields the results of the task execution.
        """
        for path in paths:
            self._logger.info(f"Running {func}(input={path})")
            yield func(path)

    def post_task(self, path: FileNamePath) -> FileNamePath:
        """
        This method is called after executing tasks.
        It receives the output FileNamePath and returns the modified FileNamePath.
        """
        return path


class DaskExecutor(FileNameBaseExecutor):
    """
    Run the filenameflow under [Dask](https://www.dask.org/).

    Dask is convenient resource manager for distributed computing.

    Using this Executor by
    ```
    FileNameTask.set_default_executor(DaskExecutor())
    ```
    """

    def __init__(self, cluster: Any = None):
        super().__init__()
        self.client = Client(cluster)  # type: ignore

    def run_task(
        self, func: Callable[..., FileNameTaskOutput], names: Iterable[FileNamePath]
    ) -> Iterable[FileNamePath | str]:
        """Using client.submit to run all tasks"""
        exes = [self.client.submit(func, name) for name in names]  # type: ignore
        self._logger.info(f"Total {len(exes)} tasks submitted")
        results = [exe.result() for exe in exes]
        self._logger.info(f"{func} Done")
        return results
