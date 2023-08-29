from __future__ import annotations
from typing import Callable, Iterable, Mapping, Any
from functools import partial, wraps
import logging
import copy

from .path import FileNamePath
from .error import *

FileNameTaskOutput = None | str | FileNamePath
FileNameTaskFunc = Callable[..., FileNameTaskOutput]


from .executor import FileNameBaseExecutor


class FileNameTask:
    """
    FileNameTask is the basic class that wraps your function to become a step in a pipeline.
    The execution can be triggered by the ">>" operator or using the compose function.

    Args:
        func: A function that defines the task to be performed on given filename (path).
        fix: A list of integers (for positional arguments) or strings (for keyword arguments)
                    specifying which wildcards to ungroup.
        executor: An optional custom executor to use for the task.

    Example:
    * Usage 1:
        Wrap the function to FileNameTask Object
        ```python
        @FileNameTask.wrapper(fix=[-1])
        def func_manyto1(input_name, other_arg="1"):
            return input_name.replace_wildcard()
        "data.{}.arg1" >> func_manyto1
        "data.{}.arg1" >> func_manyto1(other_arg="2")
        ```

    * Usage 2:
        Execute the task by ">>"
        ```python
        def func_manyto1(input_name):
            return input_name.replace_wildcard()

        "data.{}.arg1" >> FileNameTask(func_manyto1, fix=[-1])
        ```
    * Usage 3:
        Execute the task by compose
        ```python
        compose([
            "data.{}.arg1",
            func_manyto1,
            FileNameTask(func_manyto1)(other_arg="1"),  # or thisw
            partial(func_manyto1, other_arg="2"),  # or this
        ])
        ```

    * Usage 4:
        The output can be got by task.output
        ```python
        task = compose([
            "data.{}.arg1",
            func_manyto1,
        ])
        compose([
            "data.{}.arg2",
            partial(func_manyto1, other_arg=task.output),
        ])
        ```
    """

    _logger = logging.getLogger(__name__)
    _default_executor = FileNameBaseExecutor()

    def __init__(
        self,
        func: FileNameTaskFunc = lambda i: i,
        fix: Iterable[str | int] = (),
        executor: FileNameBaseExecutor | None = None,
    ):
        if isinstance(func, FileNameTask):
            self.func: FileNameTaskFunc = func.func
            self.fix: Iterable[str | int] = func.fix
            self.executor: FileNameBaseExecutor = func.executor
        elif isinstance(func, Callable):  # type: ignore
            self.func = func
        else:
            raise TypeError(
                f"func must be a Callable or FileNameTask, but got {func} {type(func)}"
            )

        # sdaved attrs
        self.fix = fix
        self.input_path: FileNamePath | None = None
        self.output_path: FileNamePath | None = None

        # executor
        if executor is None:
            executor = self._default_executor
        if not hasattr(self, "executor"):
            self.executor = executor

    def __repr__(self) -> str:
        return (
            f"FileNameTask(func={self.func} "
            f"input={self.input_path} "
            f"fix={self.fix} "
            f"output={self.output_path})"
        )

    @property
    def output(self) -> FileNamePath:
        """Get the output path of the task"""
        if self.output_path is None:
            raise FileNameFlowDataError(f"{self} has not been executed")
        return self.output_path

    def __deepcopy__(self, memo: Any) -> FileNameTask:
        """Deep copy this Object"""
        return FileNameTask(
            func=copy.deepcopy(self.func, memo),
            fix=copy.deepcopy(self.fix, memo),
            executor=self.executor,  # deepcopy this will cause error when parallel start
        )

    def _run(self, path: FileNamePath) -> FileNameTask:
        """Main function to excute the function (inplaced)"""
        self._logger.info(f"FileNameTask Init: {self}")
        self.input_path = path
        path = self.executor.pre_task(path)
        paths = self._list(path)
        paths_out = self.executor.run_task(self.func, paths)
        path = self._merge_paths(paths_out)
        path = self.executor.post_task(path)
        self.output_path = path
        self._logger.info(f"FileNameTask Done: {self}")
        return self

    def run(self, path: FileNamePath) -> FileNameTask:
        """Main function to excute the function"""
        task = copy.deepcopy(self)
        task._run(path)
        return task

    def _set_func_args(self, *args: Any, **kwargs: Any) -> FileNameTask:
        """Fill/Replace function's arguments (Inplaced)"""
        self.func = partial(self.func, *args, **kwargs)
        return self

    def __call__(self, *args: Any, **kwargs: Any) -> FileNameTask:
        """Fill/Replace function's arguments"""
        task = copy.deepcopy(self)
        task._set_func_args(*args, **kwargs)
        return task

    def _list(self, path: FileNamePath, sort: bool = False) -> Iterable[FileNamePath]:
        """List the filename by given fix"""
        paths = path.list(self.fix)
        if sort:
            paths = sorted(paths)
        files_num = 0
        for path in paths:
            files_num += 1
            yield path
        if not files_num:
            raise FileNameFlowDataError(
                f"There does not have any input files can be listed by {path}"
            )

    def _merge_paths(self, paths: Iterable[FileNameTaskOutput]) -> FileNamePath:
        """Check and Merge the output path from the func"""
        path_str = set()
        for path in paths:
            if path is None:  # skip None
                continue
            elif isinstance(path, FileNamePath):
                path_str.add(path.template)
            else:
                path_str.add(str(path))

        if len(path_str) == 0:
            raise FileNameFlowDataError(f"{self} doesn't have any output paths")

        if len(path_str) > 1:
            raise FileNameFlowDataError(
                f"{self} fail to merge the returned names {path_str}"
            )

        return FileNamePath(path_str.pop())

    def __rrshift__(self, others: Any) -> Any:
        """see compose()"""
        return compose([others, self])

    def __rshift__(self, others: Any) -> Any:
        """see compose()"""
        return compose([self, others])

    @classmethod
    def set_default_executor(cls, executor: FileNameBaseExecutor) -> None:
        """Change default executor"""
        cls._default_executor = executor

    @classmethod
    def wrapper(
        cls, func: FileNameTaskFunc | None = None, /, **kwargs: Any
    ) -> FileNameTask | Callable[[FileNameTaskFunc], FileNameTask]:
        """
        A decorator to create a FileNameTask() instance

        Example:
        ``` python
        from filenameflow import FileNameTask

        @FileNameTask.wrapper
        def doSomething(input_name):
            return input_name + ".test"

        @FileNameTask.wrapper(fix=[-1])
        def doSomething1(input_name):
            return input_name.replace_wildcard()
        ```
        """

        def _wrapper(
            func: FileNameTaskFunc,
        ) -> FileNameTask:
            return FileNameTask(func=func, **kwargs)

        if func:  # decorator itself
            return _wrapper(func)
        else:  # decorator with parameters
            return _wrapper


def compose(
    func_list: Iterable[FileNameTask | FileNamePath | FileNameTaskFunc | str],
) -> FileNameTask | FileNamePath:
    """
    Compose and execute a sequence of tasks in a pipeline.

    Args:
        func_list: An list containing a series of tasks such as FileNameTask, FileNamePath, Function, or filenames(str).

    Returns:
        FileNameTask or FileNamePath: The last task or path in the composed pipeline.

    Example:
      ```python
      from filenameflow import compose

      # Execute the tasks based on previous output filenames.
      # Equivalent to task1 = "." >> doSomething >> doSomething2
      task1 = compose([
          ".", doSomething, doSomething2
      ])

      # Assert if the output of task1 is equal to the given filename.
      # Equivalent to task2 = task1 >> "expected_result_path"
      task2 = compose([
          task1 , "expected_result_path"
      ])
      ```
    """
    # run all task
    a: FileNamePath | FileNameTask | None = None
    for item in func_list:
        if isinstance(item, FileNameTask):
            b: FileNameTask | FileNamePath = item
        elif isinstance(item, (FileNamePath, str)):
            b = FileNamePath(item)
        elif isinstance(item, Callable):  # type: ignore
            b = FileNameTask(func=item)
        else:
            raise NotImplementedError

        # first item
        if a is None:
            a = b
        # execution
        elif isinstance(a, FileNamePath) and isinstance(b, FileNameTask):
            a = b.run(a)
        elif isinstance(a, FileNameTask) and isinstance(b, FileNameTask):
            a = b.run(a.output)
        # assertion
        elif isinstance(a, FileNamePath) and isinstance(b, FileNamePath):
            if str(a) != str(b):
                raise FileNameFlowDataError(f"Assert Error: {a} != {a}")
        elif isinstance(a, FileNameTask) and isinstance(b, FileNamePath):
            if str(a.output) != str(b):
                raise FileNameFlowDataError(
                    f"Assert Error: {a} output={a.output} != {b}"
                )
        else:
            raise FileNameFlowAssert
    if a is None:  # empty list -> return a empty path
        return FileNamePath(".")
    return a
