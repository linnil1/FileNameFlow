from __future__ import annotations
import copy
import logging
from functools import partial
from typing import Iterable, Any, Callable

from .executor import BaseTaskExecutor
from .path import NamePath
from .error import *


class NameTask:
    """Task class that interact with NamePath"""

    _logger = logging.getLogger(__name__)
    _default_executor = BaseTaskExecutor()

    @staticmethod
    def set_default_executor(executor: BaseTaskExecutor) -> None:
        """Change default executor"""
        NameTask._default_executor = executor

    def __init__(
        self,
        func=None,
        func_kwargs: dict = {},
        depended_pos: list[str | int] = [],
        executor: BaseTaskExecutor | None = None,
    ):
        if isinstance(func, NameTask):
            task = func.copy()
            self.func: Callable = task.func
            self.depended_pos: list[str | int] = task.depended_pos
            self.executor: BaseTaskExecutor | None = task.executor
        self.func = partial(func, **func_kwargs)
        self.depended_pos = depended_pos
        self.executor = executor
        # each instance
        self.input_name: NamePath | None = None
        self.output_name: NamePath | None = None

    def __str__(self) -> str:
        return str(self.output_name)

    def __repr__(self) -> str:
        return (
            f"NameTask(func={self.func} "
            f"input={self.input_name} "
            f"depend={self.depended_pos} "
            f"output={self.output_name})"
        )

    def run(self, input_name: NamePath) -> NameTask:
        """
        Main function to excute the function
        """
        # init
        if self.executor is None:
            self.executor = self._default_executor
        self._logger.info(f"Init func={self.func} input={input_name}")
        self.input_name = self.executor.pre_task(input_name)

        # list names
        names = input_name.get_input_names(self.depended_pos)
        if len(names) == 0:
            raise NamePipeDataError(
                f"{self.func} has no input files globbed by {input_name}"
            )

        # main
        return_names = self.executor.run_tasks(names, self.func)

        # merge return name
        output_name = None
        for return_name in return_names:
            if isinstance(return_name, NamePath):
                new_name = return_name.template
            elif return_name is None:  # skip None
                continue
            else:
                new_name = str(return_name)
            if output_name is None:  # first one
                output_name = new_name
            elif output_name != new_name:
                raise NamePipeDataError(
                    f"{self.func} fail to merge the returned names"
                    f": {output_name} and {new_name}"
                )
            # TODO: check output files is existed

        # output_name = return_name
        if output_name is None:
            raise NamePipeDataError(
                f"{self.func} doesn't been executed once or function return None"
            )
        self.output_name = NamePath(str(output_name))  # clean up args
        self.output_name = self.executor.post_task(self.output_name)
        self._logger.info(f"Done func={self.func} output={self.output_name}")
        return self

    def copy(self) -> NameTask:
        """Deep copy the instance"""
        return copy.deepcopy(self)

    def set_args(self, *args, **kwargs) -> NameTask:
        """
        Set arguments for main function

        Note that it will return a new instance because
        this task may be used mutliple times in the same pipeline
        which may apply different arguments.

        Example:
          ``` python
          def func_need_args(input_name, index):
              # I recommand the write args in the output_name TO
              # make suffix longer, but clearer
              run(f"cat {input_name}.txt > {input_name}.add_{index}.txt")
              run(f"echo {index} >> {input_name}.add_{index}.txt")
              return input_name + f".add_{index}"

          "./test.{}" >> NameTask(partial(func_need_args, index="indexname"))
          # or
          "./test.{}" >> NameTask(func_need_args).set_args(index="indexname"))
          ```

        Return:
          A new NameTask
        """
        task = self.copy()
        task.func = partial(task.func, *args, **kwargs)
        return task

    def set_depended(self, pos: int | list[int]) -> NameTask:
        """
        Set depended fields (see NamePath.get_input_names)

        Example:
          ``` python
          @nt
          def func_merge_samples(input_name):
              files = [i + ".txt" for i in input_name.get_input_names()]
              return input_name.replace_wildcard("_merge")

          "./test.{}" >> func_merge_samples.set_depended(-1)
          ```

        Return:
          A new NameTask
        """
        task = self.copy()
        if isinstance(pos, (int, str)):
            task.depended_pos = [pos]
        else:
            task.depended_pos = list(pos)
        return task

    def set_executor(self, executor: BaseTaskExecutor):
        """
        Set executor for this task.

        See executor.BaseTaskExecutor for the implementation of executor.
        """
        task = self.copy()
        task.executor = executor
        return task

    def __rrshift__(self, others: Any) -> Any:
        """see compose()"""
        return compose([others, self])

    def __rshift__(self, others: Any) -> Any:
        """see compose()"""
        return compose([self, others])


def nt(func=None, /, **kwargs):
    """
    A decorator to create a NameTask() instance

    Example:
      ``` python
      from namepipe import nt

      @nt
      def doSomething(input_name):
          ...
          return input_name + ".test"

      @nt(depended_pos=[-1])
      def doSomething1(input_name):
          ...
          return input_name.replace_wildcard()

      "" >> doSomething()
      # or
      "" >> doSomething1()
      # or (fine. just overwrite the value)
      "" >> doSomething1(depended_pos=[-1])
      # or
      "" >> doSomething(depended_pos=[-1])

      # set parameters
      "" >> doSomething(func_kwargs=dict(index="index_path"))
      ```
    """
    def _nt(func):
        return partial(NameTask, func=func, **kwargs)
    if func:  # decorator itself
        return _nt(func)
    else:  # decorator with parameters
        return _nt


def compose(
    func_list: Iterable[NameTask | NamePath | Callable | str | None],
) -> NameTask | NamePath:
    """
    A compose way to execute the task

    Example:
      ``` python
      from namepipe import compose

      # equivalent to task1 = "" >> doSomething >> doSomething2
      task1 = compose([
          "", doSomething, doSomething2
      ])
      # equivalent to task2 = task1 >> doSomething3 >> expected_result_path
      task2 = comppse([
          task1 , doSomething3, expected_result_path
      ])
      ```
    """
    # run all task
    a = None
    for item in func_list:
        if isinstance(item, NameTask):
            b: NameTask | NamePath = item
        elif isinstance(item, Callable):  # type: ignore
            b = NameTask(func=item)
        elif isinstance(item, (NamePath, str)):
            b = NamePath(item)
        elif item is None:
            b = NamePath("")
        else:
            raise NotImplementedError

        # first item
        if a is None:
            a = b
        # execution
        elif isinstance(a, NamePath) and isinstance(b, NameTask):
            a = b.copy().run(a)
        elif isinstance(a, NameTask) and isinstance(b, NameTask):
            if a.output_name is None:
                raise NamePipeError(f"This task ({a.func}) is not run yet")
            a = b.copy().run(a.output_name)
        # assertion
        elif isinstance(a, NamePath) and isinstance(b, NamePath):
            if str(a) != str(b):
                raise NamePipeDataError(f"Assert Error: {a} != {a}")
        elif isinstance(a, NameTask) and isinstance(b, NamePath):
            if str(a.output_name) != str(b):
                raise NamePipeDataError(
                    f"Assert Error: task({a.func})'s " f"output={a.output_name} != {b}"
                )
    if a is None:  # empty list -> return a empty path
        return NamePath("")
    return a
