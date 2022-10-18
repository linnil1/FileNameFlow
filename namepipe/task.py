from __future__ import annotations
import copy
import logging
from functools import partial
from typing import Iterable, Any, Callable

from .executor import BaseTaskExecutor
from .path import NamePath
from .error import *


class NameTask:
    """ Task class that interact with NamePath """
    logger = logging.getLogger(__name__)
    default_executor = BaseTaskExecutor()

    def __init__(self, func=None,
                 depended_pos: list[str | int] = [],
                 executor: BaseTaskExecutor = None):
        self.func = partial(func)
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
            f"depend=({self.depended_pos}) "
            f"output={self.output_name})"
        )

    def run(self, input_name: NamePath) -> NameTask:
        """
        Main function to excute the function
        """
        # init
        if self.executor is None:
            self.executor = self.default_executor
        self.logger.info(f"Init func={self.func} input={input_name}")
        self.input_name = self.executor.pre_task(input_name)

        # list names
        names = input_name.get_input_names(self.depended_pos)
        if len(names) == 0:
            raise NamePipeDataError(
                f"{self.func} has no input files globbed by {input_name}"
            )

        # main
        return_names = self.executor.run_tasks(
            names, self.func
        )

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
        self.logger.info(f"Done func={self.func} output={self.output_name}")
        return self

    def copy(self) -> NameTask:
        """ Deep copy the instance """
        return copy.deepcopy(self)

    def __call__(self, *args, **kwargs) -> NameTask:
        """
        Set arguments for main function

        I recommand the write args in the output_name
        (make suffix longer, but clear)

        Note that it will return a new instance because
        this task may be used mutliple times in the same pipeline
        which may apply different arguments.

        Example:
          ``` python
          def func_need_args(input_name, index):
              run(f"cat {input_name}.txt > {input_name}.add_{index}.txt")
              run(f"echo {index} >> {input_name}.add_{index}.txt")
              return input_name + f".add_{index}"

          "./test.{}" >> NameTask(func_need_args)(index="indexname")
          # or
          "./test.{}" >> NameTask(partial(func_need_args, index="indexname"))
          ```

        Return:
          A new NameTask
        """
        # TODO: need added method without replacement ?
        # why new instance -> create new task
        task = self.copy()
        task.func = partial(task.func, *args, **kwargs)
        return task

    def set_args(self, *args, **kwargs) -> NameTask:
        """ Deprecated: see __call__ method """
        return self.__call__(*args, **kwargs)

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

    def __rrshift__(self, others: Any) -> NameTask:
        """
        Usage:
        1. Use a path as input: "path1/name" >> func
        2. Use empty path as input: None >> func
        """
        # type == NameTask is already written in rshift
        if isinstance(others, (NamePath, str)):
            return self.copy().run(NamePath(others))
        elif others is None:
            return self.copy().run(NamePath(""))
        raise NotImplementedError

    def __rshift__(self, others: Any) -> NameTask:
        """
        Usage:
        1. assert the final result: func >> "path/result_name"
        2. propagate the name to the follwing task: func1 >> func2
        """
        # execution syntax
        if isinstance(others, NameTask):
            if self.output_name is None:
                raise NamePipeError(f"This task ({self.func}) is not run yet")
            return others.copy().run(self.output_name)
        elif isinstance(others, Callable):  # type: ignore
            if self.output_name is None:
                raise NamePipeError(f"This task ({self.func}) is not run yet")
            return NameTask(func=others).run(self.output_name)
        # assert syntax
        elif isinstance(others, (str, NamePath)):
            if str(self.output_name) != str(others):
                raise NamePipeDataError(
                    f"Assert Error: task({self.func})'s "
                    f"output={self.output_name} != {others}"
                )
            return self
        raise NotImplementedError


def nt(func, depended_pos=[], executor=None):
    """
    A decorator to create a NameTask() instance

    But creating the instance makes multiprocessing fail

    Example:
      ``` python
      from namepipe import nt

      @nt
      def doSomething(input_name):
          ...
          return input_name + ".test"

      def doSomething1(input_name):
          ...
          return input_name + ".test1"

      "" >> doSomething
      # or (better)
      "" >> nt(doSomething1)
      ```
    """
    if isinstance(func, NameTask):
        task = func
    else:
        task = NameTask(func=partial(func))
    if executor is not None:
        task = task.set_executor(executor)
    if depended_pos:
        task = task.set_depended(depended_pos)
    return task


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
      # equivalent to task2 = task1 >> doSomething3
      task2 = comppse([
          task1 , doSomething3
      ])
      ```
    """
    # run all task
    current_item = None
    for item in func_list:
        if isinstance(item, NameTask):
            pass
        elif isinstance(item, Callable):  # type: ignore
            item = NameTask(func=item)
        elif isinstance(item, (NamePath, str)):
            item = NamePath(item)
        elif item is None:
            item = NamePath("")
        else:
            raise NotImplementedError

        if current_item is None:
            # Transfer first NamePath to NameTask
            # so this will work: "123" >> "123"
            if isinstance(item, NamePath):
                task = NameTask(lambda i: i)
                task.output_name = item
                item = task
            current_item = item
        else:
            current_item = current_item >> item
    if current_item is not None:
        return current_item
    else:
        return NamePath("")
