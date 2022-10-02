from __future__ import annotations
import copy
import logging
from typing import Iterable, Any, Callable

from .executor import BaseTaskExecutor
from .path import NamePath
from .error import *


class NameTask:
    """ Task class that interact with NamePath """
    logger = logging.getLogger(__name__)
    default_executor = BaseTaskExecutor()

    def __init__(self, func=None):
        self.func = func
        self.func_args = ()
        self.func_kwargs = {}
        self.depended_pos = []
        self.executor = None

        # each instance
        self.input_name = None
        self.output_name: None | NamePath = None

    def __str__(self) -> str:
        return str(self.output_name)

    def __repr__(self) -> str:
        return (
            f"NameTask(func={self.func.__name__} "
            f"{self.func_args=} {self.func_kwargs=} "
            f"input={self.input_name} depend=({self.depended_pos}) "
            f"output={self.output_name})"
        )

    def run(self, input_name: NamePath) -> NameTask:
        """
        Main function to excute the function
        """
        # init
        if self.executor is None:
            self.executor = self.default_executor
        self.logger.info(f"Init func={self.func.__name__} input={input_name}")
        self.input_name = self.executor.pre_task(input_name)

        # list names
        names = input_name.get_input_names(self.depended_pos)
        if len(names) == 0:
            raise NamePipeDataError(f"No input files glob by {input_name}")

        # main
        return_names = self.executor.run_tasks(
            names, self.func, self.func_args, self.func_kwargs
        )

        # merge return name
        output_name = None
        for return_name in return_names:
            if isinstance(return_name, NamePath):
                new_name = return_name.template
            else:
                new_name = str(return_name)
            if output_name is None:  # first one
                output_name = new_name
            elif output_name != new_name:
                raise NamePipeDataError(
                    f"Fail to merge returned name {output_name} and {new_name}"
                )
            # TODO: check output files is existed

        # output_name = return_name
        if output_name is None:
            raise NamePipeDataError(
                f"No function been executed or function return None: {repr(self)}"
            )
        self.output_name = NamePath(str(output_name))  # clean up args
        self.output_name = self.executor.post_task(self.output_name)
        self.logger.info(f"Done func={self.func.__name__} output={self.output_name}")
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
          @nt
          def func_need_args(input_name, index):
              run(f"cat {input_name}.txt > {input_name}.add_{index}.txt")
              run(f"echo {index} >> {input_name}.add_{index}.txt")
              return input_name + f".add_{index}"

          "./test.{}" >> func_need_args(index="indexname")
          ```

        Return:
          A new NameTask
        """
        # TODO: need added method without replacement ?
        # why new instance -> create new task
        task = self.copy()
        task.func_args = args
        task.func_kwargs = kwargs
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
            task.depended_pos = pos
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
                raise NamePipeError(f"The task is not run yet")
            return others.copy().run(self.output_name)
        elif isinstance(others, Callable):  # type: ignore
            if self.output_name is None:
                raise NamePipeError(f"The task is not run yet")
            return NameTask(func=others).run(self.output_name)
        # assert syntax
        elif isinstance(others, (str, NamePath)):
            if str(self.output_name) != str(others):
                raise NamePipeDataError(
                    f"Assert Error: output={self.output_name} != {others}"
                )
            return self
        raise NotImplementedError


def nt(func):
    """
    (Deprecated) Decorator makes multiprocessing difficult

    A decorator to create a NameTask() instance

    Example:
      ``` python
      from namepipe import nt

      @nt
      def doSomething(input_name):
          ...
          return input_name + ".test"

      "" >> doSomething
      ```
    """
    return NameTask(func=func)


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
            current_item = item
        else:
            current_item = current_item >> item
    if current_item is not None:
        return current_item
    else:
        return NamePath("")
