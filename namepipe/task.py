from __future__ import annotations
import copy
import logging
from typing import Iterable, Any

from .path import NamePath
from .error import *


class NameTask:
    """ Task class that interact with NamePath """
    logger = logging.getLogger(__name__)

    def __init__(self, func=None):
        self.func = func
        self.func_args = ()
        self.func_kwargs = {}
        self.depended_pos = []

        # each instance
        self.input_name = None
        self.output_name = None

    def __str__(self):
        return self.output_name

    def __repr__(self):
        return f"NameTask(func={self.func.__name__} {self.args=} {self.kwrags=} " + \
               f"input={self.input_name} depend=({self.depended_pos}) " + \
               f"output={self.output_name})"

    def run(self, input_name: NamePath) -> NameTask:
        """
        Main function to excute the function
        """
        self.logger.info(f"Init func={self.func.__name__} input={input_name}")
        self.input_name = input_name
        output_name = None

        # list names
        names = input_name.get_input_names(self.depended_pos)
        if len(names) == 0:
            raise NamePipeDataError(f"No input files glob by {input_name}")

        # TODO: concurrent
        for name in names:
            self.logger.info(f"Run  func={self.func.__name__} {name=}")
            return_name = self.func(name, *self.func_args, **self.func_kwargs)
            if isinstance(return_name, NamePath):
                new_name = return_name.template
            else:
                new_name = str(return_name)
            if output_name is None:  # first one
                output_name = new_name
            elif output_name != new_name:
                raise NamePipeDataError(f"Fail to merge returned name "
                                        f"{output_name} and {new_name}")
            # TODO: check output files is existed

        # save output
        if output_name is None:
            raise NamePipeDataError(f"No function been executed or return None"
                                    f" : {repr(self)}")
        self.output_name = NamePath(str(output_name))  # clean up args
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

    def __rrshift__(self, others) -> NameTask:
        # type == NameTask is already written in rshift
        if isinstance(others, (NamePath, str)):
            return self.copy().run(NamePath(others))
        elif others is None:
            return self.copy().run(NamePath(""))
        raise NotImplementedError

    def __rshift__(self, others) -> NameTask:
        if isinstance(others, NameTask):
            return others.copy().run(self.output_name)
        elif isinstance(others, (str, NamePath)):
            if str(self.output_name) != str(others):
                raise NamePipeDataError(
                        f"Assert Error: output={self.output_name} != {others}")
            return self
        raise NotImplementedError


def nt(func):
    """
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


def compose(func_list: Iterable[NameTask | NamePath | str | None]) -> NameTask | NamePath:
    """
    A compose way to execute the task

    Example:
      ``` python
      from namepipe import compose

      # equivalent to task1 = "" >> doSomething >> doSomething2
      task1 = compose([
          "", doSomething, doSomething2
      ])
      # equivalent to task1 >> doSomething3
      task2 = comppse([
          task1 , doSomething3
      ])
    """
    # run all task
    current_item = None
    num_item = 0
    for item in func_list:
        if not num_item:
            current_item = item
        else:
            current_item = current_item >> item  # type: ignore
        num_item += 1
    if isinstance(current_item, NameTask):
        return current_item

    # special case: only one item
    assert num_item == 1
    if isinstance(item, (NamePath, str)):
        return NamePath(item)
    elif item is None:
        return NamePath("")
    raise NotImplementedError
