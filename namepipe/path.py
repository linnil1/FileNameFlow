from __future__ import annotations
import copy
import glob
import logging
from typing import Any
from pathlib import Path
from string import Formatter
import parse

from .error import *


# class NamePath(type(Path())):
# Why not path -> cannot use "" as None
class NamePath(str):
    """
    NamePath is represented the NAME

    which is basically the input/output filename without suffix,
    but support wildcard (we use `{}` as wildcard).

    NamePath is inherited from str, so all str methods can works.
    """

    _logger = logging.getLogger(__name__)
    _suffix_key = "namepipe_catch_suffix"  # use as kwargs for eaiser implemented

    def __init__(self, name: str):
        """
        We use `template` and `template_args` to store the information
        before `construct_name()`

        Note that `template_args` is empty after we init from a str.
        In most cases, it doesn't matter.

        Example:
          The relation of `self` `template` look like this
          ```
          self = "a.00.b.{}.c"
          template = "a.{}.b.{}.c"
          template_args = ("00", "{}")
          ```
        """
        super().__init__()

        # make a copy
        if isinstance(name, NamePath):
            self.copy_others_template(name)
            return

        # we didn't extract {} from name when init
        self.template = str(self)
        self.template_args: tuple = ()
        self.template_kwargs: dict = {}

    def copy_others_template(self, others: NamePath):
        """Deep copy the template from another NamePath"""
        self.template = others.template
        self.template_args = copy.deepcopy(others.template_args)
        self.template_kwargs = copy.deepcopy(others.template_kwargs)

    def get_fields_name(self) -> list[str | int]:
        """Extract all replacement fields from string"""
        fields_str = (i[1] for i in Formatter().parse(str(self)) if i[1] is not None)
        fields = []  # type: list[str | int]
        count = 0
        for i in fields_str:
            if i and not i.isdigit():
                raise NamePipeNotImplemented("Not support keyword argument yet")
                fields.append(i)
            elif i.isdigit():
                raise NamePipeNotImplemented("Not support keyword argument yet")
                fields.append(int(i))
            else:  # auto numbering
                fields.append(count)
                count += 1
        return fields

    def find_possible_files(self) -> list[str]:
        """
        List all possible files
        by replacing any replacement fields into `*`

        But the built-in `*` matchs not only single word,
        it can capture multiple words like `xxx.ooo` in one `*`
        """
        if Path(self).exists():
            self._logger.debug(f"Skip searching: '{self}' is existed")
            return [str(self)]
        # replace into wildcard "{}/{}.csv" -> "*/*.csv*"
        fields = self.get_fields_name()
        search_pattern = str(self).format(
            *("*" for i in fields if type(i) is int),
            **{i: "*" for i in fields if type(i) is str},
        )
        # add * in the ends
        if not search_pattern.endswith("*"):
            search_pattern += "*"
        # search and return
        files = list(glob.glob(search_pattern))
        self._logger.debug(f"Searching {search_pattern=} files_count={len(files)}")
        return files

    def extract_fields(self, name: str | Path) -> parse.Result:
        """
        Extract the value of replacement fields from the name

        Example:
          ``` python
          self = "./data/xxx.{}.oo.{}.mapped"
          name = "./data/xxx.00.oo.a.mapped.bam"
          return = Result.fixed = ("00", "a")
          ```

          Note that the fields are extract from name string,
          NOT from template
          ``` python
          self = "./data/xxx.00.oo.{}.mapped"
          template = "./data/xxx.{}.oo.{}.mapped"
          name = "./data/xxx.00.oo.a.mapped.bam"
          return = Result.fixed = ("a",)
          ```
        """
        result = parse.parse(str(self) + ".{" + self._suffix_key + "}", str(name))
        self._logger.debug(f"Extract {name}: {result=}")
        if (
            result
            and not any(["." in v for v in result.fixed])
            and not any(
                ["." in v and k != self._suffix_key for k, v in result.named.items()]
            )
        ):
            return result

        # case of the path is existed
        result = parse.parse(str(self), str(name))
        self._logger.debug(f"Extract {name}: {result=}")
        if (
            result
            and not any(["." in v for v in result.fixed])
            and not any(["." in v for v in result.named.values()])
        ):
            return result
        return None

    def construct_name(self, args: tuple[Any, ...], kwargs: dict[Any, Any]) -> NamePath:
        """
        Fill the replacement field in name string with `*arg` or `**kwargs`.

        This function will integrate with template, template_args.

        Example:
          Input data:
          ``` python
          self = "a.1.b.{}.c"
          self.template = "a.{}.b.{}.c"
          self.template_args = ["1", "{}"]
          args = ["Q"]  # from extract_fields()
          ```

          It will merge self.template_args and args
          ``` python
          self = "a.1.b.Q.c"
          self.template = "a.{}.b.{}.c"
          self.template_args = ["1", "Q"]
          ```
        """
        new_name = NamePath(str(self).format(*args, **kwargs))
        new_name.copy_others_template(self)
        new_name.template_kwargs |= kwargs

        # case1: copy args, because args is not init in the first time
        if str(self) == self.template:
            new_name.template_args = args
            return new_name

        # case2 (written in example)
        tmp_template_args = []
        tmp_template_args = list(self.template_args)
        count = 0
        for i, arg in enumerate(tmp_template_args):
            if arg == "{}":
                if count >= len(args):
                    raise NamePipeAssert("resemble template args fail")
                tmp_template_args[i] = args[count]
                count += 1
        if count != len(args):
            raise NamePipeAssert("resemble template args fail")
        new_name.template_args = tuple(tmp_template_args)
        return new_name

    def get_input_names(self, depended_pos: list[str | int] = []) -> list[NamePath]:
        """
        list all names that fit the format

        Example:
          ```
          self = "./data/xxx.{}.oo.{}.mapped"
          return ["./data/xxx.00.oo.a.mapped",
                  "./data/xxx.00.oo.b.mapped",
                  "./data/xxx.01.oo.a.mapped",
                  "./data/xxx.01.oo.b.mapped"]
          ```

          With depended_pos
          ```
          self = "./data/xxx.{}.oo.{}.mapped"
          depended_pos = [-1]  # the last fields
          return ["./data/xxx.00.oo.{}.mapped",
                  "./data/xxx.01.oo.{}.mapped"]
          ```

        Args:
          depended_pos:
            The position that doesn't treat them as independed fields
        """
        # case the None
        if not str(self):
            return [NamePath("")]

        fields = self.get_fields_name()
        depended = set(i if type(i) is not int else fields[i] for i in depended_pos)

        names = set()
        for name in self.find_possible_files():
            result = self.extract_fields(name)
            if result is None:
                continue

            # replace depended field to {}
            result_named = {
                k: v for k, v in result.named.items() if k != self._suffix_key
            }
            mask_args = tuple(
                v if k not in depended else "{}" for k, v in enumerate(result.fixed)
            )
            mask_kwargs = {
                k: v if k not in depended else "{}" for k, v in result_named.items()
            }

            # main
            new_name = self.construct_name(mask_args, mask_kwargs)
            self._logger.debug(
                f"Template: {new_name.template} "
                f"args={new_name.template_args} "
                f"{new_name.template_kwargs}"
            )
            names.add(new_name)
        return sorted(names)

    def replace_wildcard(self, merge_text="_merge") -> NamePath:
        """
        A function that simply `self.replace(".{}", merge_text)`

        But it takes care about escaped string and template

        Example:
          ``` python
          print(input_name)  # "a.00.c.{}"
          print(input_name.template)  # "a.{}.c.{}"
          output_name = input_name.replace_wildcard("_merge")
          print(output_name)  # "a.00.c_merge"
          print(output_name.template)  # "a.{}.c_merge"
          ```
        """
        new_string = ""
        new_template = ""
        new_args = []
        for i, field in enumerate(Formatter().parse(self.template)):
            if field[1] is None:  # last one
                new_string += field[0]
                new_template += field[0]
                continue
            if i >= len(self.template_args):
                raise NamePipeAssert("Template or args is not match")
            if self.template_args[i] == "{}":
                if not field[0].endswith("."):
                    raise NamePipeAssert("Template or args is not match")
                new_string += field[0][:-1] + merge_text
                new_template += field[0][:-1] + merge_text
            else:
                new_string += field[0] + self.template_args[i]
                new_template += field[0] + "{}"
                new_args.append(self.template_args[i])

        new_name = NamePath(new_string)
        new_name.template = new_template
        new_name.template_args = tuple(new_args)
        return new_name

    def __add__(self, others: Any) -> NamePath:
        """
        Adding suffix (Also in template)

        Example:
          ``` python
          input_name = NamePath("a.{}.c")
          output_name = input_name + ".d"  # = a.{}.c.d
          ```
        """
        # Note that template may have "{}" but I ignore
        new_name = NamePath(str(self).__add__(others))
        new_name.copy_others_template(self)
        new_name.template = self.template + others
        return new_name

    def __radd__(self, others: Any) -> NamePath:
        """Adding prefix (Almost same as `__add__`)"""
        # Note that template may have "{}" but I ignore
        new_name = NamePath(str(others).__add__(self))
        new_name.copy_others_template(self)
        new_name.template = others + self.template
        return new_name

    def __rshift__(self, others: Any) -> Any:
        """see compose()"""
        from . import compose  # avoid recursive import

        return compose([self, others])

    def __rrshift__(self, others: Any) -> Any:
        """see compose()"""
        from . import compose

        return compose([others, self])
