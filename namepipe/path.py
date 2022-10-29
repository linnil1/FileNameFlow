from __future__ import annotations
import copy
import glob
import logging
from typing import Any, Iterator
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
        NamePath extend the str class,
        it store two additional string.

        1. The base of query string in `template`
        2. the current filled replacement fields in `template_args`.
        3.  Combine this two information we can get the current string str(self).

        See below example.

        Note that `template_args` is empty after we init from a str.
        In most cases, it doesn't matter.
        You can use `reset_template()` to initialize it.

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

        # use empty template
        # you can init by reset_template
        self.template = str(self)
        self.template_args: tuple = ()
        self.template_kwargs: dict = {}

    def is_template_init(self) -> bool:
        """Check the template is initialized or not"""
        return bool(self.template_args) or bool(self.template_kwargs)

    def reset_template(self) -> NamePath:
        """This can be called after __init__"""
        fields = self.get_fields_name(str(self))
        self.template_args = tuple("{}" for i in fields if type(i) is int)
        self.template_kwargs = {i: "{}" for i in fields if type(i) is str}
        return self

    def copy_others_template(self, others: NamePath):
        """Deep copy the template from another NamePath"""
        self.template = others.template
        self.template_args = copy.deepcopy(others.template_args)
        self.template_kwargs = copy.deepcopy(others.template_kwargs)

    @classmethod
    def get_fields_name(cls, query_string: str) -> list[str | int]:
        """Extract all replacement replacement fields from string"""
        fields_str = (i[1] for i in Formatter().parse(query_string) if i[1] is not None)
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

    @classmethod
    def globs(cls, query_string: str) -> Iterator[str]:
        """
        Using glob to search file by replacing any replacement fields into `*`

        But the built-in `*` matchs not only single word,
        it can capture multiple words like `xxx.ooo` in one `*`
        """
        if Path(query_string).exists():
            cls._logger.debug(f"Skip searching: '{query_string}' is existed")
            yield query_string
            return
        # replace into wildcard "{}/{}.csv" -> "*/*.csv*"
        fields = cls.get_fields_name(query_string)
        search_pattern = query_string.format(
            *("*" for i in fields if type(i) is int),
            **{i: "*" for i in fields if type(i) is str},
        )
        # add * in the ends
        if not search_pattern.endswith("*"):
            search_pattern += "*"
        # search and return
        files = glob.iglob(search_pattern)
        cls._logger.debug(f"Searching {search_pattern=}")
        yield from files

    @classmethod
    def extract_and_filter_fields(
        cls, query_string: str, filename: str | Path
    ) -> parse.Result:
        """
        Extract the value of replacement fields from filename with query_string template

        Example:
          ``` python
          query_string = "./data/xxx.{}.oo.{}.mapped"
          filename = "./data/xxx.00.oo.a.mapped.bam"
          return = Result.fixed = ("00", "a")
          ```

          ``` python
          query_string = "./data/xxx.{}.oo.{}.mapped"
          filename = "./data/xxx.00.12.oo.a.mapped.bam"
          return = None
          ```
        """
        filename = str(filename)
        result = parse.parse(query_string + ".{" + cls._suffix_key + "}", filename)
        cls._logger.debug(f"Extract {filename}: {result=}")
        if (
            result
            and not any(["." in v for v in result.fixed])
            and not any(
                ["." in v and k != cls._suffix_key for k, v in result.named.items()]
            )
        ):
            return result

        # case of the path is existed
        result = parse.parse(query_string, filename)
        cls._logger.debug(f"Extract {filename}: {result=}")
        if (
            result
            and not any(["." in v for v in result.fixed])
            and not any(["." in v for v in result.named.values()])
        ):
            return result
        return None

    def list_files(self) -> Iterator[str]:
        """
        list files with the name path pattern

        example:
          self = "xx.{}.oo"
          return ["xx.00.oo.bam", "xx.11.oo.fa"]
        """
        for filename in self.globs(str(self)):
            result = self.extract_and_filter_fields(str(self), filename)
            if result:
                yield filename

    def construct_name(self, args: tuple, kwargs: dict) -> NamePath:
        """
        Fill the replacement field in name string with `*arg` or `**kwargs`.

        This function will integrate with template, template_args.

        Example:
          Input data:
          ``` python
          self = "a.1.b.{}.c"
          self.template = "a.{}.b.{}.c"
          self.template_args = ["1", "{}"]
          args = ["Q"]
          ```

          It will merge self.template_args and args
          ``` python
          self = "a.1.b.Q.c"
          self.template = "a.{}.b.{}.c"
          self.template_args = ["1", "Q"]
          ```
        """
        template_args = list(self.template_args)
        template_kwargs = self.template_kwargs | kwargs

        # replace args
        count = 0
        for i, arg in enumerate(template_args):
            if arg == "{}":
                if count >= len(args):
                    raise NamePipeAssert("resemble template args fail")
                template_args[i] = args[count]
                count += 1
        if count != len(args):
            raise NamePipeAssert("resemble template args fail")

        new_name = NamePath(self.template.format(*template_args, **template_kwargs))
        new_name.template = self.template
        new_name.template_args = tuple(template_args)
        new_name.template_kwargs = template_kwargs
        # new_name = str(self).format(*args, **kwargs)
        return new_name

    def get_input_names(self, depended_pos: list[str | int] = []) -> list[NamePath]:
        """Deprecated. Use list_names"""
        return self.list_names(depended_pos)

    def list_names(self, depended_pos: list[str | int] = []) -> list[NamePath]:
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

        if not self.is_template_init():
            self.reset_template()

        fields = self.get_fields_name(str(self))
        depended = set(i if type(i) is not int else fields[i] for i in depended_pos)

        names = set()
        for filename in self.globs(str(self)):
            result = self.extract_and_filter_fields(str(self), filename)
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
        if not self.is_template_init():
            self.reset_template()
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
                if not (field[0].endswith(".") or field[0].endswith("/")):
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
