"""
FileNamePath, a core path module of FileNameFlow.

Handle the path pattern matching rule.
"""
from __future__ import annotations
from itertools import repeat
from typing import Any, Iterable, Hashable, Callable, Mapping, TypeVar
import os
import glob
import parse
import logging

from .error import *


_T = TypeVar("T")


def unique(arr: Iterable[_T], key: Callable[[_T], Hashable]) -> Iterable[_T]:
    """
    Return a unique sequence.

    Args:
      arr: An sequence to be unique.
      key: A function that returns the key of an element.

    Returns:
        A unique sequence.
    """

    unique_name = set()
    for i in arr:
        if key(i) in unique_name:
            continue
        unique_name.add(key(i))
        yield i


class FileNamePath(str):
    """
    FileNamePath is a specialized string class designed for managing filenames.

    Attributes:
        self (str): A string representing the current filename.
        template: The filename pattern serving as the basis for formatting filenames.
        args: A list containing positional arguments used for formatting `self` based on the `template`.
        kwargs: A dictionary containing keyword arguments used for formatting `self` based on the `template`.

    This class extends the functionality of Python's built-in `str` type.

    Wildcards:
    The FileNamePath class uses `{}` as wildcard characters and `*` as wildcard variables in both args and kwargs.

    Example:
    * With args:
        * self: `test.sample.regression.lasso`
        * template: `test.{}.regression.{}`
        * args: [`"sample", "lasso"]`

    * With args containing a wildcard:
        * self: `test.{}.regression.lasso`
        * template: `test.{}.regression.{}`
        * args: `["*", "lasso"]`

    * With kwargs:
        * self: `test.{}.regression.{type}`
        * template: `test.{}.regression.{type}`
        * args: `["*"]`
        * kwargs: `{"type": "*"}`

    Features:
        FileNamePath's primary feature is the ability to list files based on patterns.

        Example:
        `FileNamePath("test.{}.regression.lasso").list()` will list all files starting with `test.*.regression.lasso`
        while ignoring the file extension.

        It also excludes files with separators inside wildcards, for instance:

        * `test.sample1.regression.lasso` (included in the listing)
        * `test.sample1.regression.lasso.txt` (excluded from the listing, the filename without suffix is same as previous one)
        * `test.sample1.regression.lasso.alpha10.txt` (excluded from the listing, the filename without suffix is same as previous one)
        * `test.sample2.regression.lasso` (included in the listing)
        * `test.sample_bad.sample1.regression.lasso` (excluded from the listing)

        After listing, the number of variables in `args` and `kwargs` is preserved, but their values change.
        For example, `test.sample1.regression.lasso` has `args` set to `["sample1"]` instead of `["*"]`.

        Another argument for the `list` method is `fix`, which allows you to fix specific variables from the listing.
        Example: `FileNamePath("test.{}.regression.{}").list(fix=(0, ))` would yield:

        * `test.{}.regression.lasso` (with `args=["*", "lasso"]`)
        * `test.{}.regression.elastic`
        * `test.{}.regression.rigid`

    Operations:

    * `replace_wildcard()`: Replaces the "{}" wildcard with a specified string (e.g., "_merge").
      Example: "test.{}.regression" becomes "test_merge.regression".

    * `+`: Concatenates FileNamePath objects.
      Example: `FileNamePath("data.{}.regression") + ".lasso"`
      results in `FileNamePath("data.{}.regression.lasso")`.
      Note that args and kwargs are preserved.

    * `apply()`: Applies arguments to the FileNamePath (similar to `.format()`).
      Example: `FileNamePath("test.{}.regression.lasso").apply(0, "sample1")`
      results in `FileNamePath("test.merge.regression.lasso")`.
    """

    suffix_key = "suffix_of_filename_for_filenamepath"
    separator = "."
    _logger = logging.getLogger(__name__)

    def __init__(self, path: str):
        super().__init__()

        # make a copy
        if isinstance(path, FileNamePath):
            self.template: str = path.template
            self.args: tuple[str, ...] = tuple(path.args)
            self.kwargs: Mapping[str, str] = dict(**path.kwargs)
            return

        # str to template, arg, kwargs
        self.template = str(self)
        _, self.args, self.kwargs = self.parse(str(self))

    @classmethod
    def construct(
        cls, template: str, args: Iterable[str], kwargs: Mapping[str, str]
    ) -> FileNamePath:
        """
        Construct a new FileNamePath via template, args, kwargs.
        The filename will be auto generated.
        """
        filename = template.format(
            *[i if i != "*" else "{}" for i in args],
            **{
                k: (v if v != "*" else "{" + k + "}")
                for k, v in kwargs.items()
                if k != cls.suffix_key
            },
        )
        path = FileNamePath(filename)
        path.template = template
        path.args = tuple(args)
        path.kwargs = dict(**kwargs)
        cls._logger.debug(
            f"Construct {template} {args} {kwargs} -> "
            f"{path} {path.template} {path.args} {path.kwargs}"
        )
        return path

    def apply(self, key: str | int, value: str) -> FileNamePath:
        """
        Similar to .format(),
        but return new FileName Path that keep the template, args, kwargs

        Example:
            ```
            self: test.{}.regression.lasso
            template: test.{}.regression.{}
            args: ["*", "lasso"]

            # After .apply(0, "sample2")
            self: test.sample2.regression.{}
            template: test.{}.regression.{}
            args: ["sample2", "*"]
            ```
        """
        return self.applys([(key, value)])

    def applys(self, kv_pairs: Iterable[tuple[str | int, str]] = ()) -> FileNamePath:
        """Similar to .apply, but allow apply multiple things"""
        path = self.commit()
        path = path.overwrites(kv_pairs)
        path_or_err = self.with_filename(path)
        if path_or_err is None:
            raise FileNameFlowAssert
        return path_or_err

    def overwrite(self, key: int | str, value: str) -> FileNamePath:
        """Force to change the value in args or kwargs. see .overwrites"""
        return self.overwrites([(key, value)])

    def overwrites(
        self, kv_pairs: Iterable[tuple[str | int, str]] = ()
    ) -> FileNamePath:
        """
        Force to change the value in args or kwargs.

        Example:
            ```
            Path: cohort.{}.regression.lasso
            template: cohort.{}.regression.{}
            args: ["*", "lasso"]
            # after .overwrite(`[(1, "elastic")]`)
            Path: cohort.{}.regression.elastic
            template: cohort.{}.regression.{}
            args: ["*", "elastic"]
            ```
        """
        kvs = list(kv_pairs)
        if not len(kvs):
            return self
        args = list(self.args)
        kwargs = dict(**self.kwargs)
        for key, value in kvs:
            if isinstance(key, int):
                args[key] = value
            elif isinstance(key, str):
                assert key in kwargs
                kwargs[key] = value
            else:
                raise IndexError
        path = self.construct(self.template, args, kwargs)
        if path is None:
            raise FileNameFlowAssert
        return path

    def with_filename(self, filename: str) -> FileNamePath | None:
        """
        Create a new FileNamePath by filename and extract the args, kwargs by self.template.

        Returns:
            FileNamePath | None: A new FileNamePath object if successful, else None.
        """
        name, args, kwargs = self.parse(filename)
        if name is None:
            return None
        return self.construct(self.template, args, kwargs)

    def parse(
        self, filename: str
    ) -> tuple[str | None, tuple[str, ...], dict[str, str]]:
        """
        Parse the filename into arguments and keyword arguments according to self.template.
        The suffix is always removed.
        Returns template, args, kwargs.

        If the filename does not match the template, `("", [], {})` is returned.

        Example:
            ```
            path = "test.{}.regression.{type}"
            # After .parse("test.sample1.regression.lasso.txt")
            template = "test.{}.regression.{type}"
            args = ["sample1"]
            kwargs = {"type": "lasso"}
            ```
        """
        for template in [
            self.template + ".{" + self.suffix_key + "}",  # with suffix
            self.template,  # without suffix
        ]:
            result = parse.parse(template, filename)
            self._logger.debug(f"Try to parse {filename} with {template}: {result}")
            if not result:
                continue

            if self.suffix_key in result.named:
                del result.named[self.suffix_key]
            if any(self.separator in v for v in result.fixed) or any(
                self.separator in v for k, v in result.named.items()
            ):
                continue

            return (
                self.template.format(*result.fixed, **result.named),
                tuple(i if i != "{}" else "*" for i in result.fixed),
                {
                    k: (v if v != "{" + k + "}" else "*")
                    for k, v in result.named.items()
                },
            )
        return None, (), {}

    def commit(self) -> FileNamePath:
        """
        Substitue wildcards in the template by values from args and kwargs,
        except the value is wildcard charater '*'.

        Example:
            ``` python
            path: test.{}.regression.lasso
            template: test.{}.regression.{type}
            args: ["*"]
            kwargs: {type: "lasso"}

            # After .commit()
            path: test.{}.regression.lasso
            template: test.{}.regression.lasso
            args: ["*"]
            kwargs: {}
            ```
        """
        return FileNamePath(str(self))

    def list(self, fix: Iterable[str | int] = ()) -> Iterable[FileNamePath]:
        """
        List files that match the current(self)'s name.

        Example:
            ```
            Path: "cohort.{}.regression.{}"
            fix: [1]
            # then Return:
            * `cohort.sample1.regression.{}`
            * `cohort.sample2.regression.{}`
            * `cohort.sample3.regression.{}`
            ```

        Args:
            fix: The index of arg, kwargs that keep unlisted.
                     Note that the index is the wildcard's index on path, not the index in template.
        """
        # list files that match the glob pattern
        glob_pattern = self.template.format(*self.args, **self.kwargs)
        if os.path.isdir(glob_pattern):
            yield self
            return

        files = glob.iglob(glob_pattern + "*")
        # list files that match the filename pattern
        path = self.commit()
        paths_or_err = map(path.with_filename, files)
        paths: Iterable[FileNamePath] = filter(None, paths_or_err)
        paths = unique(paths, str)

        # apply fix arg
        wildcard_kv = tuple(zip(fix, repeat("*")))
        paths = map(lambda path: path.overwrites(wildcard_kv), paths)
        paths = unique(paths, str)

        # change path to self
        paths_or_err = map(path.with_filename, paths)
        paths = filter(
            None, paths_or_err
        )  # This will not filter anything (just for type checking)
        yield from paths

    def is_file(self) -> bool:
        """Check if the FileNamePath represents a file (no wildcards in the path)"""
        return "*" not in self.args and "*" not in self.kwargs.values()

    def replace_wildcard(self, text: str = "_merge") -> FileNamePath:
        """
        Replace the wildcard, similar to `.replace(".{}", "_merge")`.
        After replacment, the arg/kwargs will be removed.
        """
        tmp_placeholder = "tmp_text_before_replace_the_wildcard"

        # replace the wildcard
        template = self.template
        args = ["*" if i != "*" else tmp_placeholder for i in self.args]
        kwargs = {
            k: ("*" if v != "*" else tmp_placeholder) for k, v in self.kwargs.items()
        }
        path = self.construct(template, args, kwargs)

        # replace the '.wildcard'
        template = str(path).replace(self.separator + tmp_placeholder, text)
        if tmp_placeholder in template:
            logging.warning("Recommand replacing '.{}' instead of '{}'")
            template = str(path).replace(tmp_placeholder, text)

        # remove the arg that being replaced
        args = [i for i in self.args if i != "*"]
        kwargs = {k: v for k, v in self.kwargs.items() if v != "*"}
        return self.construct(template, args, kwargs)

    def get_args(self, key: int | str) -> str:
        """Get arg, kwargs by key (int for positional arg, str for keyword kwargs)"""
        if isinstance(key, int):
            return self.args[key]
        elif isinstance(key, str):
            return self.kwargs[key]
        else:
            raise IndexError

    def list_args(self) -> Mapping[str | int, str]:
        """List arg, kwargs in this FileNamePath, return a dict-like object"""
        all_args: dict[str | int, str] = {}
        for k, v in self.kwargs.items():
            all_args[k] = v
        for k, v in enumerate(self.args):  # type: ignore
            all_args[k] = v
        return all_args

    def __add__(self, other: Any) -> FileNamePath:
        """
        Concat the path.

        The function carefully handle the value in template, args, kwargs
        after concat.

        Example:
          ```
          FileNamePath("a.{}.c") + ".d"
          # result
          FileNamePath("a.{}.c.d")
          ```
        """
        # Note that template may have "{}" but I ignore
        path = FileNamePath(other)
        return self.construct(
            self.template + path.template,
            self.args + path.args,
            dict(**self.kwargs, **path.kwargs),
        )

    def __radd__(self, other: Any) -> FileNamePath:
        """Concat the path"""
        path = FileNamePath(other)
        return self.construct(
            path.template + self.template,
            path.args + self.args,
            dict(**self.kwargs, **path.kwargs),
        )

    def __rshift__(self, others: Any) -> Any:
        """see compose()"""
        from . import compose  # avoid recursive import

        return compose([self, others])

    def __rrshift__(self, others: Any) -> Any:
        """see compose()"""
        from . import compose

        return compose([others, self])
