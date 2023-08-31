import shutil
import unittest
from typing import Iterable
from pathlib import Path
from functools import partial

from filenameflow import FileNameTask, FileNamePath, compose, FileNameFlowError


def subtask_1tomany(input_name):
    output_name = input_name + ".t1M.{}"
    Path(f"{output_name.format('a')}.txt").touch()
    Path(f"{output_name.format('b')}.txt").touch()
    Path(f"{output_name.apply(0, 'c')}.txt").touch()
    return output_name


def subtask_1tomanyWithName(input_name):
    output_name = input_name + ".t1MN.{data}"
    Path(f"{output_name.format({'data': 'a'})}.txt").touch()
    Path(f"{output_name.apply('data', 'd')}.txt").touch()
    # return output_name
    return output_name.template


def subtask_1to1(input_name, test_arg=""):
    output_name = input_name + ".t11" + test_arg
    Path(f"{output_name}.txt.gz").touch()
    return output_name


def subtask_manyto1(input_name):
    output_name = input_name.replace_wildcard("_merge_this")
    Path(f"{output_name}").touch()
    return output_name


@FileNameTask.wrapper(fix=[-1])
def func_wrap1_manyto1(input_name):
    return subtask_manyto1(input_name)


@FileNameTask.wrapper
def func_wrap2_manyto1(input_name):
    return subtask_manyto1(input_name)


class TestTask(unittest.TestCase):
    def setUp(self):
        self.tmp_dir = "/tmp/test_for_filenametask"
        Path(self.tmp_dir).mkdir()
        Path(self.tmp_dir + "/init").touch()

    def tearDown(self):
        shutil.rmtree(self.tmp_dir)

    def assertLeng(self, arr: Iterable, num: int):
        self.assertEqual(len(list(arr)), num)

    def test_basic(self):
        a = FileNameTask(subtask_1to1).run(FileNamePath(self.tmp_dir + "/init")).output
        self.assertEqual(a, self.tmp_dir + "/init.t11")

        d = (
            FileNameTask(partial(subtask_1to1, test_arg="test2"))
            .run(FileNamePath(self.tmp_dir + "/init"))
            .output
        )
        self.assertEqual(d, self.tmp_dir + "/init.t11test2")

        d = (
            FileNameTask(subtask_1to1)(test_arg="test3")
            .run(FileNamePath(self.tmp_dir + "/init"))
            .output
        )
        self.assertEqual(d, self.tmp_dir + "/init.t11test3")

        b = FileNameTask(subtask_1tomany).run(a).output
        self.assertEqual(b, str(a) + ".t1M.{}")

        c = FileNameTask(subtask_manyto1, fix=[-1]).run(b).output
        self.assertEqual(c, str(a) + ".t1M_merge_this")

        with self.assertRaises(FileNameFlowError):
            FileNameTask(subtask_manyto1).output

    def test_compose(self):
        a = compose([self.tmp_dir + "/init", subtask_1to1, self.tmp_dir + "/init.t11"])
        b = (
            self.tmp_dir + "/init.t11"
            >> FileNamePath(self.tmp_dir + "/init.t11")
            >> subtask_1to1
            >> self.tmp_dir + "/init.t11.t11"
        )

        with self.assertRaises(FileNameFlowError):
            compose([b, self.tmp_dir + "/init.t11.v13"])

        compose(
            [
                self.tmp_dir + "/init",
                partial(subtask_1to1, test_arg="test3"),
                subtask_1tomany,
                partial(subtask_1to1),
                FileNameTask(subtask_manyto1, fix=[-1]),
                FileNamePath(f"{self.tmp_dir}/init.t11test3.t1M_merge_this.t11"),
            ]
        )

    def test_decorator(self):
        (
            FileNamePath(self.tmp_dir + "/init")
            >> subtask_1tomany
            >> func_wrap1_manyto1
            >> self.tmp_dir + "/init.t1M_merge_this"
        )

        (
            self.tmp_dir + "/init"
            >> FileNameTask(subtask_1tomany)
            >> FileNameTask(func_wrap2_manyto1, fix=[-1])
            >> self.tmp_dir + "/init.t1M_merge_this"
        )
