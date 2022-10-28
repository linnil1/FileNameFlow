import os
import shutil
import logging
import unittest
from typing import Iterable
from pathlib import Path
from tempfile import TemporaryDirectory
from functools import partial

from namepipe import NamePath, NameTask, compose, nt
from namepipe.error import NamePipeError
logging.basicConfig(level=logging.DEBUG)


class TestPath(unittest.TestCase):
    """
    This test the core functionality
    """
    def setUp(self):
        self.tmp_dir = tmp_dir = "/tmp/test"
        Path(self.tmp_dir).mkdir()
        Path(f"{tmp_dir}/test.0.test2.a.txt").touch()
        Path(f"{tmp_dir}/test.0.test2.a.txt.gz").touch()
        Path(f"{tmp_dir}/test.0.test2.b.txt").touch()
        Path(f"{tmp_dir}/test.0.test2.c.txt.gz").touch()
        Path(f"{tmp_dir}/test.1.test2.a.txt").touch()
        Path(f"{tmp_dir}/test.1.test2.b.txt.ga").touch()

    def tearDown(self):
        shutil.rmtree(self.tmp_dir)

    def assertLeng(self, arr: Iterable, num: int):
        self.assertEqual(len(list(arr)), num)

    def test_basic_method(self):
        """
        Test NamePath's method
        1. str inherited method
        3. str(NamePath)
        3. NamePath + str
        """
        # inherited from str
        name = NamePath("123.{}.565")
        self.assertEqual(type(name), NamePath)
        self.assertIsInstance(name, str)
        self.assertEqual(name.format("abc"), "123.abc.565")

        # add
        name = NamePath("123")
        self.assertEqual(type(name), NamePath)
        self.assertEqual(type(name + ".456"), NamePath)
        self.assertEqual(name + ".456", "123.456")
        self.assertEqual("test/" + name, "test/123")

    def test_name_glob(self):
        """ Test NamePath's glob method and depended wildcard """
        tmp_dir = self.tmp_dir
        # same as glob
        self.assertLeng(NamePath(tmp_dir + "/test.{}.test2.{}"  ).get_input_names(), 5)
        self.assertLeng(NamePath(tmp_dir + "/test.{}.test2.a"   ).get_input_names(), 2)
        self.assertLeng(NamePath(tmp_dir + "/test.1.test2.{}"   ).get_input_names(), 2)
        self.assertLeng(NamePath(tmp_dir + "/test.1.test2.a"    ).get_input_names(), 1)
        self.assertLeng(NamePath(tmp_dir + "/test.1.test2.a.txt").get_input_names(), 1)
        self.assertLeng(NamePath(tmp_dir + ""                   ).get_input_names(), 1)
        self.assertLeng(NamePath(tmp_dir + "/test1"             ).get_input_names(), 0)
        self.assertLeng(NamePath(tmp_dir + "/1test"             ).get_input_names(), 0)

        # the concept different from glob
        self.assertLeng(NamePath(tmp_dir + "/test.{}.a"         ).get_input_names(), 0)
        self.assertLeng(NamePath(tmp_dir + "/test.{}.a.txt"     ).get_input_names(), 0)

        # same as glob but do not exploded some wildcard
        self.assertLeng(NamePath(tmp_dir + "/test.{}.test2.{}").get_input_names([0   ]), 3)
        self.assertLeng(NamePath(tmp_dir + "/test.{}.test2.{}").get_input_names([1   ]), 2)
        self.assertLeng(NamePath(tmp_dir + "/test.{}.test2.{}").get_input_names([-1  ]), 2)
        self.assertLeng(NamePath(tmp_dir + "/test.{}.test2.{}").get_input_names([0, 1]), 1)

        # o1ut of range
        with self.assertRaises(IndexError):
            self.assertLeng(NamePath(tmp_dir + "/test.{}.test2.{}").get_input_names([2]), 1)

        # propogated wildcard
        testa = NamePath(tmp_dir + "/test.{}.test2.{}").get_input_names([0])[0]
        self.assertLeng(testa.get_input_names(), 2)
        test0 = NamePath(tmp_dir + "/test.{}.test2.{}").get_input_names([1])[0]
        self.assertLeng(test0.get_input_names(), 3)

    def test_folder_glob(self):
        """ Same as glob but wildcard the folder """
        tmp_dir = self.tmp_dir
        Path(self.tmp_dir + "/test").mkdir()
        Path(self.tmp_dir + "/test1").mkdir()
        Path(self.tmp_dir + "/test2").mkdir()
        Path(f"{tmp_dir}/test/test.0.txt").touch()
        Path(f"{tmp_dir}/test/test.1.txt").touch()
        Path(f"{tmp_dir}/test/test.2.txt").touch()
        Path(f"{tmp_dir}/test1/test1.0.txt").touch()
        Path(f"{tmp_dir}/test1/test1.1.txt").touch()
        Path(f"{tmp_dir}/test2/test2.0.txt").touch()
        self.assertLeng(NamePath(tmp_dir + "/{}"     ).get_input_names(), 3)
        self.assertLeng(NamePath(tmp_dir + "/{}/{}.0").get_input_names(), 3)
        self.assertLeng(NamePath(tmp_dir + "/{}/{}.1").get_input_names(), 2)
        for i in NamePath(tmp_dir + "/{}/{}.1").get_input_names():
            self.assertEqual(i.template_args[0], i.template_args[1])

    def test_template_and_args(self):
        """ Double check NamePath's template and template_args variables """
        tmp_dir = self.tmp_dir

        # 0 args
        names = NamePath(tmp_dir + "/test.0").get_input_names()
        self.assertLeng( names, 1)
        self.assertEqual(names[0].template,      tmp_dir + "/test.0")
        self.assertLeng( names[0].template_args, 0)

        # 2 args
        args = []
        for name in NamePath(tmp_dir + "/test.{}.test2.{}").get_input_names():
            self.assertEqual(name.template,         tmp_dir + "/test.{}.test2.{}")
            self.assertLeng( name.template_args,    2)
            self.assertIn(   name.template_args[0], ["0", "1"])
            self.assertIn(   name.template_args[1], ["a", "b", "c"])
            args.append(name.template_args)

        # 2 args with depended field
        args1 = []
        for names in NamePath(tmp_dir + "/test.{}.test2.{}").get_input_names([1]):
            self.assertLeng( names.template_args,    2)
            self.assertEqual(names.template,         tmp_dir + "/test.{}.test2.{}")
            self.assertIn(   names.template_args[0], ["0", "1"])
            self.assertEqual(names.template_args[1], "{}")
            self.assertTrue( names.endswith(".{}"))

            for name in names.get_input_names():
                self.assertEqual(name.template,         tmp_dir + "/test.{}.test2.{}")
                self.assertLeng( name.template_args,    2)
                self.assertIn(   name.template_args[0], names.template_args[0])
                self.assertIn(   name.template_args[1], ["a", "b", "c"])
                args1.append(name.template_args)

        self.assertEqual(args, args1)

    def test_wildcard_formatting(self):
        """
        Replace NamePath's wildcard with merge
        i.e. NamePath.replace_wildcard()
        """
        tmp_dir = self.tmp_dir
        testa = NamePath(tmp_dir + "/test.{}.test2.{}").get_input_names([0])[0]
        self.assertEqual(testa.template_args[0], "{}")
        self.assertEqual(testa.template_args[1], "a")
        self.assertEqual(testa.replace_wildcard("_merge"), tmp_dir + "/test_merge.test2.a")

        test0 = NamePath(tmp_dir + "/test.{}.test2.{}").get_input_names([1])[0]
        self.assertEqual(test0.template_args[0], "0")
        self.assertEqual(test0.template_args[1], "{}")
        self.assertEqual(test0.replace_wildcard("_merge"), tmp_dir + "/test.0.test2_merge")

        test_both = NamePath(tmp_dir + "/test.{}.test2.{}").get_input_names([0, 1])[0]
        self.assertEqual(test_both.template_args[0], "{}")
        self.assertEqual(test_both.template_args[1], "{}")
        self.assertEqual(test_both.replace_wildcard("_merge"), tmp_dir + "/test_merge.test2_merge")


def subtask(input_name):
    Path(f"{input_name}.test2.a.txt").touch()
    Path(f"{input_name}.test2.b.txt").touch()
    return input_name + ".test2.{}"


def subtask3(input_name):
    Path(f"{input_name}.test3.c.txt").touch()
    Path(f"{input_name}.test3.d.txt").touch()
    return input_name + ".test3.{}"


class TestTask(unittest.TestCase):
    def setUp(self):
        self.tmp_dir = "/tmp/test"
        Path(self.tmp_dir).mkdir()

    def tearDown(self):
        shutil.rmtree(self.tmp_dir)

    def assertLeng(self, arr: Iterable, num: int):
        self.assertEqual(len(list(arr)), num)

    def test_basic_operation(self):
        """
        Test all basic operation
        * None or str as start name
        * raise when input_name cannot find
        * assert the name in result
        * raise when file cannot be found
        """
        tmp_dir = self.tmp_dir

        # start name and assert
        None    >> NameTask(func=lambda i: "test/test1") >> "test/test1"
        ""      >> NameTask(func=lambda i: "test/test2") >> "test/test2"
        tmp_dir >> NameTask(func=lambda i: i + "/test3") >> tmp_dir + "/test3"

        # assert the result name
        with self.assertRaises(NamePipeError):
            None >> NameTask(func=lambda i: "test/test4") >> "test/test5"

        # input name not found
        with self.assertRaises(NamePipeError):
            "test1231" >> NameTask(func=lambda i: "test/test4") >> "test/test5"

    def test_basic_task_running(self):
        """
        Tests:
        * Run the task
        * cascading >>
        * tesk can be chained and saved in variable
        * Fail to execute the task
        """
        tmp_dir = self.tmp_dir
        Path(f"{tmp_dir}/test.0.txt").touch()
        Path(f"{tmp_dir}/test.1.txt").touch()
        self.assertLeng(Path(tmp_dir).iterdir(), 2)

        # run a task
        tmp_dir + "/test.{}"         >> NameTask(func=subtask)  >> tmp_dir + "/test.{}.test2.{}"
        self.assertLeng(Path(tmp_dir).iterdir(), 2 + 4)

        # run two task
        task1 = tmp_dir + "/test.{}" >> NameTask(func=subtask)  >> tmp_dir + "/test.{}.test2.{}"
        self.assertLeng(Path(tmp_dir).iterdir(), 2 + 4)
        task2 = task1                >> NameTask(func=subtask)  >> tmp_dir + "/test.{}.test2.{}.test2.{}"
        self.assertLeng(Path(tmp_dir).iterdir(), 2 + 4 + 8)

        # continue name from intermediate task (task1 instead of task2)
        task1                        >> NameTask(func=subtask3) >> tmp_dir + "/test.{}.test2.{}.test3.{}"
        self.assertLeng(Path(tmp_dir).iterdir(), 2 + 4 + 8 + 8)

        # or run two tasks in one line
        tmp_dir + "/test.{}"         >> NameTask(func=subtask3) \
                                     >> NameTask(func=subtask)  >> tmp_dir + "/test.{}.test3.{}.test2.{}"
        self.assertLeng(Path(tmp_dir).iterdir(), 2 + 4 + 8 + 8 + 4 + 8)

        # Task did not run
        with self.assertRaises(NamePipeError):
            NameTask(func=lambda i: "test/test4") >> "test/test5"

    def test_concurrent(self):
        """ Change the executor to concurrent running the tasks """
        tmp_dir = self.tmp_dir
        Path(f"{tmp_dir}/test.0.txt").touch()
        Path(f"{tmp_dir}/test.1.txt").touch()

        # set global executor
        from namepipe.executor import BaseTaskExecutor, ConcurrentTaskExecutor
        NameTask.set_default_executor(ConcurrentTaskExecutor())
        tmp_dir + "/test.{}" >> NameTask(func=subtask) >> tmp_dir + "/test.{}.test2.{}"
        self.assertLeng(Path(tmp_dir).iterdir(), 2 + 4)
        NameTask.set_default_executor(BaseTaskExecutor())

        # set the task with speicfic executor
        tmp_dir + "/test.{}" >> NameTask(func=subtask).set_executor(ConcurrentTaskExecutor()) >> tmp_dir + "/test.{}.test2.{}"
        self.assertLeng(Path(tmp_dir).iterdir(), 2 + 4)

    def test_standalone(self):
        """ Change the StandaloneTaskExecutor """
        ori_dir = os.getcwd()
        os.chdir(ori_dir + "/tests")
        tmp_dir = self.tmp_dir
        Path(f"{tmp_dir}/test.0.txt").touch()
        Path(f"{tmp_dir}/test.1.txt").touch()

        from namepipe.executor import BaseTaskExecutor, StandaloneTaskExecutor
        NameTask.set_default_executor(StandaloneTaskExecutor(auto_cleanup=False))
        tmp_dir + "/test.{}" >> NameTask(func=subtask) >> tmp_dir + "/test.{}.test2.{}"
        self.assertLeng(Path(tmp_dir).iterdir(), 2 + 4)

        # check temp files
        job_files = list(filter(lambda i: "job_" in str(i), os.listdir()))
        self.assertLeng(job_files, 4)
        self.assertLeng(filter(lambda i: i.endswith(".in"), job_files), 2)
        self.assertLeng(filter(lambda i: i.endswith(".out"), job_files), 2)

        # auto_cleanup = True
        # and test depended
        NameTask.set_default_executor(StandaloneTaskExecutor())
        tmp_dir + "/test.{}" >> NameTask(func=subtask3, depended_pos=[-1]) >> tmp_dir + "/test.{}.test3.{}"
        self.assertLeng(Path(tmp_dir).iterdir(), 2 + 4 + 2)
        job_files = list(filter(lambda i: "job_" in str(i), os.listdir()))
        self.assertLeng(job_files, 4)

        # set it back
        # use auto cleanup to remove temp job_*
        tmp_dir + "/test.{}" >> NameTask(func=subtask) >> tmp_dir + "/test.{}.test2.{}"
        os.chdir(ori_dir)
        NameTask.set_default_executor(BaseTaskExecutor())


    def test_compose(self):
        """
        Use compose instead of cascading >>
        * original compose
        * allow callable and str
        """
        tmp_dir = self.tmp_dir
        Path(f"{tmp_dir}/test.1.txt").touch()
        Path(f"{tmp_dir}/test.2.txt").touch()
        Path(f"{tmp_dir}/test.2.a.txt").touch()

        # task11 = None >> NameTask(func=lambda i: tmp_dir + "/test.{}") >> tmp_dir + "/test.{}"
        # task12 = task11 >> tmp_dir + "/test.{}"
        task11 = compose([None, NameTask(func=lambda i: tmp_dir + "/test.{}"), tmp_dir + "/test.{}"])
        task12 = compose([task11, tmp_dir + "/test.{}"])

        # task23 = tmp + "/test.{}" >> NameTask(func=lambda i: tmp_dir + ".a") >> tmp_dir + "/test.{}.a"
        task21 = compose([tmp_dir + "/test.{}"])
        task22 = compose([lambda i: i + ".a"])  # callable
        task23 = task21 >> task22 >> tmp_dir + "/test.{}.a"
        task24 = compose([task21, task22, tmp_dir + "/test.{}.a"])

        # tmp_dir + "/test.{}.a" >> tmp_dir + "/test.{}.a"]
        compose([tmp_dir + "/test.{}.a", tmp_dir + "/test.{}.a"])
        compose([tmp_dir + "/test.{}.a"]) >> compose([tmp_dir + "/test.{}.a"])
        with self.assertRaises(NamePipeError):
            compose([tmp_dir + "/test.{}.a", tmp_dir + "/test.{}.b"])

    def test_replace_wildcard_by_merge(self):
        """ Test replace_wildcard work with task execution """
        tmp_dir = self.tmp_dir
        Path(f"{tmp_dir}/test.0.test2.a.txt").touch()
        Path(f"{tmp_dir}/test.0.test2.b.txt").touch()
        Path(f"{tmp_dir}/test.0.test2.c.txt.gz").touch()
        Path(f"{tmp_dir}/test.1.test2.a.txt").touch()
        Path(f"{tmp_dir}/test.1.test2.b.txt.ga").touch()
        tmp_dir + "/test.{}.test2.{}" \
                >> NameTask(func=lambda i: i.replace_wildcard("_mergeabc") + ".csv").set_depended(-1) \
                >> (tmp_dir + "/test.{}.test2_mergeabc.csv")
        tmp_dir + "/test.{}.test2.{}" \
                >> NameTask(func=lambda i: i.replace_wildcard("_mergetest") + ".csv").set_depended(0) \
                >> (tmp_dir + "/test_mergetest.test2.{}.csv")

    def test_merge_task_result_name(self):
        """ The internal output_name merging logic """
        tmp_dir = self.tmp_dir
        Path(f"{tmp_dir}/test.0.test2.a.txt").touch()
        Path(f"{tmp_dir}/test.1.test2.b.txt").touch()
        # return NamePath
        tmp_dir + "/test.{}.test2.{}" >> NameTask(func=lambda i: i + ".a") >> tmp_dir + "/test.{}.test2.{}.a"
        # return str
        tmp_dir + "/test.{}.test2.{}" >> NameTask(func=lambda i: "123.a") >> "123.a"
        # return mixed
        def task_merge(input_name):
            if input_name.template_args[0] == "0":
                return input_name.template + ".b"
            else:
                return input_name + ".b"
        tmp_dir + "/test.{}.test2.{}" >> NameTask(func=task_merge) >> tmp_dir + "/test.{}.test2.{}.b"

    def test_args(self):
        """ Set the arguments for function i.e. .set_args """
        tmp_dir = self.tmp_dir
        Path(f"{tmp_dir}/test.0.txt").touch()
        Path(f"{tmp_dir}/test.1.txt").touch()

        # set args
        task_args = NameTask(func=lambda i, j: i + "." + j)
        task_args1 = task_args.set_args(j="a")
        tmp_dir + "/test.{}" >> task_args1                >> tmp_dir + "/test.{}.a"
        tmp_dir + "/test.{}" >> task_args.set_args(j="b") >> tmp_dir + "/test.{}.b"
        tmp_dir + "/test.{}" >> task_args1                >> tmp_dir + "/test.{}.a"

        # set kwargs
        def task_kwarg(input_name, index="123"):
            return input_name + "." + index
        task_kwarg = NameTask(func=task_kwarg)
        task_kwarg1 = task_kwarg.set_args(index="a")

        tmp_dir + "/test.{}" >> task_kwarg1                    >> tmp_dir + "/test.{}.a"
        tmp_dir + "/test.{}" >> task_kwarg.set_args(index="b") >> tmp_dir + "/test.{}.b"
        tmp_dir + "/test.{}" >> task_kwarg1                    >> tmp_dir + "/test.{}.a"
        tmp_dir + "/test.{}" >> task_kwarg                     >> tmp_dir + "/test.{}.123"

    def test_suger_syntax(self):
        """
        test >> with minimal text
        * Use callable instead of NameTask
        * Use NamePath to avoid NameTask in the first task
        """
        tmp_dir = self.tmp_dir
        Path(f"{tmp_dir}/test.0.txt").touch()
        Path(f"{tmp_dir}/test.0.123.txt").touch()
        func = lambda i: i + ".123"
        tmp_dir + "/test.{}" >> nt(func) >> tmp_dir + "/test.{}.123"
        tmp_dir + "/test.{}" >> nt(func) >> func >> tmp_dir + "/test.{}.123.123"
        NamePath(tmp_dir + "/test.{}") >> func >> func >> tmp_dir + "/test.{}.123.123"
        task1 = tmp_dir + "/test.{}" >> nt(func)
        task1 >> tmp_dir + "/test.{}.123"
        task1 >> func >> tmp_dir + "/test.{}.123.123"

        # assertion test
        NamePath(tmp_dir + "/test.{}") >> tmp_dir + "/test.{}"
        tmp_dir + "/test.{}" >> NamePath(tmp_dir + "/test.{}")

    def test_nt(self):
        """
        test nt and NameTask Init functionality
        * directed call (nt, NameTask)
        * directed call (NameTask) with parameters
        * decorator (nt)
        """
        tmp_dir = self.tmp_dir
        Path(f"{tmp_dir}/test.0.txt").touch()
        Path(f"{tmp_dir}/test.1.txt").touch()
        func = lambda i, j=".j": i + j + ".123"
        tmp_dir + "/test.{}" >> nt(func)                    >> tmp_dir + "/test.{}.j.123"
        tmp_dir + "/test.{}" >> nt(func).set_args(j=".j2")  >> tmp_dir + "/test.{}.j2.123"
        tmp_dir + "/test.{}" >> nt(partial(func, j=".j2"))  >> tmp_dir + "/test.{}.j2.123"
        tmp_dir + "/test.{}" >> NameTask(lambda i: i.replace_wildcard() + ".123",
                                         depended_pos=[0])  >> tmp_dir + "/test_merge.123"
        @nt
        def func1(i):
            return i + ".123"
        tmp_dir + "/test.{}" >> func1                       >> tmp_dir + "/test.{}.123"

        @nt
        def func2(i, j=".j"):
            return i + j + ".123"
        tmp_dir + "/test.{}" >> func2.set_args(j=".j2")     >> tmp_dir + "/test.{}.j2.123"

    def test_strange_case(self):
        """ unsupport method, but i test it, maybe someday will be move to TODO """
        tmp_dir = self.tmp_dir
        Path(f"{tmp_dir}/test.0.txt").touch()
        func = lambda i: i + ".123"

        # may be added
        with self.assertRaises(NamePipeError):
            compose([func, func])

        # never work, because of built-in type
        with self.assertRaises(TypeError):
            "" >> "1234"
        with self.assertRaises(TypeError):
            "{tmp_dir}/test.0" >> "{tmp_dir}/test.0"
        with self.assertRaises(TypeError):
            f"{tmp_dir}/test.0" >> func >> f"{tmp_dir}/test.0.123"
