import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
import shutil
import logging

from namepipe import NamePath, NameTask
from namepipe.error import NamePipeError
logging.basicConfig(level=logging.DEBUG)


class TestPath(unittest.TestCase):
    """
    This test the core functionality
    """
    def test_str_function(self):
        # inherited from str
        name = NamePath("123.{}.565")
        self.assertEqual(type(name), NamePath)
        self.assertIsInstance(name, str)
        self.assertEqual(name.format("abc"), "123.abc.565")

        # I overwrite the add
        name = NamePath("123")
        self.assertEqual(type(name), NamePath)
        self.assertEqual(type(name + ".456"), NamePath)

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

    def test_basic_list_files(self):
        tmp_dir = self.tmp_dir
        self.assertEqual(len(NamePath(tmp_dir + "/test.{}.test2.{}").get_input_names()), 5)
        self.assertEqual(len(NamePath(tmp_dir + "/test.{}.test2.a").get_input_names()), 2)
        self.assertEqual(len(NamePath(tmp_dir + "/test.1.test2.{}").get_input_names()), 2)
        self.assertEqual(len(NamePath(tmp_dir + "/test.1.test2.a").get_input_names()), 1)
        self.assertEqual(len(NamePath(tmp_dir + "/test.1.test2.a.txt").get_input_names()), 1)
        self.assertEqual(len(NamePath(tmp_dir + "").get_input_names()), 1)
        self.assertEqual(len(NamePath(tmp_dir + "/test1").get_input_names()), 0)

    def test_depended_list_files(self):
        tmp_dir = self.tmp_dir
        self.assertEqual(len(NamePath(tmp_dir + "/test.{}.test2.{}").get_input_names([0])), 3)
        self.assertEqual(len(NamePath(tmp_dir + "/test.{}.test2.{}").get_input_names([1])), 2)
        self.assertEqual(len(NamePath(tmp_dir + "/test.{}.test2.{}").get_input_names([-1])), 2)
        self.assertEqual(len(NamePath(tmp_dir + "/test.{}.test2.{}").get_input_names([0, 1])), 1)
        self.assertEqual(len(NamePath(tmp_dir + "/test.{}.test2.{}").get_input_names([1])[0].get_input_names()), 3)
        self.assertEqual(len(NamePath(tmp_dir + "/test.{}.test2.{}").get_input_names([1])[1].get_input_names()), 2)

    def test_args_files(self):
        tmp_dir = self.tmp_dir

        args = []
        for name in NamePath(tmp_dir + "/test.{}.test2.{}").get_input_names():
            self.assertEqual(tmp_dir + "/test.{}.test2.{}", name.template)
            self.assertEqual(len(name.template_args), 2)
            self.assertIn(name.template_args[0], ["0", "1"])
            self.assertIn(name.template_args[1], ["a", "b", "c"])
            args.append(name.template_args)

        depend_args = []
        for names in NamePath(tmp_dir + "/test.{}.test2.{}").get_input_names([1]):
            self.assertTrue(names.endswith(".{}"))
            self.assertEqual(tmp_dir + "/test.{}.test2.{}", names.template)
            for name in names.get_input_names():
                self.assertEqual(tmp_dir + "/test.{}.test2.{}", name.template)
                depend_args.append(name.template_args)

        self.assertEqual(args, depend_args)

    def test_replace_wildcard(self):
        tmp_dir = self.tmp_dir
        for name in NamePath(tmp_dir + "/test.{}.test2.{}").get_input_names([1]):
            new_name = name.replace_wildcard("_merge")
            self.assertEqual(str(new_name), tmp_dir + f"/test.{name.template_args[0]}" + ".test2.{}".replace(".{}", "_merge"))
            self.assertEqual(str(new_name), tmp_dir + f"/test.{new_name.template_args[0]}" + ".test2.{}".replace(".{}", "_merge"))
            self.assertEqual(str(new_name.template), tmp_dir + "/test.{}" + ".test2.{}".replace(".{}", "_merge"))

    def test_folder_wildcard(self):
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
        self.assertEqual(len(NamePath(tmp_dir + "/{}").get_input_names()), 3)
        self.assertEqual(len(NamePath(tmp_dir + "/{}/{}.0").get_input_names()), 3)
        self.assertEqual(len(NamePath(tmp_dir + "/{}/{}.1").get_input_names()), 2)
        for i in NamePath(tmp_dir + "/{}/{}.1").get_input_names():
            self.assertEqual(i.template_args[0], i.template_args[1])


class TestTask(unittest.TestCase):
    def setUp(self):
        self.tmp_dir = tmp_dir = "/tmp/test"
        Path(self.tmp_dir).mkdir()

    def tearDown(self):
        shutil.rmtree(self.tmp_dir)

    def test_start(self):
        tmp_dir = self.tmp_dir
        None   >> NameTask(func=lambda i: "test/test1") >> "test/test1"
        ""     >> NameTask(func=lambda i: "test/test2") >> "test/test2"
        tmp_dir >> NameTask(func=lambda i: i + "/test3") >> tmp_dir + "/test3"

        Path(f"{self.tmp_dir}/test3.txt").touch()
        tmp_dir >> NameTask(func=lambda i: i + "/test3") >> NameTask(func=lambda i: i + ".test4") >> tmp_dir + "/test3.test4"

        with self.assertRaises(NamePipeError):
            None >> NameTask(func=lambda i: "test/test4") >> "test/test5"

        with self.assertRaises(NamePipeError):
            "test1231" >> NameTask(func=lambda i: "test/test4") >> "test/test5"

    def test_map(self):
        tmp_dir = self.tmp_dir
        Path(f"{tmp_dir}/test.0.test2.a.txt").touch()
        Path(f"{tmp_dir}/test.0.test2.b.txt").touch()
        Path(f"{tmp_dir}/test.0.test2.c.txt.gz").touch()
        Path(f"{tmp_dir}/test.1.test2.a.txt").touch()
        Path(f"{tmp_dir}/test.1.test2.b.txt.ga").touch()
        tmp_dir + "/test.{}" >> NameTask(func=lambda i: i + ".test2.{}") >> NameTask(func=lambda i: i + ".txt") >> (tmp_dir + "/test.{}.test2.{}.txt")

        a = tmp_dir + "/test.{}" >> NameTask(func=lambda i: i + ".test2")
        a >> NameTask(func=lambda i: i + ".{}") >> (tmp_dir + "/test.{}.test2.{}")
        a >> NameTask(func=lambda i: i + ".{}.txt") >> (tmp_dir + "/test.{}.test2.{}.txt")
    
    def test_merge(self):
        tmp_dir = self.tmp_dir
        Path(f"{tmp_dir}/test.0.test2.a.txt").touch()
        Path(f"{tmp_dir}/test.0.test2.b.txt").touch()
        Path(f"{tmp_dir}/test.0.test2.c.txt.gz").touch()
        Path(f"{tmp_dir}/test.1.test2.a.txt").touch()
        Path(f"{tmp_dir}/test.1.test2.b.txt.ga").touch()
        tmp_dir + "/test.{}.test2.{}" >> NameTask(func=lambda i: i.replace_wildcard("_merge") + ".123").set_depended(-1) >> (tmp_dir + "/test.{}.test2_merge.123")
