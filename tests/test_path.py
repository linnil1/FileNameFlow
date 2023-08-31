import shutil
import unittest
from typing import Iterable
from pathlib import Path

from filenameflow import FileNamePath


class TestPath(unittest.TestCase):
    """
    This test the core functionality
    """

    def assertLeng(self, arr: Iterable, num: int):
        self.assertEqual(len(list(arr)), num)

    def test_init(self):
        a = FileNamePath("")
        self.assertLeng(a.args, 0)

        a = FileNamePath("test_file")
        self.assertLeng(a.args, 0)

        a = FileNamePath("test_file.{}.test_secd")
        self.assertEqual(a.args, ("*",))
        self.assertEqual(a.kwargs, {})

        a = FileNamePath("test_folder/test_file.{sample}.test_secd.{secd}")
        self.assertEqual(a.args, ())
        self.assertEqual(a.kwargs, {"sample": "*", "secd": "*"})

        a = FileNamePath("test_folder/test_file.{}.test_secd.{secd}.{}")
        self.assertEqual(a.args, ("*", "*"))
        self.assertEqual(a.kwargs, {"secd": "*"})

    def test_construct(self):
        a = FileNamePath.construct(
            "test_file.{}.test_secd.{secd}", ["*"], {"secd": "s2"}
        )
        self.assertEqual(a, "test_file.{}.test_secd.s2")
        self.assertFalse(a.is_file())

        a = FileNamePath.construct(
            "test_file.{}.test_secd.{secd}", ["s1"], {"secd": "*"}
        )
        self.assertEqual(a, "test_file.s1.test_secd.{secd}")
        self.assertFalse(a.is_file())

        a = FileNamePath.construct(
            "test_file.{}.test_secd.{secd}", ["s1"], {"secd": "s2"}
        )
        self.assertEqual(a, "test_file.s1.test_secd.s2")
        self.assertTrue(a.is_file())

        self.assertEqual(a.get_args(0), "s1")
        self.assertEqual(a.get_args("secd"), "s2")
        self.assertEqual(a.list_args(), {0: "s1", "secd": "s2"})

    def test_parse(self):
        _, a_args, a_kwargs = FileNamePath("test_file.{}.test_secd.{secd}").parse(
            "test_file.s1.test_secd.{secd}.txt"
        )
        self.assertEqual(a_args, ("s1",))
        self.assertEqual(a_kwargs, {"secd": "*"})

        a, a_args, a_kwargs = FileNamePath("test_file.{}.test_secd.{secd}").parse(
            "test_file.s1.more.test_secd.s2"
        )
        self.assertEqual(a, None)

        _, a_args, a_kwargs = FileNamePath(
            "test_file.{sample}.test_secd.{secd}.{}"
        ).parse("test_file.s1.test_secd.s2.s3.gg.txt")
        self.assertEqual(a_args, ("s3",))
        self.assertEqual(dict(a_kwargs), {"secd": "s2", "sample": "s1"})

        a, a_args, a_kwargs = FileNamePath("test_file.gg").parse("test_file.gg")
        self.assertEqual(a, "test_file.gg")

    def test_with_filename(self):
        # with_filename = parse + construct
        a = FileNamePath("test_file.{}.test_secd.{secd}").with_filename(
            "test_file.s1.test_secd.{secd}"
        )
        self.assertEqual(a.args, ("s1",))
        self.assertEqual(a.kwargs, {"secd": "*"})

        a = a.with_filename("test_file.s1.test_secd.gg.txt")
        self.assertEqual(a.args, ("s1",))
        self.assertEqual(a.kwargs, {"secd": "gg"})

    def test_overwrite_apply(self):
        a = FileNamePath.construct("file.{}.{}.sep.{secd}", ["s1", "s2"], {"secd": "*"})
        self.assertEqual(a.overwrite(0, "s3"), "file.s3.s2.sep.{secd}")
        self.assertEqual(a.overwrite("secd", "s3"), "file.s1.s2.sep.s3")
        self.assertEqual(a.apply("secd", "s3"), "file.s1.s2.sep.s3")
        with self.assertRaises(IndexError):
            self.assertEqual(a.apply(0, "s3"), "file.s1.s2.sep.s3")
        a = FileNamePath.construct("file.{}.{}.sep.{secd}", ["s1", "*"], {"secd": "*"})
        self.assertEqual(a.apply(0, "s3"), "file.s1.s3.sep.{secd}")
        self.assertEqual(a.apply("secd", "*"), "file.s1.{}.sep.{secd}")

    def test_commit(self):
        a = FileNamePath.construct("file.{}.sep.{secd}", ["s1"], {"secd": "s2"})
        a = a.commit()
        self.assertEqual(a, "file.s1.sep.s2")
        self.assertEqual(a.args, ())
        self.assertEqual(a.kwargs, {})

        a = FileNamePath.construct(
            "file.{}.hi.{third}.{}.sep.{secd}",
            ["s1", "*"],
            {"secd": "*", "third": "gg"},
        )
        a = a.commit()
        self.assertEqual(a, "file.s1.hi.gg.{}.sep.{secd}")
        self.assertEqual(a.args, ("*",))
        self.assertEqual(a.kwargs, {"secd": "*"})

    def test_replace_wildcard(self):
        a = FileNamePath.construct("file.{}.sep.{secd}", ["s1"], {"secd": "s2"})
        b = a.replace_wildcard("_merge")
        self.assertEqual(a, b)

        a = FileNamePath.construct("file.{}.sep.{secd}", ("*",), {"secd": "s2"})
        b = a.replace_wildcard("_merge")
        self.assertEqual(b, "file_merge.sep.s2")
        self.assertEqual(b.args, ())
        self.assertEqual(b.kwargs, a.kwargs)

    def test_list(self):
        tmp_dir = "/tmp/test_filenameflow"
        Path(tmp_dir).mkdir()
        Path(f"{tmp_dir}/test.0.test2.a.txt").touch()
        Path(f"{tmp_dir}/test.0.test2.a.txt.gz").touch()
        Path(f"{tmp_dir}/test.0.test2.b.txt").touch()
        Path(f"{tmp_dir}/test.0.test2.c.txt.gz").touch()
        Path(f"{tmp_dir}/test.1.test2.a.txt").touch()
        Path(f"{tmp_dir}/test.1.test2.b.txt.ga").touch()

        # list without arg/kwargs
        self.assertLeng(FileNamePath(tmp_dir + "/test.5.test2").list(), 0)
        print(tmp_dir + "/test.5.test2")
        self.assertLeng(FileNamePath(tmp_dir + "/test.0.test2").list(), 1)
        self.assertLeng(FileNamePath(tmp_dir + "/test.{}.test2.{}").list(), 5)
        self.assertLeng(FileNamePath(tmp_dir + "/test.{}.test2.{}.txt").list(), 5)
        self.assertLeng(FileNamePath(tmp_dir + "/test.{}").list(), 2)
        self.assertLeng(FileNamePath(tmp_dir + "/test.{}.test2.{}").list(fix=[0]), 3)
        self.assertLeng(FileNamePath(tmp_dir + "/test.{}.test2.{}").list(fix=[1]), 2)

        # list with arg/kwargs
        a = FileNamePath.construct(tmp_dir + "/test.{}.test2.{}", ("*", "a"), {})
        self.assertLeng(a.list(), 2)
        a = FileNamePath.construct(tmp_dir + "/test.{}.test2.{}", ("*", "c"), {})
        self.assertLeng(a.list(), 1)
        a = FileNamePath.construct(
            tmp_dir + "/test.{sample}.test2.{}", ("*",), {"sample": "0"}
        )
        self.assertLeng(a.list(), 3)

        shutil.rmtree(tmp_dir)

    def test_concat(self):
        a = FileNamePath("test.{}.sep") + ".2"
        self.assertLeng(a.args, 1)

        a = FileNamePath("test.{}.sep") + ".{}"
        self.assertLeng(a.args, 2)

        a = (
            FileNamePath.construct("test.{sample}.test2.{}", ("*",), {"sample": "0"})
            + ".sep2"
        )
        self.assertEqual(a.args, ("*",))
        self.assertEqual(a.kwargs, {"sample": "0"})
