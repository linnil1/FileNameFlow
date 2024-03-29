import time
import logging
import subprocess
from pathlib import Path
from functools import partial

from filenameflow import FileNamePath, FileNameTask, compose

# Debug info
# logging.basicConfig(level=logging.DEBUG)


threads = 14  # manually maintain resources


def run(cmd: str) -> None:
    """Execute shell script"""
    print(cmd)
    # Don't execute your script like this
    # It is very dangerous
    time.sleep(10)
    proc = subprocess.run(cmd, shell=True, check=True)


def createFastq(input_name: FileNamePath) -> str:
    """0 -> many"""
    output_name = "data/test"
    Path("data").mkdir(exist_ok=True)
    for i in range(2):
        if Path(f"{output_name}.{i}.read.1.fq.gz").exists():  # skip when file exists
            continue
        run(f"echo sample{i}_read_1 > {output_name}.{i}.read.1.fq.gz")
        run(f"echo sample{i}_read_2 > {output_name}.{i}.read.2.fq.gz")
    return output_name + ".{}.read"  # It's OK to return str instead of FileNamePath


def downloadReference(input_name: FileNamePath) -> str:
    """0 -> 1"""
    output_name = "index/hg19"  # type: str
    if Path(f"{output_name}.fa").exists():  # skip when file is downloaded
        return output_name
    Path("index").mkdir(exist_ok=True)
    run(f"echo create Index > {output_name}.fa ")
    return output_name


def createBwaIndex(input_name: FileNamePath) -> FileNamePath:
    """1 -> 1"""
    output_name = input_name + ".bwa"  # type: FileNamePath
    if Path(f"{output_name}.bwt").exists():  # skip when index is built
        return output_name
    run(f"echo create Index {input_name}.fa > {output_name}.bwt")
    return output_name


def bwa(input_name: FileNamePath, index: str) -> FileNamePath:
    """1 -> 1"""
    f1, f2 = f"{input_name}.1.fq.gz", f"{input_name}.2.fq.gz"
    # if Path(f"{output_name}.bam").exists():  # you can write your own skip pattern
    output_name = input_name + "." + index.replace("/", "_").replace(".", "_")
    # output_name type: FileNamePath
    run(f"echo 'mapped {f1} {f2} by bwa with index {index}.bwt (threads={threads})' > {output_name}.bam")
    return output_name


@FileNameTask.wrapper
def splitChr(input_name: FileNamePath) -> FileNamePath:
    """1 -> many"""
    output_name = input_name + ".splitchr.{}"  # type: FileNamePath
    chrs = ["chr1", "chr2"]
    for chr in chrs:
        run(f"echo {input_name}.bam extract chr1 > {output_name.format(chr)}.bam")
    return output_name


def statChr(input_name: FileNamePath) -> FileNamePath:
    """1 -> 1"""
    output_name = input_name + ".stat"
    run(f"echo stat {input_name}.bam > {output_name}.txt")
    return output_name


def mergeChr(input_name: FileNamePath) -> FileNamePath:
    """many -> 1"""
    # input_name = xxx.00.oo.{}.aaa
    # output_name = xxx.00.oo_merge.aaa
    files = [name + ".txt" for name in input_name.list()]
    output_name = input_name.replace_wildcard("_mergechr")  # type: FileNamePath
    run(f"echo merge {' '.join(files)} > {output_name}.csv")
    return output_name


def printResult(input_name: FileNamePath) -> FileNamePath:
    """1 -> 1"""
    print(open(input_name + ".csv").read())
    return input_name


def mergeSample(input_name: FileNamePath) -> FileNamePath:
    output_name = input_name.replace_wildcard("_mergesample")
    files = [name + ".csv" for name in input_name.list()]
    run(f"cat {' '.join(files)} > {output_name}.txt")
    return output_name


if __name__ == "__main__":
    from filenameflow.executor import DaskExecutor
    from dask.distributed import Client, LocalCluster
    FileNameTask.set_default_executor(DaskExecutor(LocalCluster()))  # type: ignore

    bwa_index = FileNamePath(".") \
                >> downloadReference \
                >> createBwaIndex \
                >> "index/hg19.bwa"
                # download reference  # index/hg19
                # create bwa index    # index/hg19.bwa
                # assert the path is   "index/hg19.bwa"
    print(bwa_index)

    bwa_data = compose([
        ".",                                   # start from nothing
        createFastq,                           # 0 -> many   # data/test.{}.read
        partial(bwa, index=bwa_index.output),  # 1 -> 1      # data/test.{}.read.index_hg19_bwa
        splitChr,                              # 1 -> many   # data/test.{}.read.index_hg19_bwa.splitchr.{}
        statChr,                               # 1 -> 1      # data/test.{}.read.index_hg19_bwa.splitchr.{}.stat
        FileNameTask(mergeChr, fix=[-1]),      # many -> 1   # data/test.{}.read.index_hg19_bwa.splitchr_mergechr.stat.csv
        printResult,                           # 1 -> 1      # data/test.{}.read.index_hg19_bwa.splitchr_mergechr.stat.csv
    ])

    result = compose([
        bwa_data,                              # start from previous step  # data/test.{}.read.index_hg19_bwa.splitchr_mergechr.stat.csv
        FileNameTask(mergeSample, fix=[-1]),   # many -> 1                 # data/test_mergesample.read.index_hg19_bwa.splitchr_mergechr.stat.txt
    ])
