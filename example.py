import os
import logging
import subprocess
from pathlib import Path

from namepipe import nt, NameTask
# Debug info
# logging.basicConfig(level=logging.DEBUG)


def run(cmd):
    """ Execute shell script """
    print(cmd)
    # Don't execute your script like this
    # It is very dangerous
    proc = subprocess.run(cmd, shell=True)
    proc.check_returncode()


def checkExist(name):
    assert Path(name).exists()


def createFastq(input_name):
    """ 0 -> many """
    input_name = os.path.join(input_name, "test")  # type: str
    for i in range(2):
        run(f"echo {i}_1 > {input_name}.{i}.read.1.fq.gz")
        run(f"echo {i}_2 > {input_name}.{i}.read.2.fq.gz")
        checkExist(f"{input_name}.{i}.read.1.fq.gz")
        checkExist(f"{input_name}.{i}.read.2.fq.gz")
    # input_name is str type
    # so this return str
    # It's ok for 0 -> any case
    return input_name + ".{}.read"


def createBwaIndex(input_name):
    """ 0 -> 1 """
    output_file = os.path.join(input_name, "bwa")
    run(f"echo create Index > {output_file}.index")
    checkExist(output_file + ".index")
    # Return str. OK. for 0 -> 1 case
    return output_file


def bwa(input_name, index):
    """ 1 -> 1 """
    index = str(index)
    checkExist(index + ".index")
    checkExist(input_name + ".1.fq.gz")
    checkExist(input_name + ".2.fq.gz")
    f1, f2 = f"{input_name}.1.fq.gz", f"{input_name}.2.fq.gz"
    suffix = ".bwa." + index.replace("/", "_")
    output_name = input_name + suffix  # type: NamePath
    run(f"echo mapped {f1} {f2} by bwa with index {index} > {output_name}.bam")
    checkExist(output_name + ".bam")

    # str(output_name) == test.1.read.bwa_index_bwa
    # output_name is NamePath type
    # or you can return "test.{}.read.bwa_index_bwa" (str type)
    # which stored in output_name.template
    return output_name


def extractChr(input_name):
    """
    few -> many
    = for each name
        1 -> many
    """
    f_bam = f"{input_name}.bam"
    checkExist(f_bam)
    output_name = input_name + ".splitchr.{}"  # type: NamePath
    chrs = ["chr1", "chr2"]
    if ".1." in input_name:
        chrs.append("chr3")
    for chr in chrs:
        run(f"echo {f_bam} extract chr1 > {output_name.format(chr)}.bam")
        checkExist(f"{output_name.format(chr)}.bam")
    return output_name


def statChr(input_name):
    """ 1 -> 1 """
    checkExist(f"{input_name}.bam")
    suffix = ".stat"
    output_name = input_name + suffix
    run(f"echo stat {input_name}.bam > {output_name}.txt")
    checkExist(f"{output_name}.txt")
    # return output_name
    # indeed we use the template in NamePath
    return output_name.template


def mergeChr(input_name):
    """
    many -> fewer
    name = xxx.{}.ooo.{}.aaa
    input_name = xxx.00.oo.{}.aaa
    output_name = xxx.00.oo_merge.aaa
    """
    input_names = input_name.get_input_names()
    files = [name + ".txt" for name in input_names]
    [checkExist(i) for i in files]
    output_name = input_name.replace_wildcard("_merge_split_chr_stat")  # type: NamePath
    run(f"echo merge {' '.join(files)} > {output_name}.csv")
    checkExist(f"{output_name}.csv")
    return output_name


def mergeStat(input_name):
    """
    many -> 1
    splitchr.{}.csv => splitchr_merge.csv
    splitchr.merge.csv may confused with splitchar.1.csv sokutcgr,2,csv
    """
    input_names = input_name.get_input_names()
    files = [name + ".csv" for name in input_names]
    [checkExist(i) for i in files]
    output_name = input_name.replace_wildcard()
    run(f"echo merge { ' '.join(files) } > {output_name}.csv")
    checkExist(f"{output_name}.csv")
    return output_name


def create_folder(input_name, folder):
    """ 0 -> 1 """
    Path(folder).mkdir(exist_ok=True)
    # return a str
    checkExist(folder)
    return folder


def rename(input_name):
    """
    many to many
    input: data/test.{}.read.bwa.index_bwa.splitchr.{}.stat.txt
    output: data/stage2.{}.chr.{}
    """
    input_names = input_name.get_input_names()
    output_name = "data/stage2.{}.chr.{}"  # type: str
    for name in input_names:
        result = input_name.extract_fields(name)
        checkExist(f"{name}.txt")
        output_file = output_name.format(*result.fixed)
        run(f"ln -sf ../{name}.txt {output_file}.txt")
        checkExist(f"{output_file}.txt")
    # If you return str, make sure return the name with all wildcard = {}
    return output_name


def renameSwap(input_name):
    """
    1 to 1
    rename sample.{1}.chr.{2} -> stage3.chr.{2}.sample.{1}
    The wildcard 1 and 2 can be extracted from template_args
    """
    checkExist(f"{input_name}.txt")
    args = input_name.template_args
    assert len(args) == 2
    output_name = "data/stage3.chr.{}.sample.{}"
    output_file = output_name.format(args[1], args[0])
    run(f"ln -sf {Path(input_name).name}.txt {output_file}.txt")
    checkExist(f"{output_file}.txt")
    return output_name


if __name__ == "__main__":
    from namepipe.executor import ConcurrentTaskExecutor
    NameTask.default_executor = ConcurrentTaskExecutor()

    # 0 -> 1 -> 1
    bwa_index = None >> NameTask(create_folder)(folder="index") >> createBwaIndex >> "index/bwa"
    print(bwa_index)

    # 0 -> many -> (for each) 1 -> 1
    bwa_data = "." >> NameTask(create_folder)(folder="data") >> NameTask(createFastq) >> NameTask(bwa)(index=bwa_index)
    bwa_stat = bwa_data >> extractChr >> statChr >> NameTask(rename).set_depended([0, 1])
    print(bwa_stat)

    # result and swap_result is basically the same but use different strucutre
    result = bwa_stat >> NameTask(mergeChr).set_depended(-1) >> NameTask(mergeStat).set_depended(-1)
    print(result)
    swap_result = bwa_stat >> NameTask(renameSwap) >> NameTask(mergeChr).set_depended(0) >> NameTask(mergeStat).set_depended(0)
    print(swap_result)
