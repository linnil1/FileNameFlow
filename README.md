# FileNameFlow: A Filename-Driven Pipeline Framework

FileNameFlow is a lightweight framework for data processing pipelines using filename patterns.
Eliminate complex loops by harnessing the power of filename patterns with wildcards,
effortlessly listing corresponding filenames.
With task execution guided by these patterns,
user only need to focus on individual functions while FileNameFlow handles the orchestration.
Seamlessly integrate with distributed frameworks like Dask,
extending resource management to PBS, SLURM, and any other distrubuted computation system.
Whether it's bioinformatics or intricate data science tasks,
FileNameFlow streamlines filename processing and boosts efficiency.


## Concept 1: Filename Selection Patterns

### FileNamePath

We use `FileNamePath` class to represented the name selection pattern.

The `FileNamePath` will select the files by it's pattern while ignore the file extension.
The wildcard charcter is `{}` but it exclude the filename when separator(`.`) in it.

For example, if theres exists files:
```
sample.00.read.1.fq.gz
sample.00.read.2.fq.gz
sample.00.bwa.bam
sample.00.bwa.sort.bam
sample.00.bwa.sort.bqsr.bam
sample.01.read.1.fq.gz
sample.01.read.2.fq.gz
sample.01.bwa.bam
sample.01.04.read.2.fq.gz
sample1.bwa.csv
sample1.bowtie.csv
sample1.bowtie.filter.csv
```

| FileNamePath | listed FileNamePath |
| -------- | ------------ |
| `sample.{}`  | `sample.00` `sample.01` |
| `sample.{}.bwa`  | `sample.00.bwa` `sample.01.bwa` |
| `sample.{}.bwa.sort`  | `sample.00.bwa.sort` |
| `sample.{}.read`  | `sample.00.read` `sample.01.read` |
| `sample.{}.read.{}`  | `sample.00.read.1` `sample.00.read.2` `sample.01.read.1` `sample.01.read.2` |
| `sample.00.read.{}`  | `sample.00.read.1` `sample.00.read.2` |
| `sample{}`  |  |
| `sample.00.read`  | `sample.00.read` |
| `sample1.{}`  | `sample1.bwa` `sample1.bowtie` |
| `sample1.{}.csv`  | `sample1.bwa.csv` `sample1.bowtie.csv` |
| `sample1.{method}.csv`  | `sample1.bwa.csv` `sample1.bowtie.csv` |


### FileNamePath.list(fix)

Next, if you want to select a group instead of executing tasks one by one, you can use the `fix` argument to indicate that the wildcard character `{}` should remain fixed and unexpanded. This feature is especially useful for tasks that require a list of files as input.

| FileNamePath | fix | listed FileNamePath |
| -------- | ------------ | ------------ |
| `sample.{}`  | `[-1]` |`sample.{}` |
| `sample.{}.bwa`  | `[-1]` | `sample.{}.bwa` |
| `sample.{}.read.{}`  | `[-1]` | `sample.00.read.{}` `sample.01.read.{}` |
| `sample.{}.read.{}`  | `[-2]` | `sample.{}.read.1` `sample.{}.read.2` |
| `sample.{}.read.{}`  | `[-1. -2]` | `sample.{}.read.{}`  |
| `sample.{}.read.{}`  | `[]` | `sample.00.read.1` `sample.00.read.2` `sample.01.read.1` `sample.01.read.2` |


## Concept 2: Managing Workflow Steps in Filenames

In FileNameFlow, we adopt a straightforward approach to keep track of workflow steps.
We save all the steps within the filename suffix,
making it easy to understand what treatments have been applied to the data based on the filename.


| Step  | Input | Output | File(we don't care) |
| ----- | ----- | ------ | ------------------- |
| download  | `.`  | `sample.00.read` | `sample.00.read.1.fq.gz`, `sample.00.read.2.fq.gz` |
| bowtie2 | `sample.00.read` | `sample.00.read.bowtie_hg19` | `sample.00.read.bowtie_hg19.sam` |
| sortBam | `sample.00.read.bowtie_hg19` | `sample.00.read.bowtie_hg19.sort` | `sample.00.read.bowtie_hg19.sort.bam` |
| GatkBqsr | `sample.00.read.bowtie_hg19.sort` | `sample.00.read.bowtie_hg19.sort.bqsr` | `sample.00.read.bowtie_hg19.sort.bqsr.bam` |
| GatkHC | `sample.00.read.bowtie_hg19.sort.bqsr` | `sample.00.read.bowtie_hg19.sort.bqsr.hc` | `sample.00.read.bowtie_hg19.sort.bqsr.hc.vcf.gz` |

Furthermore, we incorporate parameters into the filenames to ensure that files generated with different parameters are kept separate. We use abbreviations when necessary to maintain readability.

| Function  | Input | Output |
| ----- | ----- | ------ |
| bowtie2(index="hs37d5") | `sample.00.read` | `sample.00.read.bowtie_hg19` |
| bowtie2(index="hs38DH") | `sample.00.read` | `sample.00.read.bowtie_hg38` |


Our pipeline seamlessly handles suffix concatenation (`+`) or wildcard replacement (`apply`).
Setting up pipelines is a breeze using `FileNamePath`,
and you can define functions to handle each selected filename.

Here's an example:
``` python
from functools import partial
from filenameflow import FileNamePath, FileNameTask

def bowtie2(input_name, index):
    # The function are called two times
    # where input_name =
    # 1. sample.00.read.{}
    # 2. sample.01.read.{}
    print(input_name)
    output_name = input_name + ".bowtie" + index.replace("/", "_")  # concat the suffix you want

    fqs = sorted(input_name.list())  # use build-in list to list the current path e.g. sample.00.read.1, sample.00.read.2
    os.system(f"echo bowtie {index} {fqs[0]}.fq {fqs[1]}.fq -o {output_name}.sam")  # FileNamePath works like str
    return output_name  # return the result name for furthur task chaining

# Using FileNamePath to kick start:
# FileNamePath("sample.{}.read.{}") >> partial(bowtie2, index="index/hg19")
# or using FileNameTask to start
"sample.{}.read.{}" >> FileNameTask(partial(bowtie2, index="index/hg19"), fix=[-1])
```

FileNameFlow simplifies complex data processing workflows by emphasizing functions for handling selected filenames,
significantly reducing the need for extensive loops in your code.


## Concept 3: Combining Functions Like a Pipeline

In this concept, we combine the previously discussed concepts into our pipeline.
For a complete code example, please refer to the `example.py` file in the GitHub repository.

``` python
def download(input_name):
    # 1(indeed 0) -> many
    output_name = "data/xxx.{}.read"
    if len(FileNamePath(output_name).list()):  # skip the step if file is downloaded
        return output_name
    # wget ...
    return output_name

def bowtie2(input_name, index):
    # 1 -> 1
    # input_name = "data/xxx.{}.read"
    # output_name = "data/xxx.{}.read.index_hs37d5"
    output_name = input_name + "." + index.replace("/", "_")
    if Path(output_name + ".sam").exists():  # skip the step if file exists
        return output_name
    os.system(f"bwa {index} {input_name}.1.fq {input_name}.2.fq -o {output_name}.sam")
    return output_name

def mergeCSV(input_name):
    # many -> 1
    # input_name = "data/xxx.{}.read.index_hs37d5.depth"
    # output_name = "data/xxx_merge.read.index_hs37d5.depth"
    output_name = input_name.replace_wildcard("_merge")
    if Path(output_name + ".csv").exists():
        return output_name
    files = input_name.list()
    df = pd.concat(pd.read_csv(i + ".csv") for i in files)
    df.to_csv(output_name + ".csv", index=False)
    return output_name

def summaryCSV(input_name):
    # 1 -> 1
    # doesn't change the suffix
    df = pd.read_csv(i + ".csv").groupby("chrom").describe()
    print(df)
    return input_name

# using >> to chain the tasks
FileNamePath("") >> download >> partial(bowtie2, index="index/hs37d5") >> sortBam >> getLowReadDepthPos >> FileNameTask(mergeCSV, fix=[-1]) >> summaryCSV
# Or using compose
from filenameflow import compose
compose([
    ".",
    download,                                # 0 to many
    partial(bowtie2, index="index/hs37d5"),  # 1 to 1
    sortBam,                                 # 1 to 1
    getLowReadDepthPos,                      # 1 to 1
    FileNameTask(mergeCSV, fix=[-1]),        # many to 1
    summaryCSV,                              # 1 to 1
])
```

Our pipeline appears as a simple flow due to the list already being saved in the filename pattern,
eliminating the need for explicit loops.


## Concept 4: Shipping Your Pipeline to Other Resources

We provide two basic executors for your convenience:

* FileNameBaseExecutor (Default): Executes tasks one by one.
* DaskExecutor: Executes tasks using Dask, allowing you to leverage various computational resources. Refer to [Dask](https://docs.dask.org/en/stable/deploying.html) for available resource options.


``` python
from filenameflow.executor import DaskExecutor
from dask.distributed import LocalCluster

# Set up a DaskExecutor with a LocalCluster
exe = DaskExecutor(LocalCluster())
FileNameTask.set_default_executor(exe)

# Set the executor for a specific task
"." >> download >> FileNameTask(partial(bowtie2, index="index/hs37d5"), executor=exe)
# Or set it globally
FileNameTask.set_default_executor(exe)
"." >> download >> partial(bowtie2, index="index/hs37d5")
```

With FileNameFlow, you can effortlessly adapt the filename pipeline to different computation environments for efficient data processing.


## Conclusion

FileNameFlow offers several compelling advantages that make it a valuable tool for various data science tasks, particularly in bioinformatics:

* **Streamlined Data Science**: FileNameFlow is well-suited for data science, especially in bioinformatics, where multiple tools generate numerous files. It simplifies file management and processing.
* **Self-Descriptive Names**: Each file's name serves as a self-contained record of the tasks performed on it. This self-description simplifies tracking and understanding data processing steps.
* **Auto-Versioning**: Changing the pipeline order or altering arguments automatically leads to changes in filenames. This built-in versioning mechanism facilitates reproducibility and experimentation.
* **Flexibility in Handling Filename**: FileNameFlow allows you to manage filename (i.e. prefix, suffix) within your task functions, offering flexibility and control over .
* **Resource Management**: While FileNameFlow provides a powerful framework for data pipeline orchestration, it doesn't handle resource management like threads or memory usage. This approach keeps it lightweight and adaptable to various environments.
* **Simplicity**: It offers a straightforward and intuitive syntax, minimizing the need for complex external functions or special syntax. This simplicity enhances ease of use and code readability.
* **Task Skipping**: FileNameFlow allows you to specify whether specific steps should be skipped or executed if the corresponding files already exist. The skip mechanism is defined within the functions you write, keeping the framework's arguments and design simple.
* **Handling Non-Patterned Files**: When dealing with files that don't conform to your desired filename pattern, FileNameFlow encourages a straightforward renaming process at the beginning of your pipeline, typically done using commands like `ln -s`  for symbolic links.
* **Python-Based Pipelines**: FileNameFlow pipelines are implemented in Python, leveraging the language's extensive ecosystem of libraries and tools. This makes it easy to integrate with other Python-based data science libraries and workflows.
* **Minimal Overhead**: FileNameFlow is lightweight and doesn't require additional databases or metadata storage. All information is derived directly from file existence and patterns.

In summary, FileNameFlow empowers data scientists to efficiently manage and process data, fosters collaboration, and simplifies complex tasks while maintaining flexibility and transparency in file management. Its lightweight nature and intuitive design make it a valuable addition to your data science toolkit.


## Installation

```
pip install git+https://github.com/linnil1/name-based-pipeline
```


## Run

Run example

```
python example.py
```


## Document

https://linnil1.github.io/name-based-pipeline

::: filenameflow.error
::: filenameflow.path
::: filenameflow.task
::: filenameflow.executor
