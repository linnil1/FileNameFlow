# Name-based Pipeline

Core features

* Python function as the task in the pipeline
* The input (parameters) and output (return value) of function are NAME, which is actually the filename without suffix
* The NAME can be cascading (become longer name) as task executed, it's very suitable for versioning and prevent overwritting
* The filename are read/created based on NAME, so the files listed in directory are corresponing to the NAME which can naively tell what task has done
* THE NAME support wildcards, thus it can do merging tricks.

More details are written in the document, check the link below.

--

The draft document in HackMD

https://hackmd.io/ogyqCAUZQjaGuw1Mv5BGjw

I'll copy it into readme and wiki page in the future


Code document

https://linnil1.github.io/name-based-pipeline

::: namepipe.error
::: namepipe.path
::: namepipe.task
::: namepipe.executor
