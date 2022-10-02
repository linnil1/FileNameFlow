# Name-based Pipeline

A pipeline doesn't depended on the file-extension (suffix) of filename.

All input/output are the NAME, which is actually the filename without suffix

The NAME can propagated (become longer name) as task executed on, it's very suitable for versioning and prevent overwritting

The filename is read/created based on NAME, so the filenamew listed in directory can naively tell what pipeline has done

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
