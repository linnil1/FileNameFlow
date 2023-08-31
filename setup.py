import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name="filenameflow",
    version="0.1.0",
    author="linnil1",
    author_email="linnil1.886@gmail.com",
    description="A Filename Driven pipeline framework",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/linnil1/FileNameFlow",
    packages=setuptools.find_packages(),
    install_requires=[
        "parse",
        "dask[distributed]"
    ],
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
        "License :: OSI Approved :: MIT License",
    ],
    python_requires=">=3.10",
)
