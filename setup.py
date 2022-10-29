import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name="namepipe",
    version="0.0.6",
    author="linnil1",
    author_email="linnil1.886@gmail.com",
    description="A name-based pipeline",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/linnil1/name-based-pipeline",
    packages=setuptools.find_packages(),
    install_requires=[
        "parse",
    ],
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
        "License :: OSI Approved :: MIT License",
    ],
    python_requires=">=3.10",
)
