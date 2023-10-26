import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="iea_rise",
    version="0.1.0",
    author="Lukas Trippe",
    author_email="lkstrp@pm.me",
    description="tbd.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/lkstrp/iea-rise",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)