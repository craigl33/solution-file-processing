# solution-file processing
A package for easier processing of the PLEXOS solution files.

## Table of Contents

- [Setup](#setup)
- [Usage](#usage)
- [Troubleshooting](#troubleshooting)

## Setup
Just clone the repository to create a local copy:

    git clone solution-file-processing

To install the dependencies, it is recommended to use a virtual environment. Both can be done automatically with the `environment.yml` file:

    conda env create -f environment.yml

This creates a conda environment named `solution-file-processing` and installs all relevant packages which can be installed via conda. Then activate the environment and install the relevant packages which are only available via pip and use the IEA proxy:

    conda activate iea-rise
    python -m pip install --proxy http://proxy.iea.org:8080 julia
    python -m pip install --proxy http://proxy.iea.org:8080 https://github.com/NREL/h5plexos/archive/master.zip

That's it. Julia needs also to be installed on the system and if Julia should be used within python (only for unpacking the .zips to .h5 files) it also has to be initialized within python. There is a function for that in the code.

If problems occur, see the [Troubleshooting](#troubleshooting) section below.

## Usage
In the same project folder, create a new python file and import the package:

    import solution_file_processing as sfp

It needs to be in the same folder, because right now, no python packaging is used. See [here](https://github.com/rise-iea/knowledge-database/blob/main/Python-Packaging.md) for more information.

Then initialize a new configuration based on a configuration file:
    
    config = sfp.SolutionFilesConfig('IDN.toml')
    config.install_dependencies()  # To install Julia, only when running the first time
    config.convert_solution_files_to_h5()  # To extract the Zip files, only when running the first time

To create new outputs or plots run any function from `plots.py` or `outputs.py` and pass the configuration as an argument. For example:
    
    sfp.plots.create_year_output_1(config)
    sfp.outputs.create_plot_1(config)

## Needs documentation
#todo A list of things to write documentation for. Could also be moved to docs folder:

- Project structure
- Config file explanation
- How to add new outputs


## Troubleshooting
#todo put that in docs folder
The process above should work usually. But in case it does not, here are some things to check:
1. IEA Proxy
   - Pip, Conda and also Julia need the IEA proxy to install packages.
   - For pip: You always have to pass the proxy as an argument: `python -m pip install --proxy http://proxy.iea.org:8080 <package>`
   - For conda: You can set them up globally: `conda config --set proxy_servers.http http://proxy.iea.org:8080` and `conda config --set proxy_servers.https http://proxy.iea.org:8080`
   - For julia: Open the julia console and run `ENV["HTTP_PROXY"] = "http://proxy.iea.org:8080"` and `ENV["HTTPS_PROXY"] = "http://proxy.iea.org:8080"`
2. Julia PATH variable
   - Julia needs to be installed on the system, so it can be called from python.
   - If it is installed but python can not find it, it is most likely not in the PATH variable. Add the bin/julia.exe folder to the PATH variable (e.g. `C:\Users\TRIPPE_L\AppData\Local\Programs\Julia\Julia-1.4.2\bin`). See [here](https://www.java.com/en/download/help/path.html) for how se tup a PATH variable in windows.
3. Julia registry/ packages missing
   - Sometimes the Julia registry is not linked or there are packages missing. To check or install them run:
     - Open julia console and run pkg mode: press `]`
     - `registry add https://github.com/NREL/JuliaRegistry.git`
     - `add H5PLEXOS`
     - `add ZipFile`

Also check the [Knowledge Database](https://github.com/rise-iea/knowledge-database) for more information.
