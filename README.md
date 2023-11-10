# solution-file-processing
A package for easier processing of the PLEXOS solution files.

## Description
This package is used to process the PLEXOS solution files. It can be used to unarchive the created .zip files, convert them to .h5 files and create outputs and plots from them. Since those files can be quite large, the code uses a caching system to process and save needed objects and variables. Variables is just data from objects which have been processed. All those potentially massive data objects are processed with [dask](https://www.dask.org/) for to avoid any memory overflow and better efficency. In the respective functions to create plots and outputs, these are transformed back into pandas DataFrames. 

## Documentation
- [General project information](docs/Documentation.md#general-project-information)
   - [Project structure](docs/Documentation.md#project-structure)
   - [Configuration file](docs/Documentation.md#configuration-file)
   - [Model directory](docs/Documentation.md#model-directory)

- [Contribution](docs/Documentation.md#contribution)
   - [Add new variables and objects](docs/Documentation.md#add-new-variables-and-objects)
      - [Objects](docs/Documentation.md#objects)
      - [Variables](docs/Documentation.md#variables)
   - [Add new outputs](docs/Documentation.md#add-new-outputs)
   
- [Setup process](docs/Documentation.md#setup-process)
   - [Troubleshooting](docs/Documentation.md#troubleshooting)

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

If problems occur, see the [Troubleshooting](/docs/Troubleshooting.md) page for more information.

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
