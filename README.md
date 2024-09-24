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
### julia
The SFP package requires julia to be installed and have a few (julia) packages set up, as well as the python package called julia.

**Installing julia**
- Install julia from the software centre. If errors, submit an IT ticket requesting them to install it. Add julia to PATH if given the option. 
- Check whether the path to julia.exe was added to PATH, usually the bin folder with the format `C:\Users\LASTNAME_F\AppData\Local\Programs\Julia\Julia-1.4.2\bin`. Check by either:

    - Running `where julia` in Windows command prompt. Returns the path to julia.exe if it has been added to PATH
    - Running `echo %PATH%` in Windows command prompt and checking whether the \bin folder is listed in the result
- If the path to julia.exe is not in PATH, submit an IT ticket asking for it to be added as doing so requires admin rights.

**julia proxies**

Just like python, julia will need to use the corporate proxies. To make sure these work when called from python, add these to a startup file so they are applied each time julia is started. 
- Launch julia if you haven't already so it can initialise some folders on first start up.
- Find the `.julia` folder in your user directory, normally `C:\Users\LASTNAME_F\.julia`. Make sure hidden files are visible, on Win11: View > Show > Hidden Items.
- If there is a `config` folder with a `startup.jl` file in, add the below proxy commands to the file. 
- If there is no `config` folder, create it. Add the proxies by adding the commands below to a new file e.g. in Notepad, saving as `startup.jl` in the `config` folder (ie a .jl file not .txt). 

    `ENV["HTTP_PROXY"] = "http://proxy.iea.org:8080"`

    `ENV["HTTPS_PROXY"] = "http://proxy.iea.org:8080"`

- Verify this has worked by opening a new julia terminal and running `ENV["HTTP_PROXY"]` which should display the IEA proxy address. 

**Registries and packages**

Before installing any other packages in julia, ensure julia has created a General registry. 
- Open julia and make sure that the IEA proxy is set up (see above)
- Run pkg mode by pressing `]`
- `registry add https://github.com/JuliaRegistries/General`

- _This has given 'directory not empty' errors in the past. Manually deleting the temp file folders specified in the  error message and re-running the previous step has solved this in the past._

Now install julia packages
- Open julia and make sure that the IEA proxy is set up (see above)
- Run pkg mode by pressing `]`
- `add PyCall`
- `registry add https://github.com/NREL/JuliaRegistry.git`
- `add H5PLEXOS`
- `add ZipFile`


### SFP python package
Just clone the repository to create a local copy:

    git clone https://gitlab.iea.org/iea/ems/rise/solution-file-processing.git

To install the dependencies, it is recommended to use a virtual environment. Both can be done automatically with the `environment.yml` file. This will create an environment named `solution-file-processing`:

    conda env create -f environment.yml

This creates a conda environment named `solution-file-processing` and installs all relevant packages which can be installed via conda. Then activate the environment and install the relevant packages which are only available via pip and use the IEA proxy:

    conda activate solution-file-processing
    python -m pip install --proxy http://proxy.iea.org:8080 julia
    python -m pip install --proxy http://proxy.iea.org:8080 https://github.com/NREL/h5plexos/archive/master.zip

That's it. Julia needs also to be installed on the system (see above) and if Julia should be used within python (only for unpacking the .zips to .h5 files) it also has to be initialized within python. There is a function for that in the code.

If problems occur, see the Troubleshooting section in the [Documentation](docs/Documentation.md).

The final step is to install the package locally (if you don't want to load it only relative to the project folder): 

    pip install -e .

## Usage
In the same project folder, create a new python file and import the package:

    import solution_file_processing as sfp

It needs to be in the same folder, because right now, no python packaging is used. See [here](https://github.com/rise-iea/knowledge-database/blob/main/Python-Packaging.md) for more information.

Then initialize a new configuration based on a configuration file:
    
    config = sfp.SolutionFilesConfig('IDN.toml')
    config.install_dependencies()  # To install Julia, only when running the first time
    config.convert_solution_files_to_h5()  # To extract the Zip files, only when running the first time

To create new outputs or plots run any function from `plots.py` or `outputs.py` and pass the configuration as an argument. For example:
    
    sfp.summary.create_output_1(config)
    sfp.timeseries.create_output_1(config)
    sfp.outputs.create_plot_1(config)
