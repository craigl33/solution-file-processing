<- [README](README.md)

# solution-file-processing Documentation
This is more detailed documentation for the solution-file-processing script. Only project specific information is described here, a general knowledge database can be found [here](https://github.com/rise-iea/knowledge-database).

## Table of Contents
- [General project information](#general-project-information)
   - [Project structure](#project-structure)
   - [Configuration file](#configuration-file)
   - [Model directory](#model-directory)

- [Contribution](#contribution)
   - [Add new variables and objects](#add-new-variables-and-objects)
      - [Objects](#objects)
      - [Variables](#variables)
   - [Add new outputs](#add-new-outputs)
   
- [Setup process](#setup-process)
   - [Troubleshooting](#troubleshooting)

## General project information
### Project structure
The file structure in this project follows the standard Python package structure, without being actually a package right now. This could be changed in future.

The project is structured as follows:

- `config_archive/`: Contains the configuration files and execution scripts for old runs. They are kept for reference.
- `docs/`: Contains the documentation files. Any explanation which is project specific should be written here.
- `logs/`: Contains the log files from the runs. They are created automatically and can be used to debug the code. They are not added to git.
- `solution_file_processing/` The main package folder. Contains all the code.
  - `__init__.py`: The main file which is called when the package is imported. It references the `SolutionFilesConfig` class which is used to initialize a configuration based on a configuration file.
  - `caching.py`: Contains the code for the two cached data types objects from PLEXOS and variables, as well as the code to automatically cache them.
  - `constants.py`: Contains all the constants which are used in the code. They are always in UPPER_SNAKE_CASE and never changed.
  - `solution_files.py`: Contains the Framework class to combine all the functions and create the configuration object.
  - `summary.py`: Contains all the functions to create the summary outputs (annual data).
  - `timeseries.py`: Contains all the functions to create the timeseries outputs (hourly data).
  - `plots.py`: Contains all the functions to create the plots.

For further modification the three relevant files are `summary.py`, `timeseries.py`, `plots.py` and `caching.py`. More on that in the [contribution](#contribution) section.

### Configuration file
A configuration file is needed to set some model specific settings and pass the path to the model directory. It is written in [TOML](https://toml.io/en/) format. 

Below and in the `/config_archive` folder is a template for the configuration file and needs to be adjusted for any new Solution File data. They can have any name, which then passed to the `SolutionFilesConfig` class to initialize the configuration.

To disable an optional setting, pass an empty string or empty list etc. Toml does not support null values.
```toml
[path]
# Path to general model directory. See below for more information.
model_dir = 'U:/data/Indonesia/2021_IPSE'
# Path to generator parameters excel file. Only the SolutionIndex Sheet is used.
soln_idx_path = 'U:/data/Indonesia/2023_03_28_generator_parameters_IDN.xlsx'

[model]
# Name of specific scenario
soln_choice = '20230509_IDN_APSvRUPTL_scenario'

[settings]
# Settings needed for the model. #TODO: Can be generalized probably
geo_cols = ['Island', 'Region', 'Subregion']
validation = false #TODO: Check what this is doing again

[run]
# Path to log file. Pass empty string to disable logging.
log_file_path = 'U:/code/solution-file-processing/logs/IDN.log' 
# If true, a new log file with a timestamp is created every time the script is run.
log_timestamp = true
# If true, the script uses the caching system for variables and objects. Otherwise it will always reprocess everything.
variables_cache = true
# If true, errors in the create plot and output functions are caught and logged. Otherwise they are raised.
catch_errors = true

[testing]
# Needed for the test output functions. If not set, no baseline tests are run.
baseline_output_dir = 'Y:/RED/Modelling/Indonesia/2021_IPSE/05_DataProcessing/20230509_IDN_APSvRUPTL_scenario/'
# Needed for the test output functions. If not set, no similarity tests are run.
similar_output_dirs = false
```

### Model directory
#todo discuss structure, then add describtion here


## Contribution
In general any code can be modified, but there are especially two things which are easy to add and can be useful for future runs and projects.

### Add new variables and objects
A list of PLEXOS objects are already added to the script. They can be easily accessed via the configuration object and are automatically created from the .h5 files and cached. More information on that in all Comments and Docstrings in the caching.py file. The same goes for any optional variables. If new objects (different PLEXOS objects or based on a different timescale) or new variables are needed, they can be added as a new property to the respective class and then be used in the same manner as the existing ones. Objects and variables have a very similar structure.

Note that not all properties are saved from the raw PLEXOS solution file. Instead, specific properties are filtered when creating the Objects using the FILTER_PROPS list from the constants.py file. This can be edited as necessary. For the new properties to be reflected in the Objects, you will need to delete any existing cache file.


#### Objects

Within the class add a new local variable with the desired name starting with a "_". This is just needed for the caching.

```python
class SolutionFilesConfig:

   def __init__(self, config_name):
      # ...
   
   _new_object = None
```

Then add a new property with the same name as the local variable. This is the one which will be used to access the object or variable. Always keep the structure and just change the arguments for the `get_processed_object()` function. And also add some optional processing if needed.

```python
@property
@caching('objects')
def new_objects(self):
    if self._new_object is None:
        
        self._new_object = self.c.get_processed_object('<timescale>', '<object name>')  # Function to load from .h5 file

        # Any optional processing 

    return self._new_object
```

#### Variables
Similar how it is for the Objects, add a new local variable with the desired name starting with a "_".

```python
class SolutionFilesConfig:

   def __init__(self, config_name):
      # ...
   
   _new_variable = None
```

Also keep the same structure for the property method. The only difference is that here no `get_processed_object()` function is needed. Instead the variable is created from one or more existing objects. The whole purpose of the variable is only to cache the processing step. 

```python
@property
@caching('variables')
def new_variable(self):
  """
  ...
  """
  if self._new_variable is None:
     self._new_variable = # Some processing based on any objects which can be accessed with self.c.o.<object name>
     
  return self._new_variable
```

### Add new outputs
Next to adding the caching backbone structure, new functions to create plots or outputs can be added. They will not interfere with the existing ones and can be used in the same way, also only for specific projects. Those functions are all in the `outputs.py` and `plots.py` files. Just add the `@catch_errors` decorator and pass the configuration object. Through the configuration object all relevant variables and objects needed for the output/ plot creation can be accessed.

```python
@catch_errors
def function_name(c):
   """
   ...
   """
   # Any processing based on any objects or variables which can be accessed with self.c.o.<object name> or 
```
## Setup process

### Troubleshooting
The process described in the README should work usually. But in case it does not, here are some things to check:

1. IEA Proxy
   - Pip, Conda and also Julia need the IEA proxy to install packages.
   - For pip: You always have to pass the proxy as an argument: `python -m pip install --proxy http://proxy.iea.org:8080 <package>`
   - For conda: You can set them up globally: `conda config --set proxy_servers.http http://proxy.iea.org:8080` and `conda config --set proxy_servers.https http://proxy.iea.org:8080`
   - For julia: You can set them globally in the `C:\Users\LASTNAME_F\.julia\config\startup.jl` file as described [here](https://discourse.julialang.org/t/install-packages-behind-the-proxy/23298/3), with the commands `ENV["HTTP_PROXY"] = "http://proxy.iea.org:8080"` and `ENV["HTTPS_PROXY"] = "http://proxy.iea.org:8080"`. Ensure hidden items are visible in Windows Explorer (`View>Show>Hidden items`), you may have to create the `config` folder and startup file if they don't exist yet. Run `ENV['HTTP_PROXY']` in a new julia terminal to verify proxies are recognised. 
2. Julia PATH variable
   - Julia needs to be installed on the system, so it can be called from python.
   - If it is installed but python can not find it, it is most likely not in the PATH variable. Add the bin/julia.exe folder to the PATH variable (e.g. `C:\Users\TRIPPE_L\AppData\Local\Programs\Julia\Julia-1.4.2\bin`). See [here](https://www.java.com/en/download/help/path.html) for how se tup a PATH variable in windows.
3. Julia registry/ packages missing
   - Sometimes the Julia registry is not linked or there are packages missing. To check or install them run:
     - Open julia and make sure that the IEA proxy is set up (see above)
     - Run pkg mode by pressing `]`
     - `registry add https://github.com/NREL/JuliaRegistry.git`
     - `add H5PLEXOS`
     - `add ZipFile`

Also check the [Knowledge Database](https://github.com/rise-iea/knowledge-database) for more information.
