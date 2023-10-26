<- [Knowledge Database Outline](outline.md)

# Setup
Here is a short overview on how to set up everything and install the necessary packages. At the end is also a troubleshooting section.


## Project Folder

Every setup process starts with the project folder. It is a dedicated directory for organizing and managing all script files and other project files. Data can be put somewhere else. Although scripts can be reverenced over multiple folders and can still be run with Jupyter Notebooks or as Python files (like done with the old R scripts), this has some drawbacks. Or rather you give up some advantages:

- Simplicity and better organization, the complete code is only in one place. No confusion
- No potential problems with interfering with other projects
- It is ideal for using version control (which alone is extremely helpful)
- Encourages good and up-to-date documentation practices (Write code and documentation in the same project/ editor).
- ...

When creating a new project folder, you can simply copy all files from the source or even better use version control (see here: [git](Version-Control.md)).

## Integrated Development Environment (IDE)

Which IDE is then used does not really matter. Regardless of whether you choose PyCharm, Visual Studio Code, Spyder or even Jupyter Notebooks, they all revolve around the project folder. Currently, there is an up-to-date version of PyCharm in the Software Center of the IEA. Visual Studio Code is unfortunately a bit outdated, but still can be used

Besides the countless things most IDEs can do anyway, most support additional plugins. Here are some recommendations:

- GitHub Copilot: AI-based code completion (currently only available for VS Code and PyCharm). By far the most useful plugin. Not perfect, but useful to learn new things and to speed up the coding process. (Download: [PyCharm](https://plugins.jetbrains.com/plugin/17718-github-copilot), [VS Code](https://marketplace.visualstudio.com/items?itemName=GitHub.copilot))

## Virtual Environment

A virtual environment is a self-contained, isolated environment where you can install Python packages and manage dependencies separately from your system-wide Python installation. If you don't use virtual environments all installed packages (pandas, julia, jupyter notebook). Having an own installation of those packages for each project has some advantages again:

1. Dependency Isolation: Virtual environments allow you to install specific packages and their versions for a particular project without affecting other projects or the system-wide Python installation. Otherwise, it can sometimes happen that through a package update something in another project no longer works. This can cause bugs that are sometimes very unpleasant to solve.
2. Easy setup: Virtual environments are portable, meaning you can easily transfer them to other machines, ensuring consistent setups across different environments.
3. Version Compatibility: They enable you to create an environment with a specific Python version, ensuring compatibility with your project's requirements.
4. Clean Development: With virtual environments, you can experiment with different libraries and configurations without cluttering your main Python installation.

A common package and environment manager is Conda and can be installed via the IEA Software Center. Below is a short introduction into the most important commands. Find more details here: [Conda Documentation: Managing environments](https://docs.conda.io/projects/conda/en/latest/user-guide/tasks/manage-environments.html).

If a conda environment is set up you activate it with `conda activate <env_name>` and deactivate it with `conda deactivate`. Then you can run all python commands or launch a Jupyter Notebook from there. Most IDEs also support conda environments and can be setup to use them.

### Setup Virtual Environment

All those steps can be used on the global environment as well. 

Most of the setup process can be done with the 'environment.yml' file:

      conda env create -f environment.yml

This creates a conda environment named iea-rise and installs all relevant packages which can be installed via conda. Then activate the environment and install the relevant packages which are only available via pip and use the IEA proxy:

      conda activate iea-rise
      python -m pip install --proxy http://proxy.iea.org:8080 julia
      python -m pip install --proxy http://proxy.iea.org:8080 https://github.com/NREL/h5plexos/archive/master.zip


That's it. Julia needs also to be installed on the system and if Julia should be used within python (only for unpacking the .zips to .h5 files) it also has to be initialized within python. There is a function for that in the code.

If you want to remove an environment to run the setup process again, you can do that with:
      
      conda deactivate # if the environment is active
      conda env remove --name iea-rise

If you want to use a different environment name you can just change the name in the environment.yml file and run the setup process again.

#### If stuff does not work
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


Now all packages, the environment and a project folder is set up. To get the scripts see [here](Version-Control.md).