# Setup

## Project Folder

Every setup process starts with the project folder. It is a dedicated directory for organizing and managing all script files and other project files. Data can be put somewhere else. Although scripts can be reverenced over multiple folders and can still be run with Jupyter Notebooks or as Python files (like done with the old R scripts), this has some drawbacks. Or rather you give up some advantages:

- Simplicity and better organization, the complete code is only in one place. No confusion
- No potential problems with interfering with other projects
- It is ideal for using version control (which alone is extremely helpful)
- Encourages good and up-to-date documentation practices (Write code and documentation in the same project/ editor).
- ...

When creating a new project folder, you can simply copy all files from the source or even better use version control (see here: [git](Version-Control.md)).

### Which Integrated Development Environment (IDE)

Which IDE is then used does not really matter .Regardless of whether you choose PyCharm, Visual Studio Code, Spyder or even Jupyter Notebooks, they all revolve around the project folder.Currently there is a up to date version of PyCharm in the Software Center. Visual Studio Code is unfortunately a bit outdated, but still can be used.

## Structure

The structure of the project folder is not fixed, but the rise package and kind of any other relevant python package has the following structure:

```
rise
├── configs
│   ├── config_file1.toml
│   ├── config_file2.toml
│   ├── ...
├── docs
│   ├── Solution File Processing
│   │   ├── ...
│   ├── Knowledge database
│   │   ├── ...
├── logs
│   ├── config_file1.log
│   ├── config_file2.log
│   ├── ...
├── rise
│   ├── __init__.py
│   ├── script_file1.py
│   ├── script_file2.py
│   ├── ...
├── .gitignore
├── README.md
├── requirements.txt
├── setup.py
├── main.py
```

## Virtual Environment

A virtual environment is a self-contained, isolated environment where you can install Python packages and manage dependencies separately from your system-wide Python installation. If you don't use virtual environments all installed packages (pandas, julia, jupyter notebook). Having an own installation of those packages for each project has some advantages again:

1. Dependency Isolation: Virtual environments allow you to install specific packages and their versions for a particular project without affecting other projects or the system-wide Python installation. Otherwise, it can sometimes happen that through a package update something in another project no longer works. This can cause bugs that are sometimes very unpleasant to solve.
2. Easiy setup: Virtual environments are portable, meaning you can easily transfer them to other machines, ensuring consistent setups across different environments.
3. Version Compatibility: They enable you to create an environment with a specific Python version, ensuring compatibility with your project's requirements.
4. Clean Development: With virtual environments, you can experiment with different libraries and configurations without cluttering your main Python installation.

Using Conda, a popular package and environment manager, to create and manage virtual environments is straightforward:

1. Create a Virtual Environment:
   To create a new environment, use the following command:

lua

conda create --name myenv

Replace myenv with the desired environment name.

2. Activate the Environment:
   To activate the environment, use:

   On Windows:

conda activate myenv

On macOS and Linux:

bash

source activate myenv
3. Install Packages:
   Inside the activated environment, you can use conda install to add Python packages, for example:

conda install package_name

4. Deactivate the Environment:
   To leave the environment and return to your base Python installation, simply use:

conda deactivate

5. Remove an Environment:
   If you want to remove an environment when you're done with it, use:

lua

conda env remove --name myenv

In summary, virtual environments, coupled with Conda, offer a flexible and powerful way to manage Python projects. They enable clean, isolated environments where you can control dependencies, maintain compatibility, and experiment without disrupting other projects or your system-wide Python installation.
