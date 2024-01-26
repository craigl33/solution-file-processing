# __init__.py

# Declare package wide logger
from .utils.logger import CustomLogger
log = CustomLogger('solution_file_processing')

# Adjust some dependency settings
import pandas as pd
pd.set_option('display.max_columns', None)  # Show all columns when printing
pd.set_option('display.width', None)  # Don't wrap columns when printing

# Import packages which should be available at the top level of the package
from .solution_files import SolutionFilesConfig
from . import summary
from . import timeseries
from . import plots
