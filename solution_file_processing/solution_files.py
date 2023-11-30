"""
This module contains the SolutionFilesConfig class, which is the main configuration and utility class for managing
the output files and plot creation of PLEXOS solution files outputs.
"""

import os
import re
import toml
import pandas as pd
import dask.dataframe as dd
import julia
from h5plexos.query import PLEXOSSolution
import math

from .utils.utils import get_files, enrich_df, silence_prints
from .constants import PRETTY_MODEL_NAMES, FILTER_PROPS
from .caching import Objects, Variables
from . import log

print = log.info


class SolutionFilesConfig:
    """
    Main configuration and utility class for managing solution files in Plexos modeling. This class does multiple
    things:
    - It loads the configuration file and makes the settings available as attributes for other functions.
    - It provides methods processing the PLEXOS solution files, uncompressed them and to retrieve processed data for
        specified objects and timescales.
    - It initializes the caching system for the solution files, which is used by the other functions. This is done
        automatically.
    - It provides a method to test the output files for equality with the older output files.

    But most of it is done automatically, so to use it you only need to do three things:
    1. Create an instance of the class with the name of the configuration file to use.
    2. When running the first time:
        - Make sure that Julia and H5PLEXOS are installed and accessible in your environment and install Julia also with
        the 'install_dependencies()' method.
        - Call the 'convert_solution_files_to_h5()' method to convert the solution files to H5 format.
    3. Pass the object to any output function to create the output files.

    Example:
    ```
    import solution_file_processing
    config = solution_file_processing.SolutionFilesConfig('IDN.toml')
    config.install_dependencies()  # Only when running the first time
    config.convert_solution_files_to_h5()  # Only when running the first time

    solution_file_processing.outputs.create_year_output_1(config)
    ...
    ```

    Args:
        config_name (str): The name of the configuration file to use for settings. This can be a relative path to the
            file or an absolute path.

    Attributes:
        config_name (str): The name of the configuration file.
        cfg (dict): The loaded configuration settings in dictionary format.
        soln_idx (pandas.DataFrame): DataFrame containing the solution index excel.
        DIR_04_SOLUTION_FILES (str): Path to the directory containing the uncompressed solution files.
        DIR_04_CACHE (str): Path to the directory containing the cached solution files.
        DIR_05_DATA_PROCESSING (str): Path to the directory containing the processed data.
        DIR_05_1_SUMMARY_OUT (str): Path to the directory containing the summary output files.
        DIR_05_2_TS_OUT (str): Path to the directory containing the timeseries output files.
        v (rise.caching.Variables): The caching system for variables. All variables can be accessed via
            self.v.<variable_name>. For more details see documentation in caching.py.
        o (rise.caching.Objects): The caching system for objects. All objects can be accessed via
            self.o.<object_name>. For more details see documentation in caching.py.

    Methods:
        - install_dependencies(): Install required dependencies for the Julia programming language.
        - convert_solution_files_to_h5(): Convert solution files in ZIP format to H5 format using the H5PLEXOS Julia
            library.
        - get_processed_object(timescale, object): Retrieve processed data for a specified object and timescale.
        - test_output(timescale, output_number=None, check_mode='simple'): Test the output files for consistency.
    """

    def __init__(self, config_name):
        # Apply config_name to relevant settings
        self.config_name = config_name
        # Load the configuration
        try:
            with open(config_name, 'r') as f:
                self.cfg = toml.load(f)
        except FileNotFoundError:
            raise FileNotFoundError(f'Could not find configuration file {os.path.basename(config_name)} in '
                                    f'{os.path.abspath(config_name)}.')

        ## Apply configurations
        # For logging
        if self.cfg['run']['log_file_path']:
            log_file_path = self.cfg['run']['log_file_path']
            if self.cfg['run']['log_timestamp']:
                timestamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
                log_file_path = os.path.join(os.path.dirname(log_file_path),
                                             f'{timestamp}-{os.path.basename(log_file_path)}')
                log_file_path = os.path.normpath(log_file_path)
            log.change_log_file_path(log_file_path)
            print(f'Logging to {log_file_path}.')

        # Load soln_idx table
        self.soln_idx = pd.read_excel(self.cfg['path']['soln_idx_path'], sheet_name='SolutionIndex', engine='openpyxl')

        # Load paths from configurations
        self.DIR_04_SOLUTION_FILES = os.path.join(self.cfg['path']['model_dir'], '04_SolutionFiles',
                                                  self.cfg['model']['soln_choice'])
        self.DIR_04_CACHE = os.path.join(self.cfg['path']['model_dir'], '04_SolutionFilesCache',
                                         self.cfg['model']['soln_choice'])
        self.DIR_05_DATA_PROCESSING = os.path.join(self.cfg['path']['model_dir'], '05_DataProcessing',
                                                   self.cfg['model']['soln_choice'])
        self.DIR_05_1_SUMMARY_OUT = os.path.join(self.DIR_05_DATA_PROCESSING, 'summary_out')
        self.DIR_05_2_TS_OUT = os.path.join(self.DIR_05_DATA_PROCESSING, 'timeseries_out')
        self.DIR_05_3_PLOTS = os.path.join(self.DIR_05_DATA_PROCESSING, 'plots')

        # Create all necessary directories
        os.makedirs(self.DIR_04_CACHE, exist_ok=True)
        os.makedirs(self.DIR_05_DATA_PROCESSING, exist_ok=True)
        os.makedirs(self.DIR_05_1_SUMMARY_OUT, exist_ok=True)
        os.makedirs(self.DIR_05_2_TS_OUT, exist_ok=True)
        os.makedirs(self.DIR_05_3_PLOTS, exist_ok=True)

        # Define some constants for easier access
        self.GEO_COLS = self.cfg['settings']['geo_cols']

        # Initialize caching system
        self.v = Variables(self)
        self.o = Objects(self)

        print(f'Initialized SolutionFilesConfig for {self.config_name}.')

    @staticmethod
    def install_dependencies():
        """
        Install required dependencies for the Julia programming language. This is only needed to run the
        `convert_solution_files_to_h5()` method.

        This function uses the `julia.install()` method to install any necessary packages,
        libraries, or components needed to work with the Julia programming language.
        Make sure you have the Julia programming language installed before using this function.
        """
        julia.install()

    def convert_solution_files_to_h5(self):
        """
        Converts solution files in ZIP format to H5 format using the H5PLEXOS Julia library.
        Does that automatically for all ZIP files in the subdirectory "04_SolutionFiles/model_name" in the given main
        directory. The H5 files are saved in the same directory. Existing H5 files are not overwritten, but skipped.
        Ensure that Julia and the H5PLEXOS library are installed and are accessible in your environment.
        """
        # todo maybe also allow .zips in nested folders to be converted
        from julia.api import Julia

        jl = Julia(compiled_modules=False)
        jl.using("H5PLEXOS")

        soln_zip_files = [f for f in os.listdir(self.DIR_04_SOLUTION_FILES)
                          if f.endswith('.zip')]

        missing_soln_files = [f.split('.')[0] for f in soln_zip_files if f.replace('.zip', '.h5')
                              not in os.listdir(self.DIR_04_SOLUTION_FILES)]

        print(f'Found {len(missing_soln_files)} missing h5 files. Starting conversion...')
        for h5_file in missing_soln_files:
            jl.eval("cd(\"{}\")".format(self.DIR_04_SOLUTION_FILES.replace('\\', '/')))
            jl.eval("process(\"{}\", \"{}\")".format(f'{h5_file}.zip', f'{h5_file}.h5'))

    def _get_object(self, timescale, object, return_type):
        """
        Retrieves a specific object based on the provided timescale.
        Does that by looping through all solution files (.h5) files in the 04_SolutionFiles folder and combining the
        data for the specified object in a single DataFrame. The DataFrame is then processed very minimally to make it
        more readable and usable. To allow for huge data sets, the data is processed and returned using Dask DataFrames.

        Parameters:
        timescale (str): The timescale to use when retrieving the object. Either 'interval' or 'year'.
        object (str): The name of the object to retrieve. E.g., 'nodes', 'generators', 'regions'.

        Returns:
        dd.DataFrame: The retrieved data.

        Raises:
        ValueError: If no data is found for the specified object in any of the solution files.
        """
        if timescale not in ['interval', 'year']:
            raise ValueError('type must be either "interval" or "year"')

        _, soln_h5_files = get_files(self.DIR_04_SOLUTION_FILES,
                                     file_type='.h5', id_text='Solution', return_type=1)

        dfs = []
        for file in soln_h5_files:
            # Any spaces in the file will break it
            core_name = file.split('\\')[-1].split('Model ')[-1].split(' Solution.h5')[0]

            silence_prints(True)
            with PLEXOSSolution(os.path.join(self.DIR_04_SOLUTION_FILES, file)) as db:
                silence_prints(False)
                try:
                    properties = list(db.h5file[f'data/ST/{timescale}/{object}/'].keys())
                except KeyError:
                    continue

                try:
                    obj_props = [prop for prop in properties if prop in FILTER_PROPS[object]]
                except KeyError:  # If relevant object in FILTER_PROPS is not defined, all properties are used
                    obj_props = properties

                for obj_prop in obj_props:

                    # Relations (i.e. membership-related props) have underscores in them
                    if '_' not in object:
                        db_data = db.query_object_property(
                            object_class=object[:-1],  # object class is queried without the 's' at the end of its name
                            # (this may be a bug that is corrected in future versions)
                            prop=obj_prop,
                            timescale=timescale,
                            phase="ST").reset_index(),

                    else:
                        db_data = db.query_relation_property(
                            relation=object,
                            prop=obj_prop,
                            timescale=timescale,
                            phase="ST").reset_index(),
                    if len(db_data) == 1:
                        db_data = db_data[0]
                    else:
                        raise ValueError('Multiple dataframes returned for {} {}'.format(object, obj_prop))

                    if return_type == 'dask':
                        db_data = dd.from_pandas(db_data, npartitions=1)  # Change to dask to handle cache issues
                        db_data = db_data.repartition(partition_size="100MB")  # Repartition to most efficient size
                    db_data = db_data.assign(model=core_name).reset_index()

                    dfs.append(db_data)
        if not dfs:
            raise ValueError(f'No data found for {object} object in {timescale} timescale in any of the '
                             f'{len(soln_h5_files)} solution files.')

        if return_type == 'dask':
            return dd.concat(dfs)
        else:
            return pd.concat(dfs)

    def get_processed_object(self, timescale, object, return_type):
        """
        Retrieve the processed data for a specified object for either the interval or year timescale. It needs
        the uncompressed Plexos Solution Files in the 04_SolutionFiles folder. It loops through all Solution Files and
        combines the data for the specified object in a single DataFrame. The DataFrame is then processed very
        minimally to make it more readable and usable. To allow for huge data sets, the data is processed and returned
        using Dask DataFrames.

        Parameters:
        - timescale (str): The timescale for data retrieval ('interval' or 'year').
        - object (str): The type of object to retrieve data for (e.g., 'nodes', 'generators', 'regions').
        - return_type (str): The type of DataFrame to return ('dask' or 'pandas').

        Returns:
        - dask.DataFrame or pandas.DataFrame: The retrieved data. Type depends on the return_type parameter.
        """
        if timescale not in ['interval', 'year']:
            raise ValueError('type must be either "interval" or "year"')
        if return_type not in ['dask', 'pandas']:
            raise ValueError('return_type must be either "dask" or "pandas"')

        # - common_yr
        # todo craig: Can I get the model_yrs info also from other files?
        model_yrs = self._get_object(timescale='interval', object='regions', return_type='dask') \
            .groupby(['model']) \
            .first() \
            .timestamp.dt.year.values.compute()
        if len(model_yrs) > 1:
            common_yr = model_yrs[-1]
        else:
            common_yr = None

        # - filter_regs
        # todo craig: Can I get the model_yrs info also from other files?
        filter_reg_by_gen = enrich_df(self._get_object(timescale='year', object='generators', return_type=return_type),
                                      soln_idx=self.soln_idx[
                                          self.soln_idx.Object_type.str.lower() == 'generator'].drop(
                                          columns='Object_type'), pretty_model_names=PRETTY_MODEL_NAMES)
        filter_reg_by_load = enrich_df(self._get_object(timescale='year', object='nodes', return_type=return_type),
                                       soln_idx=self.soln_idx[self.soln_idx.Object_type.str.lower() == 'node'].drop(
                                           columns='Object_type'), pretty_model_names=PRETTY_MODEL_NAMES)

        # Filter out nodes that have zero load
        filter_reg_by_load = filter_reg_by_load[
            (filter_reg_by_load.property == 'Load') & (filter_reg_by_load.value != 0)]
        filter_reg_by_gen = filter_reg_by_gen[self.GEO_COLS[-1]].unique()
        filter_reg_by_load = filter_reg_by_load[self.GEO_COLS[-1]].unique()
        filter_regs = list(set([reg for reg in filter_reg_by_gen] + [reg for reg in filter_reg_by_load]))

        # Actual processing
        df = self._get_object(timescale=timescale, object=object, return_type=return_type)
        if return_type == 'dask':
            df = df.repartition(partition_size="100MB")  # Repartition to most efficient size

        o_key = object.replace('ies', 'ys')

        if timescale == 'interval':
            if '_' in o_key:
                # Membership properties can use the generator/battery part which is the second part
                # Also o_key is for singular form, hence drops s
                o_key = o_key.split('_')[-1][:-1]
            else:
                o_key = o_key[:-1]

                # Remove unnecessary columns, so object_type can be removed for the o_idx
            o_idx = (self.soln_idx[self.soln_idx.Object_type.str.lower().str.replace(' ', '') == o_key]
                     .drop(columns='Object_type'))
            if len(o_idx) > 0:
                if '_' not in object:
                    df = enrich_df(df, soln_idx=o_idx, common_yr=common_yr,
                                   out_type='direct', pretty_model_names=PRETTY_MODEL_NAMES)
                else:
                    df = enrich_df(df, soln_idx=o_idx, common_yr=common_yr, out_type='rel',
                                   pretty_model_names=PRETTY_MODEL_NAMES)

                # Check if df is empty (faster way for dask, instead of df.empty)
                assert len(df.index) != 0, f'Merging of SolutionIndex led to empty DataFrame for {object}/{timescale}.'

                # Filter out regions with no generation nor load
                # todo commented that out for now, since it was creating empty dataframes
                # if (object == 'nodes') | (object == 'regions'):
                #     df = df[df[self.cfg['settings']['geo_cols'][-1]].isin(filter_regs)]
            else:
                raise ValueError(f'No generator parameters added for {object}. Could not find "{o_key}" in '
                                 f'soln_idx.Object_type. Please add it to the SolutionIndex excel sheet.')

        elif timescale == 'year':

            if '_' in o_key:
                # Membership properties can use the generator/battery part which is the second part
                # Also o_key is for singular form, hence drops s
                o_key = o_key.split('_')[-1][:-1]
            else:
                o_key = o_key[:-1]

            # No need to filter out solnb_idx for the annual data as the size won't be an issue
            o_idx = (self.soln_idx[self.soln_idx.Object_type.str.lower().str.replace(' ', '') == o_key]
                     .drop(columns='Object_type'))
            if len(o_idx) > 0:
                if '_' not in object:
                    df = enrich_df(df, soln_idx=o_idx, common_yr=common_yr,
                                   out_type='direct', pretty_model_names=PRETTY_MODEL_NAMES)
                else:
                    df = enrich_df(df, soln_idx=o_idx, common_yr=common_yr,
                                   out_type='rel', pretty_model_names=PRETTY_MODEL_NAMES)

                # Filter out regions with no generation nor load
                if (object == 'nodes') | (object == 'regions'):
                    df = df[df[self.GEO_COLS[-1]].isin(filter_regs)]
            else:
                raise ValueError(f'No generator parameters added for {object}. Could not find "{o_key}" in '
                                 f'soln_idx.Object_type. Please add it to the SolutionIndex excel sheet.')

        return df

    @staticmethod
    def _list_csv_files(root_dir):
        csv_files = []

        for foldername, subfolders, filenames in os.walk(root_dir):
            for filename in filenames:
                if filename.endswith(".csv"):
                    relative_path = os.path.relpath(os.path.join(foldername, filename), root_dir)
                    csv_files.append(relative_path)

        return csv_files

    def test_output(self, timescale, output_numbers: list = None):
        if output_numbers is None:
            print(f'Start tests for {timescale} output.')
        else:
            print(f'Start tests for {timescale} output {output_numbers}.')

        if timescale == 'year':
            subfolder_name = 'summary_out'
        elif timescale == 'interval':
            subfolder_name = 'timeseries_out'
        else:
            raise Exception('timescale must be either "year" or "interval"')

        output_path = os.path.join(self.cfg['path']['model_dir'],
                                   '05_DataProcessing',
                                   self.cfg['model']['soln_choice'],
                                   subfolder_name)

        # Run tests with baseline path if given
        if self.cfg['testing']['baseline_output_dir']:
            print(f'\n\nRunning baseline tests with {self.cfg["testing"]["baseline_output_dir"]}.\n')

            output_path_test_baseline = os.path.join(self.cfg['testing']['baseline_output_dir'],
                                                     subfolder_name)

            # Get all files, sort and remove duplicates
            files = ([f for f in self._list_csv_files(output_path)] +
                     [f for f in self._list_csv_files(output_path_test_baseline)])
            files = list(set(files))
            files.sort()

            for file in files:

                if output_numbers is not None:
                    if not any([re.match(f'0?{n}([a-z]*_)', file.split('\\')[-1]) for n in output_numbers]):
                        continue
                try:
                    df = pd.read_csv(os.path.join(output_path, file), low_memory=False)
                except FileNotFoundError:
                    print(f'Implementation missing: {file}.')
                    continue
                try:
                    df_test = pd.read_csv(os.path.join(output_path_test_baseline, file), low_memory=False)
                except FileNotFoundError:
                    print(f'Test file missing: {file}.')
                    continue

                cols = df.columns.to_list()
                cols_test = df_test.columns.to_list()

                matched_cols = sorted(list(set(cols) & set(cols_test)))
                redundant_cols = list(set(cols) - set(cols_test))
                missing_cols = list(set(cols_test) - set(cols))

                for col in redundant_cols:
                    print(f'\tColumn {col} is redundant.')
                for col in missing_cols:
                    print(f'\tColumn {col} is missing.')

                # Make columns match
                df = df[matched_cols]
                df_test = df_test[matched_cols]

                # Round to avoid floating point errors
                df = df.round(5)
                df_test = df_test.round(5)

                test_failed = False
                if not df_test.equals(df):

                    if df_test.shape != df.shape:
                        print(f'\tShape of {file} does not match: {df_test.shape} != {df.shape}.')
                        test_failed = True

                    for col in df.columns:
                        if pd.to_numeric(df[col], errors='coerce').notna().all():
                            if not math.isclose(df[col].sum(), df_test[col].sum()):
                                print(
                                    f'\tSum of {file} column {col} does not match: {df[col].sum()} != {df_test[col].sum()}.')
                                test_failed = True
                        else:
                            if not set(df[col].unique()) == set(df_test[col].unique()):
                                print(f'\tUnique values of {file} column {col} do not match.')
                                test_failed = True

                if test_failed:
                    print(f'Test failed: {file}.')
                else:
                    print(f'Test passed: {file}.')
        else:
            print(f'cfg.testing.baseline_output_dir not set. Skipping baseline tests.')

        # Run tests with similar outputs to check for consistency, if given
        if self.cfg['testing']['similar_output_dirs']:
            for similar_output_dir in self.cfg['testing']['similar_output_dirs']:
                print(f'\n\nRunning similarity tests with {similar_output_dir}.\n')
                output_path_test_similar = os.path.join(similar_output_dir,
                                                        subfolder_name)

                # Get all files, sort and remove duplicates
                files = ([f for f in self._list_csv_files(output_path)] +
                         [f for f in self._list_csv_files(output_path_test_similar)])
                files = list(set(files))
                files.sort()

                for file in files:

                    if output_numbers is not None:
                        if not any([re.match(f'0?{n}([a-z]*_)', file.split('\\')[-1]) for n in output_numbers]):
                            continue
                    try:
                        df = pd.read_csv(os.path.join(output_path, file), low_memory=False)
                    except FileNotFoundError:
                        print(f'Implementation missing: {file}.')
                        continue
                    try:
                        df_test = pd.read_csv(os.path.join(output_path_test_similar, file), low_memory=False)
                    except FileNotFoundError:
                        print(f'Test file missing: {file}.')
                        continue

                    cols = df.columns.to_list()
                    cols_test = df_test.columns.to_list()

                    matched_cols = sorted(list(set(cols) & set(cols_test)))
                    redundant_cols = list(set(cols) - set(cols_test))
                    missing_cols = list(set(cols_test) - set(cols))

                    for col in redundant_cols:
                        print(f'\tColumn {col} is redundant.')
                    for col in missing_cols:
                        print(f'\tColumn {col} is missing.')

                    # Make columns match
                    df = df[matched_cols]
                    df_test = df_test[matched_cols]

                    test_failed = False
                    for col in df.columns:
                        # Check if dtype of columns match
                        if df[col].dtype != df_test[col].dtype:
                            print(f'\tDtype of {file} column {col} does not match: {df[col].dtype} != '
                                  f'{df_test[col].dtype}.')
                            test_failed = True
                            continue

                        # Check if ratio of nans is either 0, 1, or inbetween for both
                        if ((df[col].isna().sum() / len(df[col])) != (df_test[col].isna().sum() / len(df_test[col])) and
                                0 < (df[col].isna().sum() / len(df[col])) < 1):
                            print(f'\tRatio of nans of {file} column {col} does not match: '
                                  f'{df[col].isna().sum() / len(df[col])} != '
                                  f'{df_test[col].isna().sum() / len(df_test[col])}.')
                            test_failed = True
                            continue

                    if test_failed:
                        print(f'Test failed: {file}.')
                    else:
                        print(f'Test passed: {file}.')

                print('Tests done.')
        else:
            print(f'cfg.testing.similar_output_dirs not set. Skipping similar tests.')
