"""
todo Docstring
"""

import os
import re
import toml
import pandas as pd
import dask.dataframe as dd
import julia
from h5plexos.query import PLEXOSSolution
import math

from .utils.logger import log
from .utils.utils import get_files, enrich_df, silence_prints
from .constants import PRETTY_MODEL_NAMES, FILTER_PROPS
from .caching import Objects, Variables

print = log.info


class SolutionFilesConfig:
    """
    todo Docstring
    """

    def __init__(self, config_name):
        print(f'Initializing {config_name} Solution Files Config...')
        # Apply config_name to relevant settings
        self.config_name = config_name
        # Load the configuration
        with open(os.path.join('configs', config_name), 'r') as f:
            self.cfg = toml.load(f)

        # Apply configurations
        # to log_path
        log.change_log_file_path(self.cfg['run']['log_file_path'])

        self.soln_idx = pd.read_excel(self.cfg['path']['soln_idx_path'], sheet_name='SolutionIndex', engine='openpyxl')

        # Load paths from configurations
        self.DIR_04_SOLUTION_FILES = os.path.join(self.cfg['path']['model_dir'], '04_SolutionFiles',
                                                  self.cfg['model']['soln_choice'])
        self.DIR_04_CACHE = os.path.join(self.cfg['path']['model_dir'], '04_SolutionFilesCache',
                                         self.cfg['model']['soln_choice'])
        self.DIR_05_DATA_PROCESSING = os.path.join(self.cfg['path']['model_dir'], '05_DataProcessing',
                                                   self.cfg['model']['soln_choice'])
        self.DIR_05_1_SUMMARY_OUT = os.path.join(self.cfg['path']['model_dir'], '05_DataProcessing',
                                                 self.cfg['model']['soln_choice'], 'summary_out')
        self.DIR_05_2_TS_OUT = os.path.join(self.cfg['path']['model_dir'], '05_DataProcessing',
                                            self.cfg['model']['soln_choice'], 'timeseries_out')

        os.makedirs(self.DIR_04_CACHE, exist_ok=True)
        os.makedirs(self.DIR_05_DATA_PROCESSING, exist_ok=True)
        os.makedirs(self.DIR_05_1_SUMMARY_OUT, exist_ok=True)
        os.makedirs(self.DIR_05_2_TS_OUT, exist_ok=True)

        self.v = Variables(self)
        self.o = Objects(self)

    @staticmethod
    def install_dependencies():
        """
        todo Docstring
        """
        julia.install()

    def convert_solution_files_to_h5(self):
        """
        todo Docstring
        """
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

    def _get_object(self, timescale, object):
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
                    print(f'{object} does not exist')
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

                    db_data = dd.from_pandas(db_data, npartitions=1)  # Change to dask to handle cache issues
                    db_data = db_data.repartition(partition_size="100MB")  # Repartition to most efficient size
                    db_data = db_data.assign(model=core_name).reset_index()

                    dfs.append(db_data)
        return dd.concat(dfs)

    def get_processed_object(self, timescale, object):
        """"
        todo Docstring
        """
        # Import necessary stuff

        # - common_yr
        # todo craig: Can I get the model_yrs info also from other files?
        model_yrs = self._get_object(timescale='interval', object='regions') \
            .groupby(['model']) \
            .first() \
            .timestamp.dt.year.values.compute()
        if len(model_yrs) > 1:
            common_yr = model_yrs[-1]
        else:
            common_yr = None

        # - filter_regs
        # todo craig: Can I get the model_yrs info also from other files?
        filter_reg_by_gen = enrich_df(self._get_object(timescale='year', object='generators'),
                                      soln_idx=self.soln_idx[
                                          self.soln_idx.Object_type.str.lower() == 'generator'].drop(
                                          columns='Object_type'), pretty_model_names=PRETTY_MODEL_NAMES)
        filter_reg_by_load = enrich_df(self._get_object(timescale='year', object='nodes'),
                                       soln_idx=self.soln_idx[self.soln_idx.Object_type.str.lower() == 'node'].drop(
                                           columns='Object_type'), pretty_model_names=PRETTY_MODEL_NAMES)

        # Filter out nodes that have zero load
        filter_reg_by_load = filter_reg_by_load[
            (filter_reg_by_load.property == 'Load') & (filter_reg_by_load.value != 0)]
        filter_reg_by_gen = filter_reg_by_gen[self.cfg['settings']['geo_cols'][-1]].unique()
        filter_reg_by_load = filter_reg_by_load[self.cfg['settings']['geo_cols'][-1]].unique()
        filter_regs = list(set([reg for reg in filter_reg_by_gen] + [reg for reg in filter_reg_by_load]))

        # Actual processing
        df = self._get_object(timescale=timescale, object=object)
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
            o_idx = self.soln_idx[self.soln_idx.Object_type.str.lower().str.replace(' ', '') == o_key].drop(
                columns='Object_type')
            if len(o_idx) > 0:
                if '_' not in object:
                    df = enrich_df(df, soln_idx=o_idx, common_yr=common_yr,
                                   out_type='direct', pretty_model_names=PRETTY_MODEL_NAMES)
                else:
                    df = enrich_df(df, soln_idx=o_idx, common_yr=common_yr, out_type='rel',
                                   pretty_model_names=PRETTY_MODEL_NAMES)

                # Filter out regions with no generation nor load
                if (object == 'nodes') | (object == 'regions'):
                    df = df[df[self.cfg['settings']['geo_cols'][-1]].isin(filter_regs)]

        elif timescale == 'year':

            if '_' in o_key:
                # Membership properties can use the generator/battery part which is the second part
                # Also o_key is for singular form, hence drops s
                o_key = o_key.split('_')[-1][:-1]
            else:
                o_key = o_key[:-1]

            # No need to filter out solnb_idx for the annual data as the size won't be an issue
            o_idx = self.soln_idx[self.soln_idx.Object_type.str.lower().str.replace(' ', '') == o_key].drop(
                columns='Object_type')
            if len(o_idx) > 0:
                if '_' not in object:
                    df = enrich_df(df, soln_idx=o_idx, common_yr=common_yr,
                                   out_type='direct', pretty_model_names=PRETTY_MODEL_NAMES)
                else:
                    df = enrich_df(df, soln_idx=o_idx, common_yr=common_yr,
                                   out_type='rel', pretty_model_names=PRETTY_MODEL_NAMES)

                # Filter out regions with no generation nor load
                if (object == 'nodes') | (object == 'regions'):
                    df = df[df[self.cfg['settings']['geo_cols'][-1]].isin(filter_regs)]

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


    def test_output(self, timescale, output_number=None, check_mode='simple'):
        if timescale == 'year':
            path = os.path.join(self.cfg['path']['model_dir'], '05_DataProcessing', self.cfg['model']['soln_choice'], 'summary_out')
            path_test = os.path.join(self.cfg['path']['model_dir_test'], '05_DataProcessing', self.cfg['model']['soln_choice'], 'summary_out')
        elif timescale == 'interval':
            path = os.path.join(self.cfg['path']['model_dir'], '05_DataProcessing', self.cfg['model']['soln_choice'], 'timeseries_out')
            path_test = os.path.join(self.cfg['path']['model_dir_test'], '05_DataProcessing', self.cfg['model']['soln_choice'], 'timeseries_out')
        else:
            raise Exception('timescale must be either "year" or "interval"')

        if output_number is None:
            print(f'Start tests for {timescale} output (check_mode={check_mode}).')
        else:
            print(f'Start tests for {timescale} output {output_number} (check_mode={check_mode}).')

        # Get all files
        files = [f for f in self._list_csv_files(path_test)]
        files.sort()

        for file in files:

            if output_number is not None:
                if not re.match(f'0?{output_number}([a-z]*_)', file.split('\\')[-1]):
                    continue
            try:
                df = pd.read_csv(os.path.join(path, file), low_memory=False)
            except FileNotFoundError:
                print(f'File missing: {file}.')
                continue
            df_test = pd.read_csv(os.path.join(path_test, file), low_memory=False)

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
                            print(f'\tSum of {file} column {col} does not match: {df[col].sum()} != {df_test[col].sum()}.')
                            test_failed = True
                    else:
                        if not set(df[col].unique()) == set(df_test[col].unique()):
                            print(f'\tUnique values of {file} column {col} do not match.')
                            test_failed = True

            if test_failed:
                print(f'Test failed: {file}.')
            else:
                print(f'Test passed: {file}.')