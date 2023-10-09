"""
todo Docstring
"""

import os
from pathlib import Path

import pandas as pd
import dask.dataframe as dd
import julia
from h5plexos.query import PLEXOSSolution

from .utils.logger import log
from .utils.utils import get_files, enrich_df, silence_prints
from .constants import PRETTY_MODEL_NAMES, FILTER_PROPS
from .settings import settings as s

print = log.info


class SolutionFiles:
    """
    todo Docstring
    """

    def __init__(self):
        self.DIR_04_SOLUTION_FILES = None
        self.DIR_04_CACHE = None
        self.DIR_05_DATA_PROCESSING = None
        self.DIR_05_1_SUMMARY_OUT = None
        self.DIR_05_2_TS_OUT = None
        self.soln_idx = None

        self.initialized = False

    def initialize(self):
        """
        todo Docstring
        """
        # Perform the actual initialization here
        self.DIR_04_SOLUTION_FILES = os.path.join(s.cfg['path']['model_dir'], '04_SolutionFiles',
                                                  s.cfg['model']['soln_choice'])
        self.DIR_04_CACHE = os.path.join(s.cfg['path']['model_dir'], '04_SolutionFilesCache',
                                         s.cfg['model']['soln_choice'])
        self.DIR_05_DATA_PROCESSING = os.path.join(s.cfg['path']['model_dir'], '05_DataProcessing',
                                                   s.cfg['model']['soln_choice'])
        self.DIR_05_1_SUMMARY_OUT = os.path.join(s.cfg['path']['model_dir'], '05_DataProcessing',
                                                 s.cfg['model']['soln_choice'], 'summary_out')
        self.DIR_05_2_TS_OUT = os.path.join(s.cfg['path']['model_dir'], '05_DataProcessing',
                                            s.cfg['model']['soln_choice'], 'timeseries_out')

        self.soln_idx = pd.read_excel(s.cfg['path']['soln_idx_path'], sheet_name='SolutionIndex', engine='openpyxl')
        self._initialized = True

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
        if not self.initialized:
            self.initialize()
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
        if not self.initialized:
            self.initialize()
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

                    # db_data = pd.DataFrame(db_data)
                    # db_data['model'] = core_name
                    db_data = add_df_column(
                        df=db_data,
                        column_name='model',
                        value=core_name)

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
        filter_reg_by_gen = filter_reg_by_gen[s.cfg['settings']['geo_cols'][-1]].unique()
        filter_reg_by_load = filter_reg_by_load[s.cfg['settings']['geo_cols'][-1]].unique()
        filter_regs = list(set([reg for reg in filter_reg_by_gen] + [reg for reg in filter_reg_by_load]))

        # Actual processing
        df = self._get_object(timescale=timescale, object=object)
        df = df.repartition(partition_size="100MB")

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
                    df = df[df[s.cfg['settings']['geo_cols'][-1]].isin(filter_regs)]

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
                    df = df[df[s.cfg['settings']['geo_cols'][-1]].isin(filter_regs)]

        return df
