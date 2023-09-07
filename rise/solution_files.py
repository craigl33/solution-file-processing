import os
from pathlib import Path
import time

import pandas as pd
# import dask.dataframe as dd
import julia
from h5plexos.query import PLEXOSSolution

from .utils import get_files, add_df_column, enrich_df
from .settings import FILTER_PROPS, FILTER_OUT_OBJS
from .constants import PRETTY_MODEL_NAMES

# todo: preliminiary, needs to better implemented
GEO_COLS = ['Island', 'Region', 'Subregion']
# Validation
validation = False
idn_actuals_2019 = pd.read_excel('R:/RISE/DOCS/04 PROJECTS/COUNTRIES/INDONESIA/Power system enhancement 2020_21/\
Modelling/01 InputData/01 Generation/20220201_generator_capacity_v23_NZE.xlsx',
                                 sheet_name='IDN_Summary_per_tech', usecols='A:T', engine='openpyxl')

ix = pd.IndexSlice
incl_regs = ['JVB', 'SUM']


# # incl_regs = ['JVB', 'SUM', 'KLM', 'SLW', 'MPN']


class SolutionFileProcessor():

    def __init__(self, model_dir, soln_choice, soln_idx_path):
        # self.MODEL_DIR = 'C:/Users/TRIPPE_L/Code/dev_dirs/Indonesia/2021_IPSE'

        self.DIR_04_SOLUTION_FILES = os.path.join(model_dir, '04_SolutionFiles', soln_choice)
        self.DIR_04_CACHE = os.path.join(model_dir, '04_SolutionFilesCache', soln_choice)
        self.DIR_05_DATA_PROCESSING = os.path.join(model_dir, '05_DataProcessing', soln_choice)
        self.DIR_05_1_SUMMARY_OUT = os.path.join(model_dir, '05_DataProcessing', soln_choice, 'summary_out')

        self.soln_idx = pd.read_excel(soln_idx_path, sheet_name='SolutionIndex', engine='openpyxl')
        # self.soln_idx = dd.from_pandas(self.soln_idx, npartitions=1)

        # TODO NEEDS also cleanup
        self.gen_addl_idx = self.soln_idx[['name', 'FlexCategory', 'CostCategory', 'CofiringCategory', 'Cofiring', 'CCUS', 'IPP']]

        # TODO PRELIMINARY, NEEDS TO BE BETTER IMPLEMENTED
        idn_actuals_2019 = pd.read_excel(
            'R:/RISE/DOCS/04 PROJECTS/COUNTRIES/INDONESIA/Power system enhancement 2020_21/Modelling/01 InputData/01 Generation/20220201_generator_capacity_v23_NZE.xlsx',
            sheet_name='IDN_Summary_per_tech', usecols='A:T', engine='openpyxl')
        # idn_actuals_2019 = dd.from_pandas(idn_actuals_2019, npartitions=1)

        idn_actuals_2019 = idn_actuals_2019.rename(columns={'PLEXOS_Name': 'name', 'SummaryEn_GWh': 'Generation'})
        idn_actuals_2019 = pd.merge(idn_actuals_2019, self.soln_idx, left_on='name', right_on='name', how='left')
        idn_actuals_2019 = idn_actuals_2019.assign(model='2019 (Historical)')
        self.idn_actuals_2019 = idn_actuals_2019

    def install_dependencies(self):
        julia.install()

    def convert_solution_files_to_h5(self):
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

    def convert_solution_files_to_properties(self, timescale, overwrite=False):
        if timescale not in ['interval', 'year']:
            raise ValueError('type must be either "interval" or "year"')

        _, soln_h5_files = get_files(self.DIR_04_SOLUTION_FILES,
                                     file_type='.h5', id_text='Solution', return_type=1)

        dfs_dict = {}
        for file in soln_h5_files:

            # Any spaces in the file will break it
            core_name = file.split('\\')[-1].split('Model ')[-1].split(' Solution.h5')[0]

            print(f'Processing {core_name} ({timescale})...')

            with PLEXOSSolution(os.path.join(self.DIR_04_SOLUTION_FILES, file)) as db:
                plexos_objs = [p for p in db.h5file[f'data/ST/{timescale}/'].keys() if
                               p not in FILTER_OUT_OBJS or timescale == 'year']  # No need to filter annual

                plexos_props = {obj: list(db.h5file[f'data/ST/{timescale}/{obj}/'].keys()) for obj in plexos_objs}

                for obj in plexos_objs:
                    # todo this is useless rn, since the saving is done in the loop below, needs to be fixed
                    # if not overwrite and os.path.exists(os.path.join(self.DIR_04_CACHE,
                    #                                                  core_name,
                    #                                                  f'{timescale}-{obj}.parquet')):
                    #     print(f'{core_name}/{obj}.parquet" already exists. Pass overwrite=True to overwrite.')
                    #     continue

                    # Filter properties for time-series data
                    try:
                        obj_props = [prop for prop in plexos_props[obj] if prop in FILTER_PROPS[obj]]
                    except KeyError:  # If relevant object in FILTER_PROPS is not defined, all properties are used
                        obj_props = plexos_props[obj]

                    for obj_prop in obj_props:

                        ### Relations (i.e. membership-related props) have underscores in them
                        if '_' not in obj:
                            db_data = db.query_object_property(
                                object_class=obj[:-1],  # object class is queried without the 's' at the end of its name
                                # (this may be a bug that is correccted in future versions)
                                prop=obj_prop,
                                timescale=timescale,
                                phase="ST").reset_index(),

                        else:
                            db_data = db.query_relation_property(
                                relation=obj,
                                prop=obj_prop,
                                timescale=timescale,
                                phase="ST").reset_index(),
                        if len(db_data) == 1:
                            db_data = db_data[0]
                        else:
                            raise ValueError('Multiple dataframes returned for {} {}'.format(obj, obj_prop))

                        # db_data = pd.DataFrame(db_data)
                        # db_data['model'] = core_name
                        db_data = add_df_column(
                            df=db_data,
                            column_name='model',
                            value=core_name)
                        dfs.append(db_data)
                    df = pd.concat(dfs, axis=0)

                    os.makedirs(os.path.join(self.DIR_04_CACHE, self.soln_choice, core_name), exist_ok=True)
                    df.to_parquet(os.path.join(self.DIR_04_CACHE, self.soln_choice, core_name, f'{timescale}_{obj}.parquet'))
                    print(f'Saved {timescale}-{obj}.parquet in {core_name}.')


        print(soln_h5_files)
