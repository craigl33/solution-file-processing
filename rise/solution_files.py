import os
from pathlib import Path

import pandas as pd
import dask.dataframe as dd
import julia
from h5plexos.query import PLEXOSSolution

from .utils.logger import log
from .utils.utils import get_files, add_df_column, enrich_df
from .constants import PRETTY_MODEL_NAMES
from .settings import config

print = log.info

class SolutionFileFramework:
    def __init__(self):
        self.DIR_04_SOLUTION_FILES = os.path.join(config['path']['model_dir'], '04_SolutionFiles', config['model']['soln_choice'])
        self.DIR_04_CACHE = os.path.join(config['path']['model_dir'], '04_SolutionFilesCache', config['model']['soln_choice'])
        self.DIR_05_DATA_PROCESSING = os.path.join(config['path']['model_dir'], '05_DataProcessing', config['model']['soln_choice'])
        self.DIR_05_1_SUMMARY_OUT = os.path.join(config['path']['model_dir'], '05_DataProcessing', config['model']['soln_choice'], 'summary_out')
        self.DIR_05_2_TS_OUT = os.path.join(config['path']['model_dir'], '05_DataProcessing', config['model']['soln_choice'], 'timeseries_out')

        self.soln_idx = pd.read_excel(config['path']['soln_idx_path'], sheet_name='SolutionIndex', engine='openpyxl')


class SolutionFileProcessor(SolutionFileFramework):

    def __init__(self, model_dir, soln_choice, soln_idx_path):
        super().__init__(model_dir, soln_choice, soln_idx_path)

    @staticmethod
    def install_dependencies():
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

    def convert_solution_files_to_properties(self, timescale):
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

                        # Relations (i.e. membership-related props) have underscores in them
                        if '_' not in obj:
                            db_data = db.query_object_property(
                                object_class=obj[:-1],  # object class is queried without the 's' at the end of its name
                                # (this may be a bug that is corrected in future versions)
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
                        if obj in dfs_dict.keys():
                            dfs_dict[obj].append(db_data)
                        else:
                            dfs_dict[obj] = [db_data]

        for obj, dfs in dfs_dict.items():
            df = dd.concat(dfs)
            os.makedirs(os.path.join(self.DIR_04_CACHE, 'unprocessed'), exist_ok=True)
            df.to_parquet(os.path.join(self.DIR_04_CACHE, 'unprocessed', f'{timescale}-{obj}.parquet'))
            print(f'Saved {timescale}-{obj}.parquet in {Path(self.DIR_04_CACHE).parts[-1]}.')

    def process_properties(self):

        # Import necessary stuff
        # - common_yr
        model_yrs = dd.read_parquet(os.path.join(self.DIR_04_CACHE, 'unprocessed', 'interval-regions.parquet'))
        model_yrs = model_yrs.groupby(['model']).first().timestamp.dt.year.values
        model_yrs = model_yrs.compute()  # Change dd.DataFrame back to pd.DataFrame
        if len(model_yrs) > 1:
            common_yr = model_yrs[-1]
        else:
            common_yr = None
        # - filter_regs
        filter_reg_by_gen = dd.read_parquet(os.path.join(self.DIR_04_CACHE, 'unprocessed', 'year-generators.parquet'))
        filter_reg_by_gen = enrich_df(filter_reg_by_gen,
                                      soln_idx=self.soln_idx[
                                          self.soln_idx.Object_type.str.lower() == 'generator'].drop(
                                          columns='Object_type'), pretty_model_names=PRETTY_MODEL_NAMES)
        filter_reg_by_load = dd.read_parquet(os.path.join(self.DIR_04_CACHE, 'unprocessed', 'year-nodes.parquet'))
        filter_reg_by_load = enrich_df(filter_reg_by_load,
                                       soln_idx=self.soln_idx[self.soln_idx.Object_type.str.lower() == 'node'].drop(
                                           columns='Object_type'), pretty_model_names=PRETTY_MODEL_NAMES)

        # Filter out nodes that have zero load
        filter_reg_by_load = filter_reg_by_load[
            (filter_reg_by_load.property == 'Load') & (filter_reg_by_load.value != 0)]
        filter_reg_by_gen = filter_reg_by_gen[GEO_COLS[-1]].unique()
        filter_reg_by_load = filter_reg_by_load[GEO_COLS[-1]].unique()
        filter_regs = list(set([reg for reg in filter_reg_by_gen] + [reg for reg in filter_reg_by_load]))

        # Actual processing
        files = [f for f in os.listdir(os.path.join(self.DIR_04_CACHE, 'unprocessed')) if f.endswith('.parquet')]
        for file in files:
            if os.path.exists(os.path.join(self.DIR_04_CACHE, 'processed', file)):
                print(f'{file} already exists. Pass overwrite=True to overwrite.')
                continue

            obj = file.split('-')[-1].split('.parquet')[0]
            timescale = file.split('-')[0]
            print(f'Processing {obj} ({timescale})...')
            df = dd.read_parquet(os.path.join(self.DIR_04_CACHE, 'unprocessed', file))
            df = df.repartition(partition_size="100MB")

            o_key = obj.replace('ies', 'ys')

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
                    if '_' not in obj:
                        df = enrich_df(df, soln_idx=o_idx, common_yr=common_yr,
                                       out_type='direct', pretty_model_names=PRETTY_MODEL_NAMES)
                    else:
                        df = enrich_df(df, soln_idx=o_idx, common_yr=common_yr, out_type='rel',
                                       pretty_model_names=PRETTY_MODEL_NAMES)

                    # Filter out regions with no generation nor load
                    if (obj == 'nodes') | (obj == 'regions'):
                        df = df[df[GEO_COLS[-1]].isin(filter_regs)]

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
                    if '_' not in obj:
                        df = enrich_df(df, soln_idx=o_idx, common_yr=common_yr,
                                       out_type='direct', pretty_model_names=PRETTY_MODEL_NAMES)
                    else:
                        df = enrich_df(df, soln_idx=o_idx, common_yr=common_yr,
                                       out_type='rel', pretty_model_names=PRETTY_MODEL_NAMES)

                    # Filter out regions with no generation nor load
                    if (obj == 'nodes') | (obj == 'regions'):
                        df = df[df[GEO_COLS[-1]].isin(filter_regs)]

            os.makedirs(os.path.join(self.DIR_04_CACHE, 'processed'), exist_ok=True)
            df.to_parquet(os.path.join(self.DIR_04_CACHE, 'processed', f'{timescale}-{obj}.parquet'))
            print(f'Saved {timescale}-{obj}.parquet in {Path(self.DIR_04_CACHE).parts[-1]} with {df.npartitions} '
                  f'partitions.')
