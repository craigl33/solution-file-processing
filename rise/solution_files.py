import os
import time

import pandas as pd
import julia
from h5plexos.query import PLEXOSSolution


from .utils import get_files, add_df_column
from .settings import FILTER_PROPS, FILTER_OUT_OBJS


class SolutionFileProcessor():

    def __init__(self, model_dir):
        # self.MODEL_DIR = 'C:/Users/TRIPPE_L/Code/dev_dirs/Indonesia/2021_IPSE'
        # self.SOL_IDX_PATH = 'C:/Users/TRIPPE_L/Code/dev_dirs/2022_06_21_generator_parameters_IDN.xlsx'

        self.DIR_04_SOLUTION_FILES = os.path.join(model_dir, '04_SolutionFiles')
        self.DIR_04_CACHE = os.path.join(model_dir, '04_SolutionFilesCache')
        self.DIR_05_DATA_PROCESSING = os.path.join(model_dir, '05_DataProcessing')

        self.soln_choice = '20230509_IDN_APSvRUPTL_scenario'

    # save_dir = 'Z:/Indonesia/2021_IPSE/ataProcessing/{}/'.format(soln_choice.value)

    def install_dependencies(self):
        julia.install()

    def convert_solution_files_to_h5(self):
        from julia.api import Julia
        from julia import Base

        jl = Julia(compiled_modules=False)
        jl.using("H5PLEXOS")

        soln_zip_files = [f for f in os.listdir(os.path.join(self.DIR_04_SOLUTION_FILES, self.soln_choice))
                          if f.endswith('.zip')]

        missing_soln_files = [f.split('.')[0] for f in soln_zip_files if f.replace('.zip', '.h5')
                              not in os.listdir(os.path.join(self.DIR_04_SOLUTION_FILES, self.soln_choice))]

        print(f'Found {len(missing_soln_files)} missing h5 files. Starting conversion...')
        for h5_file in missing_soln_files:
            jl.eval("cd(\"{}\")".format(os.path.join(self.DIR_04_SOLUTION_FILES, self.soln_choice).replace('\\', '/')))
            jl.eval("process(\"{}\", \"{}\")".format(f'{h5_file}.zip', f'{h5_file}.h5'))

    def convert_solution_files_to_properties(self, timescale, overwrite=False):
        if timescale not in ['interval', 'year']:
            raise ValueError('type must be either "interval" or "year"')

        _, soln_h5_files = get_files(os.path.join(self.DIR_04_SOLUTION_FILES, self.soln_choice),
                                     file_type='.h5', id_text='Solution', return_type=1)

        #### Read in solution file objects and properties using the first solution file
        ### This may be housed under a 'with'
        with PLEXOSSolution(os.path.join(self.DIR_04_SOLUTION_FILES, self.soln_choice, soln_h5_files[-1])) as db:
            plexos_objects = list(db.h5file[f'data/ST/{timescale}/'].keys())
            plexos_props = {}

            for o in plexos_objects:
                plexos_props[o] = list(db.h5file[f'data/ST/{timescale}/{o}/'].keys())

            ### As Zone (or sometimes region) usually has only one object, this can be used to get time_idx
            time_idx = db.region("Load").reset_index().timestamp.drop_duplicates()

        for file in soln_h5_files:

            ### Any spaces in the file will break it
            # soln_name = file.split('\\')[-1].split('Model ')[-1].split(' Solution.h5')[0]
            core_name = file.split('\\')[-1].split('Model ')[-1].split(' Solution.h5')[0]

            print(f'Processing {core_name} ({timescale})...')

            with PLEXOSSolution(os.path.join(self.DIR_04_SOLUTION_FILES, self.soln_choice, file)) as db:
                if timescale == 'interval':  # No need to filter annual
                    plexos_objs = [p for p in db.h5file['data/ST/interval/'].keys() if
                                   p not in FILTER_OUT_OBJS]
                plexos_props = {obj: list(db.h5file[f'data/ST/interval/{obj}/'].keys()) for obj in plexos_objs}

                for obj in plexos_objs:
                    if not overwrite and os.path.exists(os.path.join(self.DIR_04_CACHE,
                                                                     self.soln_choice,
                                                                     core_name,
                                                                     f'{timescale}-{obj}.parquet')):
                        print(f'{core_name}/{obj}.parquet" already exists. Pass overwrite=True to overwrite.')
                        continue

                    # Filter properties for time-series data
                    try:
                        obj_props = [prop for prop in plexos_props[obj] if prop in FILTER_PROPS[obj]]
                    except KeyError:  # If relevant object in FILTER_PROPS is not defined, all properties are used
                        obj_props = plexos_props[obj]

                    dfs = []
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
