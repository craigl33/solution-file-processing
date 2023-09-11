import os
from pathlib import Path

import pandas as pd
import dask.dataframe as dd
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

class SolutionFileFramework:
    def __init__(self, model_dir, soln_choice, soln_idx_path):
        self.DIR_04_SOLUTION_FILES = os.path.join(model_dir, '04_SolutionFiles', soln_choice)
        self.DIR_04_CACHE = os.path.join(model_dir, '04_SolutionFilesCache', soln_choice)
        self.DIR_05_DATA_PROCESSING = os.path.join(model_dir, '05_DataProcessing', soln_choice)
        self.DIR_05_1_SUMMARY_OUT = os.path.join(model_dir, '05_DataProcessing', soln_choice, 'summary_out')

        self.soln_idx = pd.read_excel(soln_idx_path, sheet_name='SolutionIndex', engine='openpyxl')


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
                        if obj in dfs_dict.keys():
                            dfs_dict[obj].append(db_data)
                        else:
                            dfs_dict[obj] = [db_data]

        for obj, dfs in dfs_dict.items():
            df = pd.concat(dfs)
            os.makedirs(os.path.join(self.DIR_04_CACHE, 'unprocessed'), exist_ok=True)
            df.to_parquet(os.path.join(self.DIR_04_CACHE, 'unprocessed', f'{timescale}-{obj}.parquet'))
            print(f'Saved {timescale}-{obj}.parquet in {Path(self.DIR_04_CACHE).parts[-1]}.')

    def process_properties(self):

        # Import necessary stuff
        # - common_yr
        model_yrs = dd.read_parquet(os.path.join(self.DIR_04_CACHE, 'unprocessed', 'interval-regions.parquet'))
        model_yrs = model_yrs.groupby(['model']).first().timestamp.dt.year.values
        model_yrs = model_yrs.compute()
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

        ### Filter out nodes that have zero load
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
            print()
            print(f'Number of partitions: {df.npartitions}')

            o_key = obj.replace('ies', 'ys')

            if timescale == 'interval':
                if '_' in o_key:
                    ### Memebrship properties can used the generator/battery part which is the second part
                    ### Also o_key is for singular form, hence drops s
                    o_key = o_key.split('_')[-1][:-1]
                else:
                    o_key = o_key[:-1]

                    ### Remove unnecessary columns, so object_type can be removed for the o_idx
                o_idx = self.soln_idx[self.soln_idx.Object_type.str.lower().str.replace(' ', '') == o_key].drop(
                    columns='Object_type')
                if len(o_idx) > 0:
                    if '_' not in obj:
                        df = enrich_df(df, soln_idx=o_idx, common_yr=common_yr,
                                       out_type='direct', pretty_model_names=PRETTY_MODEL_NAMES)
                    else:
                        df = enrich_df(df, soln_idx=o_idx, common_yr=common_yr, out_type='rel',
                                       pretty_model_names=PRETTY_MODEL_NAMES)

                    ### Filter out regions with no generation nor load
                    if (obj == 'nodes') | (obj == 'regions'):
                        df = df[df[GEO_COLS[-1]].isin(filter_regs)]

            elif timescale == 'year':

                if '_' in o_key:
                    ### Memebrship properties can used the generator/battery part which is the second part
                    ### Also o_key is for singular form, hence drops s
                    o_key = o_key.split('_')[-1][:-1]
                else:
                    o_key = o_key[:-1]

                ### No need to filter out solnb_idx for the annual data as the size wont be an issue
                o_idx = self.soln_idx[self.soln_idx.Object_type.str.lower().str.replace(' ', '') == o_key].drop(
                    columns='Object_type')
                if len(o_idx) > 0:
                    if '_' not in obj:
                        df = enrich_df(df, soln_idx=o_idx, common_yr=common_yr,
                                       out_type='direct', pretty_model_names=PRETTY_MODEL_NAMES)
                    else:
                        df = enrich_df(df, soln_idx=o_idx, common_yr=common_yr,
                                       out_type='rel', pretty_model_names=PRETTY_MODEL_NAMES)

                    ### Filter out regions with no generation nor load
                    if (obj == 'nodes') | (obj == 'regions'):
                        df = df[df[GEO_COLS[-1]].isin(filter_regs)]

            os.makedirs(os.path.join(self.DIR_04_CACHE, 'processed'), exist_ok=True)
            df.to_parquet(os.path.join(self.DIR_04_CACHE, 'processed', f'{timescale}-{obj}.parquet'))
            print(f'Saved {timescale}-{obj}.parquet in {Path(self.DIR_04_CACHE).parts[-1]}.')


class SolutionFileProperties(SolutionFileFramework):

    def __init__(self, model_dir, soln_choice, soln_idx_path):
        super().__init__(model_dir, soln_choice, soln_idx_path)

        self.model_yrs = self.reg_df.groupby(['model']).first().timestamp.dt.year.values

    _gen_yr_df = None
    _node_yr_df = None
    _gen_df = None
    _reg_df = None
    _res_gen_df = None

    @property
    def gen_yr_df(self):
        if self._gen_yr_df is None:
            _df = dd.read_parquet(os.path.join(self.DIR_04_CACHE, 'processed', 'year-generators.parquet'))

            try:
                bat_yr_df = dd.read_parquet(os.path.join(self.DIR_04_CACHE, 'processed', 'year-batteries.parquet'))
                _df = dd.concat([_df, bat_yr_df], axis=0)
            except KeyError:
                print("No batteries for current scenarios")

            #### For WEO_tech simpl. probably should add something to soln_idx
            def clean_weo_tech(x):
                if isinstance(x, float):
                    return x
                x = x. \
                    replace('NEW ', ''). \
                    replace(' 1', ''). \
                    replace(' 2', ''). \
                    replace(' 3', ''). \
                    replace(' 4', ''). \
                    replace(' 5', '')
                return x

            _df['WEO_Tech_simpl'] = _df['WEO tech'].map(clean_weo_tech)

            # todo: Stuff not sure why it exists
            ### Cofiring change here!
            ### Due to memory allocation errors, additional columns from soln_idx are used and then discarded
            cofiring_scens = [c for c in PRETTY_MODEL_NAMES.values() if
                              ('2030' in c) | (c == '2025 Base') | (c == '2025 Enforced Cofiring')]

            ### Add category
            # _df.loc[(_df.Cofiring == 'Y') & (_df.model.isin(cofiring_scens)), 'Category'] = \
            #     _df.loc[(_df.Cofiring == 'Y') & (_df.model.isin(cofiring_scens)), 'CofiringCategory']
            def update_category(df):
                condition = (df['Cofiring'] == 'Y') & (df['model'].isin(cofiring_scens))
                df.loc[condition, 'Category'] = df.loc[condition, 'CofiringCategory']
                return df

            # Use map_partitions to apply the function to each partition
            _df = _df.map_partitions(update_category)

            ### And capacity category
            # _df.loc[(_df.Cofiring == 'Y') & (_df.model.isin(cofiring_scens)), 'CapacityCategory'] = \
            #     _df.loc[(_df.Cofiring == 'Y') & (_df.model.isin(cofiring_scens)), 'CofiringCategory']
            def update_capacity_category(df):
                condition = (df['Cofiring'] == 'Y') & (df['model'].isin(cofiring_scens))
                df.loc[condition, 'CapacityCategory'] = df.loc[condition, 'CofiringCategory']
                return df

            # Use map_partitions to apply the function to each partition
            _df = _df.map_partitions(update_capacity_category)

            ### Drop addl columns for interval df
            _df = _df.drop(columns=['Cofiring', 'CofiringCategory'])

            self._gen_yr_df = _df
        return self._gen_yr_df

    @property
    def node_yr_df(self):
        if self._node_yr_df is None:
            _df = dd.read_parquet(os.path.join(self.DIR_04_CACHE, 'processed', f'year-nodes.parquet'))
            self._node_yr_df = _df
        return self._node_yr_df

    @property
    def gen_df(self):
        if self._gen_df is None:
            _df = dd.read_parquet(os.path.join(self.DIR_04_CACHE, 'processed', 'interval-generators.parquet'))

            try:
                bat_df = dd.read_parquet(os.path.join(self.DIR_04_CACHE, 'processed', 'interval-batteries.parquet'))
                _df = dd.concat([_df, bat_df], axis=0)
            except KeyError:
                print("No batteries objects")

            # Add temp columns for category and capacity category
            cofiring_scens = [c for c in PRETTY_MODEL_NAMES.values() if
                              ('2030' in c) | (c == '2025 Base') | (c == '2025 Enforced Cofiring')]
            _df = dd.merge(_df, self.gen_addl_idx[['name', 'Cofiring', 'CofiringCategory']],
                           on='name',
                           how='left')
            # Add category
            _condition = (_df['Cofiring'] == 'Y') & (_df.model.isin(cofiring_scens))
            _df['Category'] = _df.apply(lambda x: x['CofiringCategory'] if _condition else x['Category'],
                                        axis=1,
                                        meta=('Category', 'str'))

            # And capacity category
            _df.loc[(_df.Cofiring == 'Y') & (~_df.model.isin(cofiring_scens)), 'CapacityCategory'] = \
                _df.loc[(_df.Cofiring == 'Y') & (~_df.model.isin(cofiring_scens)), 'CofiringCategory']

            # Drop temp columns for interval df
            _df = _df.drop(columns=['Cofiring', 'CofiringCategory'])

            self._gen_df = _df
        return self._gen_df

    @property
    def reg_df(self):
        if self._reg_df is None:
            _df = dd.read_parquet(os.path.join(self.DIR_04_CACHE, 'processed', 'interval-regions.parquet'))
            self._reg_df = _df
        return self._reg_df

    @property
    def res_gen_df(self):
        if self._res_gen_df is None:
            _df = dd.read_parquet(os.path.join(self.DIR_04_CACHE, 'processed', 'interval-reserves_generators.parquet'))

            try:
                bat_df = dd.read_parquet(os.path.join(self.DIR_04_CACHE, 'processed', 'interval-batteries.parquet'))
                _df = dd.concat([_df, bat_df], axis=0)
            except KeyError:
                print("No batteries objects")

            self._res_gen_df = _df
        return self._res_gen_df
    def create_output_1(self, timescale):
        ### Output 1
        ### To allow scaling, the group index is maintained (i.e. as_index=True) before resetting the index

        if timescale == 'year':
            load_by_reg = self.node_yr_df[self.node_yr_df.property == 'Load'].groupby(
                ['model', 'timestamp'] + GEO_COLS).sum().value
            customer_load_by_reg = self.node_yr_df[
                (self.node_yr_df.property == 'Customer Load') | (
                        self.node_yr_df.property == 'Unserved Energy')].groupby(
                ['model', 'timestamp'] + GEO_COLS).sum().value

            os.makedirs(self.DIR_05_1_SUMMARY_OUT, exist_ok=True)
            add_df_column(load_by_reg, 'units', 'GWh').to_csv(
                os.path.join(self.DIR_05_1_SUMMARY_OUT, '01a_load_by_reg.csv'),
                index=False)
            add_df_column(customer_load_by_reg, 'units', 'GWh').to_csv(
                os.path.join(self.DIR_05_1_SUMMARY_OUT, '01b_customer_load_by_reg.csv'), index=False)

        self._dev_test_output('01a_load_by_reg.csv')
        self._dev_test_output('01b_customer_load_by_reg.csv')

    def create_output_2(self, timescale):
        ### Output 2: USE

        use_by_reg = self.node_yr_df[self.node_yr_df.property == 'Unserved Energy'].groupby(
            ['model'] + GEO_COLS).sum().value
        use_reg_daily_ts = self.node_yr_df[self.node_yr_df.property == 'Unserved Energy'].groupby(
            ['model'] + GEO_COLS + [pd.Grouper(key='timestamp', freq='D')]).sum().value

        add_df_column(use_by_reg, 'units', 'GWh').to_csv(os.path.join(self.DIR_05_1_SUMMARY_OUT, '02a_use_reg.csv'),
                                                         index=False)
        add_df_column(use_reg_daily_ts, 'units', 'GWh').to_csv(
            os.path.join(self.DIR_05_1_SUMMARY_OUT, '02b_use_reg_daily_ts.csv'),
            index=False)

        self._dev_test_output('02a_use_reg.csv')
        self._dev_test_output('02b_use_reg_daily_ts.csv')

    def create_output_3(self, timescale):
        # ### Ouput 3a:
        #
        # ## Standard
        gen_by_tech_reg = self.gen_yr_df[self.gen_yr_df.property == 'Generation']
        gen_by_tech_reg_orig = self.gen_yr_df[
            self.gen_yr_df.property == 'Generation']  ### For not seperating cofiring. good for CF comparison
        gen_techs = self.gen_yr_df.Category.drop_duplicates().values
        #
        # ### This will need to be updated for NZE
        # if 'Cofiring' in gen_techs:
        #     bio_ratio = 0.1
        #     gen_by_cofiring_bio = gen_by_tech_reg[gen_by_tech_reg.Category == 'Cofiring']
        #     gen_by_cofiring_coal = gen_by_tech_reg[gen_by_tech_reg.Category == 'Cofiring']
        #     gen_by_tech_reg = gen_by_tech_reg[gen_by_tech_reg.Category != 'Cofiring']
        #
        #     gen_by_cofiring_bio.loc[:, 'value'] = gen_by_cofiring_bio.value * bio_ratio
        #     gen_by_cofiring_bio = gen_by_cofiring_bio.replace('Cofiring', 'Bioenergy')
        #
        #     gen_by_cofiring_coal.loc[:, 'value'] = gen_by_cofiring_coal.value * (1 - bio_ratio)
        #     gen_by_cofiring_coal = gen_by_cofiring_coal.replace('Cofiring', 'Coal')
        #
        #     gen_by_tech_reg = pd.concat([gen_by_tech_reg, gen_by_cofiring_bio, gen_by_cofiring_coal], axis=0)
        #
        # gen_by_plant = gen_by_tech_reg.groupby(['model', 'name']).sum().value.unstack(level='model').fillna(0)
        gen_by_costTech_reg = gen_by_tech_reg.groupby(['model'] + GEO_COLS + ['CostCategory']).sum().value.unstack(
            level=GEO_COLS).fillna(0)
        # gen_by_tech_reg = gen_by_tech_reg.groupby(['model'] + GEO_COLS + ['Category']).sum().value.unstack(
        #     level=GEO_COLS).fillna(0)
        # gen_by_tech_reg_orig = gen_by_tech_reg_orig.groupby(['model'] + GEO_COLS + ['Category']).sum().value.unstack(
        #     level=GEO_COLS).fillna(0)
        # gen_by_weoTech_reg = self.gen_yr_df[self.gen_yr_df.property == 'Generation'].groupby(
        #     ['model'] + GEO_COLS + ['WEO_Tech_simpl']).sum().value.unstack(level=GEO_COLS).fillna(0)
        #
        # if validation:
        #     ### This needs to be fixed to the region/subregion change
        #     idn_actuals_by_tech_reg = self.idn_actuals_2019.groupby(
        #         ['model'] + GEO_COLS + ['Category']).sum().Generation.unstack(level=GEO_COLS).fillna(0)
        #     gen_by_tech_reg = pd.concat([gen_by_tech_reg, idn_actuals_by_tech_reg], axis=0)
        #
        # add_df_column(gen_by_tech_reg.stack(GEO_COLS), 'units', 'GWh').to_csv(
        #     os.path.join(self.DIR_05_1_SUMMARY_OUT, '03a_gen_by_tech_reg.csv'), index=False)
        # add_df_column(gen_by_tech_reg_orig.stack(GEO_COLS), 'units', 'GWh').to_csv(
        #     os.path.join(self.DIR_05_1_SUMMARY_OUT, '03a_gen_by_tech_reg_orig.csv'), index=False)
        # add_df_column(gen_by_tech_reg.stack(GEO_COLS), 'units', 'GWh').to_csv(
        #     os.path.join(self.DIR_05_1_SUMMARY_OUT, '03b_gen_by_tech_reg_w_Validation.csv'), index=False)
        # add_df_column(gen_by_costTech_reg.stack(GEO_COLS), 'units', 'GWh').to_csv(
        #     os.path.join(self.DIR_05_1_SUMMARY_OUT, '03c_gen_by_costTech_reg.csv'), index=False)
        # add_df_column(gen_by_weoTech_reg.stack(GEO_COLS), 'units', 'GWh').to_csv(
        #     os.path.join(self.DIR_05_1_SUMMARY_OUT, '03d_gen_by_weoTech_reg.csv'), index=False)
        # add_df_column(gen_by_plant, 'units', 'GWh').to_csv(
        #     os.path.join(self.DIR_05_1_SUMMARY_OUT, '03e_gen_by_plants.csv'),
        #     index=False)
        # # add_df_column(gen_by_tech_subreg.stack(), 'units', 'GWh').to_csv(os.path.join(save_dir_sum, '04b_gen_by_tech_subreg.csv'), index=False)
        # # add_df_column(gen_by_tech_isl.stack(), 'units', 'GWh').to_csv(os.path.join(save_dir_sum, '04c_gen_by_tech_isl.csv'), index=False)

        ### Ouput 3.0: Output by plant!

        ## Standard
        gen_by_plant = self.gen_yr_df[self.gen_yr_df.property == 'Generation']
        gen_by_plant_orig = self.gen_yr_df[
            self.gen_yr_df.property == 'Generation']  ### For not seperating cofiring. good for CF comparison
        gen_techs = self.gen_yr_df.Category.drop_duplicates().values

        ### This will need to be updated for NZE
        if 'Cofiring' in gen_techs:
            bio_ratio = 0.1
            gen_by_cofiring_bio = gen_by_plant[gen_by_plant.Category == 'Cofiring']
            gen_by_cofiring_coal = gen_by_plant[gen_by_plant.Category == 'Cofiring']
            gen_by_plant = gen_by_plant[gen_by_plant.Category != 'Cofiring']

            gen_by_cofiring_bio.loc[:, 'value'] = gen_by_cofiring_bio.value * bio_ratio
            gen_by_cofiring_bio = gen_by_cofiring_bio.replace('Cofiring', 'Bioenergy')

            gen_by_cofiring_coal.loc[:, 'value'] = gen_by_cofiring_coal.value * (1 - bio_ratio)
            gen_by_cofiring_coal = gen_by_cofiring_coal.replace('Cofiring', 'Coal')

            gen_by_plant = pd.concat([gen_by_plant, gen_by_cofiring_bio, gen_by_cofiring_coal], axis=0)

        gen_by_tech_reg = gen_by_tech_reg.groupby(['model', 'Category']).sum().fillna(0)
        gen_by_tech_reg_orig = gen_by_tech_reg_orig.groupby(['model', 'Category']).sum().fillna(0)
        gen_by_weoTech_reg = self.gen_yr_df[self.gen_yr_df.property == 'Generation'].groupby(
            ['model'] + GEO_COLS + ['WEO_Tech_simpl']).sum().value.unstack(level=GEO_COLS).fillna(0)

        if validation:
            ### This needs to be fixed to the region/subregion change
            idn_actuals_by_tech_reg = idn_actuals_2019.groupby(
                ['model'] + GEO_COLS + ['Category']).sum().Generation.unstack(level=GEO_COLS).fillna(0)
            gen_by_tech_reg = pd.concat([gen_by_tech_reg, idn_actuals_by_tech_reg], axis=0)

        # try:
        #     add_df_column(gen_by_tech_reg.stack(GEO_COLS), 'units', 'GWh').to_csv(
        #         os.path.join(self.DIR_05_1_SUMMARY_OUT, '03a_gen_by_tech_reg.csv'), index=False)
        #     add_df_column(gen_by_tech_reg_orig.stack(GEO_COLS), 'units', 'GWh').to_csv(
        #         os.path.join(self.DIR_05_1_SUMMARY_OUT, '03a_gen_by_tech_reg_orig.csv'), index=False)
        #     add_df_column(gen_by_tech_reg.stack(GEO_COLS), 'units', 'GWh').to_csv(
        #         os.path.join(self.DIR_05_1_SUMMARY_OUT, '03b_gen_by_tech_reg_w_Validation.csv'), index=False)
        #     add_df_column(gen_by_costTech_reg.stack(GEO_COLS), 'units', 'GWh').to_csv(
        #         os.path.join(self.DIR_05_1_SUMMARY_OUT, '03c_gen_by_costTech_reg.csv'), index=False)
        #     add_df_column(gen_by_weoTech_reg.stack(GEO_COLS), 'units', 'GWh').to_csv(
        #         os.path.join(self.DIR_05_1_SUMMARY_OUT, '03d_gen_by_weoTech_reg.csv'), index=False)
        # #     add_df_column(gen_by_tech_subreg.stack(), 'units', 'GWh').to_csv(os.path.join(save_dir_sum, '04b_gen_by_tech_subreg.csv'), index=False)
        # #     add_df_column(gen_by_tech_isl.stack(), 'units', 'GWh').to_csv(os.path.join(save_dir_sum, '04c_gen_by_tech_isl.csv'), index=False)
        # except PermissionError:
        #     print("Permission error: file not written")

        # self._dev_test_output('03a_gen_by_tech_reg.csv')
        # self._dev_test_output('03a_gen_by_tech_reg_orig.csv')
        # self._dev_test_output('03b_gen_by_tech_reg_w_Validation.csv')
        # self._dev_test_output('03c_gen_by_costTech_reg.csv')
        # self._dev_test_output('03d_gen_by_weoTech_reg.csv')

        # todo Stuff until here does not work

        ### Output 3b: RE/VRE Shares:

        gen_techs = list(self.soln_idx[self.soln_idx.Object_type == 'Generator'].Category.drop_duplicates())
        vre_techs = ['Solar', 'Wind']
        re_techs = ['Solar', 'Wind', 'Bioenergy', 'Geothermal', 'Other', 'Marine', 'Hydro']

        re_by_isl = gen_by_tech_reg.reset_index()
        re_by_isl.loc[:, 'RE'] = re_by_isl.Category.apply(lambda x: 'RE' if x in re_techs else 'Non-RE')

        vre_by_isl = gen_by_tech_reg.reset_index()
        vre_by_isl.loc[:, 'VRE'] = vre_by_isl.Category.apply(lambda x: 'VRE' if x in vre_techs else 'Non-VRE')

        re_by_isl = re_by_isl.groupby(['model', 'RE']).sum().groupby(level='Island', axis=1).sum()
        re_by_isl_JVBSUMonly = re_by_isl[incl_regs]
        re_by_isl.loc[:, 'IDN'] = re_by_isl.sum(axis=1)
        re_by_isl = re_by_isl.loc[ix[:, 'RE'],].droplevel('RE') / re_by_isl.groupby('model').sum()
        re_by_isl_JVBSUMonly.loc[:, 'IDN'] = re_by_isl_JVBSUMonly.sum(axis=1)
        re_by_isl_JVBSUMonly = re_by_isl_JVBSUMonly.loc[ix[:, 'RE'],].droplevel('RE') / re_by_isl_JVBSUMonly.groupby(
            'model').sum()

        vre_by_isl = vre_by_isl.groupby(['model', 'VRE']).sum().groupby(level='Island', axis=1).sum()
        vre_by_isl_JVBSUMonly = vre_by_isl[incl_regs]
        vre_by_isl.loc[:, 'IDN'] = vre_by_isl.sum(axis=1)
        vre_by_isl = vre_by_isl.loc[ix[:, 'VRE'],].droplevel('VRE') / vre_by_isl.groupby('model').sum()
        vre_by_isl_JVBSUMonly.loc[:, 'IDN'] = vre_by_isl_JVBSUMonly.sum(axis=1)
        vre_by_isl_JVBSUMonly = vre_by_isl_JVBSUMonly.loc[ix[:, 'VRE'],].droplevel(
            'VRE') / vre_by_isl_JVBSUMonly.groupby('model').sum()

        add_df_column(re_by_isl, 'units', '%').to_csv(os.path.join(self.DIR_05_1_SUMMARY_OUT, '03b_re_by_isl.csv'),
                                                      index=False)
        add_df_column(vre_by_isl, 'units', '%').to_csv(os.path.join(self.DIR_05_1_SUMMARY_OUT, '03c_vre_by_isl.csv'),
                                                       index=False)
        # add_df_column(gen_by_tech_subreg.stack(), 'units', 'GWh').to_csv(os.path.join(save_dir_sum, '04b_gen_by_tech_subreg.csv'), index=False)
        # add_df_column(gen_by_tech_isl.stack(), 'units', 'GWh').to_csv(os.path.join(save_dir_sum, '04c_gen_by_tech_isl.csv'), index=False)

        self._dev_test_output('03b_re_by_isl.csv')
        self._dev_test_output('03c_vre_by_isl.csv')

    def create_output_5(self, timescale):
        ### Ouput 5

        unit_starts_by_tech = self.gen_yr_df[self.gen_yr_df.property == 'Units Started'].groupby(
            ['model', 'Category']).sum().value.unstack(level='Category')

        add_df_column(unit_starts_by_tech.stack(), 'units', 'starts').to_csv(
            os.path.join(self.DIR_05_1_SUMMARY_OUT, '05_unit_starts_by_tech.csv'), index=False)
        self._dev_test_output('05_unit_starts_by_tech.csv')

    def _dev_test_output(self, file_name):
        df1 = pd.read_csv(
            f'Y:/RED/Modelling/Indonesia/2021_IPSE/05_DataProcessing/20230509_IDN_APSvRUPTL_scenario/summary_out/{file_name}')
        df2 = pd.read_csv(os.path.join(self.DIR_05_1_SUMMARY_OUT, file_name))

        if not df1.equals(df2):
            # Print or inspect the differing values
            for index, row in df1.iterrows():
                for column in df1:
                    if row[column] != df2.iloc[index][column]:
                        print(f'Row {index}: {column} - {row[column]} != {df2.iloc[index][column]}')

            raise Exception(f'Output {file_name} is not the same as the one in the repo. Check it out.')



    # TODO USE DASK