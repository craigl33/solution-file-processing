"""
# Todo update after splitting into multiple files
This module, `summary.py`, is responsible for all output files from the processed data. Those functions can be
accessed from the package level, e.g. `solution_file_processing.outputs.create_year_output_1(c)`. They all need a
`config` object as input, which has to be created first, e.g.
`c = solution_file_processing.config.SolutionFilesConfig('path/to/config.yaml')`.
All underlying functions and data structures are accessed via the passed configuration object. See the class 
documentation in solution_files.py for more information.

Based on the settings in cfg['run']['catch_errors'], the functions will either raise an error or log a warning if
something goes wrong.

All functions in this file are completely independent from each other. They can be run in any order or skipped if
not necessary for the analysis. Adding any new functions in any form can be done without impacting any other
functionality. But keeping a similar structure is recommended. Also make sure to add the function to the list of
functions below and add a Docstring to the function itself.

The functions are split into two groups: year outputs and interval outputs. The year outputs are based on the 
yearly aggregated data, while the interval outputs are based on the interval (hourly) data, which makes their
underlying data structures much larger and their processing time much longer.

More details on each function in the its Docstring, which can be accessed via
`help(solution_file_processing.outputs.func_name)`.

Year outputs:
# todo needs list with short description and better docstrings for all functions
"""

import os

import pandas as pd
import dask.dataframe as dd
import numpy as np

from .utils.utils import catch_errors
from .constants import VRE_TECHS
from .utils.logger import Logger

log = Logger('solution_file_processing')

print = log.info


@catch_errors
def create_output_1(c):
    """"
    ### Output 1
    ### To allow scaling, the group index is maintained (i.e. as_index=True) before resetting the index
    Creates following output files:
    - 01a_load_by_reg.csv
    - 01b_customer_load_by_reg.csv
    """
    print('Creating output 1...')

    load_by_reg = c.o.node_yr_df[c.o.node_yr_df.property == 'Load'] \
        .groupby(['model', 'timestamp'] + c.GEO_COLS) \
        .agg({'value': 'sum'})
    customer_load_by_reg = c.o.node_yr_df[(c.o.node_yr_df.property == 'Customer Load') |
                                          (c.o.node_yr_df.property == 'Unserved Energy')] \
        .groupby(['model', 'timestamp'] + c.GEO_COLS) \
        .agg({'value': 'sum'})

    os.makedirs(c.DIR_05_1_SUMMARY_OUT, exist_ok=True)

    (load_by_reg.assign(units='GWh')
     .reset_index()
     .sort_values(by=['model'])
     .to_csv(os.path.join(c.DIR_05_1_SUMMARY_OUT, '01a_load_by_reg.csv'), index=False))
    (customer_load_by_reg.assign(units='GWh')
     .reset_index()
     .sort_values(by=['model'])
     .to_csv(os.path.join(c.DIR_05_1_SUMMARY_OUT, '01b_customer_load_by_reg.csv'), index=False))

    print(f'Created file 01a_load_by_reg.csv.')
    print(f'Created file 01b_customer_load_by_reg.csv.')


@catch_errors
def create_output_2(c):
    """
    ### Output 2: USE
    Creates following output files:
    - 02a_use_reg.csv
    - 02b_use_reg_daily_ts.csv
    """
    print('Creating output 2...')

    use_by_reg = c.o.node_yr_df[c.o.node_yr_df.property == 'Unserved Energy'] \
        .groupby(['model'] + c.GEO_COLS) \
        .agg({'value': 'sum'})
    use_reg_daily_ts = c.o.node_yr_df[c.o.node_yr_df.property == 'Unserved Energy'] \
        .groupby(['model'] + c.GEO_COLS + [pd.Grouper(key='timestamp', freq='D')]) \
        .agg({'value': 'sum'})

    use_by_reg.assign(units='GWh').reset_index() \
        .to_csv(os.path.join(c.DIR_05_1_SUMMARY_OUT, '02a_use_reg.csv'), index=False)
    use_reg_daily_ts.assign(units='GWh').reset_index() \
        .to_csv(os.path.join(c.DIR_05_1_SUMMARY_OUT, '02b_use_reg_daily_ts.csv'), index=False)

    print('Created file 02a_use_reg.csv.')
    print('Created file 02b_use_reg_daily_ts.csv.')


@catch_errors
def create_output_3(c):
    """
    Creates following output files:
    - 03a_gen_by_tech_reg.csv
    - 03a_gen_by_tech_reg_orig.csv
    - 03c_gen_by_costTech_reg.csv
    - 03d_gen_by_weoTech_reg.csv
    - 03e_gen_by_plants.csv
    """
    print("Creating output 3...")
    # Standard
    gen_by_tech_reg = c.o.gen_yr_df[c.o.gen_yr_df.property == 'Generation']
    gen_by_tech_reg_orig = gen_by_tech_reg.copy()  # For not seperating cofiring. good for CF comparison
    gen_techs = c.o.gen_yr_df.Category.drop_duplicates().values

    # This will need to be updated for NZE
    if 'Cofiring' in gen_techs:
        bio_ratio = 0.1
        gen_by_cofiring_bio = gen_by_tech_reg[gen_by_tech_reg.Category == 'Cofiring']
        gen_by_cofiring_coal = gen_by_tech_reg[gen_by_tech_reg.Category == 'Cofiring']
        gen_by_tech_reg = gen_by_tech_reg[gen_by_tech_reg.Category != 'Cofiring']

        gen_by_cofiring_bio.loc[:, 'value'] = gen_by_cofiring_bio.value * bio_ratio
        gen_by_cofiring_bio = gen_by_cofiring_bio.replace('Cofiring', 'Bioenergy')

        gen_by_cofiring_coal.loc[:, 'value'] = gen_by_cofiring_coal.value * (1 - bio_ratio)
        gen_by_cofiring_coal = gen_by_cofiring_coal.replace('Cofiring', 'Coal')

        gen_by_tech_reg = pd.concat([gen_by_tech_reg, gen_by_cofiring_bio, gen_by_cofiring_coal], axis=0)

    gen_by_plant = (gen_by_tech_reg
                    .groupby(['model', 'name'])
                    .agg({'value': 'sum'})
                    .unstack(level='model')
                    .fillna(0))
    gen_by_costTech_reg = (gen_by_tech_reg
                           .groupby(['model'] + c.GEO_COLS + ['CostCategory'])
                           .agg({'value': 'sum'})
                           .unstack(level=c.GEO_COLS)
                           .fillna(0))
    gen_by_tech_reg = (gen_by_tech_reg
                       .groupby(['model'] + c.GEO_COLS + ['Category'])
                       .agg({'value': 'sum'})
                       .unstack(level=c.GEO_COLS)
                       .fillna(0))
    gen_by_tech_reg_orig = (gen_by_tech_reg_orig
                            .groupby(['model'] + c.GEO_COLS + ['Category'])
                            .agg({'value': 'sum'})
                            .unstack(level=c.GEO_COLS)
                            .fillna(0))
    gen_by_weoTech_reg = (c.o.gen_yr_df[c.o.gen_yr_df.property == 'Generation']
                          .groupby(['model'] + c.GEO_COLS + ['WEO_Tech_simpl'])
                          .agg({'value': 'sum'})
                          .unstack(level=c.GEO_COLS)
                          .fillna(0))

    (gen_by_tech_reg
     .stack(c.GEO_COLS)
     .assign(units='GWh')
     .to_csv(os.path.join(c.DIR_05_1_SUMMARY_OUT, '03a_gen_by_tech_reg.csv'), index=True))
    (gen_by_tech_reg_orig
     .stack(c.GEO_COLS)
     .assign(units='GWh')
     .to_csv(os.path.join(c.DIR_05_1_SUMMARY_OUT, '03a_gen_by_tech_reg_orig.csv'), index=True))
    (gen_by_costTech_reg
     .stack(c.GEO_COLS)
     .assign(units='GWh')
     .to_csv(os.path.join(c.DIR_05_1_SUMMARY_OUT, '03c_gen_by_costTech_reg.csv'), index=True))
    (gen_by_weoTech_reg
     .stack(c.GEO_COLS)
     .assign(units='GWh')
     .to_csv(os.path.join(c.DIR_05_1_SUMMARY_OUT, '03d_gen_by_weoTech_reg.csv'), index=True))
    (gen_by_plant
     .droplevel(level=0, axis=1)
     .assign(units='GWh')
     .to_csv(os.path.join(c.DIR_05_1_SUMMARY_OUT, '03e_gen_by_plants.csv'), index=True))

    print('Created file 03a_gen_by_tech_reg.csv.')
    print('Created file 03a_gen_by_tech_reg_orig.csv.')
    print('Created file 03c_gen_by_costTech_reg.csv.')
    print('Created file 03d_gen_by_weoTech_reg.csv.')
    print('Created file 03e_gen_by_plants.csv.')


@catch_errors
def create_output_4(c):
    """

    """
    pass


@catch_errors
def create_output_5(c):
    """
    Creates following output files:
    - 05_unit_starts_by_tech.csv
    """
    print("Creating output 5...")

    unit_starts_by_tech = c.o.gen_yr_df[c.o.gen_yr_df.property == 'Units Started'] \
        .groupby(['model', 'Category']) \
        .agg({'value': 'sum'}) \
        .unstack(level='Category')

    unit_starts_by_tech.stack().assign(units='starts').reset_index().to_csv(
        os.path.join(c.DIR_05_1_SUMMARY_OUT, '05_unit_starts_by_tech.csv'), index=False)

    print('Created file 05_unit_starts_by_tech.csv.')


@catch_errors
def create_output_6(c):
    """
    Creates following output files:
    - 06a_gen_max_by_tech_reg.csv
    """
    print("Creating output 6...")

    # Standard
    gen_max_by_tech_reg = c.o.gen_df[c.o.gen_df.property == 'Generation'] \
        .groupby(['model'] + c.GEO_COLS + ['Category']) \
        .agg({'value': 'max'}) \
        .compute() \
        .unstack(level=c.GEO_COLS) \
        .fillna(0)

    gen_max_by_tech_reg.stack(c.GEO_COLS).assign(units='MW').reset_index().to_csv(
        os.path.join(c.DIR_05_1_SUMMARY_OUT, '06a_gen_max_by_tech_reg.csv'), index=False)

    print('Created file 06a_gen_max_by_tech_reg.csv.')


@catch_errors
def create_output_7(c):
    """
    Creates following output files:
    - 07_tx_losses.csv
    """
    print("Creating output 7...")

    tx_losses = c.o.line_yr_df[c.o.line_yr_df.property == 'Loss'] \
        .groupby(['model', 'timestamp', 'name']) \
        .agg({'value': 'sum'})

    (tx_losses
     .assign(units='GWh')
     .reset_index()
     .to_csv(os.path.join(c.DIR_05_1_SUMMARY_OUT, '07_tx_losses.csv'), index=False))

    print('Created file 07_tx_losses.csv.')


@catch_errors
def create_output_8(c):
    """
    Creates following output files:
    - 08a_vre_cap.csv
    - 08b_vre_daily_abs.csv
    - 08c_vre_daily_norm.csv
    """
    print("Creating output 8...")
    time_idx = c.o.reg_df.reset_index().timestamp.drop_duplicates().compute()
    interval_periods = len(time_idx)
    nr_days = len(time_idx.dt.date.drop_duplicates())
    daily_periods = interval_periods / nr_days
    hour_corr = 24 / daily_periods

    # Output 8 & 9

    # Fill in data for regions which have no VRE (i.e. zero arrays!) to allow similar arrays for load_ts and
    # vre_add_df_columns
    # To add something for subregions

    # There is an error in PLEXOS with Available Capacity versus Generation (Gen exceeds Av Capacity)
    load_by_reg = c.o.node_yr_df[c.o.node_yr_df.property == 'Load'] \
        .groupby(['model', 'timestamp'] + c.GEO_COLS) \
        .agg({'value': 'sum'})

    vre_cap = c.o.gen_yr_df[(c.o.gen_yr_df.property == 'Installed Capacity') &
                            (c.o.gen_yr_df.Category.isin(VRE_TECHS))] \
        .groupby(['model', 'Category'] + c.GEO_COLS) \
        .agg({'value': 'max'}) \
        .unstack('Category') \
        .fillna(0) \
        .stack('Category') \
        .unstack(level=c.GEO_COLS) \
        .fillna(0)

    vre_av_abs = c.o.gen_df[(c.o.gen_df.property == 'Available Capacity') &
                            (c.o.gen_df.Category.isin(VRE_TECHS))] \
        .assign(timestamp=dd.to_datetime(c.o.gen_df['timestamp']).dt.floor('D')) \
        .groupby(['model', 'Category'] + c.GEO_COLS + ['timestamp']) \
        .agg({'value': 'sum'}) \
        .compute() \
        .unstack('Category') \
        .fillna(0) \
        .stack('Category') \
        .unstack(level=c.GEO_COLS) \
        .fillna(0) \
        .apply(lambda x: x * hour_corr)

    # ### Add zero values to regions without VRE
    geo_col_filler = pd.Series(data=np.ones(len(load_by_reg.unstack(c.GEO_COLS).columns)),
                               index=load_by_reg.unstack(c.GEO_COLS).columns)
    vre_cap = (vre_cap * geo_col_filler).fillna(0)
    vre_av_abs = (vre_av_abs * geo_col_filler).fillna(0)

    # 24 periods per day for the daily data
    vre_av_norm = (vre_av_abs / vre_cap / daily_periods).fillna(0)

    vre_cap.stack(c.GEO_COLS).assign(units='MW').reset_index().to_csv(
        os.path.join(c.DIR_05_1_SUMMARY_OUT, '08a_vre_cap.csv'),
        index=False)
    vre_av_abs.stack(c.GEO_COLS).assign(units='GWh').reset_index().to_csv(
        os.path.join(c.DIR_05_1_SUMMARY_OUT, '08b_vre_daily_abs.csv'), index=False)
    vre_av_norm.stack(c.GEO_COLS).assign(units='-').reset_index().to_csv(
        os.path.join(c.DIR_05_1_SUMMARY_OUT, '08c_vre_daily_norm.csv'), index=False)

    print('Created file 08a_vre_cap.csv.')
    print('Created file 08b_vre_daily_abs.csv.')
    print('Created file 08c_vre_daily_norm.csv.')


@catch_errors
def create_output_9(c):
    """
    Creates following output files:
    - 09a_vre_daily_curtailed.csv
    - 09b_curtailment_rate.csv
    - 09c_all_RE_daily_curtailed.csv
    - 09d_all_RE_curtailment_rate.csv
    """
    print("Creating output 9...")
    # todo Not sure if that always works
    # time_idx = db.region("Load").reset_index().timestamp.drop_duplicates()
    time_idx = c.o.reg_df.reset_index().timestamp.drop_duplicates().compute()

    interval_periods = len(time_idx)
    nr_days = len(time_idx.dt.date.drop_duplicates())
    daily_periods = interval_periods / nr_days
    hour_corr = 24 / daily_periods

    # Output 8 & 9

    # Fill in data for regions which have no VRE (i.e. zero arrays!) to allow similar arrays for load_ts and
    # vre_add_df_columns
    # To add something for subregions

    # There is an error in PLEXOS with Available Capacity versus Generation (Gen exceeds Av Capacity)
    load_by_reg = c.o.node_yr_df[c.o.node_yr_df.property == 'Load'] \
        .groupby(['model', 'timestamp'] + c.GEO_COLS) \
        .agg({'value': 'sum'})

    vre_av_abs = c.o.gen_df[(c.o.gen_df.property == 'Available Capacity') &
                            (c.o.gen_df.Category.isin(VRE_TECHS))] \
        .assign(timestamp=dd.to_datetime(c.o.gen_df['timestamp']).dt.floor('D')) \
        .groupby(['model', 'Category'] + c.GEO_COLS + ['timestamp']) \
        .agg({'value': 'sum'}) \
        .compute() \
        .unstack('Category') \
        .fillna(0) \
        .stack('Category') \
        .unstack(level=c.GEO_COLS) \
        .fillna(0) \
        .apply(lambda x: x * hour_corr)

    vre_gen_abs = c.o.gen_df[(c.o.gen_df.property == 'Generation') &
                             (c.o.gen_df.Category.isin(VRE_TECHS))] \
        .assign(timestamp=dd.to_datetime(c.o.gen_df['timestamp']).dt.floor('D')) \
        .groupby(['model', 'Category', ] + c.GEO_COLS + ['timestamp']) \
        .agg({'value': 'sum'}) \
        .compute() \
        .unstack('Category') \
        .fillna(0) \
        .stack('Category') \
        .unstack(level=c.GEO_COLS) \
        .fillna(0) \
        .apply(lambda x: x * hour_corr)

    # Add zero values to regions without VRE
    geo_col_filler = pd.Series(data=np.ones(len(load_by_reg.unstack(c.GEO_COLS).columns)),
                               index=load_by_reg.unstack(c.GEO_COLS).columns)
    vre_av_abs = (vre_av_abs * geo_col_filler).fillna(0)
    vre_gen_abs = (vre_gen_abs * geo_col_filler).fillna(0)

    # 24 periods per day for the daily data
    vre_curtailed = vre_av_abs - vre_gen_abs

    # Add non-VRE spillage/curtailment
    constr_techs = ['Hydro', 'Bioenergy', 'Geothermal']

    other_re_gen_abs = c.o.gen_df[(c.o.gen_df.property == 'Generation') &
                                  (c.o.gen_df.Category.isin(constr_techs))] \
        .assign(timestamp=dd.to_datetime(c.o.gen_df['timestamp']).dt.floor('D')) \
        .groupby(['model', 'Category', ] + c.GEO_COLS + ['timestamp']) \
        .agg({'value': 'sum'}) \
        .compute() \
        .unstack('Category') \
        .fillna(0) \
        .stack('Category') \
        .unstack(level=c.GEO_COLS) \
        .fillna(0) \
        .apply(lambda x: x * hour_corr)

    other_re_energy_vio = c.o.gen_df[(c.o.gen_df.property == 'Min Energy Violation') &
                                     (c.o.gen_df.Category.isin(constr_techs))] \
        .assign(timestamp=dd.to_datetime(c.o.gen_df['timestamp']).dt.floor('D')) \
        .groupby(['model', 'Category'] + c.GEO_COLS + ['timestamp']) \
        .agg({'value': 'sum'}) \
        .compute() \
        .unstack('Category') \
        .fillna(0) \
        .stack('Category') \
        .unstack(level=c.GEO_COLS) \
        .fillna(0) \
        .apply(lambda x: x * hour_corr)

    other_re_gen_abs = (other_re_gen_abs * geo_col_filler).fillna(0)
    other_re_energy_vio = (other_re_energy_vio * geo_col_filler).fillna(0)
    other_re_av = other_re_energy_vio + other_re_gen_abs

    all_re_av = pd.concat([vre_av_abs, other_re_av], axis=0).reset_index().groupby(
        ['model', 'timestamp', 'Category']).sum()

    all_re_curtailed = pd.concat([vre_curtailed, other_re_energy_vio], axis=0).reset_index().groupby(
        ['model', 'timestamp', 'Category']).sum()

    re_curtailment_rate = (all_re_curtailed.sum(axis=1).groupby('model').sum() / all_re_av.sum(axis=1).groupby(
        'model').sum()).fillna(0) * 100

    curtailment_rate = (vre_curtailed.sum(axis=1).groupby('model').sum() / vre_av_abs.sum(axis=1).groupby(
        'model').sum()).fillna(0) * 100

    vre_curtailed_grouped = vre_curtailed.T.groupby('Island').sum().T
    vre_av_abs_grouped = vre_av_abs.T.groupby('Island').sum().T

    curtailment_rate_isl = (vre_curtailed_grouped.groupby('model').sum() /
                            vre_av_abs_grouped.groupby('model').sum()).fillna(0) * 100

    curtailment_rate = pd.concat([curtailment_rate_isl, curtailment_rate.rename('IDN')], axis=1)

    vre_curtailed.stack(c.GEO_COLS).assign(units='GWh').reset_index().to_csv(
        os.path.join(c.DIR_05_1_SUMMARY_OUT, '09a_vre_daily_curtailed.csv'), index=False)
    curtailment_rate.assign(units='%').reset_index().to_csv(
        os.path.join(c.DIR_05_1_SUMMARY_OUT, '09b_curtailment_rate.csv'),
        index=False)
    all_re_curtailed.stack(c.GEO_COLS).assign(units='GWh') \
        .reset_index() \
        .to_csv(
        os.path.join(c.DIR_05_1_SUMMARY_OUT, '09c_all_RE_daily_curtailed.csv'), index=False)
    pd.DataFrame({'value': re_curtailment_rate, 'model': re_curtailment_rate.index}) \
        .assign(units='%') \
        .to_csv(
        os.path.join(c.DIR_05_1_SUMMARY_OUT, '09d_all_RE_curtailment_rate.csv'), index=False)

    print('Created file 09a_vre_daily_curtailed.csv.')
    print('Created file 09b_curtailment_rate.csv.')
    print('Created file 09c_all_RE_daily_curtailed.csv.')
    print('Created file 09d_all_RE_curtailment_rate.csv.')


@catch_errors
def create_output_10(c):
    """
    Creates following output files:
    - 10a_line_cap.csv
    - 10b_line_imports_exports.csv
    """
    print("Creating output 10...")
    # Output 10: a) Line flows/capacity per line/interface c) Line flow time-series per interface
    # (as % of capacity?)
    if not {'islFrom', 'nodeTo'}.issubset(set(c.soln_idx.columns)):
        print("No islFrom and nodeTo columns in soln_idx. Skipping output 10.")
        return

    line_cap = c.o.line_yr_df[(c.o.line_yr_df.property == 'Import Limit') | \
                              (c.o.line_yr_df.property == 'Export Limit')] \
        .groupby(['model', 'nodeFrom', 'nodeTo', 'islFrom', 'islTo', 'property']) \
        .agg({'value': 'sum'}) \
        .unstack(level='property')

    line_imp_exp = c.o.line_yr_df[(c.o.line_yr_df.property == 'Flow') | \
                                  (c.o.line_yr_df.property == 'Flow Back')] \
        .groupby(['model', 'nodeFrom', 'nodeTo', 'islFrom', 'islTo', 'property']) \
        .agg({'value': 'sum'}) \
        .unstack(level='property')

    ####
    line_cap_isl = line_cap.reset_index()
    line_cap_isl = line_cap_isl[line_cap_isl.islFrom != line_cap_isl.islTo]
    line_cap_isl.loc[:, 'line'] = line_cap_isl.islFrom + '-' + line_cap_isl.islTo
    line_cap_isl = line_cap_isl.groupby(['model', 'line']).sum(numeric_only=True)

    if line_cap_isl.shape[0] == 0:
        pd.DataFrame({'model': c.o.line_yr_df.model.unique(),
                      'nodeFrom': ['None'] * len(c.o.line_yr_df.model.unique()),
                      'nodeTo': ['None'] * len(c.o.line_yr_df.model.unique()),
                      'islFrom': ['None'] * len(c.o.line_yr_df.model.unique()),
                      'islTo': ['None'] * len(c.o.line_yr_df.model.unique()),
                      'value': [0] * len(c.o.line_yr_df.model.unique())})

    if line_imp_exp.shape[0] == 0:
        pd.DataFrame({'model': c.o.line_yr_df.model.unique(),
                      'nodeFrom': ['None'] * len(c.o.line_yr_df.model.unique()),
                      'nodeTo': ['None'] * len(c.o.line_yr_df.model.unique()),
                      'islFrom': ['None'] * len(c.o.line_yr_df.model.unique()),
                      'islTo': ['None'] * len(c.o.line_yr_df.model.unique()),
                      'value': [0] * len(c.o.line_yr_df.model.unique())})

    line_imp_exp_isl = line_imp_exp.reset_index()
    line_imp_exp_isl = line_imp_exp_isl[line_imp_exp_isl.islFrom != line_imp_exp_isl.islTo]
    line_imp_exp_isl.loc[:, 'line'] = line_imp_exp_isl.islFrom + '-' + line_imp_exp_isl.islTo

    # Drop the higher-level index (to get a single column line in csv file)
    line_cap.columns = line_cap.columns.droplevel(level=0)
    line_imp_exp.columns = line_imp_exp.columns.droplevel(level=0)

    line_cap \
        .assign(units='MW') \
        .reset_index() \
        .to_csv(os.path.join(c.DIR_05_1_SUMMARY_OUT, '10a_line_cap.csv'), index=False)
    line_imp_exp \
        .assign(units='GWh') \
        .reset_index() \
        .to_csv(os.path.join(c.DIR_05_1_SUMMARY_OUT, '10b_line_imports_exports.csv'), index=False)

    print('Created file 10a_line_cap.csv.')
    print('Created file 10b_line_imports_exports.csv.')


@catch_errors
def create_output_11(c):
    """
    Creates following output files:
    - 11a_cap_by_tech_reg.csv
    - 11b_gen_cap_by_subtech_reg.csv
    - 11c_gen_cap_by_costTech_reg.csv
    - 11d_gen_cap_by_weoTech_reg.csv
    - 11d_gen_cap_w_IPPs_by_tech_reg.csv
    """
    print("Creating output 11...")

    if not {'IPP'}.issubset(set(c.soln_idx.columns)):
        print("No IPP column in soln_idx. Skipping output 11.")
        return
    # Output 11 & 12 : a) capacity & CFs per technology/region b) CFs per tech only

    gen_cap_tech_reg = c.o.gen_yr_df[c.o.gen_yr_df.property == 'Installed Capacity'] \
        .groupby(['model'] + c.GEO_COLS + ['Category']) \
        .agg({'value': 'sum'}) \
        .unstack(level=c.GEO_COLS) \
        .fillna(0)

    gen_cap_tech_reg_IPPs = c.o.gen_yr_df[c.o.gen_yr_df.property == 'Installed Capacity'] \
        .groupby(['model'] + c.GEO_COLS + ['IPP', 'Category']) \
        .agg({'value': 'sum'}) \
        .unstack(level=c.GEO_COLS) \
        .fillna(0)

    # gen_cap_tech_subreg = gen_yr_df[gen_yr_df.property == 'Installed Capacity'].groupby(
    #     [ 'model', 'Subregion', 'Category']).agg({'value': 'sum'})..unstack(level='Subregion').fillna(0)

    gen_cap_subtech_reg = c.o.gen_yr_df[c.o.gen_yr_df.property == 'Installed Capacity'] \
        .groupby(['model'] + c.GEO_COLS + ['CapacityCategory']) \
        .agg({'value': 'sum'}) \
        .unstack(level=c.GEO_COLS) \
        .fillna(0)

    # For Capex calcs
    gen_cap_costTech_reg = c.o.gen_yr_df[c.o.gen_yr_df.property == 'Installed Capacity'] \
        .groupby(['model'] + c.GEO_COLS + ['CostCategory']) \
        .agg({'value': 'sum'}) \
        .unstack(level=c.GEO_COLS) \
        .fillna(0)

    gen_cap_tech_reg \
        .stack(c.GEO_COLS) \
        .assign(units='MW') \
        .reset_index() \
        .to_csv(os.path.join(c.DIR_05_1_SUMMARY_OUT, '11a_cap_by_tech_reg.csv'), index=False)
    gen_cap_subtech_reg \
        .stack(c.GEO_COLS) \
        .assign(units='MW') \
        .reset_index() \
        .to_csv(os.path.join(c.DIR_05_1_SUMMARY_OUT, '11b_gen_cap_by_subtech_reg.csv'), index=False)
    gen_cap_costTech_reg \
        .stack(c.GEO_COLS) \
        .assign(units='MW') \
        .reset_index() \
        .to_csv(os.path.join(c.DIR_05_1_SUMMARY_OUT, '11c_gen_cap_by_costTech_reg.csv'), index=False)
    gen_cap_costTech_reg \
        .stack(c.GEO_COLS) \
        .assign(units='MW') \
        .reset_index() \
        .to_csv(os.path.join(c.DIR_05_1_SUMMARY_OUT, '11d_gen_cap_by_weoTech_reg.csv'), index=False)
    gen_cap_tech_reg_IPPs \
        .stack(c.GEO_COLS) \
        .assign(units='MW') \
        .reset_index() \
        .to_csv(os.path.join(c.DIR_05_1_SUMMARY_OUT, '11d_gen_cap_w_IPPs_by_tech_reg.csv'), index=False)

    print('Created file 11a_cap_by_tech_reg.csv.')
    print('Created file 11b_gen_cap_by_subtech_reg.csv.')
    print('Created file 11c_gen_cap_by_costTech_reg.csv.')
    print('Created file 11d_gen_cap_by_weoTech_reg.csv.')
    print('Created file 11d_gen_cap_w_IPPs_by_tech_reg.csv.')


@catch_errors
def create_output_12(c):
    """
    Creates following output files:
    - 12a_cf_tech_reg.csv
    - 12c_cf_tech.csv
    """
    print("Creating output 12...")

    # todo Not sure if that always works
    # time_idx = db.region("Load").reset_index().timestamp.drop_duplicates()
    time_idx = c.o.reg_df.reset_index().timestamp.drop_duplicates().compute()

    nr_days = len(time_idx.dt.date.drop_duplicates())

    gen_by_tech_reg_orig = c.o.gen_yr_df[
        c.o.gen_yr_df.property == 'Generation']  # For not separating cofiring. good for CF comparison
    gen_by_tech_reg_orig = gen_by_tech_reg_orig \
        .groupby(['model'] + c.GEO_COLS + ['Category']) \
        .agg({'value': 'sum'}) \
        .unstack(level=c.GEO_COLS) \
        .fillna(0)

    # Output 11 & 12 : a) capacity & CFs per technology/region b) CFs per tech only

    gen_cap_tech_reg = c.o.gen_yr_df[c.o.gen_yr_df.property == 'Installed Capacity'] \
        .groupby(['model'] + c.GEO_COLS + ['Category']) \
        .agg({'value': 'sum'}) \
        .unstack(level=c.GEO_COLS) \
        .fillna(0)

    # Calculate as EN[GWh]/(Capacity[MW]/1000*hours)
    # Standard
    # As we make adjustments for co_firing on the energy values, we must re-calculate this

    cf_tech_reg = (gen_by_tech_reg_orig / (gen_cap_tech_reg / 1000 * nr_days * 24)).fillna(0)

    cf_tech = (gen_by_tech_reg_orig.sum(axis=1) / (gen_cap_tech_reg.sum(axis=1) / 1000 * nr_days * 24)).unstack(
        level='Category').fillna(0)

    cf_tech_reg \
        .stack(c.GEO_COLS) \
        .assign(units='%') \
        .reset_index() \
        .to_csv(os.path.join(c.DIR_05_1_SUMMARY_OUT, '12a_cf_tech_reg.csv'), index=False)
    cf_tech \
        .assign(units='%') \
        .reset_index() \
        .to_csv(os.path.join(c.DIR_05_1_SUMMARY_OUT, '12c_cf_tech.csv'), index=False)

    print('Created file 12a_cf_tech_reg.csv.')
    print('Created file 12c_cf_tech.csv.')


@catch_errors
def create_output_13(c):
    """
    Creates following output files:
    - 13a_co2_by_tech_reg.csv
    - 13b_co2_by_reg.csv
    """
    print("Creating output 14...")
    # Output 14: Emissions

    # Standard

    em_by_type_tech_reg = c.o.em_gen_yr_df[(c.o.em_gen_yr_df.property == 'Production')].groupby(
        ['model', 'parent'] + c.GEO_COLS + ['Category']).agg({'value': 'sum'}).reset_index()

    def get_parent(x):
        return x if '_' not in x else x.split('_')[0]

    em_by_type_tech_reg.parent = em_by_type_tech_reg.parent.apply(get_parent)

    co2_by_tech_reg = c.o.em_gen_yr_df[c.o.em_gen_yr_df.parent.str.contains('CO2') &
                                       (c.o.em_gen_yr_df.property == 'Production')] \
        .groupby(['model'] + c.GEO_COLS + ['Category']) \
        .agg({'value': 'sum'})

    co2_by_reg = c.o.em_gen_yr_df[c.o.em_gen_yr_df.parent.str.contains('CO2') &
                                  (c.o.em_gen_yr_df.property == 'Production')] \
        .groupby(['model'] + c.GEO_COLS) \
        .agg({'value': 'sum'})

    co2_by_tech_reg \
        .assign(units='tonnes') \
        .reset_index() \
        .to_csv(os.path.join(c.DIR_05_1_SUMMARY_OUT, '13a_co2_by_tech_reg.csv'), index=False)
    co2_by_reg \
        .assign(units='tonnes') \
        .reset_index() \
        .to_csv(os.path.join(c.DIR_05_1_SUMMARY_OUT, '13b_co2_by_reg.csv'), index=False)

    print('Created file 13a_co2_by_tech_reg.csv.')
    print('Created file 13b_co2_by_reg.csv.')

