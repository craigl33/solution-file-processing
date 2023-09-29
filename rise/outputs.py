"""
TODO DOCSTRING
"""

import os

import pandas as pd
import dask.dataframe as dd
import numpy as np

from .objects import objects as p
from .variables import variables as v
from .utils.logger import log
from .utils.utils import add_df_column
from .constants import VRE_TECHS
from .settings import settings as s


print = log.info

idn_actuals_2019 = pd.read_excel('R:/RISE/DOCS/04 PROJECTS/COUNTRIES/INDONESIA/Power system enhancement 2020_21/\
Modelling/01 InputData/01 Generation/20220201_generator_capacity_v23_NZE.xlsx',
                                 sheet_name='IDN_Summary_per_tech', usecols='A:T', engine='openpyxl')

ix = pd.IndexSlice

def create_year_output_1():
    """"
    ### Output 1
    ### To allow scaling, the group index is maintained (i.e. as_index=True) before resetting the index
    """
    print('Creating output 1...')

    load_by_reg = p.node_yr_df[p.node_yr_df.property == 'Load']\
        .groupby(['model', 'timestamp'] + s.cfg['settings']['geo_cols'])\
        .agg({'value': 'sum'})
    customer_load_by_reg = p.node_yr_df[(p.node_yr_df.property == 'Customer Load') |
                                        (p.node_yr_df.property == 'Unserved Energy')]\
        .groupby(['model', 'timestamp'] + s.cfg['settings']['geo_cols'])\
        .agg({'value': 'sum'})

    load_by_reg = load_by_reg.compute()  # Change dd.DataFrame back to pd.DataFrame
    customer_load_by_reg = customer_load_by_reg.compute()  # Change dd.DataFrame back to pd.DataFrame

    os.makedirs(p.DIR_05_1_SUMMARY_OUT, exist_ok=True)
    add_df_column(load_by_reg, 'units', 'GWh')\
        .to_csv(os.path.join(p.DIR_05_1_SUMMARY_OUT, '01a_load_by_reg.csv'), index=False)
    add_df_column(customer_load_by_reg, 'units', 'GWh')\
        .to_csv(os.path.join(p.DIR_05_1_SUMMARY_OUT, '01b_customer_load_by_reg.csv'), index=False)

    print(f'Saved file 01a_load_by_reg.csv.')
    print(f'Saved file 01b_customer_load_by_reg.csv.')


def create_year_output_2():
    """
    ### Output 2: USE
    """
    print('Creating output 2...')
    _node_yr_df = p.node_yr_df.compute()  # Change dd.DataFrame back to pd.DataFrame

    use_by_reg = _node_yr_df[_node_yr_df.property == 'Unserved Energy']\
        .groupby(['model'] + s.cfg['settings']['geo_cols'])\
        .agg({'value': 'sum'})
    use_reg_daily_ts = _node_yr_df[_node_yr_df.property == 'Unserved Energy']\
        .groupby(['model'] + s.cfg['settings']['geo_cols'] + [pd.Grouper(key='timestamp', freq='D')])\
        .agg({'value': 'sum'})

    add_df_column(use_by_reg, 'units', 'GWh')\
        .to_csv(os.path.join(p.DIR_05_1_SUMMARY_OUT, '02a_use_reg.csv'), index=False)
    add_df_column(use_reg_daily_ts, 'units', 'GWh')\
        .to_csv(os.path.join(p.DIR_05_1_SUMMARY_OUT, '02b_use_reg_daily_ts.csv'), index=False)

    print('Saved file 02a_use_reg.csv.')
    print('Saved file 02b_use_reg_daily_ts.csv.')


def create_year_output_3():
    """
    TODO DOCSTRING
    """
    print('Creating year output 3 is not implemented yet.')
    return


def create_year_output_4():
    """
    TODO DOCSTRING
    """
    pass


def create_year_output_5():
    """
    TODO DOCSTRING
    """
    print("Creating output 5...")

    unit_starts_by_tech = p.gen_yr_df[p.gen_yr_df.property == 'Units Started'] \
        .groupby(['model', 'Category']) \
        .agg({'value': 'sum'}) \
        .compute() \
        .unstack(level='Category')

    add_df_column(unit_starts_by_tech.stack(), 'units', 'starts').to_csv(
        os.path.join(p.DIR_05_1_SUMMARY_OUT, '05_unit_starts_by_tech.csv'), index=False)
    print("Done.")


def create_year_output_6():
    """
    TODO DOCSTRING
    """
    print("Creating output 6...")

    # Standard
    gen_max_by_tech_reg = p.gen_df[p.gen_df.property == 'Generation'] \
        .groupby(['model'] + s.cfg['settings']['geo_cols'] + ['Category']) \
        .agg({'value': 'max'}) \
        .compute() \
        .unstack(level=s.cfg['settings']['geo_cols']) \
        .fillna(0)

    add_df_column(gen_max_by_tech_reg.stack(s.cfg['settings']['geo_cols']), 'units', 'MW').to_csv(
        os.path.join(p.DIR_05_1_SUMMARY_OUT, '06a_gen_max_by_tech_reg.csv'), index=False)

    print("Done.")


def create_year_output_7():
    """
    TODO DOCSTRING
    """
    print("Creating output 7...")

    tx_losses = p.line_yr_df[p.line_yr_df.property == 'Loss'] \
        .groupby(['model', 'timestamp', 'name']) \
        .agg({'value': 'sum'}) \
        .compute()

    add_df_column(tx_losses, 'units', 'GWh') \
        .to_csv(os.path.join(p.DIR_05_1_SUMMARY_OUT, '07_tx_losses.csv'), index=False)

    print("Done.")


def create_year_output_8():
    """
    TODO DOCSTRING
    """
    print("Creating output 8...")
    time_idx = p.reg_df.reset_index().timestamp.drop_duplicates().compute()
    interval_periods = len(time_idx)
    nr_days = len(time_idx.dt.date.drop_duplicates())
    daily_periods = interval_periods / nr_days
    hour_corr = 24 / daily_periods

    # Output 8 & 9

    # Fill in data for regions which have no VRE (i.e. zero arrays!) to allow similar arrays for load_ts and
    # vre_add_df_columns
    # To add something for subregions

    # There is an error in PLEXOS with Available Capacity versus Generation (Gen exceeds Av Capacity)
    load_by_reg = p.node_yr_df[p.node_yr_df.property == 'Load'] \
        .groupby(['model', 'timestamp'] + s.cfg['settings']['geo_cols']) \
        .agg({'value': 'sum'})

    vre_cap = p.gen_yr_df[(p.gen_yr_df.property == 'Installed Capacity') &
                          (p.gen_yr_df.Category.isin(VRE_TECHS))] \
        .groupby(['model', 'Category'] + s.cfg['settings']['geo_cols']) \
        .agg({'value': 'max'}) \
        .compute() \
        .unstack('Category') \
        .fillna(0) \
        .stack('Category') \
        .unstack(level=s.cfg['settings']['geo_cols']) \
        .fillna(0)

    vre_av_abs = p.gen_df[(p.gen_df.property == 'Available Capacity') &
                          (p.gen_df.Category.isin(VRE_TECHS))] \
        .assign(timestamp=dd.to_datetime(p.gen_df['timestamp']).dt.floor('D')) \
        .groupby(['model', 'Category'] + s.cfg['settings']['geo_cols'] + ['timestamp']) \
        .agg({'value': 'sum'}) \
        .compute() \
        .unstack('Category') \
        .fillna(0) \
        .stack('Category') \
        .unstack(level=s.cfg['settings']['geo_cols']) \
        .fillna(0) \
        .apply(lambda x: x * hour_corr)

    # ### Add zero values to regions without VRE
    geo_col_filler = pd.Series(data=np.ones(len(load_by_reg.compute().unstack(s.cfg['settings']['geo_cols']).columns)),
                               index=load_by_reg.compute().unstack(s.cfg['settings']['geo_cols']).columns)
    vre_cap = (vre_cap * geo_col_filler).fillna(0)
    vre_av_abs = (vre_av_abs * geo_col_filler).fillna(0)

    # 24 periods per day for the daily data
    vre_av_norm = (vre_av_abs / vre_cap / daily_periods).fillna(0)

    add_df_column(vre_cap.stack(s.cfg['settings']['geo_cols']), 'units', 'MW').to_csv(
        os.path.join(p.DIR_05_1_SUMMARY_OUT, '08a_vre_cap.csv'),
        index=False)
    add_df_column(vre_av_abs.stack(s.cfg['settings']['geo_cols']), 'units', 'GWh').to_csv(
        os.path.join(p.DIR_05_1_SUMMARY_OUT, '08b_vre_daily_abs.csv'), index=False)
    add_df_column(vre_av_norm.stack(s.cfg['settings']['geo_cols']), 'units', '-').to_csv(
        os.path.join(p.DIR_05_1_SUMMARY_OUT, '08c_vre_daily_norm.csv'), index=False)

    print("Done.")


def create_year_output_9():
    """
    TODO DOCSTRING
    """
    print("Creating output 9...")
    # todo Not sure if that always works
    # time_idx = db.region("Load").reset_index().timestamp.drop_duplicates()
    time_idx = p.reg_df.reset_index().timestamp.drop_duplicates().compute()

    interval_periods = len(time_idx)
    nr_days = len(time_idx.dt.date.drop_duplicates())
    daily_periods = interval_periods / nr_days
    hour_corr = 24 / daily_periods

    # Output 8 & 9

    # Fill in data for regions which have no VRE (i.e. zero arrays!) to allow similar arrays for load_ts and
    # vre_add_df_columns
    # To add something for subregions

    # There is an error in PLEXOS with Available Capacity versus Generation (Gen exceeds Av Capacity)
    load_by_reg = p.node_yr_df[p.node_yr_df.property == 'Load'] \
        .groupby(['model', 'timestamp'] + s.cfg['settings']['geo_cols']) \
        .agg({'value': 'sum'})

    vre_av_abs = p.gen_df[(p.gen_df.property == 'Available Capacity') &
                          (p.gen_df.Category.isin(VRE_TECHS))] \
        .assign(timestamp=dd.to_datetime(p.gen_df['timestamp']).dt.floor('D')) \
        .groupby(['model', 'Category'] + s.cfg['settings']['geo_cols'] + ['timestamp']) \
        .agg({'value': 'sum'}) \
        .compute() \
        .unstack('Category') \
        .fillna(0) \
        .stack('Category') \
        .unstack(level=s.cfg['settings']['geo_cols']) \
        .fillna(0) \
        .apply(lambda x: x * hour_corr)

    vre_gen_abs = p.gen_df[(p.gen_df.property == 'Generation') &
                           (p.gen_df.Category.isin(VRE_TECHS))] \
        .assign(timestamp=dd.to_datetime(p.gen_df['timestamp']).dt.floor('D')) \
        .groupby(['model', 'Category', ] + s.cfg['settings']['geo_cols'] + ['timestamp']) \
        .agg({'value': 'sum'}) \
        .compute() \
        .unstack('Category') \
        .fillna(0) \
        .stack('Category') \
        .unstack(level=s.cfg['settings']['geo_cols']) \
        .fillna(0) \
        .apply(lambda x: x * hour_corr)

    # Add zero values to regions without VRE
    geo_col_filler = pd.Series(data=np.ones(len(load_by_reg.compute().unstack(s.cfg['settings']['geo_cols']).columns)),
                               index=load_by_reg.compute().unstack(s.cfg['settings']['geo_cols']).columns)
    vre_av_abs = (vre_av_abs * geo_col_filler).fillna(0)
    vre_gen_abs = (vre_gen_abs * geo_col_filler).fillna(0)

    # 24 periods per day for the daily data
    vre_curtailed = vre_av_abs - vre_gen_abs

    # Add non-VRE spillage/curtailment
    constr_techs = ['Hydro', 'Bioenergy', 'Geothermal']

    other_re_gen_abs = p.gen_df[(p.gen_df.property == 'Generation') &
                                (p.gen_df.Category.isin(constr_techs))] \
        .assign(timestamp=dd.to_datetime(p.gen_df['timestamp']).dt.floor('D')) \
        .groupby(['model', 'Category', ] + s.cfg['settings']['geo_cols'] + ['timestamp']) \
        .agg({'value': 'sum'}) \
        .compute() \
        .unstack('Category') \
        .fillna(0) \
        .stack('Category') \
        .unstack(level=s.cfg['settings']['geo_cols']) \
        .fillna(0) \
        .apply(lambda x: x * hour_corr)

    other_re_energy_vio = p.gen_df[(p.gen_df.property == 'Min Energy Violation') &
                                   (p.gen_df.Category.isin(constr_techs))] \
        .assign(timestamp=dd.to_datetime(p.gen_df['timestamp']).dt.floor('D')) \
        .groupby(['model', 'Category'] + s.cfg['settings']['geo_cols'] + ['timestamp']) \
        .agg({'value': 'sum'}) \
        .compute() \
        .unstack('Category') \
        .fillna(0) \
        .stack('Category') \
        .unstack(level=s.cfg['settings']['geo_cols']) \
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
    curtailment_rate_isl = (vre_curtailed.groupby('Island', axis=1).sum().groupby(
        'model').sum() / vre_av_abs.groupby('Island', axis=1).sum().groupby('model').sum()).fillna(0) * 100

    curtailment_rate = pd.concat([curtailment_rate_isl, curtailment_rate.rename('IDN')], axis=1)

    add_df_column(vre_curtailed.stack(s.cfg['settings']['geo_cols']), 'units', 'GWh').to_csv(
        os.path.join(p.DIR_05_1_SUMMARY_OUT, '09a_vre_daily_curtailed.csv'), index=False)
    add_df_column(curtailment_rate, 'units', '%').to_csv(
        os.path.join(p.DIR_05_1_SUMMARY_OUT, '09b_curtailment_rate.csv'),
        index=False)
    add_df_column(all_re_curtailed.stack(s.cfg['settings']['geo_cols']), 'units', 'GWh').to_csv(
        os.path.join(p.DIR_05_1_SUMMARY_OUT, '09c_all_RE_daily_curtailed.csv'), index=False)
    add_df_column(re_curtailment_rate, 'units', '%').to_csv(
        os.path.join(p.DIR_05_1_SUMMARY_OUT, '09d_all_RE_curtailment_rate.csv'), index=False)

    print("Done.")


def create_year_output_10():
    """
    TODO DOCSTRING
    """
    print("Creating output 10...")
    # Output 10: a) Line flows/capacity per line/interface c) Line flow time-series per interface
    # (as % of capacity?)

    line_cap = p.line_yr_df[(p.line_yr_df.property == 'Import Limit') | \
                            (p.line_yr_df.property == 'Export Limit')] \
        .groupby(['model', 'nodeFrom', 'nodeTo', 'islFrom', 'islTo', 'property']) \
        .agg({'value': 'sum'}) \
        .compute() \
        .unstack(level='property')

    line_imp_exp = p.line_yr_df[(p.line_yr_df.property == 'Flow') | \
                                (p.line_yr_df.property == 'Flow Back')] \
        .groupby(['model', 'nodeFrom', 'nodeTo', 'islFrom', 'islTo', 'property']) \
        .agg({'value': 'sum'}) \
        .compute().unstack(level='property')

    ####
    line_cap_isl = line_cap.reset_index()
    line_cap_isl = line_cap_isl[line_cap_isl.islFrom != line_cap_isl.islTo]
    line_cap_isl.loc[:, 'line'] = line_cap_isl.islFrom + '-' + line_cap_isl.islTo
    line_cap_isl = line_cap_isl.groupby(['model', 'line']).sum(numeric_only=True)

    if line_cap_isl.shape[0] == 0:
        pd.DataFrame({'model': p.line_yr_df.model.unique(),
                      'nodeFrom': ['None'] * len(p.line_yr_df.model.unique()),
                      'nodeTo': ['None'] * len(p.line_yr_df.model.unique()),
                      'islFrom': ['None'] * len(p.line_yr_df.model.unique()),
                      'islTo': ['None'] * len(p.line_yr_df.model.unique()),
                      'value': [0] * len(p.line_yr_df.model.unique())})

    if line_imp_exp.shape[0] == 0:
        pd.DataFrame({'model': p.line_yr_df.model.unique(),
                      'nodeFrom': ['None'] * len(p.line_yr_df.model.unique()),
                      'nodeTo': ['None'] * len(p.line_yr_df.model.unique()),
                      'islFrom': ['None'] * len(p.line_yr_df.model.unique()),
                      'islTo': ['None'] * len(p.line_yr_df.model.unique()),
                      'value': [0] * len(p.line_yr_df.model.unique())})

    line_imp_exp_isl = line_imp_exp.reset_index()
    line_imp_exp_isl = line_imp_exp_isl[line_imp_exp_isl.islFrom != line_imp_exp_isl.islTo]
    line_imp_exp_isl.loc[:, 'line'] = line_imp_exp_isl.islFrom + '-' + line_imp_exp_isl.islTo

    # Drop the higher-level index (to get a single column line in csv file)
    line_cap.columns = line_cap.columns.droplevel(level=0)
    line_imp_exp.columns = line_imp_exp.columns.droplevel(level=0)

    add_df_column(line_cap, 'units', 'MW') \
        .to_csv(os.path.join(p.DIR_05_1_SUMMARY_OUT, '10a_line_cap.csv'), index=False)
    add_df_column(line_imp_exp, 'units', 'GWh').to_csv(
        os.path.join(p.DIR_05_1_SUMMARY_OUT, '10b_line_imports_exports.csv'), index=False)

    print("Done.")


def create_year_output_11():
    """
    TODO DOCSTRING
    """
    print("Creating output 11...")

    # Output 11 & 12 : a) capacity & CFs per technology/region b) CFs per tech only

    gen_cap_tech_reg = p.gen_yr_df[p.gen_yr_df.property == 'Installed Capacity'] \
        .groupby(['model'] + s.cfg['settings']['geo_cols'] + ['Category']) \
        .agg({'value': 'sum'}) \
        .compute() \
        .unstack(level=s.cfg['settings']['geo_cols']) \
        .fillna(0)

    gen_cap_tech_reg_IPPs = p.gen_yr_df[p.gen_yr_df.property == 'Installed Capacity'] \
        .groupby(['model'] + s.cfg['settings']['geo_cols'] + ['IPP', 'Category']) \
        .agg({'value': 'sum'}) \
        .compute() \
        .unstack(level=s.cfg['settings']['geo_cols']) \
        .fillna(0)

    # gen_cap_tech_subreg = gen_yr_df[gen_yr_df.property == 'Installed Capacity'].groupby(
    #     [ 'model', 'Subregion', 'Category']).agg({'value': 'sum'})..unstack(level='Subregion').fillna(0)

    gen_cap_subtech_reg = p.gen_yr_df[p.gen_yr_df.property == 'Installed Capacity'] \
        .groupby(['model'] + s.cfg['settings']['geo_cols'] + ['CapacityCategory']) \
        .agg({'value': 'sum'}) \
        .compute() \
        .unstack(level=s.cfg['settings']['geo_cols']) \
        .fillna(0)

    # For Capex calcs
    gen_cap_costTech_reg = p.gen_yr_df[p.gen_yr_df.property == 'Installed Capacity'] \
        .groupby(['model'] + s.cfg['settings']['geo_cols'] + ['CostCategory']) \
        .agg({'value': 'sum'}) \
        .compute() \
        .unstack(level=s.cfg['settings']['geo_cols']) \
        .fillna(0)

    if s.cfg['settings']['validation']:
        idn_cap_actuals_by_tech_reg = idn_actuals_2019 \
            .groupby(['model'] + s.cfg['settings']['geo_cols'] + ['CapacityCategory']) \
            .agg({'SummaryCap_MW': 'sum'}) \
            .compute() \
            .unstack(level=s.cfg['settings']['geo_cols']) \
            .fillna(0)
        gen_cap_tech_reg = pd.concat([gen_cap_tech_reg, idn_cap_actuals_by_tech_reg], axis=0)

    add_df_column(gen_cap_tech_reg.stack(s.cfg['settings']['geo_cols']), 'units', 'MW').to_csv(
        os.path.join(p.DIR_05_1_SUMMARY_OUT, '11a_cap_by_tech_reg.csv'), index=False)
    add_df_column(gen_cap_subtech_reg.stack(s.cfg['settings']['geo_cols']), 'units', 'MW').to_csv(
        os.path.join(p.DIR_05_1_SUMMARY_OUT, '11b_gen_cap_by_subtech_reg.csv'), index=False)
    add_df_column(gen_cap_costTech_reg.stack(s.cfg['settings']['geo_cols']), 'units', 'MW').to_csv(
        os.path.join(p.DIR_05_1_SUMMARY_OUT, '11c_gen_cap_by_costTech_reg.csv'), index=False)
    add_df_column(gen_cap_costTech_reg.stack(s.cfg['settings']['geo_cols']), 'units', 'MW').to_csv(
        os.path.join(p.DIR_05_1_SUMMARY_OUT, '11d_gen_cap_by_weoTech_reg.csv'), index=False)
    add_df_column(gen_cap_tech_reg_IPPs.stack(s.cfg['settings']['geo_cols']), 'units', 'MW').to_csv(
        os.path.join(p.DIR_05_1_SUMMARY_OUT, '11d_gen_cap_w_IPPs_by_tech_reg.csv'), index=False)

    print("Done.")


def create_year_output_12():
    """
    TODO DOCSTRING
    """
    print("Creating output 12...")

    # todo Not sure if that always works
    # time_idx = db.region("Load").reset_index().timestamp.drop_duplicates()
    time_idx = p.reg_df.reset_index().timestamp.drop_duplicates().compute()

    nr_days = len(time_idx.dt.date.drop_duplicates())

    gen_by_tech_reg_orig = p.gen_yr_df[
        p.gen_yr_df.property == 'Generation']  # For not separating cofiring. good for CF comparison
    gen_by_tech_reg_orig = gen_by_tech_reg_orig \
        .groupby(['model'] + s.cfg['settings']['geo_cols'] + ['Category']) \
        .agg({'value': 'sum'}) \
        .compute() \
        .unstack(level=s.cfg['settings']['geo_cols']) \
        .fillna(0)

    # Output 11 & 12 : a) capacity & CFs per technology/region b) CFs per tech only

    gen_cap_tech_reg = p.gen_yr_df[p.gen_yr_df.property == 'Installed Capacity'] \
        .groupby(['model'] + s.cfg['settings']['geo_cols'] + ['Category']) \
        .agg({'value': 'sum'}) \
        .compute() \
        .unstack(level=s.cfg['settings']['geo_cols']) \
        .fillna(0)

    if s.cfg['settings']['validation']:
        idn_cap_actuals_by_tech_reg = idn_actuals_2019 \
            .groupby(['model'] + s.cfg['settings']['geo_cols'] + ['CapacityCategory']) \
            .agg({'SummaryCap_MW': 'sum'}) \
            .compute() \
            .unstack(level=s.cfg['settings']['geo_cols']) \
            .fillna(0)
        gen_cap_tech_reg = pd.concat([gen_cap_tech_reg, idn_cap_actuals_by_tech_reg], axis=0)

    # Calculate as EN[GWh]/(Capacity[MW]/1000*hours)
    # Standard
    # As we make adjustments for co_firing on the energy values, we must re-calculate this

    cf_tech_reg = (gen_by_tech_reg_orig / (gen_cap_tech_reg / 1000 * nr_days * 24)).fillna(0)

    cf_tech = (gen_by_tech_reg_orig.sum(axis=1) / (gen_cap_tech_reg.sum(axis=1) / 1000 * nr_days * 24)).unstack(
        level='Category').fillna(0)

    add_df_column(cf_tech_reg.stack(s.cfg['settings']['geo_cols']), 'units', '%').to_csv(
        os.path.join(p.DIR_05_1_SUMMARY_OUT, '12a_cf_tech_reg.csv'), index=False)
    add_df_column(cf_tech, 'units', '%').to_csv(os.path.join(p.DIR_05_1_SUMMARY_OUT, '12c_cf_tech.csv'),
                                                index=False)

    print("Done.")


def create_year_output_13():
    """
    TODO DOCSTRING
    """
    print("Creating output 14...")
    # Output 14: Emissions

    # Standard

    em_by_type_tech_reg = p.em_gen_yr_df[(p.em_gen_yr_df.property == 'Production')].groupby(
        ['model', 'parent'] + s.cfg['settings']['geo_cols'] + ['Category']).agg({'value': 'sum'}).reset_index()

    def get_parent(x):
        return x if '_' not in x else x.split('_')[0]

    em_by_type_tech_reg.parent = em_by_type_tech_reg.parent.apply(get_parent, meta=('parent', 'object'))

    co2_by_tech_reg = p.em_gen_yr_df[p.em_gen_yr_df.parent.str.contains('CO2') &
                                     (p.em_gen_yr_df.property == 'Production')] \
        .groupby(['model'] + s.cfg['settings']['geo_cols'] + ['Category']) \
        .agg({'value': 'sum'})

    co2_by_reg = p.em_gen_yr_df[p.em_gen_yr_df.parent.str.contains('CO2') &
                                (p.em_gen_yr_df.property == 'Production')] \
        .groupby(['model'] + s.cfg['settings']['geo_cols']) \
        .agg({'value': 'sum'})

    add_df_column(co2_by_tech_reg, 'units', 'tonnes').compute().to_csv(
        os.path.join(p.DIR_05_1_SUMMARY_OUT, '13a_co2_by_tech_reg.csv'), index=False)
    add_df_column(co2_by_reg, 'units', 'tonnes').compute().to_csv(
        os.path.join(p.DIR_05_1_SUMMARY_OUT, '13b_co2_by_reg.csv'), index=False)

    print("Done.")


def create_interval_output_1():
    """
    TODO DOCSTRING
    """
    print("Creating interval output 1...")
    # Output 1a-b: Load and USE time-series
    total_load_ts = p.reg_df[p.reg_df.property == 'Load'] \
        .groupby(['model', 'timestamp']) \
        .agg({'value': 'sum'})

    add_df_column(total_load_ts, 'units', 'MW').reset_index().compute() \
        .to_csv(os.path.join(p.DIR_05_2_TS_OUT, '01a_total_load_ts.csv'))

    use_ts = p.reg_df[p.reg_df.property == 'Unserved Energy'] \
        .groupby(['model', 'timestamp']) \
        .agg({'value': 'sum'})

    add_df_column(use_ts, 'units', 'MW').reset_index().compute() \
        .to_csv(os.path.join(p.DIR_05_2_TS_OUT, '01b_use_ts.csv'))

    # Need to calculate whether its 30mn or 1hr within but for now just assume hourly
    use_dly_ts = p.reg_df[p.reg_df.property == 'Unserved Energy'] \
        .assign(timestamp=dd.to_datetime(p.reg_df['timestamp']).dt.floor('D')) \
        .groupby(['model', 'timestamp']) \
        .agg({'value': 'sum'}) \
        .applymap(lambda x: x / 1000 if isinstance(x, float) else x)

    add_df_column(use_dly_ts, 'units', 'GWh').reset_index().compute().to_csv(
        os.path.join(p.DIR_05_2_TS_OUT, '01c_use_dly_ts.csv'))

    # load_w_use_ts = dd.concat([total_load_ts.rename('Load'), use_ts.rename('USE')])
    #
    # add_df_column(load_w_use_ts, 'units', 'MW').reset_index().compute().to_csv(
    #     os.path.join(p.DIR_05_2_TS_OUT, '01d_load_w_use_ts.csv'))

    # p._dev_test_output('01d_load_w_use_ts.csv')
    print('Done.')

    if s.cfg['settings']['reg_ts']:
        print("Creating interval special output 1 (reg_ts=True)...")
        load_by_reg_ts = p.node_df[p.node_df.property == 'Load'].groupby(
            ['model'] + s.cfg['settings']['geo_cols'] + ['timestamp']).agg({'value': 'sum'}).compute().unstack(
            level='timestamp').fillna(0).stack('timestamp')

        use_reg_ts = p.node_df[p.node_df.property == 'Unserved Energy'].groupby(
            ['model'] + s.cfg['settings']['geo_cols'] + ['timestamp']).agg({'value': 'sum'}).compute().unstack(level=s.cfg['settings']['geo_cols'])

        use_dly_reg_ts = use_reg_ts.groupby(
            [pd.Grouper(level='model'), pd.Grouper(freq='D', level='timestamp')]).sum() / 1000

        for geo in s.cfg['settings']['geo_cols']:
            geo_suffix = geo[:3].lower()
            count = ord('e')

            add_df_column(load_by_reg_ts.unstack(level=s.cfg['settings']['geo_cols'])
                          .groupby(level=geo, axis=1)
                          .sum(), 'units', 'MW') \
                .to_csv(os.path.join(p.DIR_05_2_TS_OUT, '01{}_total_load_{}_ts.csv'.format(chr(count), geo_suffix)),
                        index=False)

            add_df_column(use_reg_ts.groupby(level=geo, axis=1).sum(), 'units', 'MW').to_csv(os.path.join(
                p.DIR_05_2_TS_OUT, '01{}_use_{}_ts.csv'.format(chr(count + 1), geo_suffix)), index=False)

            add_df_column(use_dly_reg_ts.groupby(level=geo, axis=1).sum(), 'units', 'GWh').to_csv(
                os.path.join(p.DIR_05_2_TS_OUT, '01{}_use_dly_{}_ts.csv'.format(chr(count + 2), geo_suffix)),
                index=False)

            count += 3
        print("Done.")


def create_interval_output_2():
    """
    TODO DOCSTRING
    """
    print("Creating interval output 2...")
    # Output 2: Generation

    gen_by_tech_ts = p.gen_df[p.gen_df.property == 'Generation'] \
        .groupby(['model', 'Category', 'timestamp']) \
        .agg({'value': 'sum'}) \
        .compute() \
        .unstack(level='Category') \
        .fillna(0)

    gen_by_subtech_ts = p.gen_df[p.gen_df.property == 'Generation'] \
        .groupby(['model', 'CapacityCategory', 'timestamp']) \
        .agg({'value': 'sum'}) \
        .compute() \
        .unstack(level='CapacityCategory') \
        .fillna(0)

    av_cap_by_tech_ts = p.gen_df[p.gen_df.property == 'Available Capacity'] \
        .groupby(['model', 'Category', 'timestamp']) \
        .agg({'value': 'sum'}) \
        .compute() \
        .unstack(level='Category') \
        .fillna(0)

    # Drop the higher-level index (to get a single column line in csv file)
    gen_by_tech_ts.columns = gen_by_tech_ts.columns.droplevel(level=0)
    gen_by_subtech_ts.columns = gen_by_subtech_ts.columns.droplevel(level=0)
    av_cap_by_tech_ts.columns = av_cap_by_tech_ts.columns.droplevel(level=0)

    add_df_column(gen_by_tech_ts, 'units', 'MW') \
        .to_csv(os.path.join(p.DIR_05_2_TS_OUT, '02a_gen_by_tech_ts.csv'), index=False)

    add_df_column(gen_by_subtech_ts, 'units', 'MW') \
        .to_csv(os.path.join(p.DIR_05_2_TS_OUT, '02b_gen_by_tech_ts.csv'), index=False)

    add_df_column(av_cap_by_tech_ts, 'units', 'MW') \
        .to_csv(os.path.join(p.DIR_05_2_TS_OUT, '02c_av_cap_by_subtech_ts.csv'), index=False)

    print("Done.")


def create_interval_output_3():
    """
    Output 3: VRE Time-series
    """
    print("Creating interval output 3...")

    vre_av_abs_ts = p.gen_df[(p.gen_df.property == 'Available Capacity') & (p.gen_df.Category.isin(VRE_TECHS))] \
        .groupby(['model', 'Category', 'timestamp']) \
        .agg({'value': 'sum'}) \
        .compute() \
        .unstack(level='Category') \
        .fillna(0)

    vre_gen_abs_ts = p.gen_df[(p.gen_df.property == 'Generation') & (p.gen_df.Category.isin(VRE_TECHS))] \
        .groupby(['model', 'Category', 'timestamp']) \
        .agg({'value': 'sum'}) \
        .compute() \
        .unstack(level='Category') \
        .fillna(0)

    vre_curtailed_ts = vre_av_abs_ts - vre_gen_abs_ts

    constr_techs = ['Hydro', 'Bioenergy', 'Geothermal']

    min_energy_vio_tech_ts = p.gen_df[(p.gen_df.property == 'Min Energy Violation') &
                                      (p.gen_df.Category.isin(constr_techs))] \
        .groupby(['model', 'Category', 'timestamp']) \
        .agg({'value': 'sum'}) \
        .compute() \
        .unstack(level='Category') \
        .fillna(0)

    re_curtailed_ts = pd.concat([vre_curtailed_ts, min_energy_vio_tech_ts])

    # Drop the higher-level index (to get a single column line in csv file)
    vre_av_abs_ts.columns = vre_av_abs_ts.columns.droplevel(level=0)
    # vre_av_abs_ts.columns = vre_av_abs_ts.columns.droplevel(level=0)
    vre_curtailed_ts.columns = vre_curtailed_ts.columns.droplevel(level=0)
    re_curtailed_ts.columns = re_curtailed_ts.columns.droplevel(level=0)

    add_df_column(vre_av_abs_ts, 'units', 'MW') \
        .to_csv(os.path.join(p.DIR_05_2_TS_OUT, '03a_vre_available_ts.csv'), index=False)
    add_df_column(vre_av_abs_ts, 'units', 'MW') \
        .to_csv(os.path.join(p.DIR_05_2_TS_OUT, '03b_vre_gen_ts.csv'), index=False)
    add_df_column(vre_curtailed_ts, 'units', 'MW') \
        .to_csv(os.path.join(p.DIR_05_2_TS_OUT, '03c_vre_curtailed_ts.csv'), index=False)
    add_df_column(re_curtailed_ts, 'units', 'MW') \
        .to_csv(os.path.join(p.DIR_05_2_TS_OUT, '03c_RE_curtailed_ts.csv'), index=False)

    # Due to the size, we do the gen DFs per model
    if s.cfg['settings']['reg_ts']:

        load_by_reg_ts = p.node_df[p.node_df.property == 'Load'] \
            .groupby(['model'] + s.cfg['settings']['geo_cols'] + ['timestamp']) \
            .agg({'value': 'sum'}) \
            .compute() \
            .unstack(level='timestamp') \
            .fillna(0) \
            .stack('timestamp')

        # Define model regs in multi-level format
        model_regs_multi = load_by_reg_ts.unstack(s.cfg['settings']['geo_cols']).columns

        vre_av_reg_abs_ts = p.gen_df[(p.gen_df.property == 'Available Capacity') &
                                     (p.gen_df.Category.isin(VRE_TECHS))] \
            .groupby((['model'] + s.cfg['settings']['geo_cols'] + ['timestamp'])) \
            .agg({'value': 'sum'}) \
            .compute() \
            .unstack(level=s.cfg['settings']['geo_cols']) \
            .fillna(0)

        vre_gen_reg_abs_ts = p.gen_df[(p.gen_df.property == 'Generation') &
                                      (p.gen_df.Category.isin(VRE_TECHS))] \
            .groupby((['model'] + s.cfg['settings']['geo_cols'] + ['timestamp'])) \
            .agg({'value': 'sum'}) \
            .compute() \
            .unstack(level=s.cfg['settings']['geo_cols']) \
            .fillna(0)

        vre_regs = vre_av_reg_abs_ts.columns

        # Fill in data for regions which have no VRE (i.e. zero arrays!) to allow similar arrays for load_ts and
        # vre_av_ts
        for reg in model_regs_multi:
            if reg not in vre_regs:
                vre_av_reg_abs_ts[reg] = 0
                vre_gen_reg_abs_ts[reg] = 0

        # Columns in alphabetical order
        vre_av_reg_abs_ts = vre_av_reg_abs_ts[model_regs_multi]
        vre_gen_reg_abs_ts = vre_gen_reg_abs_ts[model_regs_multi]

        model_names = list(np.sort(p.reg_df.model.drop_duplicates()))

        for m in model_names:
            save_dir_model = os.path.join(p.DIR_05_2_TS_OUT, m)
            if os.path.exists(save_dir_model) is False:
                os.mkdir(save_dir_model)

            vre_av = vre_av_reg_abs_ts.loc[ix[m, :]]

            vre_gen = vre_gen_reg_abs_ts.loc[ix[m, :]]

            vre_curtailed = vre_av - vre_gen

            # For numbering purposes only
            count = ord('d')

            # Output results for every geographical aggregation
            for geo in s.cfg['settings']['geo_cols']:
                geo_suffix = geo[:3].lower()
                # We probably want to output as wind/solar in columns but maintain the regions as columns for net load
                add_df_column(vre_av.groupby(level=geo, axis=1).sum(), 'units', 'MW').to_csv(
                    os.path.join(save_dir_model, '03{}_vre_available_{}_ts.csv'.format(chr(count), geo_suffix)),
                    index=False)
                add_df_column(vre_gen.groupby(level=geo, axis=1).sum(), 'units', 'MW').to_csv(
                    os.path.join(save_dir_model, '03{}_vre_gen_{}_ts.csv'.format(chr(count + 1), geo_suffix)),
                    index=False)
                add_df_column(vre_curtailed.groupby(level=geo, axis=1).sum(), 'units', 'MW').to_csv(
                    os.path.join(save_dir_model, '03{}_vre_curtailed_{}_ts.csv'.format(chr(count + 2), geo_suffix)),
                    index=False)

                # Three different outputs
                count += 3

        av_cap_by_tech_reg_ts = p.gen_df[p.gen_df.property == 'Available Capacity'] \
            .groupby(['model', 'Category'] + s.cfg['settings']['geo_cols'] + ['timestamp']) \
            .agg({'value': 'sum'}) \
            .compute() \
            .unstack(level='Category') \
            .fillna(0)

        # Name makes no sense
        load_geo_ts = p.node_df[p.node_df.property == 'Load'].groupby(
            ['model'] + s.cfg['settings']['geo_cols'] + ['timestamp']).sum().value.compute().unstack(level=s.cfg['settings']['geo_cols'])

        # Cappacity reserves by region
        av_cap_by_reg = av_cap_by_tech_reg_ts.sum(axis=1).unstack(level=s.cfg['settings']['geo_cols'])
        # av_cap_by_reg.columns = [c.replace(' ', '_') for c in av_cap_by_reg.columns]
        res_by_reg_ts = av_cap_by_reg - load_geo_ts
        res_margin_by_reg_ts = res_by_reg_ts / load_geo_ts

        res_w_load_by_reg_ts = res_by_reg_ts.stack(s.cfg['settings']['geo_cols']).reorder_levels(
            ['model'] + s.cfg['settings']['geo_cols'] + ['timestamp']).rename(
            'CapacityReserves')
        res_w_load_by_reg_ts = pd.concat([res_w_load_by_reg_ts, load_geo_ts.stack(s.cfg['settings']['geo_cols']).reorder_levels(
            ['model'] + s.cfg['settings']['geo_cols'] + ['timestamp']).rename('Load')], axis=1)

        add_df_column(res_w_load_by_reg_ts, 'units', 'MW') \
            .to_csv(os.path.join(p.DIR_05_2_TS_OUT, '03a_cap_reserve_w_load_by_reg_ts.csv'), index=False)

        add_df_column(res_margin_by_reg_ts, 'units', '%') \
            .to_csv(os.path.join(p.DIR_05_2_TS_OUT, '03b_reserve_margin_by_reg_ts.csv'), index=False)

        print("Done.")


def create_interval_output_4():
    """
    Output 4a-b: Generation by region and capacity reserve margin by technology
    """
    print("Creating interval output 4...")

    # Output 3c: Regional generation...this is likely redundnat as the calcs are done up by CF calculations

    if s.cfg['settings']['reg_ts']:

        model_names = list(np.sort(p.reg_df.model.drop_duplicates()))
        for m in model_names:
            save_dir_model = os.path.join(p.DIR_05_2_TS_OUT, m)
            if os.path.exists(save_dir_model) is False:
                os.mkdir(save_dir_model)

            gen_by_tech_reg_model_ts = v.gen_by_tech_reg_ts.loc[ix[m,],].stack('Category')

            gen_by_subtech_reg_model_ts = v.gen_by_subtech_reg_ts.loc[ix[m,],].stack('CapacityCategory')

            add_df_column(gen_by_tech_reg_model_ts, 'units', 'MW').to_csv(
                os.path.join(save_dir_model, '04_gen_by_tech_reg_ts.csv'), index=False)
            add_df_column(gen_by_subtech_reg_model_ts, 'units', 'MW').to_csv(
                os.path.join(save_dir_model, '04_gen_by_subtech_reg_ts.csv'), index=False)
            print(f'Model {m} done.')

    # Ouput 4: Costs and savings.

    gen_cost_props_w_penalty = ['Emissions Cost', 'Fuel Cost', 'Max Energy Violation Cost',
                                'Min Energy Violation Cost', 'Ramp Down Cost', 'Ramp Up Cost', 'Start & Shutdown Cost']

    # Excludes VO&M as this is excluded for some generators because of bad scaling of the objective function
    gen_op_cost_props = ['Emissions Cost', 'Fuel Cost', 'Start & Shutdown Cost']

    # Standard costs reported as USD'000
    gen_op_costs = p.gen_yr_df[p.gen_yr_df.property.isin(gen_op_cost_props)]
    gen_op_costs_w_pen = p.gen_yr_df[p.gen_yr_df.property.isin(gen_cost_props_w_penalty)]

    # Scale costs to be also USD'000
    gen_vom = p.gen_yr_df[p.gen_yr_df.property == 'Generation'].fillna(0)
    # gen_vom.loc[:, 'value'] = gen_vom.apply(lambda x: x.value * x.VOM, axis=1).fillna(0)
    # gen_vom.loc[:, 'property'] = 'VO&M Cost'
    gen_vom.assign(value=lambda x: x.value / x.VOM)
    gen_vom.assign(property='VO&M Cost')

    gen_fom = p.gen_yr_df.loc[p.gen_yr_df.property == 'Installed Capacity', :]
    # gen_fom.loc[:, 'value'] = gen_fom.apply(lambda x: x.value * x.FOM, axis=1).fillna(0)
    # gen_fom.loc[:, 'property'] = 'FO&M Cost'
    gen_fom.assign(value=lambda x: x.value / x.FOM)
    gen_fom.assign(property='FO&M Cost')

    gen_capex = p.gen_yr_df.loc[p.gen_yr_df.property == 'Installed Capacity', :]
    # gen_capex.loc[:, 'value'] = gen_capex.apply(lambda x: x.value * x.CAPEX, axis=1).fillna(0)
    # gen_capex.loc[:, 'property'] = 'Investment Cost'
    gen_capex.assign(value=lambda x: x.value / x.CAPEX)
    gen_capex.assign(property='Investment Cost')

    gen_total_costs = dd.concat([gen_op_costs, gen_vom, gen_fom, gen_capex], axis=0)
    gen_total_costs_w_pen = dd.concat([gen_op_costs_w_pen, gen_vom, gen_fom, gen_capex], axis=0)
    gen_op_costs = dd.concat([gen_op_costs, gen_vom], axis=0)

    # Scale to USDm
    gen_op_costs_by_reg = gen_op_costs \
        .groupby(['model'] + s.cfg['settings']['geo_cols'] + ['Category', 'property']) \
        .agg({'value': 'sum'}) \
        .applymap(lambda x: x / 1e3)
    gen_total_costs_by_reg = gen_total_costs \
        .groupby(['model'] + s.cfg['settings']['geo_cols'] + ['Category', 'property']) \
        .agg({'value': 'sum'}) \
        .applymap(lambda x: x / 1e3)
    gen_total_costs_by_reg_w_pen = gen_total_costs_w_pen \
        .groupby(['model'] + s.cfg['settings']['geo_cols'] + ['Category', 'property']) \
        .agg({'value': 'sum'}) \
        .applymap(lambda x: x / 1e3)

    # TOP an other violation costs

    # Non-standard cost elements (i.e. ToP, min CF violations, etc.)
    if p.fuelcontract_yr_df.shape[0].compute() > 0:
        top_cost_reg = p.fuelcontract_yr_df[p.fuelcontract_yr_df.property == 'Take-or-Pay Violation Cost']
        top_cost_reg.assign(Category='Gas')
        top_cost_reg.assign(property='ToP Violation Cost')
        top_cost_reg = top_cost_reg \
            .groupby(['model'] + s.cfg['settings']['geo_cols'] + ['Category', 'property']) \
            .agg({'value': 'sum'}) \
            .applymap(lambda x: x / 1e3)

    else:
        top_cost_reg = pd.DataFrame(None)

    return
    # todo needs impementation, set_index is to resource intensive
    # Cost of minimum energy violation for coal contracts

    gen_min_vio_coal_reg = p.gen_yr_df[(p.gen_yr_df.property == 'Min Energy Violation') &
                                       (p.gen_yr_df.Category == 'Coal')] \
        .groupby(['model'] + s.cfg['settings']['geo_cols'] + ['Category', 'timestamp']) \
        .sum().value  # GWh
    gen_avg_price_coal_reg = p.gen_yr_df[(p.gen_yr_df.property == 'Average Cost') &
                                         (p.gen_yr_df.Category == 'Coal')] \
        .groupby(['model'] + s.cfg['settings']['geo_cols'] + ['Category', 'timestamp']) \
        .sum().value  # $/Mwh
    gen_min_vio_cost_coal_reg = gen_min_vio_coal_reg * gen_avg_price_coal_reg  # $'000
    gen_min_vio_cost_coal_reg = gen_min_vio_cost_coal_reg.reset_index()
    gen_min_vio_cost_coal_reg = gen_min_vio_cost_coal_reg.assign(property='PPA Violation Cost')
    gen_min_vio_cost_coal_reg = gen_min_vio_cost_coal_reg \
        .groupby(['model'] + s.cfg['settings']['geo_cols'] + ['Category', 'property']) \
        .agg({'value': 'sum'}) \
        .applymap(lambda x: x / 1e3)

    # Ramp costs by reg in USDm
    gen_by_name_ts = p.gen_df[p.gen_df.property == 'Generation'] \
        .set_index(['model', 'name'] + s.cfg['settings']['geo_cols'] + ['Category', 'timestamp']) \
        .value
    ramp_by_gen_name = (gen_by_name_ts - gen_by_name_ts.shift(1)).fillna(0)
    ramp_costs_by_gen_name = pd.merge(ramp_by_gen_name.reset_index(), p.soln_idx[['name', 'RampCost']], on='name',
                                      how='left').set_index(['model', 'name'] + s.cfg['settings']['geo_cols'] + ['Category', 'timestamp'])
    ramp_costs_by_gen_name.loc[:, 'value'] = (
            ramp_costs_by_gen_name.value.abs() * ramp_costs_by_gen_name.RampCost.fillna(0))
    ramp_costs_by_gen_name.loc[:, 'property'] = 'Ramp Cost'
    gen_ramp_costs_by_reg = ramp_costs_by_gen_name.reset_index().groupby(
        ['model'] + s.cfg['settings']['geo_cols'] + ['Category', 'property']).sum().value / 1e6

    # ### Final dataframes of costs
    gen_op_costs_by_reg = pd.concat([gen_op_costs_by_reg, gen_ramp_costs_by_reg], axis=0).reset_index().groupby(
        ['model'] + s.cfg['settings']['geo_cols'] + ['Category', 'property']).sum().value
    gen_op_and_vio_costs_reg = pd.concat([gen_op_costs_by_reg, top_cost_reg, gen_min_vio_cost_coal_reg],
                                         axis=0).reset_index().groupby(
        ['model'] + s.cfg['settings']['geo_cols'] + ['Category', 'property']).sum().value

    gen_total_costs_by_reg = pd.concat([gen_total_costs_by_reg, gen_ramp_costs_by_reg], axis=0).reset_index().groupby(
        ['model'] + s.cfg['settings']['geo_cols'] + ['Category', 'property']).sum().value
    # this is different from the vio costs as it includes actual penalty costs as per the model (which are much higher
    # than real violation costs due to modelling considerations)
    gen_total_costs_by_reg_w_pen = pd.concat([gen_total_costs_by_reg_w_pen, gen_ramp_costs_by_reg],
                                             axis=0).reset_index().groupby(
        ['model'] + s.cfg['settings']['geo_cols'] + ['Category', 'property']).sum().value

    # USDm/GWh = USD'000/MWh
    gen_by_tech_reg = p.gen_yr_df[p.gen_yr_df.property == 'Generation']

    lcoe_by_tech_reg = (gen_total_costs_by_reg.groupby(['model'] + s.cfg['settings']['geo_cols'] + ['Category']).sum().unstack(
        s.cfg['settings']['geo_cols']).fillna(0) / gen_by_tech_reg).fillna(0).stack(s.cfg['settings']['geo_cols']).reorder_levels(
        ['model'] + s.cfg['settings']['geo_cols'] + ['Category']) * 1000
    lcoe_tech = gen_total_costs_by_reg.groupby(['model', 'Category']).sum() / gen_by_tech_reg.stack(s.cfg['settings']['geo_cols']).groupby(
        ['model', 'Category']).sum() * 1000

    add_df_column(gen_op_costs_by_reg, 'units', 'USDm') \
        .to_csv(os.path.join(p.DIR_05_2_TS_OUT, '04a_gen_op_costs_reg.csv'), index=False)
    add_df_column(gen_op_and_vio_costs_reg, 'units', 'USDm') \
        .to_csv(os.path.join(p.DIR_05_2_TS_OUT, '04b_gen_op_and_vio_costs_reg.csv'), index=False)
    add_df_column(gen_total_costs_by_reg, 'units', 'USDm') \
        .to_csv(os.path.join(p.DIR_05_2_TS_OUT, '04c_gen_total_costs_reg.csv'), index=False)
    add_df_column(gen_total_costs_by_reg_w_pen, 'units', 'USDm') \
        .to_csv(os.path.join(p.DIR_05_2_TS_OUT, '04d_gen_total_costs_reg_w_penalty_costs.csv'), index=False)
    add_df_column(lcoe_by_tech_reg, 'units', 'USD/MWh') \
        .to_csv(os.path.join(p.DIR_05_2_TS_OUT, '04e_lcoe_tech_reg.csv'), index=False)
    add_df_column(lcoe_tech, 'units', 'USD/MWh') \
        .to_csv(os.path.join(p.DIR_05_2_TS_OUT, '04f_lcoe_tech.csv'), index=False)

    print("Done.")


def create_interval_output_5():
    """
    Output 5: to plot undispatched capacity for USE (and maybe other metrics)
    """
    print('Creating interval output 5 is not implemented yet.')
    return


def create_interval_output_6():
    """
    TODO DOCSTRING
    """
    # todo this one is to wild right now
    print('Creating interval output 6 is not implemented yet.')
    return


def create_interval_output_7():
    """
    Output 7 : Daily gap between peak and off-peak net load

    This could be future proofed with using the geo_col/suffix approach....
    """
    print("Creating interval output 7...")

    customer_load_reg_ts = p.node_df[(p.node_df.property == 'Customer Load') |
                                     (p.node_df.property == 'Unserved Energy')] \
        .groupby(['model'] + s.cfg['settings']['geo_cols'] + ['timestamp']) \
        .agg({'value': 'sum'}) \
        .compute() \
        .unstack(level=s.cfg['settings']['geo_cols'])

    vre_av_reg_abs_ts = p.gen_df[(p.gen_df.property == 'Available Capacity') &
                                 (p.gen_df.Category.isin(VRE_TECHS))] \
        .groupby((['model'] + s.cfg['settings']['geo_cols'] + ['timestamp'])) \
        .agg({'value': 'sum'}) \
        .compute() \
        .unstack(level=s.cfg['settings']['geo_cols']) \
        .fillna(0)

    net_load_reg_ts = customer_load_reg_ts - vre_av_reg_abs_ts

    nl_max_dly_reg = net_load_reg_ts \
        .groupby('Region', axis=1) \
        .sum() \
        .groupby(['model', pd.Grouper(level='timestamp', freq='D')]) \
        .max()
    nl_max_dly_subreg = net_load_reg_ts \
        .groupby('Subregion', axis=1) \
        .sum() \
        .groupby(['model', pd.Grouper(level='timestamp', freq='D')]) \
        .max()
    nl_max_dly_isl = net_load_reg_ts \
        .groupby('Island', axis=1) \
        .sum() \
        .groupby(['model', pd.Grouper(level='timestamp', freq='D')]) \
        .max()
    nl_max_dly = net_load_reg_ts \
        .sum(axis=1) \
        .groupby(['model', pd.Grouper(level='timestamp', freq='D')]) \
        .max()

    nl_min_dly_reg = net_load_reg_ts \
        .groupby('Region', axis=1) \
        .sum() \
        .groupby(['model', pd.Grouper(level='timestamp', freq='D')]) \
        .min()
    nl_min_dly_subreg = net_load_reg_ts \
        .groupby('Subregion', axis=1) \
        .sum() \
        .groupby(['model', pd.Grouper(level='timestamp', freq='D')]) \
        .min()
    nl_min_dly_isl = net_load_reg_ts \
        .groupby('Island', axis=1) \
        .sum() \
        .groupby(['model', pd.Grouper(level='timestamp', freq='D')]) \
        .min()
    nl_min_dly = net_load_reg_ts \
        .sum(axis=1) \
        .groupby(['model', pd.Grouper(level='timestamp', freq='D')]) \
        .min()

    nl_dly_gap_reg = nl_max_dly_reg - nl_min_dly_reg
    nl_dly_gap_subreg = nl_max_dly_subreg - nl_min_dly_subreg
    nl_dly_gap_isl = nl_max_dly_isl - nl_min_dly_isl
    nl_dly_gap = nl_max_dly - nl_min_dly

    nl_dly_gap_reg_pc = (nl_max_dly_reg - nl_min_dly_reg) / nl_max_dly_reg
    nl_dly_gap_subreg_pc = (nl_max_dly_subreg - nl_min_dly_subreg) / nl_max_dly_subreg
    nl_dly_gap_isl_pc = (nl_max_dly_isl - nl_min_dly_isl) / nl_max_dly_isl
    nl_dly_gap_pc = (nl_max_dly - nl_min_dly) / nl_max_dly

    add_df_column(nl_dly_gap_reg, 'units', 'MW') \
        .to_csv(os.path.join(p.DIR_05_2_TS_OUT, '07a_dly_gap_reg.csv'), index=False)
    add_df_column(nl_dly_gap_subreg, 'units', 'MW') \
        .to_csv(os.path.join(p.DIR_05_2_TS_OUT, '07b_dly_gap_subreg.csv'), index=False)
    add_df_column(nl_dly_gap_isl, 'units', 'MW') \
        .to_csv(os.path.join(p.DIR_05_2_TS_OUT, '07c_dly_gap_isl.csv'), index=False)
    add_df_column(nl_dly_gap, 'units', 'MW') \
        .to_csv(os.path.join(p.DIR_05_2_TS_OUT, '07d_dly_gap.csv'), index=False)
    add_df_column(nl_dly_gap_reg_pc, 'units', 'MW') \
        .to_csv(os.path.join(p.DIR_05_2_TS_OUT, '07e_dly_gap_reg_pc.csv'), index=False)
    add_df_column(nl_dly_gap_subreg_pc, 'units', 'MW') \
        .to_csv(os.path.join(p.DIR_05_2_TS_OUT, '07f_dly_gap_subreg_pc.csv'), index=False)
    add_df_column(nl_dly_gap_isl_pc, 'units', 'MW') \
        .to_csv(os.path.join(p.DIR_05_2_TS_OUT, '07g_dly_gap_isl_pc.csv'), index=False)
    add_df_column(nl_dly_gap_pc, 'units', 'MW') \
        .to_csv(os.path.join(p.DIR_05_2_TS_OUT, '07h_dly_gap_pc.csv'), index=False)

    print("Done.")


def create_interval_output_8i():
    """
    # ### Output 8: Inertia (total)
    TODO DOCSTRING
    """
    print("Creating interval output 8i)...")

    inertia_by_tech = v.gen_inertia.groupby(['model', 'Category', 'timestamp']).sum()
    inertia_by_isl = v.gen_inertia.groupby(['model', 'Island', 'timestamp']).sum()
    inertia_by_reg = v.gen_inertia.groupby(['model'] + s.cfg['settings']['geo_cols'] + ['timestamp']).sum()
    total_inertia_ts = v.gen_inertia.groupby(['model', 'timestamp']).sum()

    add_df_column(inertia_by_tech, 'units', 'MWs') \
        .to_csv(os.path.join(p.DIR_05_2_TS_OUT, '08a_inertia_by_tech_ts.csv'), index=False)
    add_df_column(inertia_by_isl, 'units', 'MWs') \
        .to_csv(os.path.join(p.DIR_05_2_TS_OUT, '08b_inertia_by_isl_ts.csv'), index=False)
    add_df_column(inertia_by_reg, 'units', 'MWs') \
        .to_csv(os.path.join(p.DIR_05_2_TS_OUT, '08c_inertia_by_reg_ts.csv'), index=False)
    add_df_column(total_inertia_ts, 'units', 'MWs') \
        .to_csv(os.path.join(p.DIR_05_2_TS_OUT, '08d_total_inertia_ts.csv'), index=False)
    print("Done.")

def create_interval_output_8ii():
    """
    TODO DOCSTRING
    # Output 9: Ramp time-series
    """
    print("Creating interval output 8ii)...")
    customer_load_ts = p.reg_df[(p.reg_df.property == 'Customer Load') | (p.reg_df.property == 'Unserved Energy')] \
        .groupby(['model', 'timestamp']) \
        .agg({'value': 'sum'})
    vre_av_abs_ts = p.gen_df[(p.gen_df.property == 'Available Capacity') & (p.gen_df.Category.isin(VRE_TECHS))] \
        .groupby(['model', 'Category', 'timestamp']) \
        .agg({'value': 'sum'}) \
        .compute() \
        .unstack(level='Category') \
        .fillna(0)
    net_load_ts = pd.DataFrame(
        customer_load_ts.compute().value - vre_av_abs_ts.fillna(0).sum(axis=1).groupby(['model', 'timestamp']).sum(),
        columns=['value'])
    ramp_ts = (net_load_ts.unstack(level='model') - net_load_ts.unstack(level='model').shift(1)) \
        .fillna(0) \
        .stack() \
        .sort_index(level=1) \
        .reset_index() \
        .set_index(['model', 'timestamp']) \
        .rename(columns={0: 'value'})

    th_ramp_ts = (net_load_ts.unstack(level='model') - net_load_ts.unstack(level='model').shift(3)) \
        .fillna(0) \
        .stack() \
        .sort_index(level=1) \
        .reset_index() \
        .set_index(['model', 'timestamp']) \
        .rename(columns={0: 'value'})

    customer_load_ts = customer_load_ts.compute()
    #########
    daily_pk_ts = customer_load_ts \
        .groupby(['model', pd.Grouper(level='timestamp', freq='D')]) \
        .max() \
        .reindex(customer_load_ts.index) \
        .ffill()
    ramp_pc_ts = ramp_ts.value / daily_pk_ts.value * 100
    th_ramp_pc_ts = th_ramp_ts.value / daily_pk_ts.value * 100
    gen_by_tech_ts = p.gen_df[p.gen_df.property == 'Generation'] \
        .groupby(['model', 'Category', 'timestamp']) \
        .agg({'value': 'sum'}) \
        .compute() \
        .unstack(level='Category') \
        .fillna(0)
    gen_by_subtech_ts = p.gen_df[p.gen_df.property == 'Generation'] \
        .groupby(['model', 'CapacityCategory', 'timestamp']) \
        .agg({'value': 'sum'}) \
        .compute() \
        .unstack(level='CapacityCategory') \
        .fillna(0)

    ########
    ramp_by_gen_tech_ts = (gen_by_tech_ts - gen_by_tech_ts.shift(1)).fillna(0)
    ramp_by_gen_subtech_ts = (gen_by_subtech_ts - gen_by_subtech_ts.shift(1)).fillna(0)

    th_ramp_by_gen_tech_ts = (gen_by_tech_ts - gen_by_tech_ts.shift(3)).fillna(0)
    th_ramp_by_gen_subtech_ts = (gen_by_subtech_ts - gen_by_subtech_ts.shift(3)).fillna(0)
    #####

    add_df_column(ramp_ts, 'units', 'MW.hr-1') \
        .to_csv(os.path.join(p.DIR_05_2_TS_OUT, '08a_ramp_ts.csv'), index=False)
    add_df_column(th_ramp_ts, 'units', 'MW.hr-1') \
        .to_csv(os.path.join(p.DIR_05_2_TS_OUT, '08b_3hr_ramp_ts.csv'), index=False)
    add_df_column(ramp_pc_ts, 'units', 'MW.hr-1') \
        .to_csv(os.path.join(p.DIR_05_2_TS_OUT, '08c_ramp_pc_ts.csv'), index=False)
    add_df_column(th_ramp_pc_ts, 'units', 'MW.hr-1') \
        .to_csv(os.path.join(p.DIR_05_2_TS_OUT, '08d_3hr_ramp_pc_ts.csv'), index=False)

    ramp_by_gen_tech_ts = ramp_by_gen_tech_ts.droplevel(0, axis=1)
    ramp_by_gen_subtech_ts = ramp_by_gen_subtech_ts.droplevel(0, axis=1)
    th_ramp_by_gen_tech_ts = th_ramp_by_gen_tech_ts.droplevel(0, axis=1)
    th_ramp_by_gen_subtech_ts = th_ramp_by_gen_subtech_ts.droplevel(0, axis=1)

    add_df_column(ramp_by_gen_tech_ts, 'units', 'MW.hr-1') \
        .to_csv(os.path.join(p.DIR_05_2_TS_OUT, '08e_ramp_by_gen_tech_ts.csv'), index=False)
    add_df_column(ramp_by_gen_subtech_ts, 'units', 'MW.hr-1') \
        .to_csv(os.path.join(p.DIR_05_2_TS_OUT, '08f_ramp_by_gen_subtech_ts.csv'), index=False)

    add_df_column(th_ramp_by_gen_tech_ts, 'units', 'MW.hr-1') \
        .to_csv(os.path.join(p.DIR_05_2_TS_OUT, '08g_3hr_ramp_by_gen_tech_ts.csv'), index=False)
    add_df_column(th_ramp_by_gen_subtech_ts, 'units', 'MW.hr-1') \
        .to_csv(os.path.join(p.DIR_05_2_TS_OUT, '08h_3hr_ramp_by_gen_subtech_ts.csv'), index=False)

    print("Done.")

def create_interval_output_10():
    """
    TODO DOCSTRING
    ### Output 10: Generation out-of-service
    ### Its noted that units out was done per generator. Unsure why, but this may be a more useful output
    """


    gen_out_tech_ts = p.gen_df[(p.gen_df.property == 'Forced Outage') |
                               (p.gen_df.property == 'Maintenance')]\
        .groupby(['model', 'Category', 'timestamp'])\
        .agg({'value': 'sum'})\
        .compute()\
        .unstack(level='Category')

    gen_out_by_type_ts = p.gen_df[(p.gen_df.property == 'Forced Outage') | \
                                  (p.gen_df.property == 'Maintenance')]\
        .groupby(['model', 'property', 'timestamp'])\
        .agg({'value': 'sum'})\
        .compute()\
        .unstack(level='property')

    gen_out_tech_ts = gen_out_tech_ts.droplevel(0, axis=1)
    gen_out_by_type_ts = gen_out_by_type_ts.droplevel(0, axis=1)

    add_df_column(gen_out_tech_ts, 'units', 'MW')\
        .to_csv(os.path.join(p.DIR_05_2_TS_OUT, '10a_outages_by_tech_ts.csv'), index=False)
    add_df_column(gen_out_by_type_ts, 'units', 'MW')\
        .to_csv(os.path.join(p.DIR_05_2_TS_OUT, '10b_outages_by_outtype_ts.csv'), index=False)