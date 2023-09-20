"""
TODO DOCSTRING
"""

import os

import pandas as pd
import dask.dataframe as dd
import numpy as np

from .properties import properties as p
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

    load_by_reg = p.node_yr_df[p.node_yr_df.property == 'Load'].groupby(
        ['model', 'timestamp'] + s.cfg['settings']['geo_cols']).agg({'value': 'sum'})
    customer_load_by_reg = p.node_yr_df[
        (p.node_yr_df.property == 'Customer Load') | (
                p.node_yr_df.property == 'Unserved Energy')].groupby(
        ['model', 'timestamp'] + s.cfg['settings']['geo_cols']).agg({'value': 'sum'})

    load_by_reg = load_by_reg.compute()  # Change dd.DataFrame back to pd.DataFrame
    customer_load_by_reg = customer_load_by_reg.compute()  # Change dd.DataFrame back to pd.DataFrame

    os.makedirs(p.DIR_05_1_SUMMARY_OUT, exist_ok=True)
    add_df_column(load_by_reg, 'units', 'GWh').to_csv(
        os.path.join(p.DIR_05_1_SUMMARY_OUT, '01a_load_by_reg.csv'),
        index=False)
    add_df_column(customer_load_by_reg, 'units', 'GWh').to_csv(
        os.path.join(p.DIR_05_1_SUMMARY_OUT, '01b_customer_load_by_reg.csv'), index=False)

    print('Done.')


def create_year_output_2():
    """
    ### Output 2: USE
    """
    print('Creating output 2...')
    _node_yr_df = p.node_yr_df.compute()  # Change dd.DataFrame back to pd.DataFrame

    use_by_reg = _node_yr_df[_node_yr_df.property == 'Unserved Energy'].groupby(
        ['model'] + s.cfg['settings']['geo_cols']).agg({'value': 'sum'})
    use_reg_daily_ts = _node_yr_df[_node_yr_df.property == 'Unserved Energy'].groupby(
        ['model'] + s.cfg['settings']['geo_cols'] + [pd.Grouper(key='timestamp', freq='D')]).agg({'value': 'sum'})

    add_df_column(use_by_reg, 'units', 'GWh').to_csv(os.path.join(p.DIR_05_1_SUMMARY_OUT, '02a_use_reg.csv'),
                                                     index=False)
    add_df_column(use_reg_daily_ts, 'units', 'GWh').to_csv(
        os.path.join(p.DIR_05_1_SUMMARY_OUT, '02b_use_reg_daily_ts.csv'),
        index=False)

    print('Done.')


def create_year_output_3():
    print('Creating output 3 is not implemented yet.')
    return
    print('Creating output 3...')
    # ### Output 3a:
    #
    # ## Standard
    gen_by_tech_reg = p.gen_yr_df[p.gen_yr_df.property == 'Generation']
    gen_by_tech_reg_orig = p.gen_yr_df[
        p.gen_yr_df.property == 'Generation']  # For not separating cofiring. good for CF comparison
    gen_techs = p.gen_yr_df.Category.drop_duplicates().values
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
    # gen_by_plant = gen_by_tech_reg.groupby(['model', 'name']).agg({'value': 'sum'})..unstack(level='model').fillna(0)
    gen_by_costTech_reg = gen_by_tech_reg.groupby(['model'] + GEO_COLS + ['CostCategory']).agg({'value': 'sum'}).unstack(
        level=GEO_COLS).fillna(0)
    # gen_by_tech_reg = gen_by_tech_reg.groupby(['model'] + GEO_COLS + ['Category']).agg({'value': 'sum'})..unstack(
    #     level=GEO_COLS).fillna(0)
    # gen_by_tech_reg_orig = gen_by_tech_reg_orig.groupby(['model'] + GEO_COLS + ['Category']).agg({'value': 'sum'})..unstack(
    #     level=GEO_COLS).fillna(0)
    # gen_by_weoTech_reg = p.gen_yr_df[p.gen_yr_df.property == 'Generation'].groupby(
    #     ['model'] + GEO_COLS + ['WEO_Tech_simpl']).agg({'value': 'sum'})..unstack(level=GEO_COLS).fillna(0)
    #
    # if validation:
    #     ### This needs to be fixed to the region/subregion change
    #     idn_actuals_by_tech_reg = p.idn_actuals_2019.groupby(
    #         ['model'] + GEO_COLS + ['Category']).sum().Generation.unstack(level=GEO_COLS).fillna(0)
    #     gen_by_tech_reg = pd.concat([gen_by_tech_reg, idn_actuals_by_tech_reg], axis=0)
    #
    # add_df_column(gen_by_tech_reg.stack(GEO_COLS), 'units', 'GWh').to_csv(
    #     os.path.join(p.DIR_05_1_SUMMARY_OUT, '03a_gen_by_tech_reg.csv'), index=False)
    # add_df_column(gen_by_tech_reg_orig.stack(GEO_COLS), 'units', 'GWh').to_csv(
    #     os.path.join(p.DIR_05_1_SUMMARY_OUT, '03a_gen_by_tech_reg_orig.csv'), index=False)
    # add_df_column(gen_by_tech_reg.stack(GEO_COLS), 'units', 'GWh').to_csv(
    #     os.path.join(p.DIR_05_1_SUMMARY_OUT, '03b_gen_by_tech_reg_w_Validation.csv'), index=False)
    # add_df_column(gen_by_costTech_reg.stack(GEO_COLS), 'units', 'GWh').to_csv(
    #     os.path.join(p.DIR_05_1_SUMMARY_OUT, '03c_gen_by_costTech_reg.csv'), index=False)
    # add_df_column(gen_by_weoTech_reg.stack(GEO_COLS), 'units', 'GWh').to_csv(
    #     os.path.join(p.DIR_05_1_SUMMARY_OUT, '03d_gen_by_weoTech_reg.csv'), index=False)
    # add_df_column(gen_by_plant, 'units', 'GWh').to_csv(
    #     os.path.join(p.DIR_05_1_SUMMARY_OUT, '03e_gen_by_plants.csv'),
    #     index=False)
    # # add_df_column(gen_by_tech_subreg.stack(), 'units', 'GWh').to_csv(os.path.join(save_dir_sum, '04b_gen_by_tech_subreg.csv'), index=False)
    # # add_df_column(gen_by_tech_isl.stack(), 'units', 'GWh').to_csv(os.path.join(save_dir_sum, '04c_gen_by_tech_isl.csv'), index=False)

    ### Output 3.0: Output by plant!

    ## Standard
    gen_by_plant = p.gen_yr_df[p.gen_yr_df.property == 'Generation']
    gen_by_plant_orig = p.gen_yr_df[
        p.gen_yr_df.property == 'Generation']  ### For not separating cofiring. good for CF comparison
    gen_techs = p.gen_yr_df.Category.drop_duplicates().values

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
    gen_by_weoTech_reg = p.gen_yr_df[p.gen_yr_df.property == 'Generation'].groupby(
        ['model'] + GEO_COLS + ['WEO_Tech_simpl']).agg({'value': 'sum'}).unstack(level=GEO_COLS).fillna(0)

    if validation:
        ### This needs to be fixed to the region/subregion change
        idn_actuals_by_tech_reg = idn_actuals_2019.groupby(
            ['model'] + GEO_COLS + ['Category']).agg({'Generation': 'sum'}).unstack(level=GEO_COLS).fillna(0)
        gen_by_tech_reg = pd.concat([gen_by_tech_reg, idn_actuals_by_tech_reg], axis=0)

    # try:
    #     add_df_column(gen_by_tech_reg.stack(GEO_COLS), 'units', 'GWh').to_csv(
    #         os.path.join(p.DIR_05_1_SUMMARY_OUT, '03a_gen_by_tech_reg.csv'), index=False)
    #     add_df_column(gen_by_tech_reg_orig.stack(GEO_COLS), 'units', 'GWh').to_csv(
    #         os.path.join(p.DIR_05_1_SUMMARY_OUT, '03a_gen_by_tech_reg_orig.csv'), index=False)
    #     add_df_column(gen_by_tech_reg.stack(GEO_COLS), 'units', 'GWh').to_csv(
    #         os.path.join(p.DIR_05_1_SUMMARY_OUT, '03b_gen_by_tech_reg_w_Validation.csv'), index=False)
    #     add_df_column(gen_by_costTech_reg.stack(GEO_COLS), 'units', 'GWh').to_csv(
    #         os.path.join(p.DIR_05_1_SUMMARY_OUT, '03c_gen_by_costTech_reg.csv'), index=False)
    #     add_df_column(gen_by_weoTech_reg.stack(GEO_COLS), 'units', 'GWh').to_csv(
    #         os.path.join(p.DIR_05_1_SUMMARY_OUT, '03d_gen_by_weoTech_reg.csv'), index=False)
    # #     add_df_column(gen_by_tech_subreg.stack(), 'units', 'GWh').to_csv(os.path.join(save_dir_sum, '04b_gen_by_tech_subreg.csv'), index=False)
    # #     add_df_column(gen_by_tech_isl.stack(), 'units', 'GWh').to_csv(os.path.join(save_dir_sum, '04c_gen_by_tech_isl.csv'), index=False)
    # except PermissionError:
    #     print("Permission error: file not written")

    # todo Stuff until here does not work

    ### Output 3b: RE/VRE Shares:

    gen_techs = list(p.soln_idx[p.soln_idx.Object_type == 'Generator'].Category.drop_duplicates())
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

    add_df_column(re_by_isl, 'units', '%').to_csv(os.path.join(p.DIR_05_1_SUMMARY_OUT, '03b_re_by_isl.csv'),
                                                  index=False)
    add_df_column(vre_by_isl, 'units', '%').to_csv(os.path.join(p.DIR_05_1_SUMMARY_OUT, '03c_vre_by_isl.csv'),
                                                   index=False)

def create_year_output_4():
    pass

def create_year_output_5():
    print("Creating output 5...", end=" ")

    unit_starts_by_tech = p.gen_yr_df[p.gen_yr_df.property == 'Units Started'].groupby(
        ['model', 'Category']).agg({'value': 'sum'})
    # Change back to pd.DataFrame
    unit_starts_by_tech = unit_starts_by_tech.compute()  # Change dd.DataFrame back to pd.DataFrame
    unit_starts_by_tech = unit_starts_by_tech.value.unstack(level='Category')

    add_df_column(unit_starts_by_tech.stack(), 'units', 'starts').to_csv(
        os.path.join(p.DIR_05_1_SUMMARY_OUT, '05_unit_starts_by_tech.csv'), index=False)
    print("Done.")

def create_year_output_6():
    print("Creating output 6...", end=" ")

    # Standard
    gen_max_by_tech_reg = p.gen_df[p.gen_df.property == 'Generation'] \
        .groupby(['model'] + config['settings']['geo_cols'] + ['Category']) \
        .agg({'value': 'max'}) \
        .compute() \
        .unstack(level=config['settings']['geo_cols']) \
        .fillna(0)

    add_df_column(gen_max_by_tech_reg.stack(config['settings']['geo_cols']), 'units', 'MW').to_csv(
        os.path.join(p.DIR_05_1_SUMMARY_OUT, '06a_gen_max_by_tech_reg.csv'), index=False)

    print("Done.")


def create_year_output_7():
    print("Creating output 7...", end=" ")

    tx_losses = p.line_yr_df[p.line_yr_df.property == 'Loss'] \
        .groupby(['model', 'timestamp', 'name']).agg({'value': 'sum'})
    tx_losses = tx_losses.compute()  # Change dd.DataFrame back to pd.DataFrame
    add_df_column(tx_losses, 'units', 'GWh').to_csv(os.path.join(p.DIR_05_1_SUMMARY_OUT, '07_tx_losses.csv'),
                                                    index=False)

    print("Done.")


def create_year_output_8():
    print("Creating output 8...", end=" ")
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
        .groupby(['model', 'timestamp'] + GEO_COLS) \
        .agg({'value': 'sum'})

    vre_cap = p.gen_yr_df[(p.gen_yr_df.property == 'Installed Capacity') &
                                  (p.gen_yr_df.Category.isin(VRE_TECHS))] \
        .groupby(['model', 'Category'] + GEO_COLS) \
        .agg({'value': 'max'}) \
        .compute() \
        .unstack('Category') \
        .fillna(0) \
        .stack('Category') \
        .unstack(level=GEO_COLS) \
        .fillna(0)

    vre_av_abs = p.gen_df[(p.gen_df.property == 'Available Capacity') &
                                  (p.gen_df.Category.isin(VRE_TECHS))] \
        .assign(timestamp=dd.to_datetime(p.gen_df['timestamp']).dt.floor('D')) \
        .groupby(['model', 'Category'] + GEO_COLS + ['timestamp']) \
        .agg({'value': 'sum'}) \
        .compute() \
        .unstack('Category') \
        .fillna(0) \
        .stack('Category') \
        .unstack(level=GEO_COLS) \
        .fillna(0) \
        .apply(lambda x: x * hour_corr)

    # ### Add zero values to regions without VRE
    geo_col_filler = pd.Series(data=np.ones(len(load_by_reg.compute().unstack(GEO_COLS).columns)),
                               index=load_by_reg.compute().unstack(GEO_COLS).columns)
    vre_cap = (vre_cap * geo_col_filler).fillna(0)
    vre_av_abs = (vre_av_abs * geo_col_filler).fillna(0)

    # 24 periods per day for the daily data
    vre_av_norm = (vre_av_abs / vre_cap / daily_periods).fillna(0)

    add_df_column(vre_cap.stack(GEO_COLS), 'units', 'MW').to_csv(
        os.path.join(p.DIR_05_1_SUMMARY_OUT, '08a_vre_cap.csv'),
        index=False)
    add_df_column(vre_av_abs.stack(GEO_COLS), 'units', 'GWh').to_csv(
        os.path.join(p.DIR_05_1_SUMMARY_OUT, '08b_vre_daily_abs.csv'), index=False)
    add_df_column(vre_av_norm.stack(GEO_COLS), 'units', '-').to_csv(
        os.path.join(p.DIR_05_1_SUMMARY_OUT, '08c_vre_daily_norm.csv'), index=False)

    print("Done.")

def create_year_output_9():
    print("Creating output 9...", end=" ")
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

    add_df_column(vre_curtailed.stack(GEO_COLS), 'units', 'GWh').to_csv(
        os.path.join(p.DIR_05_1_SUMMARY_OUT, '09a_vre_daily_curtailed.csv'), index=False)
    add_df_column(curtailment_rate, 'units', '%').to_csv(
        os.path.join(p.DIR_05_1_SUMMARY_OUT, '09b_curtailment_rate.csv'),
        index=False)
    add_df_column(all_re_curtailed.stack(GEO_COLS), 'units', 'GWh').to_csv(
        os.path.join(p.DIR_05_1_SUMMARY_OUT, '09c_all_RE_daily_curtailed.csv'), index=False)
    add_df_column(re_curtailment_rate, 'units', '%').to_csv(
        os.path.join(p.DIR_05_1_SUMMARY_OUT, '09d_all_RE_curtailment_rate.csv'), index=False)

    print("Done.")

def create_year_output_10():
    print("Creating output 10...", end=" ")
    # Output 10: a) Line flows/capacity per line/interface c) Line flow time-series per interface
    # (as % of capacity?)

    line_cap = p.line_yr_df[(p.line_yr_df.property == 'Import Limit') | \
                                    (p.line_yr_df.property == 'Export Limit')]\
        .groupby(['model', 'nodeFrom', 'nodeTo', 'islFrom', 'islTo', 'property'])\
        .agg({'value': 'sum'})\
        .compute()\
        .unstack(level='property')

    line_imp_exp = p.line_yr_df[(p.line_yr_df.property == 'Flow') | \
                                        (p.line_yr_df.property == 'Flow Back')]\
        .groupby(['model', 'nodeFrom', 'nodeTo', 'islFrom', 'islTo', 'property'])\
        .agg({'value': 'sum'})\
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
    print("Creating interval output 1...", end=" ")
    ### Output 1a-b: Load and USE time-series
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

    ## Need to calculate whether its 30mn or 1hr within but for now just assume hourly
    use_dly_ts = p.reg_df[p.reg_df.property == 'Unserved Energy'] \
        .assign(timestamp=dd.to_datetime(p.reg_df['timestamp']).dt.floor('D')) \
        .groupby(['model', 'timestamp']) \
        .sum() \
        .applymap(lambda x: x / 1000 if isinstance(x, float) else x)

    add_df_column(use_dly_ts, 'units', 'GWh').reset_index().compute().to_csv(
        os.path.join(p.DIR_05_2_TS_OUT, '01c_use_dly_ts.csv'))

    # load_w_use_ts = dd.concat([total_load_ts.rename('Load'), use_ts.rename('USE')])
    #
    # add_df_column(load_w_use_ts, 'units', 'MW').reset_index().compute().to_csv(
    #     os.path.join(p.DIR_05_2_TS_OUT, '01d_load_w_use_ts.csv'))

    # p._dev_test_output('01d_load_w_use_ts.csv')
    print('Done.')

    if reg_ts:
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
    ### Output 2: Generation

    gen_by_tech_ts = p.gen_df[p.gen_df.property == 'Generation'] \
        .groupby(['model', 'Category', 'timestamp']) \
        .agg({'value': 'sum'}) \
        .compute() \
        .unstack(level='Category') \
        .fillna(0)

    gen_by_subtech_ts = p.gen_df[p.gen_df.property == 'Generation']\
        .groupby(['model', 'CapacityCategory', 'timestamp'])\
        .agg({'value': 'sum'})\
        .compute()\
        .unstack(level='CapacityCategory')\
        .fillna(0)

    av_cap_by_tech_ts = p.gen_df[p.gen_df.property == 'Available Capacity']\
        .groupby(['model', 'Category', 'timestamp'])\
        .agg({'value': 'sum'})\
        .compute()\
        .unstack(level='Category')\
        .fillna(0)

    add_df_column(gen_by_tech_ts, 'units', 'MW')\
        .to_csv(os.path.join(p.DIR_05_2_TS_OUT, '02a_gen_by_tech_ts.csv'), index=False)

    add_df_column(gen_by_subtech_ts, 'units', 'MW')\
        .to_csv(os.path.join(p.DIR_05_2_TS_OUT, '02b_gen_by_tech_ts.csv'), index=False)

    add_df_column(av_cap_by_tech_ts, 'units', 'MW')\
        .to_csv(os.path.join(p.DIR_05_2_TS_OUT, '02c_av_cap_by_subtech_ts.csv'), index=False)

    print("Done.")
