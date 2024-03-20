"""
# DOCSTRING update after splitting into multiple files
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
# DOCSTRING needs list with short description and better docstrings for all functions
"""

import os

import pandas as pd

from .utils.utils import catch_errors
from . import log

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

    os.makedirs(c.DIR_05_1_SUMMARY_OUT, exist_ok=True)

    (c.v.load_by_reg.assign(units='GWh')
     .reset_index()
     .sort_values(by=['model'])
     .to_csv(os.path.join(c.DIR_05_1_SUMMARY_OUT, '01a_load_by_reg.csv'), index=False))
    (c.v.customer_load_by_reg.assign(units='GWh')
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

    c.v.use_by_reg.assign(units='GWh').reset_index() \
        .to_csv(os.path.join(c.DIR_05_1_SUMMARY_OUT, '02a_use_reg.csv'), index=False)
    c.v.use_reg_daily_ts.assign(units='GWh').reset_index() \
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

    (c.v.gen_by_tech_reg
     .stack(c.GEO_COLS)
     .rename('value')
     .to_frame()
     .assign(units='GWh')
     .to_csv(os.path.join(c.DIR_05_1_SUMMARY_OUT, '03a_gen_by_tech_reg.csv'), index=True))
    (c.v.gen_by_tech_reg_orig
     .stack(c.GEO_COLS)
     .rename('value')
     .to_frame()
     .assign(units='GWh')
     .to_csv(os.path.join(c.DIR_05_1_SUMMARY_OUT, '03a_gen_by_tech_reg_orig.csv'), index=True))
    (c.v.gen_by_costTech_reg
     .stack(c.GEO_COLS)
     .rename('value')
     .to_frame()
     .assign(units='GWh')
     .to_csv(os.path.join(c.DIR_05_1_SUMMARY_OUT, '03c_gen_by_costTech_reg.csv'), index=True))
    (c.v.gen_by_weoTech_reg
     .stack(c.GEO_COLS)
     .rename('value')
     .to_frame()
     .assign(units='GWh')
     .to_csv(os.path.join(c.DIR_05_1_SUMMARY_OUT, '03d_gen_by_weoTech_reg.csv'), index=True))
    (c.v.gen_by_plant
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
RE/VRE Shares:

    """
    print("Creating output 4...")

    c.v.re_by_reg.assign(units='-').reset_index().to_csv(
        os.path.join(c.DIR_05_1_SUMMARY_OUT, '04_re_by_reg.csv'), index=False)
    c.v.vre_by_reg.assign(units='-').reset_index().to_csv(
        os.path.join(c.DIR_05_1_SUMMARY_OUT, '04_vre_by_reg.csv'), index=False)

    print('Created file 04_re_by_reg.csv.')
    print('Created file 04_vre_by_reg.csv.')


@catch_errors
def create_output_5(c):
    """
    Creates following output files:
    - 05_unit_starts_by_tech.csv
    """
    print("Creating output 5...")

    c.v.unit_starts_by_tech.stack() \
            .rename('value') \
            .to_frame() \
            .assign(units='starts') \
            .reset_index() \
            .to_csv(os.path.join(c.DIR_05_1_SUMMARY_OUT, '05_unit_starts_by_tech.csv'), index=False)

    print('Created file 05_unit_starts_by_tech.csv.')


@catch_errors
def create_output_6(c):
    """
    Creates following output files:
    - 06a_gen_max_by_tech_reg.csv
    """
    print("Creating output 6...")

    c.v.gen_max_by_tech_reg.stack(c.GEO_COLS) \
        .rename('value') \
        .to_frame() \
        .assign(units='MW') \
        .reset_index() \
        .to_csv( os.path.join(c.DIR_05_1_SUMMARY_OUT, '06a_gen_max_by_tech_reg.csv'), index=False)

    print('Created file 06a_gen_max_by_tech_reg.csv.')


@catch_errors
def create_output_7(c):
    """
    Creates following output files:
    - 07_tx_losses.csv
    """
    print("Creating output 7...")

    (c.v.tx_losses
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

    c.v.vre_cap.stack(c.GEO_COLS).rename('value').to_frame().assign(units='MW').reset_index().to_csv(
        os.path.join(c.DIR_05_1_SUMMARY_OUT, '08a_vre_cap.csv'),
        index=False)
    c.v.vre_av_abs.stack(c.GEO_COLS).rename('value').to_frame().assign(units='GWh').reset_index().to_csv(
        os.path.join(c.DIR_05_1_SUMMARY_OUT, '08b_vre_daily_abs.csv'), index=False)
    c.v.vre_av_norm.stack(c.GEO_COLS).rename('value').to_frame().assign(units='-').reset_index().to_csv(
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

    c.v.vre_curtailed.stack(c.GEO_COLS).rename('value').to_frame().assign(units='GWh').reset_index().to_csv(
        os.path.join(c.DIR_05_1_SUMMARY_OUT, '09a_vre_daily_curtailed.csv'), index=False)
    c.v.curtailment_rate.assign(units='%').reset_index().to_csv(
        os.path.join(c.DIR_05_1_SUMMARY_OUT, '09b_curtailment_rate.csv'),
        index=False)
    c.v.all_re_curtailed.stack(c.GEO_COLS).rename('value').to_frame().assign(units='GWh') \
        .reset_index() \
        .to_csv(
        os.path.join(c.DIR_05_1_SUMMARY_OUT, '09c_all_RE_daily_curtailed.csv'), index=False)
    pd.DataFrame({'value': c.v.re_curtailment_rate, 'model': c.v.re_curtailment_rate.index}) \
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

    Capacities/flows are calculated based on defined regFrom/regTo in SolutionIndex
    Future iterations could define nodeFrom/nodeTo also in order to allow mutliple outputs 
    at different aggregations
    """
    print("Creating output 10...")

    line_cap = c.v.line_cap
    line_imp_exp = c.v.line_imp_exp
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

    c.v.gen_cap_tech_reg \
        .stack(c.GEO_COLS) \
        .rename('value') \
        .to_frame() \
        .assign(units='MW') \
        .reset_index() \
        .to_csv(os.path.join(c.DIR_05_1_SUMMARY_OUT, '11a_cap_by_tech_reg.csv'), index=False)
    c.v.gen_cap_subtech_reg \
        .stack(c.GEO_COLS) \
        .rename('value') \
        .to_frame() \
        .assign(units='MW') \
        .reset_index() \
        .to_csv(os.path.join(c.DIR_05_1_SUMMARY_OUT, '11b_gen_cap_by_subtech_reg.csv'), index=False)
    c.v.gen_cap_costTech_reg \
        .stack(c.GEO_COLS) \
        .rename('value') \
        .to_frame() \
        .assign(units='MW') \
        .reset_index() \
        .to_csv(os.path.join(c.DIR_05_1_SUMMARY_OUT, '11c_gen_cap_by_costTech_reg.csv'), index=False)

    print('Created file 11a_cap_by_tech_reg.csv.')
    print('Created file 11b_gen_cap_by_subtech_reg.csv.')
    print('Created file 11c_gen_cap_by_costTech_reg.csv.')


@catch_errors
def create_output_12(c):
    """
    Creates following output files:
    - 12a_cf_tech_reg.csv
    - 12c_cf_tech.csv
    """
    print("Creating output 12...")

    c.v.cf_tech_reg \
        .stack(c.GEO_COLS) \
        .rename('value') \
        .to_frame() \
        .assign(units='%') \
        .reset_index() \
        .to_csv(os.path.join(c.DIR_05_1_SUMMARY_OUT, '12a_cf_tech_reg.csv'), index=False)
    c.v.cf_tech \
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
    print("Creating output 13...")

    c.v.co2_by_tech_reg \
        .assign(units='tonnes') \
        .reset_index() \
        .to_csv(os.path.join(c.DIR_05_1_SUMMARY_OUT, '13a_co2_by_tech_reg.csv'), index=False)
    c.v.co2_by_reg \
        .assign(units='tonnes') \
        .reset_index() \
        .to_csv(os.path.join(c.DIR_05_1_SUMMARY_OUT, '13b_co2_by_reg.csv'), index=False)

    print('Created file 13a_co2_by_tech_reg.csv.')
    print('Created file 13b_co2_by_reg.csv.')
