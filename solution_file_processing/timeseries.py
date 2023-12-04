"""
TODO DOCSTRING
"""

import os

import pandas as pd
import dask.dataframe as dd
import numpy as np

from .utils.utils import catch_errors
from .constants import VRE_TECHS
from . import log

print = log.info

@catch_errors
def create_output_1(c):
    """
    Creates following output files:
    - 01a_total_load_ts.csv
    - 01b_use_ts.csv
    - 01c_use_dly_ts.csv
    - 01d_load_w_use_ts.csv
    - for each geo_col (e.g. isl, reg, sub):
        - 01x_total_load_{geo_col}_ts.csv
        - 01x_use_{geo_col}_ts.csv
        - 01x_use_dly_{geo_col}_ts.csv
    """
    print("Creating interval output 1...")
    # Output 1a-b: Load and USE time-series
    total_load_ts = c.o.reg_df[c.o.reg_df.property == 'Load'] \
        .groupby(['model', 'timestamp']) \
        .agg({'value': 'sum'})

    total_load_ts.assign(units='MW').reset_index().compute() \
        .to_csv(os.path.join(c.DIR_05_2_TS_OUT, '01a_total_load_ts.csv'))

    use_ts = c.o.reg_df[c.o.reg_df.property == 'Unserved Energy'] \
        .groupby(['model', 'timestamp']) \
        .agg({'value': 'sum'})

    use_ts.assign(units='MW').reset_index().compute() \
        .to_csv(os.path.join(c.DIR_05_2_TS_OUT, '01b_use_ts.csv'))

    print('Created file 01a_total_load_ts.csv.')
    print('Created file 01b_use_ts.csv.')

    # Need to calculate whether its 30mn or 1hr within but for now just assume hourly
    use_dly_ts = c.o.reg_df[c.o.reg_df.property == 'Unserved Energy'] \
        .assign(timestamp=dd.to_datetime(c.o.reg_df['timestamp']).dt.floor('D')) \
        .groupby(['model', 'timestamp']) \
        .agg({'value': 'sum'}) \
        .applymap(lambda x: x / 1000 if isinstance(x, float) else x)

    use_dly_ts.assign(units='GWh').reset_index().compute().to_csv(
        os.path.join(c.DIR_05_2_TS_OUT, '01c_use_dly_ts.csv'))

    # todo needs implentation
    # load_w_use_ts = dd.concat([total_load_ts.rename('Load'), use_ts.rename('USE')])
    #
    # add_df_column(load_w_use_ts, 'units', 'MW').reset_index().compute().to_csv(
    #     os.path.join(p.DIR_05_2_TS_OUT, '01d_load_w_use_ts.csv'))

    print('Created file 01c_use_dly_ts.csv.')

    if c.cfg['settings']['reg_ts']:
        print("Creating interval special output 1 (reg_ts=True)...")
        load_by_reg_ts = c.o.node_df[c.o.node_df.property == 'Load'].groupby(
            ['model'] + c.GEO_COLS + ['timestamp']).agg({'value': 'sum'}).compute().unstack(
            level='timestamp').fillna(0).stack('timestamp')

        use_reg_ts = c.o.node_df[c.o.node_df.property == 'Unserved Energy'].groupby(
            ['model'] + c.GEO_COLS + ['timestamp']).agg({'value': 'sum'}).compute().unstack(
            level=c.GEO_COLS)

        use_dly_reg_ts = use_reg_ts.groupby(
            [pd.Grouper(level='model'), pd.Grouper(freq='D', level='timestamp')]).sum() / 1000

        for geo in c.GEO_COLS:
            geo_suffix = geo[:3].lower()
            count = ord('e')

            load_by_reg_ts \
                .unstack(level=c.GEO_COLS) \
                .groupby(level=geo, axis=1) \
                .sum() \
                .assign(units='MW') \
                .reset_index() \
                .to_csv(os.path.join(c.DIR_05_2_TS_OUT, '01{}_total_load_{}_ts.csv'.format(chr(count), geo_suffix)),
                        index=False)

            use_reg_ts \
                .groupby(level=geo, axis=1) \
                .sum() \
                .assign(units='MW') \
                .reset_index() \
                .to_csv(os.path.join(c.DIR_05_2_TS_OUT, '01{}_use_{}_ts.csv'.format(chr(count + 1), geo_suffix)),
                        index=False)

            use_dly_reg_ts \
                .groupby(level=geo, axis=1) \
                .sum() \
                .assign(units='GWh') \
                .reset_index() \
                .to_csv(os.path.join(c.DIR_05_2_TS_OUT,
                                     '01{}_use_dly_{}_ts.csv'.format(chr(count + 2), geo_suffix)),
                        index=False)

            print(f'Created file 01{chr(count)}_total_load_{geo_suffix}_ts.csv.')
            print(f'Created file 01{chr(count + 1)}_use_{geo_suffix}_ts.csv.')
            print(f'Created file 01{chr(count + 2)}_use_dly_{geo_suffix}_ts.csv.')

            count += 3


@catch_errors
def create_output_2(c):
    """
    Creates following output files:
    - 02a_gen_by_tech_ts.csv
    - 02b_gen_by_subtech_ts.csv
    - 02c_av_cap_by_subtech_ts.csv
    """
    print("Creating interval output 2...")
    # Output 2: Generation

    gen_by_tech_ts = c.o.gen_df[c.o.gen_df.property == 'Generation'] \
        .groupby(['model', 'Category', 'timestamp']) \
        .agg({'value': 'sum'}) \
        .compute() \
        .unstack(level='Category') \
        .fillna(0)

    gen_by_subtech_ts = c.o.gen_df[c.o.gen_df.property == 'Generation'] \
        .groupby(['model', 'CapacityCategory', 'timestamp']) \
        .agg({'value': 'sum'}) \
        .compute() \
        .unstack(level='CapacityCategory') \
        .fillna(0)

    av_cap_by_tech_ts = c.o.gen_df[c.o.gen_df.property == 'Available Capacity'] \
        .groupby(['model', 'Category', 'timestamp']) \
        .agg({'value': 'sum'}) \
        .compute() \
        .unstack(level='Category') \
        .fillna(0)

    # Drop the higher-level index (to get a single column line in csv file)
    gen_by_tech_ts.columns = gen_by_tech_ts.columns.droplevel(level=0)
    gen_by_subtech_ts.columns = gen_by_subtech_ts.columns.droplevel(level=0)
    av_cap_by_tech_ts.columns = av_cap_by_tech_ts.columns.droplevel(level=0)

    gen_by_tech_ts \
        .assign(units='MW') \
        .reset_index() \
        .to_csv(os.path.join(c.DIR_05_2_TS_OUT, '02a_gen_by_tech_ts.csv'), index=False)

    gen_by_subtech_ts \
        .assign(units='MW') \
        .reset_index() \
        .to_csv(os.path.join(c.DIR_05_2_TS_OUT, '02b_gen_by_tech_ts.csv'), index=False)

    av_cap_by_tech_ts \
        .assign(units='MW') \
        .reset_index() \
        .to_csv(os.path.join(c.DIR_05_2_TS_OUT, '02c_av_cap_by_subtech_ts.csv'), index=False)

    print('Created file 02a_gen_by_tech_ts.csv.')
    print('Created file 02b_gen_by_subtech_ts.csv.')
    print('Created file 02c_av_cap_by_subtech_ts.csv.')


@catch_errors
def create_output_3(c):
    """
    Output 3: VRE Time-series
    Creates following output files:
    - 03a_vre_available_ts.csv
    - 03b_vre_gen_ts.csv
    - 03c_vre_curtailed_ts.csv
    - 03c_RE_curtailed_ts.csv
    - for each model in /{model}/:
        - for each geo_col (e.g. isl, reg, sub):
            - 03x_vre_available_{geo_col}_ts.csv
            - 03x_vre_gen_{geo_col}_ts.csv
            - 03x_vre_curtailed_{geo_col}_ts.csv
    """
    print("Creating interval output 3...")

    vre_av_abs_ts = c.o.gen_df[(c.o.gen_df.property == 'Available Capacity') & (c.o.gen_df.Category.isin(VRE_TECHS))] \
        .groupby(['model', 'Category', 'timestamp']) \
        .agg({'value': 'sum'}) \
        .compute() \
        .unstack(level='Category') \
        .fillna(0)

    vre_gen_abs_ts = c.o.gen_df[(c.o.gen_df.property == 'Generation') & (c.o.gen_df.Category.isin(VRE_TECHS))] \
        .groupby(['model', 'Category', 'timestamp']) \
        .agg({'value': 'sum'}) \
        .compute() \
        .unstack(level='Category') \
        .fillna(0)

    vre_curtailed_ts = vre_av_abs_ts - vre_gen_abs_ts

    constr_techs = ['Hydro', 'Bioenergy', 'Geothermal']

    min_energy_vio_tech_ts = c.o.gen_df[(c.o.gen_df.property == 'Min Energy Violation') &
                                        (c.o.gen_df.Category.isin(constr_techs))] \
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

    vre_av_abs_ts \
        .assign(units='MW') \
        .reset_index() \
        .to_csv(os.path.join(c.DIR_05_2_TS_OUT, '03a_vre_available_ts.csv'), index=False)
    vre_av_abs_ts \
        .assign(units='MW') \
        .reset_index() \
        .to_csv(os.path.join(c.DIR_05_2_TS_OUT, '03b_vre_gen_ts.csv'), index=False)
    vre_curtailed_ts \
        .assign(units='MW') \
        .reset_index() \
        .to_csv(os.path.join(c.DIR_05_2_TS_OUT, '03c_vre_curtailed_ts.csv'), index=False)
    re_curtailed_ts \
        .assign(units='MW') \
        .reset_index() \
        .to_csv(os.path.join(c.DIR_05_2_TS_OUT, '03c_RE_curtailed_ts.csv'), index=False)

    print('Created file 03a_vre_available_ts.csv.')
    print('Created file 03b_vre_gen_ts.csv.')
    print('Created file 03c_vre_curtailed_ts.csv.')
    print('Created file 03c_RE_curtailed_ts.csv.')

    # Due to the size, we do the gen DFs per model
    if c.cfg['settings']['reg_ts']:

        load_by_reg_ts = c.o.node_df[c.o.node_df.property == 'Load'] \
            .groupby(['model'] + c.GEO_COLS + ['timestamp']) \
            .agg({'value': 'sum'}) \
            .compute() \
            .unstack(level='timestamp') \
            .fillna(0) \
            .stack('timestamp')

        # Define model regs in multi-level format
        model_regs_multi = load_by_reg_ts.unstack(c.GEO_COLS).columns

        vre_av_reg_abs_ts = c.o.gen_df[(c.o.gen_df.property == 'Available Capacity') &
                                       (c.o.gen_df.Category.isin(VRE_TECHS))] \
            .groupby((['model'] + c.GEO_COLS + ['timestamp'])) \
            .agg({'value': 'sum'}) \
            .compute() \
            .unstack(level=c.GEO_COLS) \
            .fillna(0)

        vre_gen_reg_abs_ts = c.o.gen_df[(c.o.gen_df.property == 'Generation') &
                                        (c.o.gen_df.Category.isin(VRE_TECHS))] \
            .groupby((['model'] + c.GEO_COLS + ['timestamp'])) \
            .agg({'value': 'sum'}) \
            .compute() \
            .unstack(level=c.GEO_COLS) \
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

        for m in c.v.model_names:
            save_dir_model = os.path.join(c.DIR_05_2_TS_OUT, m)
            if os.path.exists(save_dir_model) is False:
                os.mkdir(save_dir_model)

            vre_av = vre_av_reg_abs_ts.loc[pd.IndexSlice[m, :]]

            vre_gen = vre_gen_reg_abs_ts.loc[pd.IndexSlice[m, :]]

            vre_curtailed = vre_av - vre_gen

            # For numbering purposes only
            count = ord('d')

            # Output results for every geographical aggregation
            for geo in c.GEO_COLS:
                geo_suffix = geo[:3].lower()
                # We probably want to output as wind/solar in columns but maintain the regions as columns for net load
                vre_av \
                    .groupby(level=geo, axis=1) \
                    .sum() \
                    .assign(units='MW') \
                    .reset_index() \
                    .to_csv(os.path.join(save_dir_model, '03{}_vre_available_{}_ts.csv'.format(chr(count), geo_suffix)),
                            index=False)
                vre_gen \
                    .groupby(level=geo, axis=1) \
                    .sum() \
                    .assign(units='MW') \
                    .reset_index() \
                    .to_csv(os.path.join(save_dir_model, '03{}_vre_gen_{}_ts.csv'.format(chr(count + 1), geo_suffix)),
                            index=False)
                vre_curtailed \
                    .groupby(level=geo, axis=1) \
                    .sum() \
                    .assign(units='MW') \
                    .reset_index() \
                    .to_csv(os.path.join(save_dir_model, '03{}_vre_curtailed_{}_ts.csv'.format(chr(count + 2),
                                                                                               geo_suffix)),
                            index=False)

                print(f'Created file 03{chr(count)}_vre_available_{geo_suffix}_ts.csv.')
                print(f'Created file 03{chr(count + 1)}_vre_gen_{geo_suffix}_ts.csv.')
                print(f'Created file 03{chr(count + 2)}_vre_curtailed_{geo_suffix}_ts.csv.')

                # Three different outputs
                count += 3

        av_cap_by_tech_reg_ts = c.o.gen_df[c.o.gen_df.property == 'Available Capacity'] \
            .groupby(['model', 'Category'] + c.GEO_COLS + ['timestamp']) \
            .agg({'value': 'sum'}) \
            .compute() \
            .unstack(level='Category') \
            .fillna(0)

        # Name makes no sense
        load_geo_ts = c.o.node_df[c.o.node_df.property == 'Load'].groupby(
            ['model'] + c.GEO_COLS + ['timestamp']).sum().value.compute().unstack(
            level=c.GEO_COLS)

        # Cappacity reserves by region
        av_cap_by_reg = av_cap_by_tech_reg_ts.sum(axis=1).unstack(level=c.GEO_COLS)
        # av_cap_by_reg.columns = [c.replace(' ', '_') for c in av_cap_by_reg.columns]
        res_by_reg_ts = av_cap_by_reg - load_geo_ts
        res_margin_by_reg_ts = res_by_reg_ts / load_geo_ts

        res_w_load_by_reg_ts = res_by_reg_ts.stack(c.GEO_COLS).reorder_levels(
            ['model'] + c.GEO_COLS + ['timestamp']).rename(
            'CapacityReserves')
        res_w_load_by_reg_ts = pd.concat(
            [res_w_load_by_reg_ts, load_geo_ts.stack(c.GEO_COLS).reorder_levels(
                ['model'] + c.GEO_COLS + ['timestamp']).rename('Load')], axis=1)

        res_w_load_by_reg_ts \
            .assign(units='MW') \
            .reset_index() \
            .to_csv(os.path.join(c.DIR_05_2_TS_OUT, '03a_cap_reserve_w_load_by_reg_ts.csv'), index=False)

        res_margin_by_reg_ts \
            .assign(units='%') \
            .reset_index() \
            .to_csv(os.path.join(c.DIR_05_2_TS_OUT, '03b_reserve_margin_by_reg_ts.csv'), index=False)

        print('Created file 03a_cap_reserve_w_load_by_reg_ts.csv.')
        print('Created file 03b_reserve_margin_by_reg_ts.csv.')


@catch_errors
def create_output_4(c):
    """
    todo not implemented
    """
    print("Creating interval output 4...")

    # Output 3c: Regional generation...this is likely redundnat as the calcs are done up by CF calculations

    if c.cfg['settings']['reg_ts']:

        for m in c.v.model_names:
            save_dir_model = os.path.join(c.DIR_05_2_TS_OUT, m)
            if os.path.exists(save_dir_model) is False:
                os.mkdir(save_dir_model)

            gen_by_tech_reg_model_ts = c.v.gen_by_tech_reg_ts.loc[pd.IndexSlice[m,],].stack('Category')

            gen_by_subtech_reg_model_ts = c.v.gen_by_subtech_reg_ts.loc[pd.IndexSlice[m,],].stack('CapacityCategory')

            gen_by_tech_reg_model_ts \
                .assign(units='MW') \
                .reset_index() \
                .to_csv(os.path.join(save_dir_model, '04_gen_by_tech_reg_ts.csv'), index=False)
            gen_by_subtech_reg_model_ts \
                .assign(units='MW') \
                .reset_index() \
                .to_csv(os.path.join(save_dir_model, '04_gen_by_subtech_reg_ts.csv'), index=False)

            print(f'Created file 04_gen_by_tech_reg_ts.csv.')
            print(f'Created file 04_gen_by_subtech_reg_ts.csv.')

    return

    # Ouput 4: Costs and savings.

    gen_cost_props_w_penalty = ['Emissions Cost', 'Fuel Cost', 'Max Energy Violation Cost',
                                'Min Energy Violation Cost', 'Ramp Down Cost', 'Ramp Up Cost', 'Start & Shutdown Cost']

    # Excludes VO&M as this is excluded for some generators because of bad scaling of the objective function
    gen_op_cost_props = ['Emissions Cost', 'Fuel Cost', 'Start & Shutdown Cost']

    # Standard costs reported as USD'000
    gen_op_costs = c.o.gen_yr_df[c.o.gen_yr_df.property.isin(gen_op_cost_props)]
    gen_op_costs_w_pen = c.o.gen_yr_df[c.o.gen_yr_df.property.isin(gen_cost_props_w_penalty)]

    # Scale costs to be also USD'000
    gen_vom = c.o.gen_yr_df[c.o.gen_yr_df.property == 'Generation'].fillna(0)
    # gen_vom.loc[:, 'value'] = gen_vom.apply(lambda x: x.value * x.VOM, axis=1).fillna(0)
    # gen_vom.loc[:, 'property'] = 'VO&M Cost'
    gen_vom.assign(value=lambda x: x.value / x.VOM)
    gen_vom.assign(property='VO&M Cost')

    gen_fom = c.o.gen_yr_df.loc[c.o.gen_yr_df.property == 'Installed Capacity', :]
    # gen_fom.loc[:, 'value'] = gen_fom.apply(lambda x: x.value * x.FOM, axis=1).fillna(0)
    # gen_fom.loc[:, 'property'] = 'FO&M Cost'
    gen_fom.assign(value=lambda x: x.value / x.FOM)
    gen_fom.assign(property='FO&M Cost')

    gen_capex = c.o.gen_yr_df.loc[c.o.gen_yr_df.property == 'Installed Capacity', :]
    # gen_capex.loc[:, 'value'] = gen_capex.apply(lambda x: x.value * x.CAPEX, axis=1).fillna(0)
    # gen_capex.loc[:, 'property'] = 'Investment Cost'
    gen_capex.assign(value=lambda x: x.value / x.CAPEX)
    gen_capex.assign(property='Investment Cost')

    gen_total_costs = dd.concat([gen_op_costs, gen_vom, gen_fom, gen_capex], axis=0)
    gen_total_costs_w_pen = dd.concat([gen_op_costs_w_pen, gen_vom, gen_fom, gen_capex], axis=0)
    gen_op_costs = dd.concat([gen_op_costs, gen_vom], axis=0)

    # Scale to USDm
    gen_op_costs_by_reg = gen_op_costs \
        .groupby(['model'] + c.GEO_COLS + ['Category', 'property']) \
        .agg({'value': 'sum'}) \
        .applymap(lambda x: x / 1e3)
    gen_total_costs_by_reg = gen_total_costs \
        .groupby(['model'] + c.GEO_COLS + ['Category', 'property']) \
        .agg({'value': 'sum'}) \
        .applymap(lambda x: x / 1e3)
    gen_total_costs_by_reg_w_pen = gen_total_costs_w_pen \
        .groupby(['model'] + c.GEO_COLS + ['Category', 'property']) \
        .agg({'value': 'sum'}) \
        .applymap(lambda x: x / 1e3)

    # TOP another violation costs

    # Non-standard cost elements (i.e. ToP, min CF violations, etc.)
    if c.o.fuelcontract_yr_df.shape[0].compute() > 0:
        top_cost_reg = c.o.fuelcontract_yr_df[c.o.fuelcontract_yr_df.property == 'Take-or-Pay Violation Cost']
        top_cost_reg.assign(Category='Gas')
        top_cost_reg.assign(property='ToP Violation Cost')
        top_cost_reg = top_cost_reg \
            .groupby(['model'] + c.GEO_COLS + ['Category', 'property']) \
            .agg({'value': 'sum'}) \
            .applymap(lambda x: x / 1e3)

    else:
        top_cost_reg = pd.DataFrame(None)

    return
    # todo needs impementation, set_index is to resource intensive
    # Cost of minimum energy violation for coal contracts

    gen_min_vio_coal_reg = c.o.gen_yr_df[(c.o.gen_yr_df.property == 'Min Energy Violation') &
                                         (c.o.gen_yr_df.Category == 'Coal')] \
        .groupby(['model'] + c.GEO_COLS + ['Category', 'timestamp']) \
        .sum().value  # GWh
    gen_avg_price_coal_reg = c.o.gen_yr_df[(c.o.gen_yr_df.property == 'Average Cost') &
                                           (c.o.gen_yr_df.Category == 'Coal')] \
        .groupby(['model'] + c.GEO_COLS + ['Category', 'timestamp']) \
        .sum().value  # $/Mwh
    gen_min_vio_cost_coal_reg = gen_min_vio_coal_reg * gen_avg_price_coal_reg  # $'000
    gen_min_vio_cost_coal_reg = gen_min_vio_cost_coal_reg.reset_index()
    gen_min_vio_cost_coal_reg = gen_min_vio_cost_coal_reg.assign(property='PPA Violation Cost')
    gen_min_vio_cost_coal_reg = gen_min_vio_cost_coal_reg \
        .groupby(['model'] + c.GEO_COLS + ['Category', 'property']) \
        .agg({'value': 'sum'}) \
        .applymap(lambda x: x / 1e3)

    # Ramp costs by reg in USDm
    gen_by_name_ts = c.o.gen_df[c.o.gen_df.property == 'Generation'] \
        .set_index(['model', 'name'] + c.GEO_COLS + ['Category', 'timestamp']) \
        .value
    ramp_by_gen_name = (gen_by_name_ts - gen_by_name_ts.shift(1)).fillna(0)
    ramp_costs_by_gen_name = pd.merge(ramp_by_gen_name.reset_index(), c.soln_idx[['name', 'RampCost']], on='name',
                                      how='left').set_index(
        ['model', 'name'] + c.GEO_COLS + ['Category', 'timestamp'])
    ramp_costs_by_gen_name.loc[:, 'value'] = (
            ramp_costs_by_gen_name.value.abs() * ramp_costs_by_gen_name.RampCost.fillna(0))
    ramp_costs_by_gen_name.loc[:, 'property'] = 'Ramp Cost'
    gen_ramp_costs_by_reg = ramp_costs_by_gen_name.reset_index().groupby(
        ['model'] + c.GEO_COLS + ['Category', 'property']).sum().value / 1e6

    # ### Final dataframes of costs
    gen_op_costs_by_reg = pd.concat([gen_op_costs_by_reg, gen_ramp_costs_by_reg], axis=0).reset_index().groupby(
        ['model'] + c.GEO_COLS + ['Category', 'property']).sum().value
    gen_op_and_vio_costs_reg = pd.concat([gen_op_costs_by_reg, top_cost_reg, gen_min_vio_cost_coal_reg],
                                         axis=0).reset_index().groupby(
        ['model'] + c.GEO_COLS + ['Category', 'property']).sum().value

    gen_total_costs_by_reg = pd.concat([gen_total_costs_by_reg, gen_ramp_costs_by_reg], axis=0).reset_index().groupby(
        ['model'] + c.GEO_COLS + ['Category', 'property']).sum().value
    # this is different from the vio costs as it includes actual penalty costs as per the model (which are much higher
    # than real violation costs due to modelling considerations)
    gen_total_costs_by_reg_w_pen = pd.concat([gen_total_costs_by_reg_w_pen, gen_ramp_costs_by_reg],
                                             axis=0).reset_index().groupby(
        ['model'] + c.GEO_COLS + ['Category', 'property']).sum().value

    # USDm/GWh = USD'000/MWh
    gen_by_tech_reg = c.o.gen_yr_df[c.o.gen_yr_df.property == 'Generation']

    lcoe_by_tech_reg = (gen_total_costs_by_reg.groupby(
        ['model'] + c.GEO_COLS + ['Category']).sum().unstack(
        c.GEO_COLS).fillna(0) / gen_by_tech_reg).fillna(0).stack(
        c.GEO_COLS).reorder_levels(
        ['model'] + c.GEO_COLS + ['Category']) * 1000
    lcoe_tech = gen_total_costs_by_reg.groupby(['model', 'Category']).sum() / gen_by_tech_reg.stack(
        c.GEO_COLS).groupby(
        ['model', 'Category']).sum() * 1000

    gen_op_costs_by_reg \
        .assign(units='USDm') \
        .reset_index() \
        .to_csv(os.path.join(c.DIR_05_2_TS_OUT, '04a_gen_op_costs_reg.csv'), index=False)
    gen_op_and_vio_costs_reg \
        .assign(units='USDm') \
        .reset_index() \
        .to_csv(os.path.join(c.DIR_05_2_TS_OUT, '04b_gen_op_and_vio_costs_reg.csv'), index=False)
    gen_total_costs_by_reg \
        .assign(units='USDm') \
        .reset_index() \
        .to_csv(os.path.join(c.DIR_05_2_TS_OUT, '04c_gen_total_costs_reg.csv'), index=False)
    gen_total_costs_by_reg_w_pen \
        .assign(units='USDm') \
        .reset_index() \
        .to_csv(os.path.join(c.DIR_05_2_TS_OUT, '04d_gen_total_costs_reg_w_penalty_costs.csv'), index=False)
    lcoe_by_tech_reg \
        .assign(units='USD/MWh') \
        .reset_index() \
        .to_csv(os.path.join(c.DIR_05_2_TS_OUT, '04e_lcoe_tech_reg.csv'), index=False)
    lcoe_tech \
        .assign(units='USD/MWh') \
        .reset_index() \
        .to_csv(os.path.join(c.DIR_05_2_TS_OUT, '04f_lcoe_tech.csv'), index=False)

    print('Created file 04a_gen_op_costs_reg.csv.')
    print('Created file 04b_gen_op_and_vio_costs_reg.csv.')
    print('Created file 04c_gen_total_costs_reg.csv.')
    print('Created file 04d_gen_total_costs_reg_w_penalty_costs.csv.')
    print('Created file 04e_lcoe_tech_reg.csv.')
    print('Created file 04f_lcoe_tech.csv.')


@catch_errors
def create_output_5(c):
    """
    Output 5: to plot undispatched capacity for USE (and maybe other metrics)
    todo not implemented
    """
    print('Creating interval output 5 is not implemented yet.')
    return


@catch_errors
def create_output_6(c):
    """
    Output 6: Net-load timeseries and LDCs
    Creates following output files:
    - 06a_net_load_ts.csv
    - 06b_ldc.csv
    - 06c_net_ldc.csv
    - 06d_net_ldc_curtail.csv
    - 06e_net_load_orig_ts.csv
    - 06f_ldc_orig.csv
    - 06g_net_ldc_orig.csv
    """
    print("Creating interval output 6...")

    # So that we can see the total load before USE and pump load
    customer_load_ts = c.o.reg_df[
        (c.o.reg_df.property == 'Customer Load') | (c.o.reg_df.property == 'Unserved Energy')].groupby(
        ['model', 'timestamp']).sum().value.compute()

    ######
    # Model filler for comparison of models with different inputs (e.g. DSM or EVs not included)
    # Series with indices matching the columns of the DF for filling in missing columns
    model_filler = pd.Series(data=[1] * len(c.v.model_names), index=c.v.model_names).rename_axis('model')

    purch_df = c.o.purch_df.compute()
    if purch_df.shape[0] > 0:
        ev_profiles_ts = purch_df[
            purch_df.name.str.contains('_EV') & (purch_df.property == 'Load')].groupby(
            ['model', 'timestamp']).sum().value
        ev_profiles_orig_ts = purch_df[
            purch_df.name.str.contains('_EV') & (purch_df.property == 'x')].groupby(
            ['model', 'timestamp']).sum().value
        if not ev_profiles_ts.shape[0] == 0:
            ev_profiles_orig_ts = (ev_profiles_orig_ts.unstack('model') * model_filler).fillna(0).stack(
                'model').reorder_levels(['model', 'timestamp'])

        dsm_profiles_ts = purch_df[
            purch_df.name.str.contains('_Shift') & (purch_df.property == 'Load')].groupby(
            ['model', 'timestamp']).sum().value
        dsm_profiles_orig_ts = purch_df[
            purch_df.name.str.contains('_Shift') & (purch_df.property == 'x')].groupby(
            ['model', 'timestamp']).sum().value
        if not dsm_profiles_ts.shape[0] == 0:
            dsm_profiles_orig_ts = (dsm_profiles_orig_ts.unstack('model') * model_filler).fillna(0).stack(
                'model').reorder_levels(['model', 'timestamp'])

        electr_profiles_ts = purch_df[
            purch_df.name.str.contains('_Elec') & (purch_df.property == 'Load')].groupby(
            ['model', 'timestamp']).sum().value
        electr_profiles_orig_ts = purch_df[
            purch_df.name.str.contains('_Elec') & (purch_df.property == 'x')].groupby(
            ['model', 'timestamp']).sum().value
        if electr_profiles_ts.shape[0] == 0:
            electr_profiles_orig_ts = pd.Series(data=[0] * len(customer_load_ts.index),
                                                index=customer_load_ts.index)
        else:
            electr_profiles_orig_ts = (electr_profiles_orig_ts.unstack('model') * model_filler).fillna(0).stack(
                'model').reorder_levels(['model', 'timestamp'])

        native_load_ts = c.o.node_df[c.o.node_df.property == 'Native Load'].groupby(
            ['model', 'timestamp']).sum().value.compute()

        customer_load_orig_ts = (native_load_ts + ev_profiles_orig_ts + dsm_profiles_orig_ts + electr_profiles_orig_ts)
        # .fillna(customer_load_ts) ### For those profiles where EVs are missing, for e.g. ... other DSM to be added
    else:
        customer_load_orig_ts = customer_load_ts

    vre_av_abs_ts = c.o.gen_df[(c.o.gen_df.property == 'Available Capacity') & (c.o.gen_df.Category.isin(VRE_TECHS))] \
        .groupby(['model', 'Category', 'timestamp']) \
        .agg({'value': 'sum'}) \
        .compute() \
        .unstack(level='Category') \
        .fillna(0)
    vre_gen_abs_ts = c.o.gen_df[(c.o.gen_df.property == 'Generation') & (c.o.gen_df.Category.isin(VRE_TECHS))] \
        .groupby(['model', 'Category', 'timestamp']) \
        .agg({'value': 'sum'}) \
        .compute() \
        .unstack(level='Category') \
        .fillna(0)

    #  net_load_ts is calculated as a series (as we obtain load 'value' and some across the x-axis (technologies)
    #  of vre_abs)
    net_load_ts = pd.DataFrame(
        customer_load_ts - vre_av_abs_ts.fillna(0).sum(axis=1).groupby(['model', 'timestamp']).sum(), columns=['value'])
    net_load_curtail_ts = pd.DataFrame(
        customer_load_ts - vre_gen_abs_ts.fillna(0).sum(axis=1).groupby(['model', 'timestamp']).sum(),
        columns=['value'])
    net_load_orig_ts = pd.DataFrame(
        customer_load_orig_ts - vre_av_abs_ts.fillna(0).sum(axis=1).groupby(['model', 'timestamp']).sum(),
        columns=['value'])

    # Calculation of all the different variations of LDC for later comparison
    ldc = customer_load_ts.unstack('model')
    ldc = pd.DataFrame(np.flipud(np.sort(ldc.values, axis=0)), index=ldc.index, columns=ldc.columns)

    ldc_orig = customer_load_orig_ts.unstack('model')
    ldc_orig = pd.DataFrame(np.flipud(np.sort(ldc_orig.values, axis=0)), index=ldc_orig.index, columns=ldc_orig.columns)

    nldc = net_load_ts.value.unstack('model')
    nldc = pd.DataFrame(np.flipud(np.sort(nldc.values, axis=0)), index=nldc.index, columns=nldc.columns)

    nldc_orig = net_load_orig_ts.value.unstack('model')
    nldc_orig = pd.DataFrame(np.flipud(np.sort(nldc_orig.values, axis=0)), index=nldc_orig.index,
                             columns=nldc_orig.columns)

    nldc_curtail = net_load_curtail_ts.value.unstack('model')
    nldc_curtail = pd.DataFrame(np.flipud(np.sort(nldc_curtail.values, axis=0)), index=nldc_curtail.index,
                                columns=nldc_curtail.columns)

    net_load_ts.value.unstack('model').assign(units='MW') \
        .to_csv(os.path.join(c.DIR_05_2_TS_OUT, '06a_net_load_ts.csv'), index=True)
    ldc.reset_index(drop=True).assign(units='MW') \
        .to_csv(os.path.join(c.DIR_05_2_TS_OUT, '06b_ldc.csv'), index=True)
    nldc.reset_index(drop=True).assign(units='MW') \
        .to_csv(os.path.join(c.DIR_05_2_TS_OUT, '06c_net_ldc.csv'), index=True)
    nldc_curtail.reset_index(drop=True).assign(units='MW') \
        .to_csv(os.path.join(c.DIR_05_2_TS_OUT, '06d_net_ldc_curtail.csv'), index=True)
    net_load_orig_ts.value.unstack('model').assign(units='MW') \
        .to_csv(os.path.join(c.DIR_05_2_TS_OUT, '06e_net_load_orig_ts.csv'), index=True)
    ldc_orig.reset_index(drop=True).assign(units='MW') \
        .to_csv(os.path.join(c.DIR_05_2_TS_OUT, '06f_ldc_orig.csv'), index=True)
    nldc_orig.reset_index(drop=True).assign(units='MW') \
        .to_csv(os.path.join(c.DIR_05_2_TS_OUT, '06g_net_ldc_orig.csv'), index=True)

    print('Created file 06a_net_load_ts.csv.')
    print('Created file 06b_ldc.csv.')
    print('Created file 06c_net_ldc.csv.')
    print('Created file 06d_net_ldc_curtail.csv.')
    print('Created file 06e_net_load_orig_ts.csv.')
    print('Created file 06f_ldc_orig.csv.')
    print('Created file 06g_net_ldc_orig.csv.')


@catch_errors
def create_output_7(c):
    """
    Output 7 : Daily gap between peak and off-peak net load

    This could be future proofed with using the geo_col/suffix approach....
    Creates following output files:
    - 07a_dly_gap_reg.csv
    - 07b_dly_gap_subreg.csv
    - 07c_dly_gap_isl.csv
    - 07d_dly_gap.csv
    - 07e_dly_gap_reg_pc.csv
    - 07f_dly_gap_subreg_pc.csv
    - 07g_dly_gap_isl_pc.csv
    - 07h_dly_gap_pc.csv
    """
    print("Creating interval output 7...")

    customer_load_reg_ts = c.o.node_df[(c.o.node_df.property == 'Customer Load') |
                                       (c.o.node_df.property == 'Unserved Energy')] \
        .groupby(['model'] + c.cfg['settings']['geo_cols'] + ['timestamp']) \
        .agg({'value': 'sum'}) \
        .compute() \
        .unstack(level=c.cfg['settings']['geo_cols'])

    vre_av_reg_abs_ts = c.o.gen_df[(c.o.gen_df.property == 'Available Capacity') &
                                   (c.o.gen_df.Category.isin(VRE_TECHS))] \
        .groupby((['model'] + c.cfg['settings']['geo_cols'] + ['timestamp'])) \
        .agg({'value': 'sum'}) \
        .compute() \
        .unstack(level=c.cfg['settings']['geo_cols']) \
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
    try:
        nl_max_dly_isl = net_load_reg_ts \
            .groupby('Island', axis=1) \
            .sum() \
            .groupby(['model', pd.Grouper(level='timestamp', freq='D')]) \
            .max()
    except KeyError:
        nl_max_dly_isl = net_load_reg_ts \
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
    try:
        nl_min_dly_isl = net_load_reg_ts \
            .groupby('Island', axis=1) \
            .sum() \
            .groupby(['model', pd.Grouper(level='timestamp', freq='D')]) \
            .min()
    except KeyError:
        nl_min_dly_isl = net_load_reg_ts \
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

    nl_dly_gap_reg \
        .assign(units='MW') \
        .reset_index() \
        .to_csv(os.path.join(c.DIR_05_2_TS_OUT, '07a_dly_gap_reg.csv'), index=False)
    nl_dly_gap_subreg \
        .assign(units='MW') \
        .reset_index() \
        .to_csv(os.path.join(c.DIR_05_2_TS_OUT, '07b_dly_gap_subreg.csv'), index=False)
    nl_dly_gap_isl \
        .assign(units='MW') \
        .reset_index() \
        .to_csv(os.path.join(c.DIR_05_2_TS_OUT, '07c_dly_gap_isl.csv'), index=False)
    nl_dly_gap \
        .reset_index() \
        .assign(units='MW') \
        .to_csv(os.path.join(c.DIR_05_2_TS_OUT, '07d_dly_gap.csv'), index=False)
    nl_dly_gap_reg_pc \
        .assign(units='MW') \
        .reset_index() \
        .to_csv(os.path.join(c.DIR_05_2_TS_OUT, '07e_dly_gap_reg_pc.csv'), index=False)
    nl_dly_gap_subreg_pc \
        .assign(units='MW') \
        .reset_index() \
        .to_csv(os.path.join(c.DIR_05_2_TS_OUT, '07f_dly_gap_subreg_pc.csv'), index=False)
    nl_dly_gap_isl_pc \
        .assign(units='MW') \
        .reset_index() \
        .to_csv(os.path.join(c.DIR_05_2_TS_OUT, '07g_dly_gap_isl_pc.csv'), index=False)
    nl_dly_gap_pc \
        .reset_index() \
        .assign(units='MW') \
        .to_csv(os.path.join(c.DIR_05_2_TS_OUT, '07h_dly_gap_pc.csv'), index=False)

    print('Created file 07a_dly_gap_reg.csv.')
    print('Created file 07b_dly_gap_subreg.csv.')
    print('Created file 07c_dly_gap_isl.csv.')
    print('Created file 07d_dly_gap.csv.')
    print('Created file 07e_dly_gap_reg_pc.csv.')
    print('Created file 07f_dly_gap_subreg_pc.csv.')
    print('Created file 07g_dly_gap_isl_pc.csv.')
    print('Created file 07h_dly_gap_pc.csv.')


@catch_errors
def create_output_8i(c):
    """
    # ### Output 8: Inertia (total)
    Creates following output files:
    - 08a_inertia_by_tech_ts.csv
    - 08b_inertia_by_isl_ts.csv
    - 08c_inertia_by_reg_ts.csv
    - 08d_total_inertia_ts.csv
    """
    print("Creating interval output 8i)...")

    inertia_by_tech = c.v.gen_inertia.groupby(['model', 'Category', 'timestamp']).sum()
    try:
        inertia_by_isl = c.v.gen_inertia.groupby(['model', 'Island', 'timestamp']).sum()
    except KeyError:
        inertia_by_isl = c.v.gen_inertia.groupby(['model', 'timestamp']).sum()
    inertia_by_reg = c.v.gen_inertia.groupby(['model'] + c.cfg['settings']['geo_cols'] + ['timestamp']).sum()
    total_inertia_ts = c.v.gen_inertia.groupby(['model', 'timestamp']).sum()

    inertia_by_tech \
        .assign(units='MWs') \
        .reset_index() \
        .to_csv(os.path.join(c.DIR_05_2_TS_OUT, '08a_inertia_by_tech_ts.csv'), index=False)
    inertia_by_isl \
        .assign(units='MWs') \
        .reset_index() \
        .to_csv(os.path.join(c.DIR_05_2_TS_OUT, '08b_inertia_by_isl_ts.csv'), index=False)
    inertia_by_reg \
        .assign(units='MWs') \
        .reset_index() \
        .to_csv(os.path.join(c.DIR_05_2_TS_OUT, '08c_inertia_by_reg_ts.csv'), index=False)
    total_inertia_ts \
        .assign(units='MWs') \
        .reset_index() \
        .to_csv(os.path.join(c.DIR_05_2_TS_OUT, '08d_total_inertia_ts.csv'), index=False)

    print('Created file 08a_inertia_by_tech_ts.csv.')
    print('Created file 08b_inertia_by_isl_ts.csv.')
    print('Created file 08c_inertia_by_reg_ts.csv.')
    print('Created file 08d_total_inertia_ts.csv.')


@catch_errors
def create_output_8ii(c):
    """
    # Output 9: Ramp time-series
    Creates following output files:
    - 08a_ramp_ts.csv
    - 08b_3hr_ramp_ts.csv
    - 08c_ramp_pc_ts.csv
    - 08d_3hr_ramp_pc_ts.csv
    - 08e_ramp_by_gen_tech_ts.csv
    - 08f_ramp_by_gen_subtech_ts.csv
    - 08g_3hr_ramp_by_gen_tech_ts.csv
    - 08g_3hr_ramp_reg_pc_ts.csv
    """
    print("Creating interval output 8ii)...")
    customer_load_ts = c.o.reg_df[
        (c.o.reg_df.property == 'Customer Load') | (c.o.reg_df.property == 'Unserved Energy')] \
        .groupby(['model', 'timestamp']) \
        .agg({'value': 'sum'})
    vre_av_abs_ts = c.o.gen_df[(c.o.gen_df.property == 'Available Capacity') & (c.o.gen_df.Category.isin(VRE_TECHS))] \
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
    gen_by_tech_ts = c.o.gen_df[c.o.gen_df.property == 'Generation'] \
        .groupby(['model', 'Category', 'timestamp']) \
        .agg({'value': 'sum'}) \
        .compute() \
        .unstack(level='Category') \
        .fillna(0)
    gen_by_subtech_ts = c.o.gen_df[c.o.gen_df.property == 'Generation'] \
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

    ramp_ts \
        .assign(units='MW.hr-1') \
        .reset_index() \
        .to_csv(os.path.join(c.DIR_05_2_TS_OUT, '08a_ramp_ts.csv'), index=False)
    th_ramp_ts \
        .assign(units='MW.hr-1') \
        .reset_index() \
        .to_csv(os.path.join(c.DIR_05_2_TS_OUT, '08b_3hr_ramp_ts.csv'), index=False)
    ramp_pc_ts \
        .reset_index() \
        .assign(units='MW.hr-1') \
        .to_csv(os.path.join(c.DIR_05_2_TS_OUT, '08c_ramp_pc_ts.csv'), index=False)
    th_ramp_pc_ts \
        .reset_index() \
        .assign(units='MW.hr-1') \
        .to_csv(os.path.join(c.DIR_05_2_TS_OUT, '08d_3hr_ramp_pc_ts.csv'), index=False)

    print('Created file 08a_ramp_ts.csv.')
    print('Created file 08b_3hr_ramp_ts.csv.')
    print('Created file 08c_ramp_pc_ts.csv.')
    print('Created file 08d_3hr_ramp_pc_ts.csv.')

    ramp_by_gen_tech_ts = ramp_by_gen_tech_ts.droplevel(0, axis=1)
    ramp_by_gen_subtech_ts = ramp_by_gen_subtech_ts.droplevel(0, axis=1)
    th_ramp_by_gen_tech_ts = th_ramp_by_gen_tech_ts.droplevel(0, axis=1)
    th_ramp_by_gen_subtech_ts = th_ramp_by_gen_subtech_ts.droplevel(0, axis=1)

    ramp_by_gen_tech_ts \
        .assign(units='MW.hr-1') \
        .reset_index() \
        .to_csv(os.path.join(c.DIR_05_2_TS_OUT, '08e_ramp_by_gen_tech_ts.csv'), index=False)
    ramp_by_gen_subtech_ts \
        .assign(units='MW.hr-1') \
        .reset_index() \
        .to_csv(os.path.join(c.DIR_05_2_TS_OUT, '08f_ramp_by_gen_subtech_ts.csv'), index=False)

    th_ramp_by_gen_tech_ts \
        .assign(units='MW.hr-1') \
        .reset_index() \
        .to_csv(os.path.join(c.DIR_05_2_TS_OUT, '08g_3hr_ramp_by_gen_tech_ts.csv'), index=False)
    th_ramp_by_gen_subtech_ts \
        .assign(units='MW.hr-1') \
        .reset_index() \
        .to_csv(os.path.join(c.DIR_05_2_TS_OUT, '08h_3hr_ramp_by_gen_subtech_ts.csv'), index=False)

    print('Created file 08e_ramp_by_gen_tech_ts.csv.')
    print('Created file 08f_ramp_by_gen_subtech_ts.csv.')
    print('Created file 08g_3hr_ramp_by_gen_tech_ts.csv.')
    print('Created file 08h_3hr_ramp_by_gen_subtech_ts.csv.')

@catch_errors
def create_output_9(c):
    print("Creating interval output 9 not implemented yet.")


@catch_errors
def create_output_10(c):
    """
    ### Output 10: Generation out-of-service
    ### Its noted that units out was done per generator. Unsure why, but this may be a more useful output
    Creates following output files:
    - 10a_outages_by_tech_ts.csv
    - 10b_outages_by_outtype_ts.csv
    """
    print("Creating interval output 10...")

    gen_out_tech_ts = c.o.gen_df[(c.o.gen_df.property == 'Forced Outage') |
                                 (c.o.gen_df.property == 'Maintenance')] \
        .groupby(['model', 'Category', 'timestamp']) \
        .agg({'value': 'sum'}) \
        .compute() \
        .unstack(level='Category')

    gen_out_by_type_ts = c.o.gen_df[(c.o.gen_df.property == 'Forced Outage') | \
                                    (c.o.gen_df.property == 'Maintenance')] \
        .groupby(['model', 'property', 'timestamp']) \
        .agg({'value': 'sum'}) \
        .compute() \
        .unstack(level='property')

    gen_out_tech_ts = gen_out_tech_ts.droplevel(0, axis=1)
    gen_out_by_type_ts = gen_out_by_type_ts.droplevel(0, axis=1)

    gen_out_tech_ts \
        .assign(units='MW') \
        .reset_index() \
        .to_csv(os.path.join(c.DIR_05_2_TS_OUT, '10a_outages_by_tech_ts.csv'), index=False)
    gen_out_by_type_ts \
        .assign(units='MW') \
        .reset_index() \
        .to_csv(os.path.join(c.DIR_05_2_TS_OUT, '10b_outages_by_outtype_ts.csv'), index=False)

    print('Created file 10a_outages_by_tech_ts.csv.')
    print('Created file 10b_outages_by_outtype_ts.csv.')


@catch_errors
def create_output_11(c):
    """
    ### Output 11a: Calculate days of interest & values
    ### This being max avg net load, min avg net load, max net/total load,, min net/total load, max ramp, min inertia
    """
    print("Creating interval output 11...")
    # Temp get variables
    gen_by_tech_ts = c.o.gen_df[c.o.gen_df.property == 'Generation'] \
        .groupby(['model', 'Category', 'timestamp']) \
        .agg({'value': 'sum'}) \
        .compute() \
        .unstack(level='Category') \
        .fillna(0) \
        .droplevel(0, axis=1)

    total_load_ts = c.o.reg_df[c.o.reg_df.property == 'Load'] \
        .groupby(['model', 'timestamp']) \
        .agg({'value': 'sum'})

    load_by_reg_ts = c.o.node_df[c.o.node_df.property == 'Load'] \
        .groupby(['model'] + c.GEO_COLS + ['timestamp']) \
        .agg({'value': 'sum'}) \
        .compute() \
        .unstack(level='timestamp') \
        .fillna(0) \
        .stack('timestamp')
    ### Define model regs in multi-level format
    model_regs_multi = load_by_reg_ts.unstack(c.GEO_COLS).columns
    model_regs_multi = pd.MultiIndex.from_tuples([[i for i in x if i != 'value'] for x in model_regs_multi])

    vre_av_reg_abs_ts = (c.o.gen_df[(c.o.gen_df.property == 'Available Capacity') &
                                    (c.o.gen_df.Category.isin(VRE_TECHS))]
                         .groupby((['model'] + c.GEO_COLS + ['timestamp']))
                         .sum()
                         .value
                         .compute()
                         .unstack(level=c.GEO_COLS)
                         .fillna(0))

    vre_gen_reg_abs_ts = (c.o.gen_df[(c.o.gen_df.property == 'Generation') &
                                     (c.o.gen_df.Category.isin(VRE_TECHS))]
                          .groupby((['model'] + c.GEO_COLS + ['timestamp']))
                          .sum()
                          .value
                          .compute()
                          .unstack(level=c.GEO_COLS)
                          .fillna(0))

    vre_regs = vre_av_reg_abs_ts.columns

    ## Fill in data for regions which have no VRE (i.e. zero arrays!) to allow similar arrays for load_ts and vre_av_ts
    for reg in list(model_regs_multi):
        if reg not in vre_regs:
            print(reg)
            vre_av_reg_abs_ts.loc[:, reg] = 0
            vre_gen_reg_abs_ts.loc[:, reg] = 0

    ### Columns in alphabetical order
    vre_av_reg_abs_ts = vre_av_reg_abs_ts[model_regs_multi]
    vre_gen_reg_abs_ts = vre_gen_reg_abs_ts[model_regs_multi]

    vre_curtailed_reg_ts = vre_av_reg_abs_ts - vre_gen_reg_abs_ts

    vre_av_abs_ts = (c.o.gen_df[(c.o.gen_df.property == 'Available Capacity') &
                                (c.o.gen_df.Category.isin(VRE_TECHS))]
                     .groupby(['model', 'Category', 'timestamp'])
                     .sum()
                     .value
                     .compute()
                     .unstack(level='Category')
                     .fillna(0))

    net_load_ts = pd.DataFrame(
        c.v.customer_load_ts.value - vre_av_abs_ts.fillna(0).sum(axis=1).groupby(['model', 'timestamp']).sum(),
        columns=['value'])
    ramp_ts = (net_load_ts.unstack(level='model') - net_load_ts.unstack(level='model').shift(1)).fillna(
        0).stack().sort_index(level=1).reset_index().set_index(['model', 'timestamp']).rename(columns={0: 'value'})
    total_inertia_ts = c.v.gen_inertia.groupby(['model', 'timestamp']).sum()[['InertiaHi', 'InertiaLo']]
    use_ts = c.o.reg_df[c.o.reg_df.property == 'Unserved Energy'].groupby(['model', 'timestamp']).sum()[['value']]
    use_dly_ts = (use_ts
                  .compute()
                  .groupby([pd.Grouper(level='model'), pd.Grouper(freq='D', level='timestamp')])
                  .sum()
                  / 1000)

    # todo c.v.model_names[0] this is just temporary, check if always the same ouput

    net_load_avg = c.v.net_load_ts.unstack(level='model').resample('D').mean().stack().reorder_levels(
        ['model', 'timestamp'])
    ### Time dataframes done nationally////

    wet_season = gen_by_tech_ts.loc[pd.IndexSlice[c.v.model_names[0], :]]['Hydro'].groupby(
        [pd.Grouper(level='timestamp', freq='M')]).sum().idxmax()
    dry_season = gen_by_tech_ts.loc[pd.IndexSlice[c.v.model_names[0], :]]['Hydro'].groupby(
        [pd.Grouper(level='timestamp', freq='M')]).sum().idxmin()

    ##########
    net_load_avg_max = net_load_avg.groupby(level='model').max().rename(columns={'value': 'net_load_avg_max'})
    net_load_avg_min = net_load_avg.groupby(level='model').min().rename(columns={'value': 'net_load_avg_min'})
    net_load_max = c.v.net_load_ts.groupby(level='model').max().rename(columns={'value': 'net_load_max'})
    net_load_min = c.v.net_load_ts.groupby(level='model').min().rename(columns={'value': 'net_load_min'})
    net_load_sto_min = c.v.net_load_sto_ts.groupby(level='model').min().rename(columns={'value': 'net_load_sto_min'})
    curtail_max = vre_curtailed_reg_ts.sum(axis=1).groupby(level='model').max().rename('curtail_max').to_frame()

    net_load_max_wet = c.v.net_load_ts[
        c.v.net_load_ts.index.get_level_values('timestamp').month == wet_season.month].groupby(
        level='model').max().rename(columns={'value': 'net_load_max_wet'})
    net_load_min_wet = c.v.net_load_ts[
        c.v.net_load_ts.index.get_level_values('timestamp').month == wet_season.month].groupby(
        level='model').min().rename(columns={'value': 'net_load_min_wet'})
    net_load_max_dry = c.v.net_load_ts[
        c.v.net_load_ts.index.get_level_values('timestamp').month == dry_season.month].groupby(
        level='model').max().rename(columns={'value': 'net_load_max_dry'})
    net_load_min_dry = c.v.net_load_ts[
        c.v.net_load_ts.index.get_level_values('timestamp').month == wet_season.month].groupby(
        level='model').min().rename(columns={'value': 'net_load_min_dry'})
    total_load_max = total_load_ts.compute().groupby(level='model').max().rename(columns={'value': 'total_load_max'})
    total_load_min = total_load_ts.compute().groupby(level='model').min().rename(columns={'value': 'total_load_min'})
    ramp_max = ramp_ts.groupby(level='model').max().rename(columns={'value': 'ramp_max'})
    inertia_min = total_inertia_ts.groupby(level='model').min().drop(columns='InertiaHi').rename(
        columns={'InertiaLo': 'inertia_min'})
    use_max = use_ts.compute().groupby(level='model').max().rename(columns={'value': 'use_max'})
    use_dly_max = use_dly_ts.groupby(level='model').max().rename(columns={'value': 'use_dly_max'})

    # ###########
    net_load_max['time_nlmax'] = c.v.net_load_ts.unstack(level='model').idxmax().values
    net_load_min['time_nlmin'] = c.v.net_load_ts.unstack(level='model').idxmin().values
    net_load_sto_min['time_nlstomin'] = c.v.net_load_sto_ts.unstack(level='model').idxmin().values
    curtail_max['time_curtailmax'] = vre_curtailed_reg_ts.sum(axis=1).unstack(level='model').idxmax().values

    net_load_max_wet['time_nlmax_wet'] = (
        c.v.net_load_ts[net_load_ts.index.get_level_values('timestamp').month == wet_season.month]
        .unstack(level='model')
        .idxmax()
        .values)
    net_load_min_wet['time_nlmin_wet'] = (
        c.v.net_load_ts[net_load_ts.index.get_level_values('timestamp').month == wet_season.month]
        .unstack(level='model')
        .idxmin()
        .values)
    net_load_max_dry['time_nlmax_dry'] = (
        c.v.net_load_ts[net_load_ts.index.get_level_values('timestamp').month == dry_season.month]
        .unstack(level='model')
        .idxmax()
        .values)
    net_load_min_dry['time_nlmin_dry'] = (
        c.v.net_load_ts[net_load_ts.index.get_level_values('timestamp').month == dry_season.month]
        .unstack(level='model')
        .idxmin()
        .values)
    net_load_avg_max['time_nlamax'] = net_load_avg.unstack(level='model').idxmax().values
    net_load_avg_min['time_nlamin'] = net_load_avg.unstack(level='model').idxmin().values
    total_load_max['time_tlmax'] = total_load_ts.compute().unstack(level='model').idxmax().values
    total_load_min['time_tlmin'] = total_load_ts.compute().unstack(level='model').idxmin().values
    ramp_max['time_ramp'] = ramp_ts.unstack(level='model').idxmax().values
    inertia_min['time_H'] = total_inertia_ts.InertiaLo.unstack(level='model').idxmin().values
    use_max['time_usemax'] = use_ts.compute().unstack(level='model').idxmax().values
    use_dly_max['time_usedmax'] = use_dly_ts.unstack(level='model').idxmax().values

    doi_summary = pd.concat(
        [net_load_max, net_load_min, net_load_sto_min, curtail_max, net_load_avg_max, net_load_avg_min,
         net_load_max_wet, net_load_min_wet, net_load_max_dry, net_load_min_dry,
         total_load_max, total_load_min, ramp_max, inertia_min, use_max, use_dly_max],
        axis=1).stack().unstack(level='model').rename_axis('property', axis=0)

    doi_summary.to_csv(os.path.join(c.DIR_05_2_TS_OUT, '11a_days_of_interest_summary.csv'), index=True)
    print('Created file 11a_days_of_interest_summary.csv.')