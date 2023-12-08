"""
# DOCSTRING update after splitting into multiple files
This module, `calculate.py`, is responsible for caluclation functions for more elaborate parts of the solution processing.

These act as an alternative to caching all variables (which may be inefficient), and instead allows easy, one-line pieces of code for calculation of key outputs that can
then be used elsewhere in other functions.

More details on each function in the its Docstring, which can be accessed via
`help(solution_file_processing.outputs.func_name)`.

Year outputs:
# DOCSTRING needs list with short description and better docstrings for all functions
"""



@catch_errors
def get_cf_tech(c):
    """
    Get the capacity factor(%) per technology in the overall model
    """
    
    time_idx = c.o.reg_df.reset_index().timestamp.drop_duplicates().compute()
    nr_days = len(time_idx.dt.date.drop_duplicates())

    gen_by_tech_reg_orig = c.o.gen_yr_df[
        c.o.gen_yr_df.property == 'Generation']  # For not separating cofiring. good for CF comparison
    gen_by_tech_reg_orig = gen_by_tech_reg_orig \
        .groupby(['model'] + c.GEO_COLS + ['Category']) \
        .agg({'value': 'sum'}) \
        .unstack(level=c.GEO_COLS) \
        .fillna(0)

    gen_cap_tech_reg = c.o.gen_yr_df[c.o.gen_yr_df.property == 'Installed Capacity'] \
        .groupby(['model'] + c.GEO_COLS + ['Category']) \
        .agg({'value': 'sum'}) \
        .unstack(level=c.GEO_COLS) \
        .fillna(0)

    # Calculate as EN[GWh]/(Capacity[MW]/1000*hours)
    # Standard
    # As we make adjustments for co_firing on the energy values, we must re-calculate this

    
    cf_tech = (gen_by_tech_reg_orig.sum(axis=1) / (gen_cap_tech_reg.sum(axis=1) / 1000 * nr_days * 24)).unstack(
        level='Category').fillna(0)
        
    return cf_tech

   
@catch_errors
def get_cf_tech_reg(c):
    """
    Get the capacity factor(%) per technology in each region
    """  
    time_idx = c.o.reg_df.reset_index().timestamp.drop_duplicates().compute()
    nr_days = len(time_idx.dt.date.drop_duplicates())

    gen_by_tech_reg_orig = c.o.gen_yr_df[
        c.o.gen_yr_df.property == 'Generation']  # For not separating cofiring. good for CF comparison
    gen_by_tech_reg_orig = gen_by_tech_reg_orig \
        .groupby(['model'] + c.GEO_COLS + ['Category']) \
        .agg({'value': 'sum'}) \
        .unstack(level=c.GEO_COLS) \
        .fillna(0)

    gen_cap_tech_reg = c.o.gen_yr_df[c.o.gen_yr_df.property == 'Installed Capacity'] \
        .groupby(['model'] + c.GEO_COLS + ['Category']) \
        .agg({'value': 'sum'}) \
        .unstack(level=c.GEO_COLS) \
        .fillna(0)

    # Calculate as EN[GWh]/(Capacity[MW]/1000*hours)
    # Standard
    # As we make adjustments for co_firing on the energy values, we must re-calculate this

    cf_tech_reg = (gen_by_tech_reg_orig / (gen_cap_tech_reg / 1000 * nr_days * 24)).fillna(0)
    
    return cf_tech_reg

@catch_errors
def get_line_cap_reg(c):
    """
    Get the line capacity total between each region
    """    
    line_cap = c.o.line_yr_df[(c.o.line_yr_df.property == 'Import Limit') | \
                        (c.o.line_yr_df.property == 'Export Limit')] \
    .groupby(['model', 'regFrom', 'regTo', 'property']) \
    .agg({'value': 'sum'}) \
    .unstack(level='property')

    line_cap_reg = line_cap.reset_index()
    line_cap_reg = line_cap_reg[line_cap_reg.regFrom != line_cap_reg.regTo]
    line_cap_reg.loc[:, 'line'] = line_cap_reg.regFrom + '-' + line_cap_reg.regTo
    line_cap_reg = line_cap_reg.groupby(['model', 'line']).sum(numeric_only=True)

    if line_cap_reg.shape[0] == 0:
        pd.DataFrame({'model': c.o.line_yr_df.model.unique(),
                    'reg_from': ['None'] * len(c.o.line_yr_df.model.unique()),
                    'reg_to': ['None'] * len(c.o.line_yr_df.model.unique()),
                    'value': [0] * len(c.o.line_yr_df.model.unique())})
    
    return line_cap_reg

@catch_errors
def get_line_imp_exp(c):
    """
    Get the net annual flow across each transmission corridor (i.e. between each region)
    """
    
    line_imp_exp = c.o.line_yr_df[(c.o.line_yr_df.property == 'Flow') | \
                                (c.o.line_yr_df.property == 'Flow Back')] \
    . groupby(['model', 'regFrom', 'regTo', 'property']) \
        .agg({'value': 'sum'}) \
        .unstack(level='property')


    if line_imp_exp.shape[0] == 0:
        pd.DataFrame({'model': c.o.line_yr_df.model.unique(),
                    'reg_from': ['None'] * len(c.o.line_yr_df.model.unique()),
                    'reg_to': ['None'] * len(c.o.line_yr_df.model.unique()),
                    'value': [0] * len(c.o.line_yr_df.model.unique())})

    line_imp_exp_reg = line_imp_exp.reset_index()
    line_imp_exp_reg = line_imp_exp_reg[line_imp_exp_reg.regFrom != line_imp_exp_reg.regTo]
    line_imp_exp_reg.loc[:, 'line'] = line_imp_exp_reg.regFrom + '-' + line_imp_exp_reg.regTo

    
    return line_imp_exp_reg

def get_vre_share_by_reg(c):
    """
    Get the share of a subset of generation (e.g. RE or VRE), with the subset technologies passed as a list
    """

    vre_techs =['Solar', 'Wind']

    gen_by_tech_reg = gen_by_tech_reg = c.o.gen_yr_df[c.o.gen_yr_df.property == 'Generation']
    vre_by_reg = gen_by_tech_reg.reset_index()
    vre_by_reg.loc[:,'VRE'] = vre_by_reg.Category.apply(lambda x: 'VRE' if x in vre_techs else 'Non-VRE')
    vre_by_reg = vre_by_reg.groupby(['model', 'VRE']).sum().groupby(level=geo_cols[0], axis=1).sum()
    vre_by_reg.loc[:,'Overall'] = vre_by_reg.sum(axis=1)
    vre_by_reg = vre_by_reg.loc[ix[:,'VRE'],].droplevel('VRE')/vre_by_reg.groupby('model').sum()

    return vre_by_reg

def get_re_share_by_reg(c, gen_subset=['Wind', 'Solar']):
    """
    Get the share of a RE generation
    """

    re_techs = ['Solar', 'Wind', 'Bioenergy', 'Geothermal', 'Other', 'Marine', 'Hydro']
    
    gen_by_tech_reg = gen_by_tech_reg = c.o.gen_yr_df[c.o.gen_yr_df.property == 'Generation']
    re_by_reg = gen_by_tech_reg.reset_index()
    re_by_reg.loc[:,'RE'] = re_by_reg.Category.apply(lambda x: 'RE' if x in re_techs else 'Non-RE')
    re_by_reg = re_by_reg.groupby(['model','RE']).sum().groupby(level=geo_cols[0], axis=1).sum()
    re_by_reg.loc[:,'Overall'] = re_by_reg.sum(axis=1)
    re_by_reg = re_by_reg.loc[ix[:,'RE'],].droplevel('RE')/re_by_reg.groupby('model').sum()

    return re_by_reg

def get_re_curtailed_by_tech(c):
    """
    Get the curtailment / minimum energy violations by technology
    """

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

    re_curtailment_rate_by_tech = (all_re_curtailed.sum(axis=1).groupby(['model','Category']).sum().unstack('Category')/all_re_av.sum(axis=1).groupby(['model','Category']).sum().unstack('Category')).fillna(0)*100
    re_curtailment_rate = (all_re_curtailed.sum(axis=1).groupby('model').sum()/all_re_av.sum(axis=1).groupby('model').sum()).fillna(0)*100
    re_curtailment_rate_by_tech = pd.concat([re_curtailment_rate_by_tech, re_curtailment_rate.rename('All')], axis=1)


    return re_curtailment_rate_by_tech


def get_curtailment_rate(c):
    """
    Get the curtailment rate
    """

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

    curtailment_rate = (vre_curtailed.sum(axis=1).groupby('model').sum() / vre_av_abs.sum(axis=1).groupby(
        'model').sum()).fillna(0) * 100

    ### Note that this is the new implementation for ISLAND-style aggregation. using c.GEO_COLS[0] i.e. the top tier of regional grouping.
    vre_curtailed_grouped = vre_curtailed.T.groupby(c.GEO_COLS[0]).sum().T
    vre_av_abs_grouped = vre_av_abs.T.groupby(c.GEO_COLS[0]).sum().T

    ## This could be renamed _reg. But should be done in consistent manner, so leaving for now.
    curtailment_rate_reg = (vre_curtailed_grouped.groupby('model').sum() /
                            vre_av_abs_grouped.groupby('model').sum()).fillna(0) * 100

    curtailment_rate = pd.concat([curtailment_rate_reg, curtailment_rate.rename('Overall')], axis=1)

    return curtailment_rate


def get_gen_op_costs_by_reg(c):
    """
    TODO: DOCSTRING
    """


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
    gen_capex.loc[:, 'property'] = 'Investment Cost'
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
    
    return gen_op_costs_by_reg
