""""
TODO DOCSTRING
"""
import numpy as np
import pandas as pd
import dask.dataframe as dd

from solution_file_processing.constants import VRE_TECHS, CONSTR_TECHS

from solution_file_processing.utils.utils import drive_cache, memory_cache
from solution_file_processing import log

print = log.info


class Variables:
    """
    # todo needs to be updated with non cached variables
    This is class handles the complete data access to any Variable and works very similar to the Objects class.

    A variable is an optional data object that can but must not be used. It is just an option to cache any processing
    steps which are taken on a single or multiple objects.

    This could be also be done over and over again in the code of the calling function (e.g. create outputs or
    plots functions in summary.py, timeseries.py or plots.py). But this needs a lot of processing time, specially during
    the development phase of new output and plot functions. To avoid this, the processing steps can be added to this
    Variables class and the result is cached in the cache folder (04_SolutionFilesCache/<soln_choice>/variables/).

    Similar to the Object class, the data is loaded from the cache folder if it exists and not processed again. If
    the data in the cache folder is deleted, the data is processed again. To reprocess the data, just delete the
    data in the cache folder.

    Again any new variables can be added to the class in a similar way as the existing properties. There is also no
    limitation, any processing steps can be added. And the underlying objects must exist in the Objects class. Since
    the full data is stored in the cache folder for each variable, selecting code parts which are added to this class
    must be done thoughtfully. If a variable (the specific code) is only used for a single output or plot function and
    the caching functionality is not needed, it should be added to the output or plot function directly.

    To get more information about the Variables, please also refer to the properties docstring.

    Currently, the following properties are available:

    time_idx: #DOCSTRING
    gen_by_tech_reg_ts: #DOCSTRING
    gen_by_subtech_reg_ts: #DOCSTRING
    customer_load_ts: #DOCSTRING
    vre_av_abs_ts: #DOCSTRING
    net_load_ts: #DOCSTRING
    net_load_reg_ts: #DOCSTRING
    gen_inertia: #DOCSTRING

    """

    def __init__(self, configuration_object):
        self.c = configuration_object

    @property
    @memory_cache
    def gen_by_tech_reg_ts(self):
        """"
        TODO DOCSTRING
        """
        df = self.c.o.gen_df[self.c.o.gen_df.property == 'Generation'] \
            .groupby(['model', 'Category'] + self.c.GEO_COLS + ['timestamp']) \
            .agg({'value': 'sum'}) \
            .compute() \
            .unstack(level='Category') \
            .fillna(0)
        return df

    @property
    @memory_cache
    def gen_by_subtech_reg_ts(self):
        """"
        TODO DOCSTRING
        """
        df = self.c.o.gen_df[self.c.o.gen_df.property == 'Generation'] \
            .groupby(['model', 'CapacityCategory'] + self.c.GEO_COLS + ['timestamp']) \
            .agg({'value': 'sum'}) \
            .compute() \
            .unstack(level='CapacityCategory') \
            .fillna(0)

        return df

    @property
    @memory_cache
    def customer_load_ts(self):
        """"
        TODO DOCSTRING
        """
        df = self.c.o.reg_df[(self.c.o.reg_df.property == 'Customer Load') |
                             (self.c.o.reg_df.property == 'Unserved Energy')] \
            .groupby(['model', 'timestamp']) \
            .sum() \
            .value \
            .to_frame() \
            .compute()
        return df

    @property
    @memory_cache
    def vre_av_abs_ts(self):
        """"
        TODO DOCSTRING
        """
        df = self.c.o.gen_df[(self.c.o.gen_df.property == 'Available Capacity') &
                             (self.c.o.gen_df.Category.isin(VRE_TECHS))] \
            .groupby(['model', 'Category', 'timestamp']) \
            .sum().value.compute().unstack(level='Category').fillna(0)

        return df

    @property
    @memory_cache
    def net_load_ts(self):
        """"
        TODO DOCSTRING
        """
        df = pd.DataFrame(
            self.customer_load_ts.value - self.vre_av_abs_ts.fillna(0).sum(axis=1).groupby(
                ['model', 'timestamp']).sum(),
            columns=['value'])
        return df

    @property
    @memory_cache
    def net_load_sto_ts(self):
        """
        TODO DOCSTRING
        """
        customer_load_ts = (self.c.o.reg_df[(self.c.o.reg_df.property == 'Customer Load') |
                                            (self.c.o.reg_df.property == 'Unserved Energy')]
                            .groupby(['model', 'timestamp'])
                            .agg({'value': 'sum'})
                            .compute())
        storage_load_ts = self.c.o.reg_df[
            (self.c.o.reg_df.property == 'Battery Load') | (self.c.o.reg_df.property == 'Pump Load')].groupby(
            ['model', 'timestamp']).agg({'value': 'sum'}).compute()
        vre_av_abs_ts = self.c.o.gen_df[
            (self.c.o.gen_df.property == 'Available Capacity') & (
                self.c.o.gen_df.Category.isin(VRE_TECHS))].groupby(
            ['model', 'Category', 'timestamp']).agg({'value': 'sum'}).compute().unstack(
            level='Category').fillna(0)
        gen_by_tech_ts = (self.c.o.gen_df[self.c.o.gen_df.property == 'Generation']
                          .groupby(['model', 'Category', 'timestamp'])
                          .agg({'value': 'sum'})
                          .compute()
                          .unstack(level='Category')
                          .fillna(0)
                          .droplevel(0, axis=1))
        storage_gen_ts = gen_by_tech_ts.Storage.rename('value').to_frame()

        _data = customer_load_ts.value.ravel() - storage_gen_ts.value.ravel() + storage_load_ts.value.ravel() - (
            vre_av_abs_ts
            .fillna(0)
            .sum(axis=1)
            .groupby(['model', 'timestamp'])
            .sum()
            .rename('value'))

        df = pd.DataFrame(_data,
                          columns=['value'])

        return df

    @property
    @memory_cache
    def net_load_reg_ts(self):
        """"
        TODO DOCSTRING
        """
        customer_load_reg_ts = self.c.o.node_df[(self.c.o.node_df.property == 'Customer Load') |
                                                (self.c.o.node_df.property == 'Unserved Energy')] \
            .groupby(['model'] + self.c.GEO_COLS + ['timestamp']) \
            .sum() \
            .value \
            .compute() \
            .unstack(level=self.c.GEO_COLS)
        vre_av_reg_abs_ts = self.c.o.gen_df[(self.c.o.gen_df.property == 'Available Capacity') &
                                            (self.c.o.gen_df.Category.isin(VRE_TECHS))] \
            .groupby((['model'] + self.c.GEO_COLS + ['timestamp'])) \
            .sum() \
            .value \
            .compute() \
            .unstack(level=self.c.GEO_COLS).fillna(0)

        df = customer_load_reg_ts - vre_av_reg_abs_ts

        return df

    @property
    @memory_cache
    def gen_inertia(self):
        """"
        TODO DOCSTRING
        """
        gen_units_gen = self.c.o.gen_df[self.c.o.gen_df.property == 'Units Generating'] \
            .groupby(['model', 'name', 'timestamp']) \
            .agg({'value': 'sum'}) \
            .compute()

        gen_units = self.c.o.gen_df[self.c.o.gen_df.property == 'Units'] \
            .groupby(['model', 'name', 'timestamp']) \
            .agg({'value': 'sum'}) \
            .compute()

        # Take only the sum to maintain the capacity value & inertia constant in the dataframe
        gen_cap = self.c.o.gen_df[self.c.o.gen_df.property == 'Installed Capacity'] \
            .groupby(['model', 'name', 'timestamp']) \
            .agg({'value': 'sum'}) \
            .compute()

        gen_cap = pd.merge(gen_cap.reset_index(),
                           self.c.soln_idx[['name', 'InertiaLOW', 'InertiaHI']], on='name', how='left') \
            .set_index(['model', 'name', 'timestamp'])

        #  As installed capacity is [Units] * [Max Capacity], we must calculate the unit capacity
        gen_inertia_lo = (gen_units_gen.value / gen_units.value) * (gen_cap.value * gen_cap.InertiaLOW)
        gen_inertia_hi = (gen_units_gen.value / gen_units.value) * (gen_cap.value * gen_cap.InertiaHI)

        gen_inertia = pd.merge(pd.DataFrame(gen_inertia_lo, columns=['InertiaLo']),
                               pd.DataFrame(gen_inertia_hi, columns=['InertiaHi']),
                               left_index=True,
                               right_index=True)

        try:
            gen_inertia = pd.merge(gen_inertia.reset_index(),
                                   self.c.soln_idx[
                                       ['name', 'Island', 'Region', 'Subregion', 'Category', 'CapacityCategory']],
                                   on='name')
        except KeyError:
            gen_inertia = pd.merge(gen_inertia.reset_index(),
                                   self.c.soln_idx[
                                       ['name', 'Region', 'Subregion', 'Category', 'CapacityCategory']],
                                   on='name')

        df = gen_inertia

        return df

    @property
    @memory_cache
    def model_names(self):
        """"
        TODO DOCSTRING
        """
        model_names = list(np.sort(self.c.o.reg_df.model.drop_duplicates()))
        return model_names

    @property
    @memory_cache
    def time_idx(self):
        """
        TODO DOCSTRING
        """
        return self.c.o.reg_df.reset_index().timestamp.drop_duplicates()

    @property
    @memory_cache
    def interval_periods(self):
        """
        TODO DOCSTRING
        """
        return len(self.time_idx)

    @property
    @memory_cache
    def nr_days(self):
        """
        TODO DOCSTRING
        """
        return len(self.time_idx.dt.date.drop_duplicates())

    @property
    @memory_cache
    def daily_periods(self):
        """
        TODO DOCSTRING
        """
        return self.interval_periods / self.nr_days

    @property
    @memory_cache
    def hour_corr(self):
        """
        TODO DOCSTRING
        """
        return 24 / self.daily_periods

    @property
    @memory_cache
    def gen_cap_tech_reg(self):
        """
        TODO DOCSTRING
        """
        gen_cap_tech_reg = self.c.o.gen_yr_df[self.c.o.gen_yr_df.property == 'Installed Capacity'] \
            .groupby(['model'] + self.c.GEO_COLS + ['Category']) \
            .agg({'value': 'sum'}) \
            .unstack(level=self.c.GEO_COLS) \
            .fillna(0)
        return gen_cap_tech_reg

    @property
    @memory_cache
    def gen_cap_costTech_reg(self):
        """
        TODO DOCSTRING
        """
        # For Capex calcs
        gen_cap_costTech_reg = self.c.o.gen_yr_df[self.c.c.o.gen_yr_df.property == 'Installed Capacity'] \
            .groupby(['model'] + self.c.c.GEO_COLS + ['CostCategory']) \
            .agg({'value': 'sum'}) \
            .unstack(level=self.c.GEO_COLS) \
            .fillna(0)
        return gen_cap_costTech_reg

    @property
    @memory_cache
    def gen_cap_subtech_reg(self):
        """
        TODO DOCSTRING
        """
        gen_cap_subtech_reg = self.c.o.gen_yr_df[self.c.o.gen_yr_df.property == 'Installed Capacity'] \
            .groupby(['model'] + self.c.GEO_COLS + ['CapacityCategory']) \
            .agg({'value': 'sum'}) \
            .unstack(level=self.c.GEO_COLS) \
            .fillna(0)
        return gen_cap_subtech_reg

    @property
    @memory_cache
    def cf_tech(self):
        """
        Get the capacity factor(%) per technology in the overall model
        """
        gen_by_tech_reg_orig = self.c.o.gen_yr_df[
            self.c.o.gen_yr_df.property == 'Generation']  # For not separating cofiring. good for CF comparison
        gen_by_tech_reg_orig = gen_by_tech_reg_orig \
            .groupby(['model'] + self.c.GEO_COLS + ['Category']) \
            .agg({'value': 'sum'}) \
            .unstack(level=self.c.GEO_COLS) \
            .fillna(0)

        # Calculate as EN[GWh]/(Capacity[MW]/1000*hours)
        # Standard
        # As we make adjustments for co_firing on the energy values, we must re-calculate this

        df = ((gen_by_tech_reg_orig.sum(axis=1) / (self.gen_cap_tech_reg.sum(axis=1) / 1000 * self.nr_days * 24))
              .unstack(level='Category').fillna(0))

        return df

    @property
    @memory_cache
    def cf_tech_reg(self):
        """
        Get the capacity factor(%) per technology in each region
        """

        gen_by_tech_reg_orig = self.c.o.gen_yr_df[
            self.c.o.gen_yr_df.property == 'Generation']  # For not separating cofiring. good for CF comparison
        gen_by_tech_reg_orig = gen_by_tech_reg_orig \
            .groupby(['model'] + self.c.GEO_COLS + ['Category']) \
            .agg({'value': 'sum'}) \
            .unstack(level=self.c.GEO_COLS) \
            .fillna(0)

        gen_cap_tech_reg = self.c.o.gen_yr_df[self.c.o.gen_yr_df.property == 'Installed Capacity'] \
            .groupby(['model'] + self.c.GEO_COLS + ['Category']) \
            .agg({'value': 'sum'}) \
            .unstack(level=self.c.GEO_COLS) \
            .fillna(0)

        # Calculate as EN[GWh]/(Capacity[MW]/1000*hours)
        # Standard
        # As we make adjustments for co_firing on the energy values, we must re-calculate this

        df = (gen_by_tech_reg_orig / (gen_cap_tech_reg / 1000 * self.nr_days * 24)).fillna(0)

        return df

    @property
    @memory_cache
    def line_cap(self):
        """
        TODO DOCSTRING
        """
        line_cap = self.c.o.line_yr_df[(self.c.o.line_yr_df.property == 'Import Limit') | \
                                       (self.c.o.line_yr_df.property == 'Export Limit')] \
            .groupby(['model', 'regFrom', 'regTo', 'property']) \
            .agg({'value': 'sum'}) \
            .unstack(level='property')
        return line_cap

    @property
    @memory_cache
    def line_cap_reg(self):
        """
        Get the line capacity total between each region
        """
        df = self.line_cap.reset_index()
        df = df[df.regFrom != df.regTo]
        df.loc[:, 'line'] = df.regFrom + '-' + df.regTo
        df = df.groupby(['model', 'line']).sum(numeric_only=True)

        if df.shape[0] == 0:
            pd.DataFrame({'model': self.c.o.line_yr_df.model.unique(),
                          'reg_from': ['None'] * len(self.c.o.line_yr_df.model.unique()),
                          'reg_to': ['None'] * len(self.c.o.line_yr_df.model.unique()),
                          'value': [0] * len(self.c.o.line_yr_df.model.unique())})

        return df

    @property
    @memory_cache
    def line_imp_exp(self):
        line_imp_exp = self.c.o.line_yr_df[(self.c.o.line_yr_df.property == 'Flow') | \
                                           (self.c.o.line_yr_df.property == 'Flow Back')] \
            .groupby(['model', 'regFrom', 'regTo', 'property']) \
            .agg({'value': 'sum'}) \
            .unstack(level='property')

        if line_imp_exp.shape[0] == 0:
            pd.DataFrame({'model': self.c.o.line_yr_df.model.unique(),
                          'reg_from': ['None'] * len(self.c.o.line_yr_df.model.unique()),
                          'reg_to': ['None'] * len(self.c.o.line_yr_df.model.unique()),
                          'value': [0] * len(self.c.o.line_yr_df.model.unique())})

        return line_imp_exp

    @property
    @memory_cache
    def line_imp_exp_reg(self):
        """
        TODO DOCSTRING
        """

        line_imp_exp_reg = self.line_imp_exp.reset_index()
        line_imp_exp_reg = line_imp_exp_reg[line_imp_exp_reg.regFrom != line_imp_exp_reg.regTo]
        line_imp_exp_reg.loc[:, 'line'] = line_imp_exp_reg.regFrom + '-' + line_imp_exp_reg.regTo

    @property
    @memory_cache
    def line_imp_exp_reg(self):
        """
        Get the net annual flow across each transmission corridor (i.e. between each region)
        """
        line_imp_exp = self.c.o.line_yr_df[(self.c.o.line_yr_df.property == 'Flow') | \
                                           (self.c.o.line_yr_df.property == 'Flow Back')] \
            .groupby(['model', 'regFrom', 'regTo', 'property']) \
            .agg({'value': 'sum'}) \
            .unstack(level='property')

        if line_imp_exp.shape[0] == 0:
            pd.DataFrame({'model': self.c.o.line_yr_df.model.unique(),
                          'reg_from': ['None'] * len(self.c.o.line_yr_df.model.unique()),
                          'reg_to': ['None'] * len(self.c.o.line_yr_df.model.unique()),
                          'value': [0] * len(self.c.o.line_yr_df.model.unique())})

        line_imp_exp_reg = line_imp_exp.reset_index()
        line_imp_exp_reg = line_imp_exp_reg[line_imp_exp_reg.regFrom != line_imp_exp_reg.regTo]
        line_imp_exp_reg.loc[:, 'line'] = line_imp_exp_reg.regFrom + '-' + line_imp_exp_reg.regTo

        return line_imp_exp_reg

    @property
    @memory_cache
    def other_re_gen_abs(self):
        """
        TODO DOCSTRING
        """
        other_re_gen_abs = self.c.o.gen_df[(self.c.o.gen_df.property == 'Generation') &
                                           (self.c.o.gen_df.Category.isin(CONSTR_TECHS))] \
            .assign(timestamp=dd.to_datetime(self.c.o.gen_df['timestamp']).dt.floor('D')) \
            .groupby(['model', 'Category', ] + self.c.GEO_COLS + ['timestamp']) \
            .agg({'value': 'sum'}) \
            .unstack('Category') \
            .fillna(0) \
            .stack('Category') \
            .unstack(level=self.c.GEO_COLS) \
            .fillna(0) \
            .apply(lambda x: x * self.hour_corr)
        other_re_gen_abs = (other_re_gen_abs * self.geo_col_filler).fillna(0)
        return other_re_gen_abs

    @property
    @memory_cache
    def other_re_energy_vio(self):
        """
        TODO DOCSTRING
        """
        other_re_energy_vio = self.c.o.gen_df[(self.c.o.gen_df.property == 'Min Energy Violation') &
                                              (self.c.o.gen_df.Category.isin(CONSTR_TECHS))] \
            .assign(timestamp=dd.to_datetime(self.c.o.gen_df['timestamp']).dt.floor('D')) \
            .groupby(['model', 'Category'] + self.c.GEO_COLS + ['timestamp']) \
            .agg({'value': 'sum'}) \
            .unstack('Category') \
            .fillna(0) \
            .stack('Category') \
            .unstack(level=self.c.GEO_COLS) \
            .fillna(0) \
            .apply(lambda x: x * self.hour_corr)
        other_re_energy_vio = (other_re_energy_vio * self.geo_col_filler).fillna(0)
        return other_re_energy_vio

    @property
    @memory_cache
    def other_re_av(self):
        """
        TODO DOCSTRING
        """
        other_re_av = self.other_re_energy_vio + self.other_re_gen_abs
        return other_re_av

    @property
    @memory_cache
    def re_curtailment_rate_by_tech(self):
        """
        Get the curtailment / minimum energy violations by technology
        """

        # Output 8 & 9

        # Fill in data for regions which have no VRE (i.e. zero arrays!) to allow similar arrays for load_ts and
        # vre_add_df_columns
        # To add something for subregions

        all_re_av = pd.concat([self.vre_av_abs, self.other_re_av], axis=0).reset_index().groupby(
            ['model', 'timestamp', 'Category']).sum()

        re_curtailment_rate_by_tech = (self.all_re_curtailed.sum(axis=1).groupby(['model', 'Category']).sum().unstack(
            'Category') / all_re_av.sum(axis=1).groupby(['model', 'Category']).sum().unstack('Category')).fillna(
            0) * 100
        re_curtailment_rate_by_tech = pd.concat([re_curtailment_rate_by_tech, self.re_curtailment_rate.rename('All')],
                                                axis=1)

        return re_curtailment_rate_by_tech

    @property
    @memory_cache
    def all_re_curtailed(self):
        all_re_curtailed = pd.concat([self.vre_curtailed, self.other_re_energy_vio], axis=0).reset_index().groupby(
            ['model', 'timestamp', 'Category']).sum()
        return all_re_curtailed

    @property
    @memory_cache
    def re_curtailment_rate(self):
        all_re_av = pd.concat([self.vre_av_abs, self.other_re_av], axis=0).reset_index().groupby(
            ['model', 'timestamp', 'Category']).sum()
        re_curtailment_rate = (self.all_re_curtailed.sum(axis=1).groupby('model').sum() / all_re_av.sum(axis=1).groupby(
            'model').sum()).fillna(0) * 100
        return re_curtailment_rate

    @property
    @memory_cache
    def curtailment_rate(self):
        """
        Get the curtailment rate
        """

        # Output 8 & 9

        # Fill in data for regions which have no VRE (i.e. zero arrays!) to allow similar arrays for load_ts and
        # vre_add_df_columns
        # To add something for subregions

        # There is an error in PLEXOS with Available Capacity versus Generation (Gen exceeds Av Capacity)

        curtailment_rate = (self.vre_curtailed.sum(axis=1).groupby('model').sum() / self.vre_av_abs.sum(axis=1).groupby(
            'model').sum()).fillna(0) * 100

        ### Note that this is the new implementation for ISLAND-style aggregation. using self.c.GEO_COLS[0] i.e. the top tier of regional grouping.
        vre_curtailed_grouped = self.vre_curtailed.T.groupby(self.c.GEO_COLS[0]).sum().T
        vre_av_abs_grouped = self.vre_av_abs.T.groupby(self.c.GEO_COLS[0]).sum().T

        ## This could be renamed _reg. But should be done in consistent manner, so leaving for now.
        curtailment_rate_reg = (vre_curtailed_grouped.groupby('model').sum() /
                                vre_av_abs_grouped.groupby('model').sum()).fillna(0) * 100

        curtailment_rate = pd.concat([curtailment_rate_reg, curtailment_rate.rename('Overall')], axis=1)

        return curtailment_rate

    @property
    @memory_cache
    def gen_op_costs_by_reg(self):
        """
        TODO: DOCSTRING
        """

        gen_cost_props_w_penalty = ['Emissions Cost', 'Fuel Cost', 'Max Energy Violation Cost',
                                    'Min Energy Violation Cost', 'Ramp Down Cost', 'Ramp Up Cost',
                                    'Start & Shutdown Cost']

        # Excludes VO&M as this is excluded for some generators because of bad scaling of the objective function
        gen_op_cost_props = ['Emissions Cost', 'Fuel Cost', 'Start & Shutdown Cost']

        # Standard costs reported as USD'000
        gen_op_costs = self.c.o.gen_yr_df[self.c.o.gen_yr_df.property.isin(gen_op_cost_props)]
        gen_op_costs_w_pen = self.c.o.gen_yr_df[self.c.o.gen_yr_df.property.isin(gen_cost_props_w_penalty)]

        # Scale costs to be also USD'000
        gen_vom = self.c.o.gen_yr_df[self.c.o.gen_yr_df.property == 'Generation'].fillna(0)
        # gen_vom.loc[:, 'value'] = gen_vom.apply(lambda x: x.value * x.VOM, axis=1).fillna(0)
        # gen_vom.loc[:, 'property'] = 'VO&M Cost'
        gen_vom.assign(value=lambda x: x.value / x.VOM)
        gen_vom.assign(property='VO&M Cost')

        gen_fom = self.c.o.gen_yr_df.loc[self.c.o.gen_yr_df.property == 'Installed Capacity', :]
        # gen_fom.loc[:, 'value'] = gen_fom.apply(lambda x: x.value * x.FOM, axis=1).fillna(0)
        # gen_fom.loc[:, 'property'] = 'FO&M Cost'
        gen_fom.assign(value=lambda x: x.value / x.FOM)
        gen_fom.assign(property='FO&M Cost')
        self.gen_capex.loc[:, 'property'] = 'Investment Cost'
        self.gen_capex.assign(value=lambda x: x.value / x.CAPEX)
        self.gen_capex.assign(property='Investment Cost')

        gen_total_costs = dd.concat([gen_op_costs, gen_vom, gen_fom, self.gen_capex], axis=0)
        gen_total_costs_w_pen = dd.concat([gen_op_costs_w_pen, gen_vom, gen_fom, self.gen_capex], axis=0)
        gen_op_costs = dd.concat([gen_op_costs, gen_vom], axis=0)

        # Scale to USDm
        gen_op_costs_by_reg = gen_op_costs \
            .groupby(['model'] + self.c.GEO_COLS + ['Category', 'property']) \
            .agg({'value': 'sum'}) \
            .applymap(lambda x: x / 1e3)
        gen_total_costs_by_reg = gen_total_costs \
            .groupby(['model'] + self.c.GEO_COLS + ['Category', 'property']) \
            .agg({'value': 'sum'}) \
            .applymap(lambda x: x / 1e3)
        gen_total_costs_by_reg_w_pen = gen_total_costs_w_pen \
            .groupby(['model'] + self.c.GEO_COLS + ['Category', 'property']) \
            .agg({'value': 'sum'}) \
            .applymap(lambda x: x / 1e3)

        # Ramp costs by reg in USDm
        gen_by_name_ts = self.c.o.gen_df[self.c.o.gen_df.property == 'Generation'] \
            .set_index(['model', 'name'] + self.c.GEO_COLS + ['Category', 'timestamp']) \
            .value
        ramp_by_gen_name = (gen_by_name_ts - gen_by_name_ts.shift(1)).fillna(0)
        ramp_costs_by_gen_name = pd.merge(ramp_by_gen_name.reset_index(), self.c.soln_idx[['name', 'RampCost']],
                                          on='name',
                                          how='left').set_index(
            ['model', 'name'] + self.c.GEO_COLS + ['Category', 'timestamp'])
        ramp_costs_by_gen_name.loc[:, 'value'] = (
                ramp_costs_by_gen_name.value.abs() * ramp_costs_by_gen_name.RampCost.fillna(0))
        ramp_costs_by_gen_name.loc[:, 'property'] = 'Ramp Cost'
        gen_ramp_costs_by_reg = ramp_costs_by_gen_name.reset_index().groupby(
            ['model'] + self.c.GEO_COLS + ['Category', 'property']).sum().value / 1e6

        # ### Final dataframes of costs
        gen_op_costs_by_reg = pd.concat([gen_op_costs_by_reg, gen_ramp_costs_by_reg], axis=0).reset_index().groupby(
            ['model'] + self.c.GEO_COLS + ['Category', 'property']).sum().value

        return gen_op_costs_by_reg

    # ----------------------------------------------------------------------------------------------------------------------
    # Summary Output Variables
    # ----------------------------------------------------------------------------------------------------------------------

    # -----
    # Output 1
    @property
    @memory_cache
    def load_by_reg(self):
        """"
        TODO DOCSTRING
        """
        # There is an error in PLEXOS with Available Capacity versus Generation (Gen exceeds Av Capacity)
        load_by_reg = self.c.o.node_yr_df[self.c.o.node_yr_df.property == 'Load'] \
            .groupby(['model', 'timestamp'] + self.c.GEO_COLS) \
            .agg({'value': 'sum'})
        return load_by_reg

    @property
    @memory_cache
    def customer_load_by_reg(self):
        """
        TODO DOCSTRING
        """
        customer_load_by_reg = self.c.o.node_yr_df[(self.c.o.node_yr_df.property == 'Customer Load') |
                                                   (self.c.o.node_yr_df.property == 'Unserved Energy')] \
            .groupby(['model', 'timestamp'] + self.c.GEO_COLS) \
            .agg({'value': 'sum'})

        return customer_load_by_reg

    # -----
    # Output 2
    @property
    @memory_cache
    def use_by_reg(self):
        """"
        TODO DOCSTRING
        """
        use_by_reg = self.c.o.node_yr_df[self.c.o.node_yr_df.property == 'Unserved Energy'] \
            .groupby(['model'] + self.c.GEO_COLS) \
            .agg({'value': 'sum'})
        return use_by_reg

    @property
    @memory_cache
    def use_reg_daily_ts(self):
        """"
        TODO DOCSTRING
        """
        use_reg_daily_ts = self.c.o.node_yr_df[self.c.o.node_yr_df.property == 'Unserved Energy'] \
            .groupby(['model'] + self.c.GEO_COLS + [pd.Grouper(key='timestamp', freq='D')]) \
            .agg({'value': 'sum'})
        return use_reg_daily_ts

    # -----
    # Output 3

    def _get_output_3_variables(self):
        """
        TODO DOCSTRING
        """

        # todo this needs improvements, this is different implemented in outputs 3 and output 12 (summary)

        gen_by_tech_reg = self.c.o.gen_yr_df[self.c.o.gen_yr_df.property == 'Generation']
        gen_by_tech_reg_orig = gen_by_tech_reg.copy()  # For not seperating cofiring. good for CF comparison
        gen_techs = self.c.o.gen_yr_df.Category.drop_duplicates().values

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

        return gen_by_tech_reg, gen_by_tech_reg_orig

    @property
    @memory_cache
    def gen_by_tech_reg(self):
        """"
        TODO DOCSTRING
        """
        gen_by_tech_reg, _ = self._get_output_3_variables()
        gen_by_tech_reg = (gen_by_tech_reg
                           .groupby(['model'] + self.c.GEO_COLS + ['Category'])
                           .agg({'value': 'sum'})
                           .unstack(level=self.c.GEO_COLS)
                           .fillna(0))
        return gen_by_tech_reg

    @property
    @memory_cache
    def gen_by_tech_reg_orig(self):
        """
        TODO DOCSTRING
        """
        _, gen_by_tech_reg_orig = self._get_output_3_variables()
        gen_by_tech_reg_orig = (gen_by_tech_reg_orig
                                .groupby(['model'] + self.c.GEO_COLS + ['Category'])
                                .agg({'value': 'sum'})
                                .unstack(level=self.c.GEO_COLS)
                                .fillna(0))
        return gen_by_tech_reg_orig

    @property
    @memory_cache
    def gen_by_costTech_reg(self):
        """
        TODO DOCSTRING
        """
        gen_by_tech_reg, _ = self._get_output_3_variables()
        gen_by_costTech_reg = (gen_by_tech_reg
                               .groupby(['model'] + self.c.GEO_COLS + ['CostCategory'])
                               .agg({'value': 'sum'})
                               .unstack(level=self.c.GEO_COLS)
                               .fillna(0))
        return gen_by_costTech_reg

    @property
    @memory_cache
    def gen_by_weoTech_reg(self):
        gen_by_weoTech_reg = (self.c.o.gen_yr_df[self.c.o.gen_yr_df.property == 'Generation']
                              .groupby(['model'] + self.c.GEO_COLS + ['WEO_Tech_simpl'])
                              .agg({'value': 'sum'})
                              .unstack(level=self.c.GEO_COLS)
                              .fillna(0))
        return gen_by_weoTech_reg

    @property
    @memory_cache
    def gen_by_plant(self):
        """
        TODO DOCSTRING
        """
        gen_by_tech_reg, _ = self._get_output_3_variables()
        gen_by_plant = (gen_by_tech_reg
                        .groupby(['model', 'name'])
                        .agg({'value': 'sum'})
                        .unstack(level='model')
                        .fillna(0))
        return gen_by_plant

    # -----
    # Output 4
    @property
    @memory_cache
    def re_by_reg(self):
        """
        Get the share of a RE generation
        """
        re_techs = ['Solar', 'Wind', 'Bioenergy', 'Geothermal', 'Other', 'Marine', 'Hydro']
        gen_by_tech_reg = self.c.o.gen_yr_df[self.c.o.gen_yr_df.property == 'Generation']
        re_by_reg = gen_by_tech_reg.reset_index()
        re_by_reg.loc[:, 'RE'] = re_by_reg.Category.apply(lambda x: 'RE' if x in re_techs else 'Non-RE')
        print(re_by_reg)
        re_by_reg = (re_by_reg
                     .groupby(['model', 'RE'])
                     .sum()
                     .groupby(level=self.c.GEO_COLS[0], axis=1)
                     .sum())
        re_by_reg.loc[:, 'Overall'] = re_by_reg.sum(axis=1)
        re_by_reg = (re_by_reg
                     .loc[pd.IndexSlice[:, 'RE'],]
                     .droplevel('RE')
                     / re_by_reg.groupby('model')
                     .sum())

        return re_by_reg

    @property
    @memory_cache
    def vre_by_reg(self):
        """
        Get the share of a subset of generation (e.g. RE or VRE), with the subset technologies passed as a list
        """

        gen_by_tech_reg = self.c.o.gen_yr_df[self.c.o.gen_yr_df.property == 'Generation']
        vre_by_reg = gen_by_tech_reg.reset_index()
        vre_by_reg.loc[:, 'VRE'] = vre_by_reg.Category.apply(lambda x: 'VRE' if x in VRE_TECHS else 'Non-VRE')
        vre_by_reg = vre_by_reg.groupby(['model', 'VRE']).sum().groupby(level=self.c.GEO_COLS[0], axis=1).sum()
        vre_by_reg.loc[:, 'Overall'] = vre_by_reg.sum(axis=1)
        vre_by_reg = vre_by_reg.loc[pd.IndexSlice[:, 'VRE'],].droplevel('VRE') / vre_by_reg.groupby('model').sum()

        return vre_by_reg

    # -----
    # Output 5

    @property
    @memory_cache
    def unit_starts_by_tech(self):
        """
        TODO DOCSTRING
        """
        unit_starts_by_tech = self.c.o.gen_yr_df[self.c.o.gen_yr_df.property == 'Units Started'] \
            .groupby(['model', 'Category']) \
            .agg({'value': 'sum'}) \
            .unstack(level='Category')

        return unit_starts_by_tech

    # -----
    # Output 6
    @property
    @memory_cache
    def gen_max_by_tech_reg(self):
        """
        TODO DOCSTRING
        """
        gen_max_by_tech_reg = self.c.o.gen_df[self.c.o.gen_df.property == 'Generation'] \
            .groupby(['model'] + self.c.GEO_COLS + ['Category']) \
            .agg({'value': 'max'}) \
            .compute() \
            .unstack(level=self.c.GEO_COLS) \
            .fillna(0)
        return gen_max_by_tech_reg

    # -----
    # Output 7
    @property
    @memory_cache
    def tx_losses(self):
        tx_losses = self.c.o.line_yr_df[self.c.o.line_yr_df.property == 'Loss'] \
            .groupby(['model', 'timestamp', 'name']) \
            .agg({'value': 'sum'})
        return tx_losses

    # -----
    # Output 8

    @property
    @memory_cache
    def geo_col_filler(self):
        """
        TODO DOCSTRING
        """
        # Add zero values to regions without VRE
        geo_col_filler = pd.Series(data=np.ones(len(self.load_by_reg.unstack(self.c.GEO_COLS).columns)),
                                   index=self.load_by_reg.unstack(self.c.GEO_COLS).columns)
        return geo_col_filler

    @property
    @memory_cache
    def vre_cap(self):

        # Fill in data for regions which have no VRE (i.e. zero arrays!) to allow similar arrays for load_ts and
        # vre_add_df_columns
        # To add something for subregions
        vre_cap = self.c.o.gen_yr_df[(self.c.o.gen_yr_df.property == 'Installed Capacity') &
                                     (self.c.o.gen_yr_df.Category.isin(VRE_TECHS))] \
            .groupby(['model', 'Category'] + self.c.GEO_COLS) \
            .agg({'value': 'max'}) \
            .unstack('Category') \
            .fillna(0) \
            .stack('Category') \
            .unstack(level=self.c.GEO_COLS) \
            .fillna(0)

        vre_cap = (vre_cap * self.geo_col_filler).fillna(0)

        return vre_cap

    @property
    @memory_cache
    def vre_av_abs(self):
        vre_av_abs = self.c.o.gen_df[(self.c.o.gen_df.property == 'Available Capacity') &
                                     (self.c.o.gen_df.Category.isin(VRE_TECHS))] \
            .assign(timestamp=dd.to_datetime(self.c.o.gen_df['timestamp']).dt.floor('D')) \
            .groupby(['model', 'Category'] + self.c.GEO_COLS + ['timestamp']) \
            .agg({'value': 'sum'}) \
            .compute() \
            .unstack('Category') \
            .fillna(0) \
            .stack('Category') \
            .unstack(level=self.c.GEO_COLS) \
            .fillna(0) \
            .apply(lambda x: x * self.hour_corr)
        vre_av_abs = (vre_av_abs * self.geo_col_filler).fillna(0)

        return vre_av_abs

    @property
    @memory_cache
    def vre_av_norm(self):
        # 24 periods per day for the daily data
        vre_av_norm = (self.vre_av_abs / self.vre_cap / self.daily_periods).fillna(0)
        return vre_av_norm

    @property
    @memory_cache
    def vre_gen_abs(self):
        vre_gen_abs = self.c.o.gen_df[(self.c.o.gen_df.property == 'Generation') &
                                      (self.c.o.gen_df.Category.isin(VRE_TECHS))] \
            .assign(timestamp=dd.to_datetime(self.c.o.gen_df['timestamp']).dt.floor('D')) \
            .groupby(['model', 'Category', ] + self.c.GEO_COLS + ['timestamp']) \
            .agg({'value': 'sum'}) \
            .compute() \
            .unstack('Category') \
            .fillna(0) \
            .stack('Category') \
            .unstack(level=self.c.GEO_COLS) \
            .fillna(0) \
            .apply(lambda x: x * self.hour_corr)
        vre_gen_abs = (vre_gen_abs * self.geo_col_filler).fillna(0)
        return vre_gen_abs

    # -----
    # Output 9

    @property
    @memory_cache
    def vre_curtailed(self):
        """
        TODO DOCSTRING
        """
        # 24 periods per day for the daily data
        vre_curtailed = self.vre_av_abs - self.vre_gen_abs
        return vre_curtailed

    @property
    @memory_cache
    def co2_by_tech_reg(self):
        """
        TODO DOCSTRING
        """
        co2_by_tech_reg = self.c.o.em_gen_yr_df[self.c.o.em_gen_yr_df.parent.str.contains('CO2') &
                                                (self.c.o.em_gen_yr_df.property == 'Production')] \
            .groupby(['model'] + self.c.GEO_COLS + ['Category']) \
            .agg({'value': 'sum'})

        return co2_by_tech_reg

    @property
    @memory_cache
    def co2_by_reg(self):
        co2_by_reg = self.c.o.em_gen_yr_df[self.c.o.em_gen_yr_df.parent.str.contains('CO2') &
                                           (self.c.o.em_gen_yr_df.property == 'Production')] \
            .groupby(['model'] + self.c.GEO_COLS) \
            .agg({'value': 'sum'})

        return co2_by_reg
