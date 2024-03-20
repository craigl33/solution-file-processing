""""
TODO DOCSTRING
"""
import numpy as np
import pandas as pd
import dask.dataframe as dd
import os

from solution_file_processing.constants import VRE_TECHS, CONSTR_TECHS
from solution_file_processing.timeseries import create_output_11

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
            .value \
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
            .value \
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
            .agg({'value': 'sum'}) \
            .compute()
        return df

    @property
    @memory_cache
    def vre_av_abs_ts(self):
        """"
        TODO DOCSTRING
        """
        df = (self.c.o.gen_df[(self.c.o.gen_df.property == 'Available Capacity') &
                              (self.c.o.gen_df.Category.isin(VRE_TECHS))]
              .groupby(['model', 'Category', 'timestamp'])
              .agg({'value': 'sum'})
              .compute()
              .value
              .unstack(level='Category')
              .fillna(0))

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
        vre_av_abs_ts = self.c.o.gen_df[(self.c.o.gen_df.property == 'Available Capacity') & (
                self.c.o.gen_df.Category.isin(VRE_TECHS))] \
                            .groupby(['model', 'Category', 'timestamp']) \
                            .agg({'value': 'sum'}) \
                            .compute() \
                            .value \
                            .unstack(level='Category') \
                            .fillna(0)
               
        ### There is a slight issue with gen_by_tech in the case of key missing technologies (e.g. Storage and Hydro, perhaps others like wind and solar)
        ### It may be worth using the SolutionIndex itself to try to "fill" these
        try:
            storage_gen_ts = self.c.v.gen_by_tech_ts.Storage.rename('value').to_frame()
        except AttributeError:
            storage_gen_ts = pd.DataFrame(data={'value':[0]*self.c.v.gen_by_tech_ts.shape[0]}, index=self.c.v.gen_by_tech_ts.index, )

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
    def net_load_sto_curtail_ts(self):
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
        vre_gen_abs_ts = self.c.o.gen_df[
            (self.c.o.gen_df.property == 'Generation') & (
                self.c.o.gen_df.Category.isin(VRE_TECHS))].groupby(
            ['model', 'Category', 'timestamp']).agg({'value': 'sum'}).compute().value.unstack(
            level='Category').fillna(0)
        D = (self.c.o.gen_df[self.c.o.gen_df.property == 'Generation']
                          .groupby(['model', 'Category', 'timestamp'])
                          .agg({'value': 'sum'})
                          .compute()
                          .value
                          .unstack(level='Category')
                          .fillna(0)
                          .droplevel(0, axis=1))
        
        ### There is a slight issue with gen_by_tech in the case of key missing technologies (e.g. Storage and Hydro, perhaps others like wind and solar)
        ### It may be worth using the SolutionIndex itself to try to "fill" these
        try:
            storage_gen_ts = self.c.v.gen_by_tech_ts.Storage.rename('value').to_frame()
        except AttributeError:
            storage_gen_ts = pd.DataFrame(data={'value':[0]*self.c.v.gen_by_tech_ts.shape[0]}, index=self.c.v.gen_by_tech_ts.index, )

        _data = customer_load_ts.value.ravel() - storage_gen_ts.value.ravel() + storage_load_ts.value.ravel() - (
            vre_gen_abs_ts
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
    def customer_load_reg_ts(self):
        """
        TODO DOCSTRING
        """

            ## Fill in data for regions which have no VRE (i.e. zero arrays!) to allow similar arrays for load_ts and vre_av_ts
        

        customer_load_reg_ts = self.c.o.node_df[(self.c.o.node_df.property == 'Customer Load') |
                                                (self.c.o.node_df.property == 'Unserved Energy')] \
            .groupby(['model'] + self.c.GEO_COLS + ['timestamp']) \
            .agg({'value': 'sum'}) \
            .compute() \
            .value \
            .unstack(level=self.c.GEO_COLS)
        return customer_load_reg_ts

    @property
    @memory_cache
    def vre_av_reg_abs_ts(self):
        """
        TODO DOCSTRING
        """
        vre_av_reg_abs_ts = self.c.o.gen_df[(self.c.o.gen_df.property == 'Available Capacity') &
                                            (self.c.o.gen_df.Category.isin(VRE_TECHS))] \
            .groupby((['model'] + self.c.GEO_COLS + ['timestamp'])) \
            .agg({'value': 'sum'}) \
            .compute() \
            .value \
            .unstack(level=self.c.GEO_COLS).fillna(0)
        
        all_regs = self.customer_load_reg_ts.columns
        all_reg_unity = pd.Series(index=all_regs, data = [1]*len(all_regs))
        vre_av_reg_abs_ts = (vre_av_reg_abs_ts * all_reg_unity).fillna(0)

        return vre_av_reg_abs_ts
    
    @property
    @memory_cache
    def vre_gen_reg_abs_ts(self):
        """
        TODO DOCSTRING
        """
        vre_gen_reg_abs_ts = self.c.o.gen_df[(self.c.o.gen_df.property == 'Generation') &
                                            (self.c.o.gen_df.Category.isin(VRE_TECHS))] \
            .groupby((['model'] + self.c.GEO_COLS + ['timestamp'])) \
            .agg({'value': 'sum'}) \
            .compute() \
            .value \
            .unstack(level=self.c.GEO_COLS).fillna(0)
        
        all_regs = self.customer_load_reg_ts.columns
        all_reg_unity = pd.Series(index=all_regs, data = [1]*len(all_regs))
        vre_gen_reg_abs_ts = (vre_gen_reg_abs_ts * all_reg_unity).fillna(0)

        return vre_gen_reg_abs_ts
    
    @property
    @memory_cache
    def vre_curtailed_reg_ts(self):
        """
        TODO DOCSTRING
        """
                # Get vre_curtailed_reg_ts
        ### To move to variables.py
        ### Define model regs in multi-level format
        ### This could be done when calculating the vre_gen and av stuff too
        model_regs_multi = self.model_regs_multi
        model_regs_multi = pd.MultiIndex.from_tuples([[i for i in x if i != 'value'] for x in model_regs_multi])

        vre_av_reg_abs_ts = self.vre_av_reg_abs_ts
        vre_gen_reg_abs_ts = self.vre_gen_reg_abs_ts
        vre_regs = vre_av_reg_abs_ts.columns

        ## Fill in data for regions which have no VRE (i.e. zero arrays!) to allow similar arrays for load_ts and vre_av_ts
        for reg in list(model_regs_multi):
            if reg not in vre_regs:
                vre_av_reg_abs_ts.loc[:, reg] = 0
                vre_gen_reg_abs_ts.loc[:, reg] = 0

        ### Columns in alphabetical order
        vre_av_reg_abs_ts = vre_av_reg_abs_ts[model_regs_multi]
        vre_gen_reg_abs_ts = vre_gen_reg_abs_ts[model_regs_multi]
        vre_curtailed_reg_ts = vre_av_reg_abs_ts - vre_gen_reg_abs_ts

        return vre_curtailed_reg_ts

    @property
    @memory_cache
    def net_load_reg_ts(self):
        """"
        TODO DOCSTRING
        """
        net_load_reg_ts = self.customer_load_reg_ts - self.vre_av_reg_abs_ts
        
        return net_load_reg_ts

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
    def model_regs_multi(self):
        """
        TODO DOCSTRING
        """
        # Define model regs in multi-level format
        model_regs_multi = self.load_by_reg_ts.value.unstack(self.c.GEO_COLS).columns
        return model_regs_multi

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
            .value \
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
        gen_cap_costTech_reg = self.c.o.gen_yr_df[self.c.o.gen_yr_df.property == 'Installed Capacity'] \
            .groupby(['model'] + self.c.GEO_COLS + ['CostCategory']) \
            .agg({'value': 'sum'}) \
            .value \
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
            .value \
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
            .value \
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
            .value \
            .unstack(level=self.c.GEO_COLS) \
            .fillna(0)

        gen_cap_tech_reg = self.c.o.gen_yr_df[self.c.o.gen_yr_df.property == 'Installed Capacity'] \
            .groupby(['model'] + self.c.GEO_COLS + ['Category']) \
            .agg({'value': 'sum'}) \
            .value \
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
            .value \
            .unstack(level='property')
        return line_cap

    @property
    @memory_cache
    def line_cap_reg(self):
        """
        Get the line capacity total between each region
        """
        df = self.line_cap.stack().rename('value').reset_index()
        df = df[(df.regFrom != df.regTo)&(df.property == 'Export Limit')]
        df.loc[:, 'line'] = df.regFrom + '-' + df.regTo
        #todo ? change to groupby agg?
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
        """
        TODO DOCSTRING
        """
        line_imp_exp = self.c.o.line_yr_df[(self.c.o.line_yr_df.property == 'Flow') | \
                                           (self.c.o.line_yr_df.property == 'Flow Back')] \
            .groupby(['model', 'regFrom', 'regTo', 'property']) \
            .agg({'value': 'sum'}) \
            .value \
            .unstack(level='property')

        if line_imp_exp.shape[0] == 0:
           line_imp_exp = pd.DataFrame({'model': self.c.o.line_yr_df.model.unique(),
                          'reg_from': ['None'] * len(self.c.o.line_yr_df.model.unique()),
                          'reg_to': ['None'] * len(self.c.o.line_yr_df.model.unique()),
                          'Flow': [0] * len(self.c.o.line_yr_df.model.unique()),
                          'Flow Back': [0] * len(self.c.o.line_yr_df.model.unique())})
           line_imp_exp = line_imp_exp.set_index(['model', 'regFrom', 'regTo'])  
                  

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
        line_imp_exp_reg = line_imp_exp_reg[line_imp_exp_reg.regFrom != line_imp_exp_reg.regTo]
        line_imp_exp_reg = line_imp_exp_reg.set_index(['model', 'line'])

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
            .compute() \
            .value \
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
            .compute() \
            .value \
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

        all_re_av = pd.concat([self.vre_av_abs, self.other_re_av], axis=0) \
                                .reset_index() \
                                .groupby(['model', 'timestamp', 'Category']) \
                                .sum()

        re_curtailment_rate_by_tech = (self.all_re_curtailed.sum(axis=1).groupby(['model', 'Category']).sum().unstack(
            'Category') / all_re_av.sum(axis=1).groupby(['model', 'Category']).sum().unstack('Category')).fillna(
            0) * 100
        re_curtailment_rate_by_tech = pd.concat([re_curtailment_rate_by_tech, self.re_curtailment_rate.rename('All')],
                                                axis=1)

        return re_curtailment_rate_by_tech


    @property
    @memory_cache
    def all_re_curtailed(self):
        """
        TODO DOCSTRING
        """
        all_re_curtailed = pd.concat([self.vre_curtailed, self.other_re_energy_vio], axis=0).reset_index().groupby(
            ['model', 'timestamp','Category']).sum()
        return all_re_curtailed

    @property
    @memory_cache
    def re_curtailment_rate(self):
        """
        TODO DOCSTRING
        """
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

        # Note that this is the new implementation for ISLAND-style aggregation. using self.c.GEO_COLS[0] i.e. the top
        # tier of regional grouping.
        vre_curtailed_grouped = self.vre_curtailed.T.groupby(self.c.GEO_COLS[0]).sum().T
        vre_av_abs_grouped = self.vre_av_abs.T.groupby(self.c.GEO_COLS[0]).sum().T

        # This could be renamed _reg. But should be done in consistent manner, so leaving for now.
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

        # raise NotImplementedError("This is not working yet. self.gen_capex needs to be implemented")
        # Excludes VO&M as this is excluded for some generators because of bad scaling of the objective function
        gen_op_cost_props = ['Emissions Cost', 'Fuel Cost', 'Start & Shutdown Cost']

        # Standard costs reported as USD'000
        gen_op_costs = self.c.o.gen_yr_df[self.c.o.gen_yr_df.property.isin(gen_op_cost_props)]

        # Scale costs to be also USD'000
        gen_vom = self.c.o.gen_yr_df[self.c.o.gen_yr_df.property == 'Generation'].fillna(0)
        # gen_vom.loc[:, 'value'] = gen_vom.apply(lambda x: x.value * x.VOM, axis=1).fillna(0)
        # gen_vom.loc[:, 'property'] = 'VO&M Cost'
        gen_vom.assign(value=lambda x: x.value / x.VOM)
        gen_vom.assign(property='VO&M Cost')

        gen_op_costs = pd.concat([gen_op_costs, gen_vom], axis=0)

        # Scale to USDm
        gen_op_costs_by_reg = gen_op_costs \
            .groupby(['model'] + self.c.GEO_COLS + ['Category', 'property']) \
            .agg({'value': 'sum'}) \
            .applymap(lambda x: x / 1e3)
        
        gen_by_name_ts = self.c.o.gen_df[self.c.o.gen_df.property == 'Generation'].set_index('timestamp')

        # Ramp costs by reg in USDm
        # Calculated outside of PLEXOS as these are not costed into the optimisation
        # As DASK DFs cannot be multiindex, we need to proceed carefully with the following gen_by_name_calculation
        ramp_by_gen_name = gen_by_name_ts[ ['model', 'name'] + self.c.GEO_COLS + ['Category', 'property', 'value']]
        ramp_by_gen_name.assign(value=ramp_by_gen_name.value - ramp_by_gen_name.value.shift(1))
        ramp_by_gen_name.assign(property = 'Ramp')

        ramp_costs_by_gen_name = dd.merge(ramp_by_gen_name, self.c.soln_idx[['name', 'RampCost']],
                                            on='name',
                                            how='left')

        ramp_costs_by_gen_name.assign(value = ramp_costs_by_gen_name.value.abs() * ramp_costs_by_gen_name.RampCost.fillna(0))
        ramp_costs_by_gen_name.assign(property = 'Ramp Cost')

        gen_ramp_costs_by_reg = ramp_costs_by_gen_name.reset_index().groupby(
            ['model'] + self.c.GEO_COLS + ['Category', 'property']).agg({'value': 'sum'}).compute() / 1e6

        # ### Final dataframes of costs
        gen_op_costs_by_reg = pd.concat([gen_op_costs_by_reg, gen_ramp_costs_by_reg], axis=0).reset_index().groupby(
            ['model'] + self.c.GEO_COLS + ['Category', 'property']).sum()

        return gen_op_costs_by_reg
    

    @property
    @memory_cache
    def gen_total_costs_by_reg(self):
        """
        TODO: Double check the implementation of these outputs

        First draft implementation of total costs. Very likely that these havent been implemented properly and this will need to be edited

        """

        # raise NotImplementedError("This is not working yet. self.gen_capex needs to be implemented")
        # Excludes VO&M as this is excluded for some generators because of bad scaling of the objective function
        gen_op_cost_props = ['Emissions Cost', 'Fuel Cost', 'Start & Shutdown Cost']

        # Standard costs reported as USD'000
        gen_op_costs = self.c.o.gen_yr_df[self.c.o.gen_yr_df.property.isin(gen_op_cost_props)]

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

        try:
            # self.c.v.gen_capex.loc[:, 'property'] = 'Investment Cost'
            # self.gen_capex.assign(value=lambda x: x.value / x.CAPEX)
            # self.gen_capex.assign(property='Investment Cost')
            gen_capex = self.c.o.gen_yr_df.loc[self.c.o.gen_yr_df.property == 'Installed Capacity',:]
            gen_capex.assign(value = lambda x: x.value*x.CAPEX)
            gen_capex.assign(property = "Investment Cost")
        except KeyError:
            ### If CAPEX isn't defined in the generators parameters sheet, this won't work
            gen_capex = dd.from_pandas(pd.DataFrame(None))

        gen_total_costs = dd.concat([gen_op_costs, gen_vom, gen_fom, gen_capex], axis=0)


        # Scale to USDm
        gen_total_costs_by_reg = gen_total_costs \
            .groupby(['model'] + self.c.GEO_COLS + ['Category', 'property']) \
            .agg({'value': 'sum'}) \
            .applymap(lambda x: x / 1e3)

        return gen_total_costs_by_reg


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

    def _get_cofiring_generation(
            self):
        """
        Function for getting the Generation by technology data, taking into account cofiring aspects. 
        This is a legacy calculation from the Indonesia IPSE report. This should be updated and changed 
        TODO this needs better implementation, also see bug with different versions in outputs 3 and 12

        """

        # todo this needs improvements, this is different implemented in outputs 3 and output 12 (summary)

        gen_by_tech_reg = self.c.o.gen_yr_df[self.c.o.gen_yr_df.property == 'Generation']
        gen_by_tech_reg_orig = gen_by_tech_reg.copy()  # For not seperating cofiring. good for CF comparison
        gen_techs = self.c.o.gen_yr_df.Category.drop_duplicates().values

        # This will need to be updated for NZE
        if 'Cofiring' in gen_techs:
            # Ratio could (and should!) be an input 
            # This would include ammonia, biofuels and hydrogen
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
        gen_by_tech_reg, _ = self._get_cofiring_generation()
        gen_by_tech_reg = (gen_by_tech_reg
                           .groupby(['model'] + self.c.GEO_COLS + ['Category'])
                           .agg({'value': 'sum'})
                           .value
                           .unstack(level=self.c.GEO_COLS)
                           .fillna(0))
        return gen_by_tech_reg

    @property
    @memory_cache
    def gen_by_tech_reg_orig(self):
        """
        TODO DOCSTRING
        """
        _, gen_by_tech_reg_orig = self._get_cofiring_generation()
        gen_by_tech_reg_orig = (gen_by_tech_reg_orig
                                .groupby(['model'] + self.c.GEO_COLS + ['Category'])
                                .agg({'value': 'sum'})
                                .unstack(level=self.c.GEO_COLS)
                                .value
                                .fillna(0))
        return gen_by_tech_reg_orig

    @property
    @memory_cache
    def gen_by_costTech_reg(self):
        """
        TODO DOCSTRING
        """
        gen_by_tech_reg, _ = self._get_cofiring_generation()
        gen_by_costTech_reg = (gen_by_tech_reg
                               .groupby(['model'] + self.c.GEO_COLS + ['CostCategory'])
                               .agg({'value': 'sum'})
                               .value
                               .unstack(level=self.c.GEO_COLS)
                               .fillna(0))
        return gen_by_costTech_reg

    @property
    @memory_cache
    def gen_by_weoTech_reg(self):
        """
        TODO DOCSTRING
        """
        gen_by_weoTech_reg = (self.c.o.gen_yr_df[self.c.o.gen_yr_df.property == 'Generation']
                              .groupby(['model'] + self.c.GEO_COLS + ['WEO_Tech_simpl'])
                              .agg({'value': 'sum'})
                              .value
                              .unstack(level=self.c.GEO_COLS)
                              .fillna(0))
        return gen_by_weoTech_reg

    @property
    @memory_cache
    def gen_by_plant(self):
        """
        TODO DOCSTRING
        """
        gen_by_tech_reg, _ = self._get_cofiring_generation()
        gen_by_plant = (gen_by_tech_reg
                        .groupby(['model', 'name'])
                        .agg({'value': 'sum'})
                        .value
                        .unstack(level='model')
                        .fillna(0))
        return gen_by_plant

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
        ### New simplified implementation to avoid errors. 
        re_by_reg = (re_by_reg
                     .groupby(['model', 'RE', self.c.GEO_COLS[0]])
                     .agg({'value':'sum'}).value.unstack('RE'))
        re_by_reg = (re_by_reg['RE']/re_by_reg.sum(axis=1)).unstack(self.c.GEO_COLS[0])

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
        vre_by_reg = vre_by_reg.groupby(['model', 'VRE', self.c.GEO_COLS[0]]).agg({'value':'sum'}).value.unstack('VRE')
        vre_by_reg = (vre_by_reg['VRE']/vre_by_reg.sum(axis=1)).unstack(self.c.GEO_COLS[0])

        return vre_by_reg
    

    @property
    @memory_cache
    def unit_starts_by_tech(self):
        """
        TODO DOCSTRING
        """
        unit_starts_by_tech = self.c.o.gen_yr_df[self.c.o.gen_yr_df.property == 'Units Started'] \
            .groupby(['model', 'Category']) \
            .agg({'value': 'sum'}) \
            .value \
            .unstack(level='Category')

        return unit_starts_by_tech

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
            .value \
            .unstack(level=self.c.GEO_COLS) \
            .fillna(0)
        return gen_max_by_tech_reg

    @property
    @memory_cache
    def tx_losses(self):
        """
        TODO DOCSTRING
        """
        tx_losses = self.c.o.line_yr_df[self.c.o.line_yr_df.property == 'Loss'] \
            .groupby(['model', 'timestamp', 'name']) \
            .agg({'value': 'sum'})
        return tx_losses

    @property
    @memory_cache
    def geo_col_filler(self):
        """
        TODO DOCSTRING
        """
        # Add zero values to regions without VRE
        geo_col_filler = pd.Series(data=np.ones(len(self.load_by_reg.value.unstack(self.c.GEO_COLS).columns)),
                                   index=self.load_by_reg.value.unstack(self.c.GEO_COLS).columns)
        return geo_col_filler

    @property
    @memory_cache
    def vre_cap(self):
        """
        TODO DOCSTRING
        """

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
            .value \
            .unstack(level=self.c.GEO_COLS) \
            .fillna(0)

        vre_cap = (vre_cap * self.geo_col_filler).fillna(0)

        return vre_cap

    @property
    @memory_cache
    def vre_av_abs(self):
        """
        Daily available VRE generation by region
        """
        vre_av_abs = self.c.o.gen_df[(self.c.o.gen_df.property == 'Available Capacity') &
                                     (self.c.o.gen_df.Category.isin(VRE_TECHS))] \
            .assign(timestamp=dd.to_datetime(self.c.o.gen_df['timestamp']).dt.floor('D')) \
            .groupby(['model', 'Category'] + self.c.GEO_COLS + ['timestamp']) \
            .agg({'value': 'sum'}) \
            .compute() \
            .unstack('Category') \
            .fillna(0) \
            .stack('Category') \
            .value \
            .unstack(level=self.c.GEO_COLS) \
            .fillna(0) \
            .apply(lambda x: x * self.hour_corr)
        vre_av_abs = (vre_av_abs * self.geo_col_filler).fillna(0)

        return vre_av_abs

    @property
    @memory_cache
    def vre_av_norm(self):
        """
        TODO DOCSTRING
        """
        # 24 periods per day for the daily data
        vre_av_norm = (self.vre_av_abs / self.vre_cap / self.daily_periods).fillna(0)
        return vre_av_norm

    @property
    @memory_cache
    def vre_gen_abs(self):
        """
        TODO DOCSTRING
        """
        vre_gen_abs = self.c.o.gen_df[(self.c.o.gen_df.property == 'Generation') &
                                      (self.c.o.gen_df.Category.isin(VRE_TECHS))] \
            .assign(timestamp=dd.to_datetime(self.c.o.gen_df['timestamp']).dt.floor('D')) \
            .groupby(['model', 'Category', ] + self.c.GEO_COLS + ['timestamp']) \
            .agg({'value': 'sum'}) \
            .compute() \
            .unstack('Category') \
            .fillna(0) \
            .stack('Category') \
            .value \
            .unstack(level=self.c.GEO_COLS) \
            .fillna(0) \
            .apply(lambda x: x * self.hour_corr)
        vre_gen_abs = (vre_gen_abs * self.geo_col_filler).fillna(0)
        return vre_gen_abs
    
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
    def fuel_by_type(self):
        """
        TODO DOCSTRING
        """
        fuel_by_type = self.c.o.fuel_yr_df[(self.c.o.fuel_yr_df.property == 'Offtake')] \
            .groupby(['model'] + self.c.GEO_COLS + ['Type']) \
            .agg({'value': 'sum'})
     
        
        return fuel_by_type
    

    @property
    @memory_cache
    def co2_by_fuel_reg(self):
        """
        TODO DOCSTRING
        """
        co2_by_fuel_reg = self.c.o.em_fuel_yr_df[self.c.o.em_fuel_yr_df.parent.str.contains('CO2') &
                                           (self.c.o.em_fuel_yr_df.property == 'Production')] \
            .groupby(['model'] + self.c.GEO_COLS + ['Type']) \
            .agg({'value': 'sum'})

        return co2_by_fuel_reg


    @property
    @memory_cache
    def co2_by_reg(self):
        """
        TODO DOCSTRING
        """
        co2_by_reg = self.c.o.em_gen_yr_df[self.c.o.em_gen_yr_df.parent.str.contains('CO2') &
                                           (self.c.o.em_gen_yr_df.property == 'Production')] \
            .groupby(['model'] + self.c.GEO_COLS) \
            .agg({'value': 'sum'})

        return co2_by_reg

    @property
    @memory_cache
    def total_load_ts(self):
        """
        TODO DOCSTRING
        """
        total_load_ts = self.c.o.reg_df[self.c.o.reg_df.property == 'Load'] \
            .groupby(['model', 'timestamp']) \
            .agg({'value': 'sum'}).compute()
        return total_load_ts

    @property
    @memory_cache
    def use_ts(self):
        """
        TODO DOCSTRING
        """
        use_ts = self.c.o.reg_df[self.c.o.reg_df.property == 'Unserved Energy'] \
            .groupby(['model', 'timestamp']) \
            .agg({'value': 'sum'}).compute()
        return use_ts

    @property
    @memory_cache
    def use_dly_ts(self):
        """
        TODO DOCSTRING
        """
        # Need to calculate whether its 30mn or 1hr within but for now just assume hourly
        use_dly_ts = (self.c.o.reg_df[self.c.o.reg_df.property == 'Unserved Energy']
                      .assign(timestamp=dd.to_datetime(self.c.o.reg_df['timestamp']).dt.floor('D'))
                      .groupby(['model', 'timestamp'])
                      .agg({'value': 'sum'})
                      .applymap(lambda x: x / 1000 if isinstance(x, float) else x)
                      .compute())
        return use_dly_ts

    @property
    @memory_cache
    def load_w_use_ts(self):
        """
        TODO DOCSTRING
        """
        # todo needs implentation
        raise NotImplementedError
        # load_w_use_ts = dd.concat([total_load_ts.rename('Load'), use_ts.rename('USE')])

    @property
    @memory_cache
    def load_by_reg_ts(self):
        """
        TODO DOCSTRING
        """
        load_by_reg_ts = (self.c.o.node_df[self.c.o.node_df.property == 'Load']
                          .groupby(['model'] + self.c.GEO_COLS + ['timestamp'])
                          .agg({'value': 'sum'})
                          .compute()
                          .unstack(level='timestamp')
                          .fillna(0)
                          .stack('timestamp'))
        return load_by_reg_ts

    @property
    @memory_cache
    def use_reg_ts(self):
        """
        TODO DOCSTRING
        """
        use_reg_ts = self.c.o.node_df[self.c.o.node_df.property == 'Unserved Energy'] \
                                .groupby( ['model'] + self.c.GEO_COLS + ['timestamp']) \
                                .agg({'value': 'sum'}) \
                                .compute() \
                                .value \
                                .unstack(level=self.c.GEO_COLS)
        return use_reg_ts

    @property
    @memory_cache
    def gen_by_tech_ts(self):
        """
        TODO DOCSTRING
        """
        gen_by_tech_ts = self.c.o.gen_df[self.c.o.gen_df.property == 'Generation'] \
            .groupby(['model', 'Category', 'timestamp']) \
            .agg({'value': 'sum'}) \
            .compute() \
            .value \
            .unstack(level='Category') \
            .fillna(0)
        return gen_by_tech_ts

    @property
    @memory_cache
    def gen_by_subtech_ts(self):
        """
        TODO DOCSTRING
        """
        gen_by_subtech_ts = self.c.o.gen_df[self.c.o.gen_df.property == 'Generation'] \
            .groupby(['model', 'CapacityCategory', 'timestamp']) \
            .agg({'value': 'sum'}) \
            .compute() \
            .value \
            .unstack(level='CapacityCategory') \
            .fillna(0)
        return gen_by_subtech_ts

    @property
    @memory_cache
    def av_cap_by_tech_ts(self):
        """
        TODO DOCSTRING
        """
        av_cap_by_tech_ts = self.c.o.gen_df[self.c.o.gen_df.property == 'Available Capacity'] \
            .groupby(['model', 'Category', 'timestamp']) \
            .agg({'value': 'sum'}) \
            .compute() \
            .value \
            .unstack(level='Category') \
            .fillna(0)
        return av_cap_by_tech_ts

    @property
    @memory_cache
    def vre_gen_abs_ts(self):
        """
        TODO DOCSTRING
        """
        vre_gen_abs_ts = self.c.o.gen_df[
            (self.c.o.gen_df.property == 'Generation') & (self.c.o.gen_df.Category.isin(VRE_TECHS))] \
            .groupby(['model', 'Category', 'timestamp']) \
            .agg({'value': 'sum'}) \
            .compute() \
            .value \
            .unstack(level='Category') \
            .fillna(0)
        return vre_gen_abs_ts

    @property
    @memory_cache
    def vre_curtailed_ts(self):
        """
        TODO DOCSTRING
        """
        vre_curtailed_ts = self.vre_av_abs_ts - self.vre_gen_abs_ts
        return vre_curtailed_ts

    @property
    @memory_cache
    def re_curtailed_ts(self):
        """
        TODO DOCSTRING
        """
        min_energy_vio_tech_ts = self.c.o.gen_df[(self.c.o.gen_df.property == 'Min Energy Violation') &
                                                 (self.c.o.gen_df.Category.isin(CONSTR_TECHS))] \
            .groupby(['model', 'Category', 'timestamp']) \
            .agg({'value': 'sum'}) \
            .compute() \
            .value \
            .unstack(level='Category') \
            .fillna(0)
        re_curtailed_ts = pd.concat([self.vre_curtailed_ts, min_energy_vio_tech_ts])
        return re_curtailed_ts

    @property
    @memory_cache
    def customer_load_orig_ts(self):
        """
        TODO DOCSTRING
        """
        # Model filler for comparison of models with different inputs (e.g. DSM or EVs not included)
        # Series with indices matching the columns of the DF for filling in missing columns
        model_filler = pd.Series(data=[1] * len(self.c.v.model_names), index=self.c.v.model_names).rename_axis('model')
        try:
            purch_df = self.c.o.purch_df.compute()
        except ValueError:
            purch_df = pd.DataFrame(None)

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
                electr_profiles_orig_ts = pd.Series(data=[0] * len(self.c.v.customer_load_ts.index),
                                                    index=self.c.v.customer_load_ts.index)
            else:
                electr_profiles_orig_ts = (electr_profiles_orig_ts.unstack('model') * model_filler).fillna(0).stack(
                    'model').reorder_levels(['model', 'timestamp'])

            native_load_ts = self.c.o.node_df[self.c.o.node_df.property == 'Native Load'].groupby(
                ['model', 'timestamp']).sum().value.compute()

            customer_load_orig_ts = (
                    native_load_ts + ev_profiles_orig_ts + dsm_profiles_orig_ts + electr_profiles_orig_ts)
            # .fillna(customer_load_ts) ### For those profiles where EVs are missing, for e.g. ... other DSM to be added
        else:
            customer_load_orig_ts = self.c.v.customer_load_ts

        return customer_load_orig_ts

    
    @property
    @memory_cache
    def net_load_curtail_ts(self):
        """
        TODO DOCSTRING
        """
        #  net_load_ts is calculated as a series (as we obtain load 'value' and some across the x-axis (technologies)
        #  of vre_abs)
        net_load_curtail_ts = pd.DataFrame(
            self.c.v.customer_load_ts['value'] - self.c.v.vre_gen_abs_ts.fillna(0).sum(axis=1), columns=['value'])
        return net_load_curtail_ts

    @property
    @memory_cache
    def net_load_orig_ts(self):
        """
        TODO DOCSTRING
        """
        #  net_load_ts is calculated as a series (as we obtain load 'value' and some across the x-axis (technologies)
        #  of vre_abs)
        net_load_orig_ts = pd.DataFrame(
            self.customer_load_orig_ts['value'] - self.c.v.vre_av_abs_ts.fillna(0).sum(axis=1), columns=['value'])
        return net_load_orig_ts

    @property
    @memory_cache
    def inertia_by_tech(self):
        """
        TODO DOCSTRING
        """
        inertia_by_tech = self.c.v.gen_inertia.groupby(['model', 'Category', 'timestamp']).sum()
        return inertia_by_tech

    @property
    @memory_cache
    def inertia_by_reg(self):
        """
        TODO DOCSTRING
        """
        inertia_by_reg = self.c.v.gen_inertia.groupby(
            ['model'] + self.c.cfg['settings']['geo_cols'] + ['timestamp']).sum()
        return inertia_by_reg

    @property
    @memory_cache
    def total_inertia_ts(self):
        """
        TODO DOCSTRING
        """
        total_inertia_ts = self.c.v.gen_inertia.groupby(['model', 'timestamp']).sum()
        return total_inertia_ts

    @property
    @memory_cache
    def ramp_ts(self):
        """
        TODO DOCSTRING
        """
        ramp_ts = (self.net_load_ts.unstack(level='model') - self.net_load_ts.unstack(level='model').shift(1)) \
            .fillna(0) \
            .stack() \
            .sort_index(level=1) \
            .reset_index() \
            .set_index(['model', 'timestamp']) \
            .rename(columns={0: 'value'})
        return ramp_ts
    

    @property
    @memory_cache
    def th_ramp_ts(self):
        """
        TODO DOCSTRING
        """
        th_ramp_ts = (self.net_load_ts.unstack(level='model') - self.net_load_ts.unstack(level='model').shift(3)) \
            .fillna(0) \
            .stack() \
            .sort_index(level=1) \
            .reset_index() \
            .set_index(['model', 'timestamp']) \
            .rename(columns={0: 'value'})
        return th_ramp_ts

    @property
    @memory_cache
    def daily_pk_ts(self):
        """
        TODO DOCSTRING
        """
        daily_pk_ts = self.customer_load_ts \
            .groupby(['model', pd.Grouper(level='timestamp', freq='D')]) \
            .max() \
            .reindex(self.customer_load_ts.index) \
            .ffill()
        return daily_pk_ts

    @property
    @memory_cache
    def ramp_pc_ts(self):
        """
        TODO DOCSTRING
        """
        ramp_pc_ts = self.c.v.ramp_ts.value / self.daily_pk_ts.value * 100
        return ramp_pc_ts

    @property
    @memory_cache
    def th_ramp_pc_ts(self):
        """
        TODO DOCSTRING
        """
        th_ramp_pc_ts = self.c.v.th_ramp_ts.value / self.daily_pk_ts.value * 100
        return th_ramp_pc_ts
    
    @property
    @memory_cache
    def ramp_reg_ts(self):
        """
        TODO DOCSTRING
        """
        ramp_reg_ts =  (self.c.v.net_load_reg_ts.unstack(level='model') - self.c.v.net_load_reg_ts.unstack(level='model').shift(1)) \
            .fillna(0) \
            .stack(level='model') \
            .sort_index(level='model') \
            .stack(self.c.GEO_COLS) \
            .rename('value') \
            .to_frame() \
            .reorder_levels(['model'] + self.c.GEO_COLS +['timestamp'])
    
        return ramp_reg_ts

    @property
    @memory_cache
    def th_ramp_reg_ts(self):
        """
        TODO DOCSTRING
        """
        th_ramp_reg_ts = (self.c.v.net_load_reg_ts.unstack(level='model') - self.c.v.net_load_reg_ts.unstack(level='model').shift(3)) \
            .fillna(0) \
            .stack(level='model') \
            .sort_index(level='model') \
            .stack(self.c.GEO_COLS) \
            .rename('value') \
            .to_frame() \
            .reorder_levels(['model'] + self.c.GEO_COLS +['timestamp'])
    
        return th_ramp_reg_ts
    
    @property
    @memory_cache
    def daily_pk_reg_ts (self):
        """
        TODO DOCSTRING
        """
        daily_pk_reg_ts = self.c.v.customer_load_reg_ts.groupby([pd.Grouper(level='model'), pd.Grouper(level='timestamp', freq='D')]) \
                .max() \
                .reindex(self.c.v.customer_load_reg_ts.index).ffill()
    
        return daily_pk_reg_ts 
    
    
    @property
    @memory_cache
    def ramp_reg_pc_ts(self):
        """
        TODO DOCSTRING
        """
        ramp_reg_pc_ts = (self.c.v.ramp_reg_ts.value.unstack(self.c.GEO_COLS)/self.c.v.daily_pk_reg_ts) \
                            .stack(self.c.GEO_COLS).rename('value').to_frame()*100
    
        return ramp_reg_pc_ts

    
    @property
    @memory_cache
    def th_ramp_reg_pc_ts(self):
        """
        TODO DOCSTRING
        """
        th_ramp_reg_pc_ts = (self.c.v.th_ramp_reg_ts.value.unstack(self.c.GEO_COLS)/self.c.v.daily_pk_reg_ts) \
                            .stack(self.c.GEO_COLS).rename('value').to_frame()*100
    
        return th_ramp_reg_pc_ts

    @property
    @memory_cache
    def ramp_by_gen_tech_ts(self):
        """
        TODO DOCSTRING
        """
        ramp_by_gen_tech_ts = (self.gen_by_tech_ts - self.gen_by_tech_ts.shift(1)).fillna(0)
        return ramp_by_gen_tech_ts

    @property
    @memory_cache
    def ramp_by_gen_subtech_ts(self):
        """
        TODO DOCSTRING
        """
        ramp_by_gen_subtech_ts = (self.gen_by_subtech_ts - self.gen_by_subtech_ts.shift(1)).fillna(0)
        return ramp_by_gen_subtech_ts

    @property
    @memory_cache
    def th_ramp_by_gen_tech_ts(self):
        """
        TODO DOCSTRING
        """
        th_ramp_by_gen_tech_ts = (self.gen_by_tech_ts - self.gen_by_tech_ts.shift(3)).fillna(0)
        return th_ramp_by_gen_tech_ts

    @property
    @memory_cache
    def th_ramp_by_gen_subtech_ts(self):
        """
        TODO DOCSTRING
        """
        th_ramp_by_gen_subtech_ts = (self.gen_by_subtech_ts - self.gen_by_subtech_ts.shift(3)).fillna(0)
        return th_ramp_by_gen_subtech_ts

    @property
    @memory_cache
    def gen_out_tech_ts(self):
        """
        TODO DOCSTRING
        """
        gen_out_tech_ts = self.c.o.gen_df[(self.c.o.gen_df.property == 'Forced Outage') |
                                          (self.c.o.gen_df.property == 'Maintenance')] \
            .groupby(['model', 'Category', 'timestamp']) \
            .agg({'value': 'sum'}) \
            .compute() \
            .value \
            .unstack(level='Category')
        return gen_out_tech_ts

    @property
    @memory_cache
    def gen_out_by_type_ts(self):
        """
        TODO DOCSTRING
        """
        gen_out_by_type_ts = self.c.o.gen_df[(self.c.o.gen_df.property == 'Forced Outage') | \
                                             (self.c.o.gen_df.property == 'Maintenance')] \
            .groupby(['model', 'property', 'timestamp']) \
            .agg({'value': 'sum'}) \
            .compute() \
            .value \
            .unstack(level='property')
        return gen_out_by_type_ts
    
    @property
    @memory_cache
    def ldc(self):
        """
        TODO DOCSTRING
        """
        ldc = self.c.v.customer_load_ts.unstack('model')
        ldc = pd.DataFrame(np.flipud(np.sort(ldc.values, axis=0)), index=ldc.index, columns=ldc.columns)
        # Index = 0-8760
        ldc = ldc.reset_index(drop=True)

        return ldc
    
    @property
    @memory_cache
    def ldc_orig(self):
        """
        TODO DOCSTRING
        """
        ldc_orig = self.c.v.customer_load_orig_ts.unstack('model')
        ldc_orig = pd.DataFrame(np.flipud(np.sort(ldc_orig.values, axis=0)),
                                index=ldc_orig.index, columns=ldc_orig.columns)
        # Index = 0-8760
        ldc_orig = ldc_orig.reset_index(drop=True)
        
        return ldc_orig
    
    @property
    @memory_cache
    def nldc(self):
        """
        TODO DOCSTRING
        """
        nldc = self.c.v.net_load_ts.unstack('model')
        nldc = pd.DataFrame(np.flipud(np.sort(nldc.values, axis=0)),
                            index=nldc.index, columns=nldc.columns)
        # Index = 0-8760
        nldc = nldc.reset_index(drop=True)
        
        return nldc

    @property
    @memory_cache
    def nldc_orig(self):
        """
        TODO DOCSTRING
        still to do
        """
        nldc_orig = self.c.v.net_load_orig_ts.unstack('model')
        nldc_orig = pd.DataFrame(np.flipud(np.sort(nldc_orig.values, axis=0)),
                            index=nldc_orig.index, columns=nldc_orig.columns)
        # Index = 0-8760
        nldc_orig = nldc_orig.reset_index(drop=True)
        
        return nldc_orig
    

    @property
    @memory_cache
    def nldc_sto(self):
        """
        TODO DOCSTRING
        """
        nldc_sto = self.c.v.net_load_sto_ts.unstack('model')
        nldc_sto = pd.DataFrame(np.flipud(np.sort(nldc_sto.values, axis=0)),
                            index=nldc_sto.index, columns=nldc_sto.columns)
        # Index = 0-8760
        nldc_sto = nldc_sto.reset_index(drop=True)
        
        return nldc_sto

    @property
    @memory_cache
    def nldc_curtail(self):
        """
        TODO DOCSTRING
        """
        nldc_curtail = self.c.v.net_load_curtail_ts.unstack('model')
        nldc_curtail = pd.DataFrame(np.flipud(np.sort(nldc_curtail.values, axis=0)),
                            index=nldc_curtail.index, columns=nldc_curtail.columns)
        # Index = 0-8760
        nldc_curtail = nldc_curtail.reset_index(drop=True)
        
        return nldc_curtail
    
    @property
    @memory_cache
    def nldc_sto_curtail(self):
        """
        TODO DOCSTRING
        """
        nldc_sto_curtail = self.c.v.net_load_sto_curtail_ts.unstack('model')
        nldc_sto_curtail = pd.DataFrame(np.flipud(np.sort(nldc_sto_curtail.values, axis=0)),
                            index=nldc_sto_curtail.index, columns=nldc_sto_curtail.columns)
        # Index = 0-8760
        nldc_sto_curtail = nldc_sto_curtail.reset_index(drop=True)
        
        return nldc_sto_curtail
    

    @property
    @memory_cache
    def exports_by_reg_ts(self):
        """
        Time-series dataframe of the exports (+ve) and imports (-ve) between two nodes
        This variable was set-up specifically for monitoring trade with the EU in the Ukraine model
        This could also be defined as islands or otherwise in other regions
        """
        df = self.c.o.node_df[self.c.o.node_df.property == 'Net DC Export']\
                            .groupby(['model'] + self.c.GEO_COLS + ['timestamp']) \
                            .agg({'value': 'sum'}) \
                            .compute()

        return df
    
    @property
    @memory_cache
    def exports_ts(self):
        """
        Time-series dataframe of the exports (+ve) and imports (-ve)  between two nodes, summed over all national nodes.
        Currently, only national nodes are processed. This may change in the future or in different projects. 
        """
        df = self.c.o.node_df[self.c.o.node_df.property == 'Net DC Export']\
                            .groupby(['model', 'timestamp']) \
                            .agg({'value': 'sum'}) \
                            .compute()

        return df
    
    @property
    @memory_cache
    def load_w_exports_reg_ts(self):
        """
         ime-series dataframe of the load with exports representing as load in regions

        """
        load_w_exports_reg_ts = self.load_by_reg_ts + self.exports_by_reg_ts.where(self.exports_by_reg_ts > 0).fillna(0)

        return load_w_exports_reg_ts
    
    @property
    @memory_cache
    def net_load_w_exports_reg_ts(self):
        """
        Time-series dataframe of the net load with exports representing as load in regions
        """
        net_load_by_reg_ts = self.net_load_reg_ts.stack(self.c.GEO_COLS).reorder_levels(['model'] + self.c.GEO_COLS + ['timestamp'])
    
        net_load_w_exports_reg_ts = net_load_by_reg_ts + self.exports_by_reg_ts.where(self.exports_by_reg_ts > 0).fillna(0)

        return net_load_w_exports_reg_ts
        
    @property
    @memory_cache
    def load_w_exports_ts(self):
        """
        Time-series dataframe of the load with exports to external systems.

        """
        
        load_w_exports_reg_ts = self.load_by_reg_ts + self.exports_by_reg_ts.where(self.exports_by_reg_ts > 0).fillna(0)

        return load_w_exports_reg_ts
    
    @property
    @memory_cache
    def net_load_w_exports_ts(self):
        """
        Time-series dataframe of the net load with exports to external systems.
        Non-national nodes are currently excluded from the SFP.
        """
        net_load_ts = self.net_load_ts 
        
        net_load_w_exports_ts = net_load_ts + self.exports_ts.where(self.exports_ts > 0).fillna(0)

        return net_load_w_exports_ts
    

    @property
    @memory_cache
    def pumpload_reg_ts(self):
        pumpload_reg_ts = (self.c.o.node_df[(self.c.o.node_df.property == 'Pump Load') |
                                (self.c.o.node_df.property == 'Battery Load')]
                    .groupby(['model'] + self.c.GEO_COLS + ['timestamp'])
                    .agg({'value':'sum'})
                    .compute())
        
        return pumpload_reg_ts
    
    @property
    @memory_cache
    def reg_ids(self):
        reg_ids = list(np.unique(np.append(
                self.load_by_reg.value.unstack(self.c.GEO_COLS).droplevel(level=[r for r in self.c.GEO_COLS if r != self.c.GEO_COLS[0]], axis=1).replace(0,np.nan).dropna(
                    how='all', axis=1).columns,
                self.gen_by_tech_reg.droplevel(level=[r for r in self.c.GEO_COLS if r != self.c.GEO_COLS[0]], axis=1).replace(0,np.nan).dropna(
                    how='all', axis=1).columns)))
        # Make reg_ids flat list ('value', 'Region') -> 'Region'
        # Doesnt seem relevant in new implementation
        # reg_ids = [x[1] for x in reg_ids]
        return reg_ids


    @property
    @memory_cache
    def gen_stack_by_reg(self):
        # -----
        # Get: reg_ids

        load_by_reg = self.c.o.node_yr_df[self.c.o.node_yr_df.property == 'Load'] \
            .groupby(['model', 'timestamp'] + self.c.GEO_COLS) \
            .agg({'value': 'sum'})

        # Get gen_by_tech_reg
        gen_by_tech_reg = self.c.o.gen_yr_df[self.c.o.gen_yr_df.property == 'Generation']
        gen_techs = self.c.o.gen_yr_df.Category.drop_duplicates().values
        
        if 'Cofiring' in gen_techs:
        ### This would differ from project to project. Should probably be explicitly calculated
            
            bio_ratio = 0.1
            gen_by_cofiring_bio = gen_by_tech_reg[gen_by_tech_reg.Category == 'Cofiring']
            gen_by_cofiring_coal = gen_by_tech_reg[gen_by_tech_reg.Category == 'Cofiring']
            gen_by_tech_reg = gen_by_tech_reg[gen_by_tech_reg.Category != 'Cofiring']

            gen_by_cofiring_bio.loc[:, 'value'] = gen_by_cofiring_bio.value * bio_ratio
            gen_by_cofiring_bio = gen_by_cofiring_bio.replace('Cofiring', 'Bioenergy')

            gen_by_cofiring_coal.loc[:, 'value'] = gen_by_cofiring_coal.value * (1 - bio_ratio)
            gen_by_cofiring_coal = gen_by_cofiring_coal.replace('Cofiring', 'Coal')

            gen_by_tech_reg = pd.concat([gen_by_tech_reg, gen_by_cofiring_bio, gen_by_cofiring_coal], axis=0)

        gen_by_tech_reg = (gen_by_tech_reg
                        .groupby(['model'] + self.c.GEO_COLS + ['Category'])
                        .agg({'value': 'sum'})
                        .unstack(level=self.c.GEO_COLS)
                        .fillna(0))

        reg_ids = list(np.unique(np.append(
            self.load_by_reg.unstack(self.c.GEO_COLS).droplevel(level=[region for region in self.c.GEO_COLS if region != 'Region'],
                                                    axis=1).replace(0,
                                                                    np.nan).dropna(
                how='all', axis=1).columns,
            gen_by_tech_reg.droplevel(level=[region for region in self.c.GEO_COLS if region != 'Region'], axis=1).replace(0,
                                                                                                                    np.nan).dropna(
                how='all', axis=1).columns)))
        # Make reg_ids flat list ('value', 'Region') -> 'Region'
        reg_ids = [x[1] for x in reg_ids]

        # -----
        # Get: doi_summary
        if not os.path.exists(os.path.join(self.c.DIR_05_2_TS_OUT, '11a_days_of_interest_summary.csv')):
            create_output_11(self.c)
        doi_summary = pd.read_csv(os.path.join(self.c.DIR_05_2_TS_OUT, '11a_days_of_interest_summary.csv'),
                                index_col=0,
                                parse_dates=True)

        # -----
        # Get: use_reg_ts

        use_reg_ts = self.c.o.node_df[self.c.o.node_df.property == 'Unserved Energy'].groupby(
            ['model'] + self.c.GEO_COLS + ['timestamp']).agg({'value': 'sum'}).compute().unstack(
            level=self.c.GEO_COLS)

        # -----
        # Get: gen_stack_by_reg

        # Get needed variables
        load_by_reg_ts = self.load_by_reg_ts.unstack(
            level='timestamp').fillna(0).stack('timestamp')

        pumpload_reg_ts = self.pumpload_reg_ts
        underlying_load_reg = load_by_reg_ts - pumpload_reg_ts

        exports_by_reg_ts = self.exports_by_reg_ts
        exports_by_reg_ts = self.exports_by_reg_ts

        
        imports_by_reg_ts = self.exports_by_reg_ts.where(
            self.exports_by_reg_ts < 0).fillna(0).abs()
        exports_by_reg_ts = self.exports_by_reg_ts.where(self.exports_by_reg_ts > 0).fillna(0)

        net_load_reg_sto_ts = self.net_load_reg_ts.stack(self.c.GEO_COLS).reorder_levels(
            ['model'] + self.c.GEO_COLS + ['timestamp']).rename('value').to_frame() + pumpload_reg_ts

        net_load_w_exports_reg_ts = net_load_reg_sto_ts + exports_by_reg_ts
        

        # Get vre_curtailed_reg_ts
        ### To move to variables.py
    
        vre_curtailed_reg_ts = self.vre_curtailed_reg_ts

        gen_stack_by_reg = pd.concat([self.gen_by_tech_reg_ts.fillna(0),
                                    exports_by_reg_ts.value.rename('Exports'),
                                    imports_by_reg_ts.value.rename('Imports'),
                                    net_load_reg_sto_ts.value.rename('Net Load'),
                                    net_load_w_exports_reg_ts.value.rename('Net Load w/ Exports'),
                                    underlying_load_reg.value.rename('Underlying Load'),
                                    pumpload_reg_ts.value.rename('Storage Load'),
                                    load_by_reg_ts.value.rename('Total Load'),
                                    load_by_reg_ts.value.rename('Load2'),
                                    vre_curtailed_reg_ts.stack(self.c.GEO_COLS)
                                    .reorder_levels(['model'] + self.c.GEO_COLS + ['timestamp'])
                                    .rename('Curtailment'),
                                    use_reg_ts.stack(self.c.GEO_COLS)
                                    .reorder_levels(['model'] + self.c.GEO_COLS + ['timestamp'])
                                    .rename(columns={'value': 'Unserved Energy'}),
                                    ], axis=1)
        
        return gen_stack_by_reg

        
