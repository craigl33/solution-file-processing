""""
TODO DOCSTRING
"""
import numpy as np
import pandas as pd
import dask.dataframe as dd

from solution_file_processing.constants import VRE_TECHS

from solution_file_processing.utils.utils import drive_cache, mem_cache
from solution_file_processing import log

print = log.info


class VariablesCached:
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
    @mem_cache
    @drive_cache('variables')
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
    @mem_cache
    @drive_cache('variables')
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
    @mem_cache
    @drive_cache('variables')
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
    @mem_cache
    @drive_cache('variables')
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
    @mem_cache
    @drive_cache('variables')
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
    @mem_cache
    @drive_cache('variables')
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
    @mem_cache
    @drive_cache('variables')
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
    @mem_cache
    @drive_cache('variables')
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


class VariablesUncached:
    """
    TODO: DOCSTRING
    """

    def __init__(self, configuration_object):
        self.c = configuration_object

    @property
    @mem_cache
    def model_names(self):
        """"
        TODO DOCSTRING
        """
        model_names = list(np.sort(self.c.o.reg_df.model.drop_duplicates()))
        return model_names

    @property
    @mem_cache
    def cf_tech(self):
        """
        Get the capacity factor(%) per technology in the overall model
        """
        time_idx = self.c.o.reg_df.reset_index().timestamp.drop_duplicates().compute()
        nr_days = len(time_idx.dt.date.drop_duplicates())

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

        df = ((gen_by_tech_reg_orig.sum(axis=1) / (gen_cap_tech_reg.sum(axis=1) / 1000 * nr_days * 24))
              .unstack(level='Category').fillna(0))

        return df

    @property
    @mem_cache
    def cf_tech_reg(self):
        """
        Get the capacity factor(%) per technology in each region
        """
        time_idx = self.c.o.reg_df.reset_index().timestamp.drop_duplicates().compute()
        nr_days = len(time_idx.dt.date.drop_duplicates())

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

        df = (gen_by_tech_reg_orig / (gen_cap_tech_reg / 1000 * nr_days * 24)).fillna(0)

        return df

    @property
    @mem_cache
    def line_cap_reg(self):
        """
        Get the line capacity total between each region
        """
        line_cap = self.c.o.line_yr_df[(self.c.o.line_yr_df.property == 'Import Limit') | \
                                       (self.c.o.line_yr_df.property == 'Export Limit')] \
            .groupby(['model', 'regFrom', 'regTo', 'property']) \
            .agg({'value': 'sum'}) \
            .unstack(level='property')

        df = line_cap.reset_index()
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
    @mem_cache
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
    @mem_cache
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

    @property
    @mem_cache
    def re_by_reg(self):
        """
        Get the share of a RE generation
        """
        re_techs = ['Solar', 'Wind', 'Bioenergy', 'Geothermal', 'Other', 'Marine', 'Hydro']
        gen_by_tech_reg = self.c.o.gen_yr_df[self.c.o.gen_yr_df.property == 'Generation']
        re_by_reg = gen_by_tech_reg.reset_index()
        re_by_reg.loc[:, 'RE'] = re_by_reg.Category.apply(lambda x: 'RE' if x in re_techs else 'Non-RE')
        re_by_reg = re_by_reg.groupby(['model', 'RE']).sum().groupby(level=self.c.GEO_COLS[0], axis=1).sum()
        re_by_reg.loc[:, 'Overall'] = re_by_reg.sum(axis=1)
        df = re_by_reg.loc[pd.IndexSlice[:, 'RE'],].droplevel('RE') / re_by_reg.groupby('model').sum()

        return df

    @property
    @mem_cache
    def re_curtailment_rate_by_tech(self):
        """
        Get the curtailment / minimum energy violations by technology
        """

        time_idx = self.c.o.reg_df.reset_index().timestamp.drop_duplicates().compute()

        interval_periods = len(time_idx)
        nr_days = len(time_idx.dt.date.drop_duplicates())
        daily_periods = interval_periods / nr_days
        hour_corr = 24 / daily_periods

        # Output 8 & 9

        # Fill in data for regions which have no VRE (i.e. zero arrays!) to allow similar arrays for load_ts and
        # vre_add_df_columns
        # To add something for subregions

        # There is an error in PLEXOS with Available Capacity versus Generation (Gen exceeds Av Capacity)
        load_by_reg = self.c.o.node_yr_df[self.c.o.node_yr_df.property == 'Load'] \
            .groupby(['model', 'timestamp'] + self.c.GEO_COLS) \
            .agg({'value': 'sum'})

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
            .apply(lambda x: x * hour_corr)

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
            .apply(lambda x: x * hour_corr)

        # Add zero values to regions without VRE
        geo_col_filler = pd.Series(data=np.ones(len(load_by_reg.unstack(self.c.GEO_COLS).columns)),
                                   index=load_by_reg.unstack(self.c.GEO_COLS).columns)
        vre_av_abs = (vre_av_abs * geo_col_filler).fillna(0)
        vre_gen_abs = (vre_gen_abs * geo_col_filler).fillna(0)

        # 24 periods per day for the daily data
        vre_curtailed = vre_av_abs - vre_gen_abs

        # Add non-VRE spillage/curtailment
        constr_techs = ['Hydro', 'Bioenergy', 'Geothermal']

        other_re_gen_abs = self.c.o.gen_df[(self.c.o.gen_df.property == 'Generation') &
                                           (self.c.o.gen_df.Category.isin(constr_techs))] \
            .assign(timestamp=dd.to_datetime(self.c.o.gen_df['timestamp']).dt.floor('D')) \
            .groupby(['model', 'Category', ] + self.c.GEO_COLS + ['timestamp']) \
            .agg({'value': 'sum'}) \
            .compute() \
            .unstack('Category') \
            .fillna(0) \
            .stack('Category') \
            .unstack(level=self.c.GEO_COLS) \
            .fillna(0) \
            .apply(lambda x: x * hour_corr)

        other_re_energy_vio = self.c.o.gen_df[(self.c.o.gen_df.property == 'Min Energy Violation') &
                                              (self.c.o.gen_df.Category.isin(constr_techs))] \
            .assign(timestamp=dd.to_datetime(self.c.o.gen_df['timestamp']).dt.floor('D')) \
            .groupby(['model', 'Category'] + self.c.GEO_COLS + ['timestamp']) \
            .agg({'value': 'sum'}) \
            .compute() \
            .unstack('Category') \
            .fillna(0) \
            .stack('Category') \
            .unstack(level=self.c.GEO_COLS) \
            .fillna(0) \
            .apply(lambda x: x * hour_corr)

        other_re_gen_abs = (other_re_gen_abs * geo_col_filler).fillna(0)
        other_re_energy_vio = (other_re_energy_vio * geo_col_filler).fillna(0)
        other_re_av = other_re_energy_vio + other_re_gen_abs

        all_re_av = pd.concat([vre_av_abs, other_re_av], axis=0).reset_index().groupby(
            ['model', 'timestamp', 'Category']).sum()

        all_re_curtailed = pd.concat([vre_curtailed, other_re_energy_vio], axis=0).reset_index().groupby(
            ['model', 'timestamp', 'Category']).sum()

        re_curtailment_rate_by_tech = (all_re_curtailed.sum(axis=1).groupby(['model', 'Category']).sum().unstack(
            'Category') / all_re_av.sum(axis=1).groupby(['model', 'Category']).sum().unstack('Category')).fillna(
            0) * 100
        re_curtailment_rate = (all_re_curtailed.sum(axis=1).groupby('model').sum() / all_re_av.sum(axis=1).groupby(
            'model').sum()).fillna(0) * 100
        re_curtailment_rate_by_tech = pd.concat([re_curtailment_rate_by_tech, re_curtailment_rate.rename('All')],
                                                axis=1)

        return re_curtailment_rate_by_tech

    @property
    @mem_cache
    def curtailment_rate(self):
        """
        Get the curtailment rate
        """

        time_idx = self.c.o.reg_df.reset_index().timestamp.drop_duplicates().compute()

        interval_periods = len(time_idx)
        nr_days = len(time_idx.dt.date.drop_duplicates())
        daily_periods = interval_periods / nr_days
        hour_corr = 24 / daily_periods

        # Output 8 & 9

        # Fill in data for regions which have no VRE (i.e. zero arrays!) to allow similar arrays for load_ts and
        # vre_add_df_columns
        # To add something for subregions

        # There is an error in PLEXOS with Available Capacity versus Generation (Gen exceeds Av Capacity)
        load_by_reg = self.c.o.node_yr_df[self.c.o.node_yr_df.property == 'Load'] \
            .groupby(['model', 'timestamp'] + self.c.GEO_COLS) \
            .agg({'value': 'sum'})

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
            .apply(lambda x: x * hour_corr)

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
            .apply(lambda x: x * hour_corr)

        # Add zero values to regions without VRE
        geo_col_filler = pd.Series(data=np.ones(len(load_by_reg.unstack(self.c.GEO_COLS).columns)),
                                   index=load_by_reg.unstack(self.c.GEO_COLS).columns)
        vre_av_abs = (vre_av_abs * geo_col_filler).fillna(0)
        vre_gen_abs = (vre_gen_abs * geo_col_filler).fillna(0)

        # 24 periods per day for the daily data
        vre_curtailed = vre_av_abs - vre_gen_abs

        curtailment_rate = (vre_curtailed.sum(axis=1).groupby('model').sum() / vre_av_abs.sum(axis=1).groupby(
            'model').sum()).fillna(0) * 100

        ### Note that this is the new implementation for ISLAND-style aggregation. using self.c.GEO_COLS[0] i.e. the top tier of regional grouping.
        vre_curtailed_grouped = vre_curtailed.T.groupby(self.c.GEO_COLS[0]).sum().T
        vre_av_abs_grouped = vre_av_abs.T.groupby(self.c.GEO_COLS[0]).sum().T

        ## This could be renamed _reg. But should be done in consistent manner, so leaving for now.
        curtailment_rate_reg = (vre_curtailed_grouped.groupby('model').sum() /
                                vre_av_abs_grouped.groupby('model').sum()).fillna(0) * 100

        curtailment_rate = pd.concat([curtailment_rate_reg, curtailment_rate.rename('Overall')], axis=1)

        return curtailment_rate

    @property
    @mem_cache
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
        gen_capex.loc[:, 'property'] = 'Investment Cost'
        gen_capex.assign(value=lambda x: x.value / x.CAPEX)
        gen_capex.assign(property='Investment Cost')

        gen_total_costs = dd.concat([gen_op_costs, gen_vom, gen_fom, gen_capex], axis=0)
        gen_total_costs_w_pen = dd.concat([gen_op_costs_w_pen, gen_vom, gen_fom, gen_capex], axis=0)
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
