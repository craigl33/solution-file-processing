""""
TODO DOCSTRING
"""
import numpy as np
import pandas as pd
import dask.dataframe as dd

from solution_file_processing.constants import VRE_TECHS

from solution_file_processing.utils.utils import caching
from solution_file_processing.constants import PRETTY_MODEL_NAMES
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

    # Uncached variables
    _model_names = None

    # Cached variables
    _time_idx = None
    _gen_by_tech_reg_ts = None
    _gen_by_subtech_reg_ts = None
    _customer_load_ts = None
    _vre_av_abs_ts = None
    _net_load_ts = None
    _net_load_sto_ts = None
    _net_load_reg_ts = None
    _gen_inertia = None

    # -----
    # Uncached variables
    # -----

    @property
    def model_names(self):
        """"
        TODO DOCSTRING
        """
        if self._model_names is None:
            self._model_names = list(np.sort(self.c.o.reg_df.model.drop_duplicates()))
        return self._model_names

    # -----
    # Cached variables
    # -----

    @property
    @caching('variables')
    def gen_by_tech_reg_ts(self):
        """"
        TODO DOCSTRING
        """
        if self._gen_by_tech_reg_ts is None:
            self._gen_by_tech_reg_ts = self.c.o.gen_df[self.c.o.gen_df.property == 'Generation'] \
                .groupby(['model', 'Category'] + self.c.GEO_COLS + ['timestamp']) \
                .agg({'value': 'sum'}) \
                .compute() \
                .unstack(level='Category') \
                .fillna(0)

        return self._gen_by_tech_reg_ts

    @property
    @caching('variables')
    def gen_by_subtech_reg_ts(self):
        """"
        TODO DOCSTRING
        """
        if self._gen_by_subtech_reg_ts is None:
            self._gen_by_subtech_reg_ts = self.c.o.gen_df[self.c.o.gen_df.property == 'Generation'] \
                .groupby(['model', 'CapacityCategory'] + self.c.GEO_COLS + ['timestamp']) \
                .agg({'value': 'sum'}) \
                .compute() \
                .unstack(level='CapacityCategory') \
                .fillna(0)

        return self._gen_by_subtech_reg_ts

    @property
    @caching('variables')
    def customer_load_ts(self):
        """"
        TODO DOCSTRING
        """
        if self._customer_load_ts is None:
            self._customer_load_ts = self.c.o.reg_df[(self.c.o.reg_df.property == 'Customer Load') |
                                                     (self.c.o.reg_df.property == 'Unserved Energy')] \
                .groupby(['model', 'timestamp']) \
                .sum() \
                .value \
                .to_frame() \
                .compute()
        return self._customer_load_ts

    @property
    @caching('variables')
    def vre_av_abs_ts(self):
        """"
        TODO DOCSTRING
        """
        if self._vre_av_abs_ts is None:
            self._vre_av_abs_ts = self.c.o.gen_df[(self.c.o.gen_df.property == 'Available Capacity') &
                                                  (self.c.o.gen_df.Category.isin(VRE_TECHS))] \
                .groupby(['model', 'Category', 'timestamp']) \
                .sum().value.compute().unstack(level='Category').fillna(0)

        return self._vre_av_abs_ts

    @property
    @caching('variables')
    def net_load_ts(self):
        """"
        TODO DOCSTRING
        """
        if self._net_load_ts is None:
            self._net_load_ts = pd.DataFrame(
                self.customer_load_ts.value - self.vre_av_abs_ts.fillna(0).sum(axis=1).groupby(
                    ['model', 'timestamp']).sum(),
                columns=['value'])
        return self._net_load_ts

    @property
    @caching('variables')
    def net_load_sto_ts(self):
        """
        TODO DOCSTRING
        """
        if self._net_load_sto_ts is None:
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

            self._net_load_sto_ts = pd.DataFrame(_data,
                                                 columns=['value'])

        return self._net_load_sto_ts

    @property
    @caching('variables')
    def net_load_reg_ts(self):
        """"
        TODO DOCSTRING
        """
        if self._net_load_reg_ts is None:
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

            self._net_load_reg_ts = customer_load_reg_ts - vre_av_reg_abs_ts

        return self._net_load_reg_ts

    @property
    @caching('variables')
    def gen_inertia(self):
        """"
        TODO DOCSTRING
        """
        if self._gen_inertia is None:
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

            self._gen_inertia = gen_inertia

        return self._gen_inertia