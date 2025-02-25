""""
TODO DOCSTRING
"""
import numpy as np
import pandas as pd
import dask.dataframe as dd
import os

from solution_file_processing.constants import VRE_TECHS, CONSTR_TECHS
from solution_file_processing.timeseries import create_output_11
from .utils.write_excel import STACK_PALETTE, IEA_PALETTE_16, IEA_PALETTE_PLUS, EXTENDED_PALETTE, IEA_PALETTE
from .constants import VRE_TECHS, PRETTY_MODEL_NAMES

from solution_file_processing.utils.utils import drive_cache, memory_cache, get_median_index
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
    def combined_palette(self):
        """
        Returns a combined palette for the plots
        """

        # would be good to make palettes etc based on the regions for consistency purposes
        reg_ids = list(set(self.load_by_reg.reset_index()[self.c.GEO_COLS[0]].values))
        reg_palette = {reg_ids[i]: IEA_PALETTE_16[i] for i in range(len(reg_ids))}

        # Model palette
        model_ids = [m for m in PRETTY_MODEL_NAMES.values() if m in self.model_names] + [m for m in self.model_names if m not in PRETTY_MODEL_NAMES.values()]
        # Use extended palette so it can have more than 16 variables
        if len(model_ids) < len(IEA_PALETTE_PLUS):
            ## This is a bit of a hack to get the model ids to match the palette
            ## This is because the IEA_PALETTE_PLUS is a dictionary with string keys and the model_ids are integers
            model_palette = {model_ids[i]: IEA_PALETTE_PLUS[str(i)] for i in range(len(model_ids))}
        else:
            model_palette = {model_ids[i]: IEA_PALETTE_PLUS[str(i)] for i in range(16)} + {model_ids[i]: IEA_PALETTE_PLUS[i] for i in range(16, len(model_ids))}

        # Regions and technologies will always be consistent this way. May need to be copied to other parts of the code
        combined_palette = dict(STACK_PALETTE, **reg_palette, **model_palette)
        
        return combined_palette


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
        
        ### There is a slight issue with gen_by_tech in the case of key missing technologies (e.g. Storage and Hydro, perhaps others like wind and solar)
        ### It may be worth using the SolutionIndex itself to try to "fill" these
        try:
            storage_gen_ts = self.c.v.gen_by_tech_ts.Storage.rename('value').to_frame()
        except AttributeError:
            storage_gen_ts = pd.DataFrame(data={'value':[0]*self.c.v.gen_by_tech_ts.shape[0]}, index=self.c.v.gen_by_tech_ts.index, )

        df = self.customer_load_ts - storage_gen_ts + self.storage_load_ts - self.vre_av_abs_ts.sum(axis=1).rename('value').to_frame()

        return df
    
    @property
    @memory_cache
    def storage_load_ts(self):
        """
        TODO DOCSTRING
        """
        
        storage_load_ts = self.c.o.reg_df[
            (self.c.o.reg_df.property == 'Battery Load') | (self.c.o.reg_df.property == 'Pump Load')].groupby(
            ['model', 'timestamp']).agg({'value': 'sum'}).compute()
        
        return storage_load_ts


    @property
    @memory_cache
    def net_load_sto_curtail_ts(self):
        """
        TODO DOCSTRING
        """
        
        ### There is a slight issue with gen_by_tech in the case of key missing technologies (e.g. Storage and Hydro, perhaps others like wind and solar)
        ### It may be worth using the SolutionIndex itself to try to "fill" these
        try:
            storage_gen_ts = self.c.v.gen_by_tech_ts.Storage.rename('value').to_frame()
        except AttributeError:
            storage_gen_ts = pd.DataFrame(data={'value':[0]*self.c.v.gen_by_tech_ts.shape[0]}, index=self.c.v.gen_by_tech_ts.index, )

        df = self.customer_load_ts - storage_gen_ts + self.storage_load_ts - self.vre_gen_abs_ts.sum(axis=1).rename('value').to_frame()

        

        return df


    @property
    @memory_cache
    def customer_load_reg_ts(self):
        """
        TODO DOCSTRING
        """

            ## Fill in data for regions which have no VRE (i.e. zero arrays!) to allow similar arrays for load_ts and vre_av_ts
        

        customer_load_reg_ts = self.c.o.reg_df[(self.c.o.reg_df.property == 'Customer Load') |
                                                (self.c.o.reg_df.property == 'Unserved Energy')] \
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
        ## Convert model regions to multi-index if there are multiple levels (i.e. region, subregion, etc.)
        if len(self.c.GEO_COLS) > 1:
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
            .groupby(['model', 'PLEXOSname', 'timestamp']) \
            .agg({'value': 'sum'}) \
            .compute()

        gen_units = self.c.o.gen_df[self.c.o.gen_df.property == 'Units'] \
            .groupby(['model', 'PLEXOSname', 'timestamp']) \
            .agg({'value': 'sum'}) \
            .compute()

        # Take only the sum to maintain the capacity value & inertia constant in the dataframe
        gen_cap = self.c.o.gen_df[self.c.o.gen_df.property == 'Installed Capacity'] \
            .groupby(['model', 'PLEXOSname', 'timestamp']) \
            .agg({'value': 'sum'}) \
            .compute()

        gen_cap = pd.merge(gen_cap.reset_index(),
                           self.c.soln_idx[['PLEXOSname', 'InertiaLOW', 'InertiaHI']], on='PLEXOSname', how='left') \
            .set_index(['model', 'PLEXOSname', 'timestamp'])

        #  As installed capacity is [Units] * [Max Capacity], we must calculate the unit capacity
        gen_inertia_lo = (gen_units_gen.value / gen_units.value) * (gen_cap.value * gen_cap.InertiaLOW)
        gen_inertia_hi = (gen_units_gen.value / gen_units.value) * (gen_cap.value * gen_cap.InertiaHI)

        gen_inertia = pd.merge(pd.DataFrame(gen_inertia_lo, columns=['InertiaLo']),
                               pd.DataFrame(gen_inertia_hi, columns=['InertiaHi']),
                               left_index=True,
                               right_index=True)

        
        gen_inertia = pd.merge(gen_inertia.reset_index(),
                                self.c.soln_idx[
                                    ['PLEXOSname'] + self.c.GEO_COLS + ['Category', 'CapacityCategory']],
                                on='PLEXOSname')

        df = gen_inertia

        return df
    

    @property
    @memory_cache
    def model_names(self):
        """"
        Model names as per the generator annual object. 
        """
        model_names = list(np.sort(self.c.o.gen_yr_df.model.drop_duplicates()))
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
        year = self.c.o.gen_yr_df.timestamp.dt.year.drop_duplicates().values[0]
        if year % 4 == 0:
            return 366
        else:
            return 365


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
        gen_cap_tech_reg = self.c.o.gen_yr_df[((self.c.o.gen_yr_df.property == 'Installed Capacity')&(self.c.o.gen_yr_df.WEO_tech != 'Battery')) |
                                              ((self.c.o.gen_yr_df.WEO_tech == 'Battery') & (self.c.o.gen_yr_df.property == 'Generation Capacity'))] \
            .groupby(['model'] + self.c.GEO_COLS + ['Category']) \
            .agg({'value': 'sum'}) \
            .value \
            .unstack(level=self.c.GEO_COLS) \
            .fillna(0)
        return gen_cap_tech_reg

    @property
    @memory_cache
    def gen_cap_plexos_tech_reg(self):
        """
        TODO DOCSTRING
        """
        # For Capex calcs
        gen_cap_plexos_tech_reg = self.c.o.gen_yr_df[((self.c.o.gen_yr_df.property == 'Installed Capacity')&(self.c.o.gen_yr_df.WEO_tech != 'Battery')) |
                                              ((self.c.o.gen_yr_df.WEO_tech == 'Battery') & (self.c.o.gen_yr_df.property == 'Generation Capacity'))] \
            .groupby(['model'] + self.c.GEO_COLS + ['PLEXOS technology']) \
            .agg({'value': 'sum'}) \
            .value \
            .unstack(level=self.c.GEO_COLS) \
            .fillna(0)
        return gen_cap_plexos_tech_reg
    
    @property
    @memory_cache
    def gen_cap_plant(self):
        """
        TODO DOCSTRING
        """
        # For error checking
        gen_cap_plant = self.c.o.gen_yr_df[((self.c.o.gen_yr_df.property == 'Installed Capacity')&(self.c.o.gen_yr_df.WEO_tech == 'Battery')) |
                                              ((self.c.o.gen_yr_df.WEO_tech == 'Battery') & (self.c.o.gen_yr_df.property == 'Generation Capacity'))] \
            .groupby(['model', 'name']) \
            .agg({'value': 'sum'}) \
            .value \
            .unstack(level='model') \
            .fillna(0)
        return gen_cap_plant

    @property
    @memory_cache
    def gen_cap_subtech_reg(self):
        """
        TODO DOCSTRING
        """
        gen_cap_subtech_reg = self.c.o.gen_yr_df[((self.c.o.gen_yr_df.property == 'Installed Capacity')&(self.c.o.gen_yr_df.WEO_tech == 'Battery')) |
                                              ((self.c.o.gen_yr_df.WEO_tech == 'Battery') & (self.c.o.gen_yr_df.property == 'Generation Capacity'))] \
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
    def curtailment_rate_reg(self):
        """
        Get the curtailment rate
        """
        # Fill in data for regions which have no VRE (i.e. zero arrays!) to allow similar arrays for load_ts and
        # vre_add_df_columns
        # To add something for subregions

        # There is an error in PLEXOS with Available Capacity versus Generation (Gen exceeds Av Capacity)

        vre_gen_reg = self.c.o.gen_yr_df[
            (self.c.o.gen_yr_df.property == 'Generation') & (self.c.o.gen_yr_df.Category.isin(VRE_TECHS))] \
            .groupby(['model', self.c.GEO_COLS[0]]) \
            .agg({'value': 'sum'}) \
            .value \
            .unstack(level=self.c.GEO_COLS[0]) \
            .fillna(0)
        
        vre_av_reg = self.c.o.gen_yr_df[
            (self.c.o.gen_yr_df.property == 'Available Energy') & (self.c.o.gen_yr_df.Category.isin(VRE_TECHS))] \
            .groupby(['model', self.c.GEO_COLS[0]]) \
            .agg({'value': 'sum'}) \
            .value \
            .unstack(level=self.c.GEO_COLS[0]) \
            .fillna(0)

        curtailment_rate_reg = vre_gen_reg / vre_av_reg

        return curtailment_rate_reg
    

    @property
    @memory_cache
    def curtailment_rate(self):
        """
        Get the curtailment rate
        """

        # Fill in data for regions which have no VRE (i.e. zero arrays!) to allow similar arrays for load_ts and
        # vre_add_df_columns
        # To add something for subregions

        # There is an error in PLEXOS with Available Capacity versus Generation (Gen exceeds Av Capacity)

        vre_gen = self.c.o.gen_yr_df[
            (self.c.o.gen_yr_df.property == 'Generation') & (self.c.o.gen_yr_df.Category.isin(VRE_TECHS))] \
            .groupby(['model']) \
            .agg({'value': 'sum'}) \
            .fillna(0)
        
        vre_av = self.c.o.gen_yr_df[
            (self.c.o.gen_yr_df.property == 'Available Energy') & (self.c.o.gen_yr_df.Category.isin(VRE_TECHS))] \
            .groupby(['model']) \
            .agg({'value': 'sum'}) \
            .fillna(0)
        
        curtailment_rate = vre_gen/ vre_av


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
        gen_vom = gen_vom.assign(value=lambda x: x.value * x.VOM.fillna(0))
        gen_vom = gen_vom.assign(property='VO&M Cost')

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
        ramp_by_gen_name = gen_by_name_ts[ ['model', 'PLEXOSname'] + self.c.GEO_COLS + ['Category', 'property', 'value']]
        ramp_by_gen_name = ramp_by_gen_name.assign(value=ramp_by_gen_name.value - ramp_by_gen_name.value.shift(1))
        ramp_by_gen_name = ramp_by_gen_name.assign(property = 'Ramp')

        ramp_costs_by_gen_name = dd.merge(ramp_by_gen_name, self.c.soln_idx[['PLEXOSname', 'RampCost']],
                                            on='PLEXOSname',
                                            how='left')

        ramp_costs_by_gen_name = ramp_costs_by_gen_name.assign(value = ramp_costs_by_gen_name.value.abs() * ramp_costs_by_gen_name.RampCost.fillna(0))
        ramp_costs_by_gen_name = ramp_costs_by_gen_name.assign(property = 'Ramp Cost')

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

        gen_op_costs_by_reg = self.gen_op_costs_by_reg

        try:
            # self.c.v.gen_capex.loc[:, 'property'] = 'Investment Cost'
            # self.gen_capex.assign(value=lambda x: x.value / x.CAPEX)
            # self.gen_capex.assign(property='Investment Cost')
            gen_capex = self.c.o.gen_yr_df.loc[self.c.o.gen_yr_df.property == 'Installed Capacity',:]
            gen_capex = gen_capex.assign(value = lambda x: x.value*x.CAPEX)
            gen_capex = gen_capex.assign(property = "Investment Cost")
        except KeyError:
            ### If CAPEX isn't defined in the generators parameters sheet, this won't work
            gen_capex = pd.DataFrame(None)

        gen_capex_by_reg  = gen_capex.groupby(
            ['model'] + self.c.GEO_COLS + ['Category', 'property']) \
                .agg({'value': 'sum'}) \
                .applymap(lambda x: x / 1e3)

        gen_total_costs_by_reg = pd.concat([gen_op_costs_by_reg, gen_capex_by_reg], axis=0)

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
    @drive_cache('variables')
    def use_reg_daily_ts(self):
        """"
        TODO DOCSTRING
        """
        use_reg_daily_ts = self.c.o.reg_df[self.c.o.reg_df.property == 'Unserved Energy'] \
            .groupby(['model'] + self.c.GEO_COLS + ['timestamp']) \
            .agg({'value': 'sum'}) \
            .compute()
        
        ## This step is done because of  issues with pd.GroupBy objects and Dask
        use_reg_daily_ts = use_reg_daily_ts.groupby(['model'] + self.c.GEO_COLS +[pd.Grouper(level='timestamp', freq='D')]).agg({'value': 'sum'})

        return use_reg_daily_ts

    def _get_cofiring_generation(
            self):
        """
        Function for getting the Generation by technology data, taking into account cofiring aspects. 
        This is a legacy calculation from the Indonesia IPSE report. This should be updated and changed 
        TODO this needs better implementation, also see bug with different versions in outputs 3 and 12

        """

        # Currently not used. But need to figure out how to make this calculation work for all 
        # models using the TOML file

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
        # _, gen_by_tech_reg_orig = self._get_cofiring_generation()
        gen_by_tech_reg = self.c.o.gen_yr_df[self.c.o.gen_yr_df.property == 'Generation']
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
        # _, gen_by_tech_reg_orig = self._get_cofiring_generation()
        gen_by_tech_reg_orig = self.c.o.gen_yr_df[self.c.o.gen_yr_df.property == 'Generation']
        gen_by_tech_reg_orig = (gen_by_tech_reg_orig
                                .groupby(['model'] + self.c.GEO_COLS + ['Category'])
                                .agg({'value': 'sum'})
                                .unstack(level=self.c.GEO_COLS)
                                .value
                                .fillna(0))
        return gen_by_tech_reg_orig

    # @property
    # @memory_cache
    # def gen_by_costTech_reg(self):
    #     """
    #     TODO DOCSTRING
    #     """
    #     # gen_by_tech_reg, _ = self._get_cofiring_generation()
    #     gen_by_tech_reg = self.c.o.gen_yr_df[self.c.o.gen_yr_df.property == 'Generation']
    #     gen_by_costTech_reg = (gen_by_tech_reg
    #                            .groupby(['model'] + self.c.GEO_COLS + ['CostCategory'])
    #                            .agg({'value': 'sum'})
    #                            .value
    #                            .unstack(level=self.c.GEO_COLS)
    #                            .fillna(0))
    #     return gen_by_costTech_reg

    @property
    @memory_cache
    def gen_by_plexos_tech_reg(self):
        """
        TODO DOCSTRING
        """
        gen_by_plexos_tech_reg = (self.c.o.gen_yr_df[self.c.o.gen_yr_df.property == 'Generation']
                              .groupby(['model'] + self.c.GEO_COLS + ['PLEXOS technology'])
                              .agg({'value': 'sum'})
                              .value
                              .unstack(level=self.c.GEO_COLS)
                              .fillna(0))
        return gen_by_plexos_tech_reg

    @property
    @memory_cache
    def gen_by_plant(self):
        """
        TODO DOCSTRING
        """
        # gen_by_tech_reg, _ = self._get_cofiring_generation()
        gen_by_tech_reg = self.c.o.gen_yr_df[self.c.o.gen_yr_df.property == 'Generation']
        gen_by_plant = (gen_by_tech_reg
                        .groupby(['model', 'PLEXOSname'])
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
    def vre_av_by_reg(self):
        """
        Get the share of a subset of generation (e.g. RE or VRE), with the subset technologies passed as a list
        """

        gen_by_tech_reg = self.c.o.gen_yr_df[self.c.o.gen_yr_df.property == 'Generation'].reset_index()
        
        vre_av_by_reg = self.vre_av_reg_abs_ts.groupby("model") \
                           .sum() \
                           .groupby(self.c.GEO_COLS[0], axis=1) \
                           .sum()/1000
        
        gen_by_tech_reg.loc[:, 'VRE'] = gen_by_tech_reg.Category.apply(lambda x: 'VRE' if x in VRE_TECHS else 'Non-VRE')
        
        non_vre_gen_by_reg = gen_by_tech_reg.groupby(['model', 'VRE', self.c.GEO_COLS[0]]) \
            .agg({'value':'sum'}) \
            .value \
            .unstack('VRE')['Non-VRE'].rename('value').unstack(self.c.GEO_COLS[0])
        
        vre_av_by_reg = vre_av_by_reg/(vre_av_by_reg + non_vre_gen_by_reg)

        return vre_av_by_reg

    @property
    @memory_cache
    def re_share(self):
        """
        Get the share of a RE generation
        """
        re_techs = ['Solar', 'Wind', 'Bioenergy', 'Geothermal', 'Other', 'Marine', 'Hydro']
        gen_by_tech = self.c.o.gen_yr_df[self.c.o.gen_yr_df.property == 'Generation']
        re_share = gen_by_tech.reset_index()
        re_share.loc[:, 'RE'] = re_share.Category.apply(lambda x: 'RE' if x in re_techs else 'Non-RE')
        ### New simplified implementation to avoid errors. 
        re_share = (re_share
                     .groupby(['model', 'RE'])
                     .agg({'value':'sum'}).value.unstack('RE'))
        re_share = (re_share['RE']/re_share.sum(axis=1))

        return re_share

    @property
    @memory_cache
    def vre_share(self):
        """
        Get the share of a subset of generation (e.g. RE or VRE), with the subset technologies passed as a list
        """

        gen_by_tech = self.c.o.gen_yr_df[self.c.o.gen_yr_df.property == 'Generation']
        vre_share = gen_by_tech.reset_index()
        vre_share.loc[:, 'VRE'] = vre_share.Category.apply(lambda x: 'VRE' if x in VRE_TECHS else 'Non-VRE')
        vre_share = vre_share.groupby(['model', 'VRE']).agg({'value':'sum'}).value.unstack('VRE')
        vre_share = (vre_share['VRE']/vre_share.sum(axis=1))

        return vre_share
    
    
    @property
    @memory_cache
    def vre_av_share(self):
        """
        Get the share of a subset of generation (e.g. RE or VRE), with the subset technologies passed as a list
        """

        gen_by_tech = self.c.o.gen_yr_df[self.c.o.gen_yr_df.property == 'Generation'].reset_index()
        vre_av_gen = self.vre_av_abs_ts.groupby(['model']).sum().sum(axis=1)/1000
        gen_by_tech.loc[:, 'VRE'] = gen_by_tech.Category.apply(lambda x: 'VRE' if x in VRE_TECHS else 'Non-VRE')
        
        non_vre_gen = gen_by_tech.groupby(['model', 'VRE']) \
            .agg({'value':'sum'}) \
            .value \
            .unstack('VRE')['Non-VRE'].rename('value')
        
        vre_av_share = (vre_av_gen/(vre_av_gen + non_vre_gen))

        return vre_av_share    

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
            .groupby(['model', 'timestamp', 'PLEXOSname']) \
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
    @drive_cache('variables')
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
            .groupby(['model'] + self.c.GEO_COLS + ['Category']) \
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
            .groupby(['model'] + self.c.GEO_COLS + ['Category']) \
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
    @drive_cache('variables')
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
    @drive_cache('variables')
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
    @drive_cache('variables')
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
    @drive_cache('variables')
    def load_by_reg_ts(self):
        """
        TODO DOCSTRING
        """
        load_by_reg_ts = (self.c.o.reg_df[self.c.o.reg_df.property == 'Load']
                          .groupby(['model'] + self.c.GEO_COLS + ['timestamp'])
                          .agg({'value': 'sum'})
                          .compute()
                          .unstack(level='timestamp')
                          .fillna(0)
                          .stack('timestamp'))
        return load_by_reg_ts

    @property
    @memory_cache
    @drive_cache('variables')
    def use_reg_ts(self):
        """
        TODO DOCSTRING
        """
        use_reg_ts = self.c.o.reg_df[self.c.o.reg_df.property == 'Unserved Energy'] \
                                .groupby( ['model'] + self.c.GEO_COLS + ['timestamp']) \
                                .agg({'value': 'sum'}) \
                                .compute() \
                                .value \
                                .unstack(level=self.c.GEO_COLS)
        return use_reg_ts

    @property
    @memory_cache
    @drive_cache('variables')
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
    @drive_cache('variables')
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
    @drive_cache('variables')
    def av_cap_by_tech_ts(self):
        """
        TODO DOCSTRING
        """
        # Temporary fix for Battery whereby we use the entire generation capacity as the available capacity
        # Future iterations should probably only count this if if there is charge in the battery
        av_cap_by_tech_ts = self.c.o.gen_df[(self.c.o.gen_df.property == 'Available Capacity') | 
                                            ((self.c.o.gen_df.WEO_tech == 'Battery')&
                                             (self.c.o.gen_df.property == 'Generation Capacity'))] \
            .groupby(['model', 'Category', 'timestamp']) \
            .agg({'value': 'sum'}) \
            .compute() \
            .value \
            .unstack(level='Category') \
            .fillna(0)
        return av_cap_by_tech_ts

    @property
    @memory_cache
    @drive_cache('variables')
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
    @drive_cache('variables')
    def vre_curtailed_ts(self):
        """
        TODO DOCSTRING
        """
        vre_curtailed_ts = self.vre_av_abs_ts - self.vre_gen_abs_ts
        return vre_curtailed_ts
    

    @property
    @memory_cache
    @drive_cache('variables')
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
    @drive_cache('variables')
    def ev_profiles_ts(self):
        """
        EV load profiles
        """
        # Model filler for comparison of models with different inputs (e.g. DSM or EVs not included)
        # Series with indices matching the columns of the DF for filling in missing columns
        
        model_filler = pd.Series(data=[1] * len(self.c.v.model_names), index=self.c.v.model_names).rename_axis('model')
        
        try:
            df_len = len(self.c.o.purch_df)
            if df_len > 0:
                purch_df = self.c.o.purch_df
        except ValueError:
            purch_df = dd.from_pandas(pd.DataFrame(None), npartitions=1)
            df_len = 0

        if df_len > 0:
            ev_profiles_ts = purch_df[
                purch_df.name.str.contains('_EV') & (purch_df.property == 'Load')].groupby(
                ['model', 'timestamp']).agg({'value': 'sum'}).compute()
            if not ev_profiles_ts.shape[0] == 0:
                ev_profiles_ts = ((ev_profiles_ts.value.unstack('model') * model_filler).fillna(0).stack('model').reorder_levels(
                ['model', 'timestamp']).rename('value')).to_frame()
            else:
                ev_profiles_ts = pd.Series(data=[0] * len(self.c.v.customer_load_ts.index),
                                       index=self.c.v.customer_load_ts.index).rename('value').to_frame()
        else:
            ev_profiles_ts = pd.Series(data=[0] * len(self.c.v.customer_load_ts.index),
                                       index=self.c.v.customer_load_ts.index).rename('value').to_frame()
        return ev_profiles_ts
            
    @property
    @memory_cache
    @drive_cache('variables')
    def ev_profiles_orig_ts(self):
        """
        EV original load profiles before shifting. This uses the [x] property on the Purchaser object to get the original profiles
        """
        # Model filler for comparison of models with different inputs (e.g. DSM or EVs not included)
        # Series with indices matching the columns of the DF for filling in missing columns
        
        model_filler = pd.Series(data=[1] * len(self.c.v.model_names), index=self.c.v.model_names).rename_axis('model')
        
        try:
            df_len = len(self.c.o.purch_df)
            if df_len > 0:
                purch_df = self.c.o.purch_df
        except ValueError:
            purch_df = dd.from_pandas(pd.DataFrame(None), npartitions=1)
            df_len = 0

        if df_len > 0:
            ev_profiles_orig_ts = purch_df[
                purch_df.name.str.contains('_EV') & (purch_df.property == 'x')].groupby(
                ['model', 'timestamp']).agg({'value': 'sum'}).compute()
            if not ev_profiles_orig_ts.shape[0] == 0:
                ev_profiles_orig_ts = ((ev_profiles_orig_ts.value.unstack('model') * model_filler).fillna(0).stack(
                    'model').reorder_levels(['model', 'timestamp']).rename('value')).to_frame()
            else:
                ev_profiles_orig_ts = pd.Series(data=[0] * len(self.c.v.customer_load_ts.index),
                                        index=self.c.v.customer_load_ts.index).rename('value').to_frame()
        else:
            ev_profiles_orig_ts = pd.Series(data=[0] * len(self.c.v.customer_load_ts.index),
                                        index=self.c.v.customer_load_ts.index).rename('value').to_frame()
            
        return ev_profiles_orig_ts
    

    @property
    @memory_cache
    @drive_cache('variables')
    def dsm_profiles_ts(self):
        """
        DSM profiles
        """
        # Model filler for comparison of models with different inputs (e.g. DSM or EVs not included)
        # Series with indices matching the columns of the DF for filling in missing columns
        
        model_filler = pd.Series(data=[1] * len(self.c.v.model_names), index=self.c.v.model_names).rename_axis('model')
        
        try:
            # Check if the dataframe is empty.
            # Use len() instead of .shape[0] because the latter is very slow for Dask dataframes
            df_len = len(self.c.o.purch_df)
            if df_len > 0:
                purch_df = self.c.o.purch_df
        except ValueError:
            purch_df = dd.from_pandas(pd.DataFrame(None), npartitions=1)
            df_len = 0

        if df_len > 0:
            dsm_profiles_ts = purch_df[
                purch_df.name.str.contains('_Shift') & (purch_df.property == 'Load')].groupby(
                ['model', 'timestamp']).agg({'value': 'sum'}).compute()
            if not dsm_profiles_ts.shape[0] == 0:
                dsm_profiles_ts = ((dsm_profiles_ts.value.unstack('model') * model_filler).fillna(0).stack('model').reorder_levels(
                    ['model', 'timestamp']).rename('value')).to_frame()
            else:
                dsm_profiles_ts = pd.Series(data=[0] * len(self.c.v.customer_load_ts.index),
                                       index=self.c.v.customer_load_ts.index).rename('value').to_frame()
        else:
            dsm_profiles_ts = pd.Series(data=[0] * len(self.c.v.customer_load_ts.index),    
                                       index=self.c.v.customer_load_ts.index).rename('value').to_frame()
        return dsm_profiles_ts
    
    @property
    @memory_cache
    @drive_cache('variables')
    def dsm_profiles_orig_ts(self):
        """
        DSM profiles
        """
        # Model filler for comparison of models with different inputs (e.g. DSM or EVs not included)
        # Series with indices matching the columns of the DF for filling in missing columns
        
        model_filler = pd.Series(data=[1] * len(self.c.v.model_names), index=self.c.v.model_names).rename_axis('model')

        try:
            df_len = len(self.c.o.purch_df)
            if df_len > 0:
                purch_df = self.c.o.purch_df
        except ValueError:
            purch_df = dd.from_pandas(pd.DataFrame(None), npartitions=1)
            df_len = 0

        if df_len > 0:
            dsm_profiles_orig_ts = purch_df[
                purch_df.name.str.contains('_Shift') & (purch_df.property == 'x')].groupby(
                ['model', 'timestamp']).agg({'value': 'sum'}).compute()
            if not dsm_profiles_orig_ts.shape[0] == 0:
                dsm_profiles_orig_ts = ((dsm_profiles_orig_ts.value.unstack('model') * model_filler).fillna(0).stack('model').reorder_levels(
                    ['model', 'timestamp']).rename('value')).to_frame()
            else:
                dsm_profiles_orig_ts = pd.DataFrame(data=[0] * len(self.c.v.customer_load_ts.index),
                                       index=self.c.v.customer_load_ts.index, columns=['value'])
        else:
            dsm_profiles_orig_ts = pd.DataFrame(data=[0] * len(self.c.v.customer_load_ts.index),    
                                       index=self.c.v.customer_load_ts.index, columns=['value'])
            
        return dsm_profiles_orig_ts
    
    @property
    @memory_cache
    @drive_cache('variables')
    def electrolyser_profiles_ts(self):
        """
        Electrolyser profiles
        """
        # Model filler for comparison of models with different inputs (e.g. DSM or EVs not included)
        # Series with indices matching the columns of the DF for filling in missing columns
        
        model_filler = pd.Series(data=[1] * len(self.c.v.model_names), index=self.c.v.model_names).rename_axis('model')

        try:
            # Check if the dataframe is empty.
            # Use len() instead of .shape[0] because the latter is very slow for Dask dataframes
            df_len = len(self.c.o.purch_df)
            if df_len > 0:
                purch_df = self.c.o.purch_df
        except ValueError:
            purch_df = dd.from_pandas(pd.DataFrame(None), npartitions=1)
            df_len = 0
        
        if df_len > 0:
            electrolyser_profiles_ts = purch_df[
                purch_df.name.str.contains('_Elec') & (purch_df.property == 'Load')].groupby(
                ['model', 'timestamp']).agg({'value': 'sum'}).compute()
            if not electrolyser_profiles_ts.shape[0] == 0:
                electrolyser_profiles_ts = ((electrolyser_profiles_ts.value.unstack('model') * model_filler).fillna(0).stack('model').reorder_levels(
                    ['model', 'timestamp']).rename('value')).to_frame()
            else:
                electrolyser_profiles_ts = pd.Series(data=[0] * len(self.c.v.customer_load_ts.index),
                                       index=self.c.v.customer_load_ts.index).rename('value').to_frame()
        else:
            electrolyser_profiles_ts = pd.Series(data=[0] * len(self.c.v.customer_load_ts.index),    
                                       index=self.c.v.customer_load_ts.index).rename('value').to_frame()
        return electrolyser_profiles_ts
    
    @property
    @memory_cache
    @drive_cache('variables')
    def electrolyser_profiles_orig_ts(self):
        """
        DSM profiles
        """
        # Model filler for comparison of models with different inputs (e.g. DSM or EVs not included)
        # Series with indices matching the columns of the DF for filling in missing columns
        
        model_filler = pd.Series(data=[1] * len(self.c.v.model_names), index=self.c.v.model_names).rename_axis('model')

        try:
            df_len = len(self.c.o.purch_df)
            if df_len > 0:
                purch_df = self.c.o.purch_df
        except ValueError:
            purch_df = dd.from_pandas(pd.DataFrame(None), npartitions=1)
            df_len = 0
        
        if df_len > 0:
            electrolyser_profiles_orig_ts = purch_df[
                purch_df.name.str.contains('_Elec') & (purch_df.property == 'x')].groupby(
                ['model', 'timestamp']).agg({'value': 'sum'}).compute()
            if not electrolyser_profiles_orig_ts.shape[0] == 0:
                electrolyser_profiles_orig_ts = ((electrolyser_profiles_orig_ts.value.unstack('model') * model_filler).fillna(0).stack('model').reorder_levels(
                    ['model', 'timestamp']).rename('value')).to_frame()
            else:
                electrolyser_profiles_orig_ts = pd.Series(data=[0] * len(self.c.v.customer_load_ts.index),
                                       index=self.c.v.customer_load_ts.index).rename('value').to_frame()
        else:
            electrolyser_profiles_orig_ts = pd.Series(data=[0] * len(self.c.v.customer_load_ts.index),    
                                       index=self.c.v.customer_load_ts.index).rename('value').to_frame()
        return electrolyser_profiles_orig_ts


    @property
    @memory_cache
    @drive_cache('variables')
    def native_load_ts(self):
        """
        Calculate native load, which is used for the caluclation of original load profiles with   the 
        """
        # Model filler for comparison of models with different inputs (e.g. DSM or EVs not included)
        # Series with indices matching the columns of the DF for filling in missing columns


        native_load_ts = self.c.o.reg_df[(self.c.o.reg_df.property == 'Native Load')|(self.c.o.reg_df.property == 'Unserved Energy')].groupby(
            ['model', 'timestamp']).agg({'value': 'sum'}).compute()
        
        return native_load_ts

    @property
    @memory_cache
    @drive_cache('variables')
    def customer_load_orig_ts(self):
        """
        TODO DOCSTRING
        """
        # Model filler for comparison of models with different inputs (e.g. DSM or EVs not included)
        # Series with indices matching the columns of the DF for filling in missing columns


        native_load_ts = self.c.o.reg_df[(self.c.o.reg_df.property == 'Native Load')|(self.c.o.reg_df.property == 'Unserved Energy')].groupby(
            ['model', 'timestamp']).agg({'value': 'sum'}).compute()
        customer_load_orig_ts = ( native_load_ts + self.ev_profiles_orig_ts + self.dsm_profiles_orig_ts + self.electrolyser_profiles_orig_ts)
            # .fillna(customer_load_ts) ### For those profiles where EVs are missing, for e.g. ... other DSM to be added

        return customer_load_orig_ts

    
    @property
    @memory_cache
    @drive_cache('variables')
    def net_load_curtail_ts(self):
        """
        TODO DOCSTRING
        """
        #  net_load_ts is calculated as a series (as we obtain load 'value' and some across the x-axis (technologies)
        #  of vre_abs)
        net_load_curtail_ts = pd.DataFrame(
            self.c.v.customer_load_ts.value - self.c.v.vre_gen_abs_ts.fillna(0).sum(axis=1), columns=['value'])
        return net_load_curtail_ts

    @property
    @memory_cache
    @drive_cache('variables')
    def net_load_orig_ts(self):
        """
        TODO DOCSTRING
        """
        #  net_load_ts is calculated as a series (as we obtain load 'value' and some across the x-axis (technologies)
        #  of vre_abs)
        net_load_orig_ts = self.customer_load_orig_ts - self.vre_av_abs_ts.sum(axis=1).fillna(0).rename('value').to_frame()
        return net_load_orig_ts

    @property
    @memory_cache
    @drive_cache('variables')
    def inertia_by_tech(self):
        """
        TODO DOCSTRING
        """
        inertia_by_tech = self.c.v.gen_inertia.groupby(['model', 'Category', 'timestamp']).agg({'InertiaLo':'sum'}).rename({'InertiaLo':'value'}, axis=1)
        return inertia_by_tech

    @property
    @memory_cache
    @drive_cache('variables')
    def inertia_by_reg(self):
        """
        TODO DOCSTRING
        """
        inertia_by_reg = self.c.v.gen_inertia.groupby(
            ['model'] + self.c.cfg['settings']['geo_cols'] + ['timestamp']).agg({'InertiaLo':'sum'}).rename({'InertiaLo':'value'}, axis=1)
        return inertia_by_reg

    @property
    @memory_cache
    @drive_cache('variables')
    def total_inertia_ts(self):
        """
        TODO DOCSTRING
        """
        total_inertia_ts = self.c.v.gen_inertia.groupby(['model', 'timestamp']).agg({'InertiaLo':'sum'}).rename({'InertiaLo':'value'}, axis=1)
        return total_inertia_ts

    @property
    @memory_cache
    @drive_cache('variables')
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
    @drive_cache('variables')
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
    @drive_cache('variables')
    def ramp_orig_ts(self):
        """
        TODO DOCSTRING
        """
        ramp_orig_ts = (self.net_load_orig_ts.unstack(level='model') - self.net_load_orig_ts.unstack(level='model').shift(1)) \
            .fillna(0) \
            .stack() \
            .sort_index(level=1) \
            .reset_index() \
            .set_index(['model', 'timestamp']) \
            .rename(columns={0: 'value'})
        return ramp_orig_ts
    

    @property
    @memory_cache
    @drive_cache('variables')
    def th_ramp_orig_ts(self):
        """
        TODO DOCSTRING
        """
        th_ramp_orig_ts = (self.net_load_orig_ts.unstack(level='model') - self.net_load_orig_ts.unstack(level='model').shift(3)) \
            .fillna(0) \
            .stack() \
            .sort_index(level=1) \
            .reset_index() \
            .set_index(['model', 'timestamp']) \
            .rename(columns={0: 'value'})
        return th_ramp_orig_ts


    @property
    @memory_cache
    @drive_cache('variables')
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
    @drive_cache('variables')
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
    @drive_cache('variables')
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
    @drive_cache('variables')
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
    @drive_cache('variables')
    def ramp_reg_pc_ts(self):
        """
        TODO DOCSTRING
        """
        ramp_reg_pc_ts = (self.c.v.ramp_reg_ts.value.unstack(self.c.GEO_COLS)/self.c.v.daily_pk_reg_ts) \
                            .stack(self.c.GEO_COLS).rename('value').to_frame()*100
    
        return ramp_reg_pc_ts

    
    @property
    @memory_cache
    @drive_cache('variables')
    def th_ramp_reg_pc_ts(self):
        """
        TODO DOCSTRING
        """
        th_ramp_reg_pc_ts = (self.c.v.th_ramp_reg_ts.value.unstack(self.c.GEO_COLS)/self.c.v.daily_pk_reg_ts) \
                            .stack(self.c.GEO_COLS).rename('value').to_frame()*100
    
        return th_ramp_reg_pc_ts

    @property
    @memory_cache
    @drive_cache('variables')
    def ramp_by_gen_tech_ts(self):
        """
        TODO DOCSTRING
        """
        ramp_by_gen_tech_ts = (self.gen_by_tech_ts - self.gen_by_tech_ts.shift(1)).fillna(0)
        return ramp_by_gen_tech_ts

    @property
    @memory_cache
    @drive_cache('variables')
    def ramp_by_gen_subtech_ts(self):
        """
        TODO DOCSTRING
        """
        ramp_by_gen_subtech_ts = (self.gen_by_subtech_ts - self.gen_by_subtech_ts.shift(1)).fillna(0)
        return ramp_by_gen_subtech_ts

    @property
    @memory_cache
    @drive_cache('variables')
    def th_ramp_by_gen_tech_ts(self):
        """
        TODO DOCSTRING
        """
        th_ramp_by_gen_tech_ts = (self.gen_by_tech_ts - self.gen_by_tech_ts.shift(3)).fillna(0)
        return th_ramp_by_gen_tech_ts

    @property
    @memory_cache
    @drive_cache('variables')
    def th_ramp_by_gen_subtech_ts(self):
        """
        TODO DOCSTRING
        """
        th_ramp_by_gen_subtech_ts = (self.gen_by_subtech_ts - self.gen_by_subtech_ts.shift(3)).fillna(0)
        return th_ramp_by_gen_subtech_ts

    @property
    @memory_cache
    @drive_cache('variables')
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
    @drive_cache('variables')
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
        ldc = self.c.v.customer_load_ts.value.unstack('model')
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
        nldc = self.c.v.net_load_ts.value.unstack('model')
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
        nldc_orig = self.c.v.net_load_orig_ts.value.unstack('model')
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
        nldc_sto = self.c.v.net_load_sto_ts.value.unstack('model')
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
        nldc_curtail = self.c.v.net_load_curtail_ts.value.unstack('model')
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
        nldc_sto_curtail = self.c.v.net_load_sto_curtail_ts.value.unstack('model')
        nldc_sto_curtail = pd.DataFrame(np.flipud(np.sort(nldc_sto_curtail.values, axis=0)),
                            index=nldc_sto_curtail.index, columns=nldc_sto_curtail.columns)
        # Index = 0-8760
        nldc_sto_curtail = nldc_sto_curtail.reset_index(drop=True)
        
        return nldc_sto_curtail
    
    @property
    @memory_cache
    def curtailment_dc(self):
        """
        Obtains the curtailment duration curve (DC) for the VRE technologies
        """
        curtailment_dc = self.vre_curtailed_ts.sum(axis=1).unstack('model')
        curtailment_dc = pd.DataFrame(np.flipud(np.sort(curtailment_dc.values, axis=0)), 
                                    index=curtailment_dc.index, columns=curtailment_dc.columns)
        
        curtailment_dc = curtailment_dc.reset_index(drop=True)
        
        return curtailment_dc


    @property
    @memory_cache
    @drive_cache('variables')
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
    @drive_cache('variables')
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
    @drive_cache('variables')
    def pumpload_reg_ts(self):
        pumpload_reg_ts = (self.c.o.reg_df[(self.c.o.reg_df.property == 'Pump Load') |
                                (self.c.o.reg_df.property == 'Battery Load')]
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
    @drive_cache('variables')
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

        use_reg_ts = self.c.o.reg_df[self.c.o.reg_df.property == 'Unserved Energy'].groupby(
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

    @property
    @memory_cache
    @drive_cache('variables')
    def gen_stack(self):
        '''
        Create a variable for generation stack for overall model
        '''

        gen_stack = self.gen_stack_by_reg.groupby(['model','timestamp']).sum()

        return gen_stack


    @property
    @memory_cache
    @drive_cache('variables')
    def undisp_cap_by_tech_reg_ts(self):
        """
        Create a variable to debug whether all available capacity is dispatched when there is Unserved Energy at region level
        """

        av_cap_by_tech_reg_ts = self.c.o.gen_df[self.c.o.gen_df.property == 'Available Capacity'].groupby(
                        ['model', 'Category', self.c.GEO_COLS[0], 'timestamp']).agg({'value':'sum'}).compute().value.unstack(level='Category').fillna(0)

        gen_by_tech_reg_ts = self.c.o.gen_df[self.c.o.gen_df.property == 'Generation'].groupby(
                            ['model', 'Category', self.c.GEO_COLS[0],'timestamp']).agg({'value':'sum'}).compute().value.unstack(level='Category').fillna(0)


        ### Spare capacity by region and technology
        undisp_cap_by_tech_reg_ts = av_cap_by_tech_reg_ts - gen_by_tech_reg_ts
        undisp_cap_by_tech_reg_ts = undisp_cap_by_tech_reg_ts.mask(undisp_cap_by_tech_reg_ts < 0, 0)

        return undisp_cap_by_tech_reg_ts

    
    @property
    @memory_cache
    @drive_cache('variables')
    def reserve_reg_ts(self):
        """
        Obtain the reserve margin by region in absolute terms (GW)
        """
        av_cap_by_reg_ts = self.c.o.gen_df[self.c.o.gen_df.property == 'Available Capacity'].groupby(
                        ['model', self.c.GEO_COLS[0] ,'timestamp']).agg({'value':'sum'}).compute().value.unstack(level=self.c.GEO_COLS[0]).fillna(0)
        
        ### Name makes no sense
        reserve_reg_ts = av_cap_by_reg_ts - self.load_by_reg_ts.unstack(level=self.c.GEO_COLS[0]).fillna(0)

        ### Cappacity reserves by region
        # av_cap_by_reg.columns = [c.replace(' ', '_') for c in av_cap_by_reg.columns]
        reserve_reg_ts = av_cap_by_reg_ts - self.load_by_reg_ts.unstack(level=self.c.GEO_COLS[0]).fillna(0)
        
        return reserve_reg_ts
    
    @property
    @memory_cache
    @drive_cache('variables')
    def reserve_margin_reg_ts(self):
        '''
        Obtain the reserve margin by region in percentage terms
        '''
        av_cap_by_reg_ts = self.c.o.gen_df[self.c.o.gen_df.property == 'Available Capacity'].groupby(
                        ['model', self.c.GEO_COLS[0], 'timestamp']).agg({'value':'sum'}).compute().value.unstack(level=self.c.GEO_COLS[0]).fillna(0)
        
        ### Name makes no sense
        res_by_reg_ts = av_cap_by_reg_ts - self.load_by_reg_ts.unstack(level=self.c.GEO_COLS[0]).fillna(0)

        ### Cappacity reserves by region
        # av_cap_by_reg.columns = [c.replace(' ', '_') for c in av_cap_by_reg.columns]
        res_margin_by_reg_ts = res_by_reg_ts/self.load_by_reg_ts.unstack(level=self.c.GEO_COLS[0]).fillna(0)

        return res_margin_by_reg_ts
    
    @property
    @memory_cache
    @drive_cache('variables')
    def reserve_margin_ts(self):
        '''
        Obtain the reserve margin by region in percentage terms
        '''
        av_cap_ts = self.av_cap_by_tech_ts.sum(axis=1).rename('value').to_frame()
        load_ts = self.load_by_reg_ts.groupby(['model', 'timestamp']).agg({'value':'sum'})
        daily_pk_load_ts = load_ts.groupby([pd.Grouper(level='model'), pd.Grouper(level='timestamp', freq='D')]).max()
        daily_pk_load_ts = daily_pk_load_ts.reindex(load_ts.index).ffill()

        res_margin_ts = ((av_cap_ts -  load_ts) / daily_pk_load_ts )


        return res_margin_ts
    

    @property
    @memory_cache
    @drive_cache('variables')
    def spinres_av_ts(self):
        """
        Obtain the spinning reserve available by region
        """

        spinres_av_ts = self.c.o.res_gen_df[(self.c.o.res_gen_df.property == 'Available Response')& (self.c.o.res_gen_df.ResType == 'Spinning')].groupby(
        ['model','CapacityCategory', 'Category','timestamp']).agg({'value':'sum'}).compute()
        

        return spinres_av_ts
    
    @property
    @memory_cache
    @drive_cache('variables')
    def spinres_prov_ts(self):
        """
        Obtain the spinning reserve provided by region
        """

        spinres_prov_ts = self.c.o.res_gen_df[(self.c.o.res_gen_df.property == 'Provision')& (self.c.o.res_gen_df.ResType == 'Spinning')].groupby(
                ['model','CapacityCategory', 'Category','timestamp']).agg({'value':'sum'}).compute()
        
        return spinres_prov_ts


    @property
    @memory_cache
    @drive_cache('variables')
    def regres_av_ts(self):
        '''
        Obtain the regulating reserve available by region
        '''

        regres_av_ts = self.c.o.res_gen_df[(self.c.o.res_gen_df.property == 'Available Response')& (self.c.o.res_gen_df.ResType == 'Regulating')].groupby(
        ['model','CapacityCategory', 'Category','timestamp']).agg({'value':'sum'}).compute()
        
        return regres_av_ts
    
    @property
    @memory_cache
    @drive_cache('variables')
    def regres_prov_ts(self):
        '''
        Obtain the regulating reserve provided by region
        '''

        regres_prov_ts = self.c.o.res_gen_df[(self.c.o.res_gen_df.property == 'Provision') & (self.c.o.res_gen_df.ResType == 'Regulating')].groupby(
        ['model','CapacityCategory', 'Category','timestamp']).agg({'value':'sum'}).compute()
        
        return regres_prov_ts

    @property
    @memory_cache
    @drive_cache('variables')
    def res_shorage_by_type_ts(self):
        '''
        Obtain the reserve shortage by region and type
        '''

        res_shorage_by_type_ts = self.c.o.res_df[self.c.o.res_df.property == 'Shortage']\
            .groupby(['model','ResType','timestamp']) \
            .agg({'value':'sum'}) \
            .value \
            .compute() \
            .unstack(level='ResType')

        return res_shorage_by_type_ts
    

    @property
    @memory_cache
    def net_load_avg(self):
        """
        Obtain the average net load by model
        """

        net_load_avg = self.net_load_ts.unstack(level='model').resample('D').mean().stack().reorder_levels(
            ['model', 'timestamp'])

        return net_load_avg
    
    @property
    @memory_cache
    def gen_build_cost_by_tech(self):
        """
        Obtain the build cost by technology using PLEXOS technology
        """

        gen_build_cost_by_tech = self.c.o.gen_yr_df[self.c.o.gen_yr_df.property == 'Annualized Build Cost'].groupby(
                            ['model', 'Category']).agg({'value':'sum'}).value.unstack(level='Category').fillna(0)

        return gen_build_cost_by_tech

    @property
    @memory_cache
    def doi_summary(self):
        """
        Obtain a summary table of the days of interest
        """

        net_load_avg = self.net_load_avg

        wet_season = self.gen_by_tech_ts.loc[pd.IndexSlice[self.model_names[0], :]]['Hydro'].groupby(
            [pd.Grouper(level='timestamp', freq='M')]).sum().idxmax()
        dry_season = self.gen_by_tech_ts.loc[pd.IndexSlice[self.model_names[0], :]]['Hydro'].groupby(
            [pd.Grouper(level='timestamp', freq='M')]).sum().idxmin()
                
        

        ##########
        min_cap = self.av_cap_by_tech_ts.sum(axis=1).groupby(level='model').min().rename('min_cap').to_frame()
        
        nl_median = self.net_load_ts.value.unstack(level='model').apply(lambda x: x.quantile(0.5, 'lower').max()).rename('net_load_median').to_frame()
        nl_25pc_quantile = self.net_load_ts.value.unstack(level='model').apply(lambda x: x.quantile(0.25, 'lower').max()).rename('net_load_25pc_qt').to_frame()
        nl_75pc_quantile = self.net_load_ts.value.unstack(level='model').apply(lambda x: x.quantile(0.75, 'lower').max()).rename('net_load_75pc_qt').to_frame()

        net_load_avg_max = net_load_avg.groupby(level='model').max().rename(columns={'value': 'net_load_avg_max'})
        net_load_avg_min = net_load_avg.groupby(level='model').min().rename(columns={'value': 'net_load_avg_min'})
        net_load_avg_med = net_load_avg.groupby(level='model').median().rename(columns={'value': 'net_load_avg_med'})

        net_load_max = self.net_load_ts.groupby(level='model').max().rename(columns={'value': 'net_load_max'})
        net_load_min = self.net_load_ts.groupby(level='model').min().rename(columns={'value': 'net_load_min'})
        net_load_sto_min = self.net_load_sto_ts.groupby(level='model').min().rename(columns={'value': 'net_load_sto_min'})
        curtail_max = self.vre_curtailed_reg_ts.sum(axis=1).groupby(level='model').max().rename('curtail_max').to_frame()

        net_load_max_wet = self.net_load_ts[
            self.net_load_ts.index.get_level_values('timestamp').month == wet_season.month].groupby(
            level='model').max().rename(columns={'value': 'net_load_max_wet'})
        net_load_min_wet = self.net_load_ts[
            self.net_load_ts.index.get_level_values('timestamp').month == wet_season.month].groupby(
            level='model').min().rename(columns={'value': 'net_load_min_wet'})
        net_load_max_dry = self.net_load_ts[
            self.net_load_ts.index.get_level_values('timestamp').month == dry_season.month].groupby(
            level='model').max().rename(columns={'value': 'net_load_max_dry'})
        net_load_min_dry = self.net_load_ts[
            self.net_load_ts.index.get_level_values('timestamp').month == wet_season.month].groupby(
            level='model').min().rename(columns={'value': 'net_load_min_dry'})
        total_load_max = self.total_load_ts.groupby(level='model').max().rename(
            columns={'value': 'total_load_max'})
        total_load_min = self.total_load_ts.groupby(level='model').min().rename(
            columns={'value': 'total_load_min'})
        ramp_max = self.ramp_ts.groupby(level='model').max().rename(columns={'value': 'ramp_max'})
        inertia_min = self.total_inertia_ts.groupby(level='model').min().rename(
            columns={'value': 'inertia_min'})
        use_max = self.use_ts.groupby(level='model').max().rename(columns={'value': 'use_max'})
        use_dly_max = self.use_dly_ts.groupby(level='model').max().rename(columns={'value': 'use_dly_max'})

        # ###########
        min_cap['time_min_cap'] = self.av_cap_by_tech_ts.sum(axis=1).unstack(level='model').idxmin().values

        net_load_max['time_nlmax'] = self.net_load_ts.value.unstack(level='model').idxmax().values
        net_load_min['time_nlmin'] = self.net_load_ts.value.unstack(level='model').idxmin().values
        net_load_sto_min['time_nlstomin'] = self.net_load_sto_ts.value.unstack(level='model').idxmin().values
        curtail_max['time_curtailmax'] = self.vre_curtailed_reg_ts.sum(axis=1).unstack(level='model').idxmax().values

        net_load_max_wet['time_nlmax_wet'] = (
            self.net_load_ts[self.net_load_ts.index.get_level_values('timestamp').month == wet_season.month]
            .unstack(level='model')
            .idxmax()
            .values)
        net_load_min_wet['time_nlmin_wet'] = (
            self.net_load_ts[self.net_load_ts.index.get_level_values('timestamp').month == wet_season.month]
            .unstack(level='model')
            .idxmin()
            .values)
        net_load_max_dry['time_nlmax_dry'] = (
            self.net_load_ts[self.net_load_ts.index.get_level_values('timestamp').month == dry_season.month]
            .unstack(level='model')
            .idxmax()
            .values)
        net_load_min_dry['time_nlmin_dry'] = (
            self.net_load_ts[self.net_load_ts.index.get_level_values('timestamp').month == dry_season.month]
            .unstack(level='model')
            .idxmin()
            .values)
        net_load_avg_max['time_nlamax'] = net_load_avg.unstack(level='model').idxmax().values
        net_load_avg_med['time_nlamed'] = net_load_avg.unstack(level='model').apply(get_median_index, axis=1)

        nl_median['time_nl_med'] = self.net_load_ts.value.unstack(level='model').apply(lambda x: (x == x.quantile(0.5, 'lower')).idxmax()).values
        nl_25pc_quantile['time_nl_25pc_qt']  = self.net_load_ts.value.unstack(level='model').apply(lambda x: (x == x.quantile(0.75, 'lower')).idxmax()).values
        nl_75pc_quantile['time_nl_75pc_qt']  = self.net_load_ts.value.unstack(level='model').apply(lambda x: (x == x.quantile(0.25, 'lower')).idxmax()).values

        net_load_avg_min['time_nlamin'] = net_load_avg.unstack(level='model').idxmin().values
        total_load_max['time_tlmax'] = self.total_load_ts.value.unstack(level='model').idxmax().values
        total_load_min['time_tlmin'] = self.total_load_ts.value.unstack(level='model').idxmin().values
        ramp_max['time_ramp'] = self.ramp_ts.value.unstack(level='model').idxmax().values
        inertia_min['time_H'] = self.total_inertia_ts.value.unstack(level='model').idxmin().values
        use_max['time_usemax'] = self.use_ts.value.unstack(level='model').idxmax().values
        use_dly_max['time_usedmax'] = self.use_dly_ts.value.unstack(level='model').idxmax().values

        doi_summary = pd.concat(
            [min_cap, net_load_max, net_load_min, nl_median, nl_25pc_quantile, nl_75pc_quantile, net_load_sto_min, curtail_max, net_load_avg_max, net_load_avg_min, net_load_avg_med,
            net_load_max_wet, net_load_min_wet, net_load_max_dry, net_load_min_dry,
            total_load_max, total_load_min, ramp_max, inertia_min, use_max, use_dly_max],
            axis=1).stack().unstack(level='model').rename_axis('property', axis=0)

        return doi_summary
    
    @property
    @memory_cache
    def use_summary(self):
        """
        Obtain a summary table of the days of system strain where USE is max
        """

        ##########
               
        use_summary = self.use_ts.value.unstack('model').groupby(pd.Grouper(freq='M')).max()
        use_summary.index = use_summary.index.strftime('M%m')
        use_summary_idx = self.use_ts.value.unstack('model').groupby(pd.Grouper(freq='M')).idxmax()
        use_summary_idx.index = use_summary_idx.index.strftime('M%m_time')

        use_summary = pd.concat([use_summary, use_summary_idx], axis=0).sort_index()


        return use_summary
    

    @memory_cache
    def min_cap_summary(self):
        """
        Obtain a summary table of the days of system strain where min cap is least
        """

        ##########
              
        min_cap_summary = self.av_cap_by_tech_ts.sum(axis=1).unstack('model').groupby(pd.Grouper(freq='M')).min()
        min_cap_summary.index = min_cap_summary.index.strftime('M%m')
        min_cap_summary_idx = self.av_cap_by_tech_ts.sum(axis=1).unstack('model').groupby(pd.Grouper(freq='M')).idxmin()
        min_cap_summary_idx.index = min_cap_summary_idx.index.strftime('M%m_time')

        min_cap_summary = pd.concat([min_cap_summary, min_cap_summary_idx], axis=0).sort_index()



        return min_cap_summary
    


    @property
    @memory_cache
    def vre_gen_monthly_ts(self):
        """
        Obtain the monthly generation of VRE technologies
        """

        
        df = self.c.v.gen_by_tech_ts[['Wind', 'Solar']].groupby([pd.Grouper(level='model'),
                    pd.Grouper(level='timestamp', freq='M')]).sum()/1000
        df['VRE'] = df.sum(axis=1)

        
        return df
    

    @property
    @memory_cache
    def vre_cf_monthly_ts(self):
        """
        Obtain the monthly generation of VRE technologies
        """

        
        df_gen = self.c.v.vre_av_abs_ts[['Wind', 'Solar']].groupby([pd.Grouper(level='model'),
                    pd.Grouper(level='timestamp', freq='M')]).sum()/1000
        df_cap = self.c.v.gen_cap_tech_reg.sum(axis=1).unstack('Category')[['Wind', 'Solar']]

        df = df_gen/df_cap

        
        return df
    
    @property
    @memory_cache
    def vre_cf_gen_monthly_ts(self):
        """
        Obtain the monthly generation of VRE technologies
        """
        
        df_gen = self.c.v.gen_by_tech_reg_ts[['Wind', 'Solar']].groupby([pd.Grouper(level='model'),
                    pd.Grouper(level='timestamp', freq='M')]).sum()/1000
        df_cap = self.c.v.gen_cap_tech_reg.sum(axis=1).unstack('Category')[['Wind', 'Solar']]

        df = df_gen/df_cap

        
        return df
    

    @property
    @memory_cache
    def srmc_reg_ts(self):
        """
        Obtain the SRMC by region
        """

        df = self.c.o.reg_df[self.c.o.reg_df.property == 'SRMC'].groupby(['model', self.c.GEO_COLS[0] , 'timestamp']).agg({'value':'max'}).compute()
        
        return df
    
    @property
    @memory_cache
    def srmc_ts(self):
        """
        Obtain the demand-weighted SRMC by region
        """

        # Define a lambda function to compute the demand-weighted mean across the different regions
        # Note that this works for anything grouped by model, region, and timeslice
        wm = lambda x: np.average(x, weights=self.c.v.srmc_reg_ts.loc[x.index, 'value'])
        df = self.c.v.srmc_reg_ts.groupby(['model', 'timestamp']).agg({'value':wm})

        return df
    
    @property
    @memory_cache
    def srmc_dc(self):
        """
        The SRMC duration curve based on the national maximum. 
        This could be also load-weighted or otherwise in different projects.

        """
        srmc_dc = self.c.v.srmc_ts.value.unstack('model')
        srmc_dc = pd.DataFrame(np.flipud(np.sort(srmc_dc.values, axis=0)), index=srmc_dc.index, columns=srmc_dc.columns)
        # Index = 0-8760
        srmc_dc = srmc_dc.reset_index(drop=True)

        return srmc_dc
    

    @property
    @memory_cache
    def ramp_dc(self):
        """
        The ramp duration curve based on the national aggregate.

        """
        ramp_dc = self.c.v.ramp_ts.value.unstack('model')
        ramp_dc = pd.DataFrame(np.flipud(np.sort(ramp_dc.values, axis=0)), index=ramp_dc.index, columns=ramp_dc.columns)
        # Index = 0-8760
        ramp_dc = ramp_dc.reset_index(drop=True)

        return ramp_dc
    
    @property
    @memory_cache
    def th_ramp_dc(self):
        """
        The 3-hour ramp duration curve based on the national aggregate.

        """
        th_ramp_dc = self.c.v.th_ramp_ts.value.unstack('model')
        th_ramp_dc = pd.DataFrame(np.flipud(np.sort(th_ramp_dc.values, axis=0)), index=th_ramp_dc.index, columns=th_ramp_dc.columns)
        # Index = 0-8760
        th_ramp_dc = th_ramp_dc.reset_index(drop=True)

        return th_ramp_dc
    

    @property
    @memory_cache
    def ramp_pc_dc(self):
        """
        The ramp_pc duration curve based on the national aggregate.

        """
        ramp_pc_dc = self.c.v.ramp_pc_ts.unstack('model')
        ramp_pc_dc = pd.DataFrame(np.flipud(np.sort(ramp_pc_dc.values, axis=0)), index=ramp_pc_dc.index, columns=ramp_pc_dc.columns)
        # Index = 0-8760
        ramp_pc_dc = ramp_pc_dc.reset_index(drop=True)

        return ramp_pc_dc
    
    @property
    @memory_cache
    def th_ramp_pc_dc(self):
        """
        The 3-hour ramp_pc duration curve based on the national aggregate.

        """
        th_ramp_pc_dc = self.c.v.th_ramp_pc_ts.unstack('model')
        th_ramp_pc_dc = pd.DataFrame(np.flipud(np.sort(th_ramp_pc_dc.values, axis=0)), index=th_ramp_pc_dc.index, columns=th_ramp_pc_dc.columns)
        # Index = 0-8760
        th_ramp_pc_dc = th_ramp_pc_dc.reset_index(drop=True)

        return th_ramp_pc_dc

    @property
    @memory_cache
    def gen_built_by_tech_reg(self):
        """"
        TODO DOCSTRING
        """
        df = self.c.o.gen_yr_df[((self.c.o.gen_yr_df.property == 'Capacity Built')&(self.c.o.gen_yr_df.WEO_tech != 'Battery')) |
                                              ((self.c.o.gen_yr_df.WEO_tech == 'Battery') & (self.c.o.gen_yr_df.property == 'Generation Capacity Built'))] \
                                              
        if df.shape[0] == 0:
            return pd.DataFrame()
        else: 
            df = df.groupby(['model', 'Category', self.c.GEO_COLS[0]] ) \
                .agg({'value': 'sum'}) \
                .fillna(0)
            
        return df
    

    @property
    @memory_cache
    def cap_shortage_ts(self):
        """"
        TODO DOCSTRING
        """
        df = self.res_shorage_by_type_ts.sum(axis=1).rename('value').to_frame() + self.use_ts
                                              
            
        return df
    
    @property
    @memory_cache
    def cap_shortage_dc(self):
        """"
        TODO DOCSTRING
        """
        cap_shortage_dc = self.c.v.cap_shortage_ts.value.unstack('model')
        cap_shortage_dc = pd.DataFrame(np.flipud(np.sort(cap_shortage_dc.values, axis=0)), index=cap_shortage_dc.index, columns=cap_shortage_dc.columns)
        # Index = 0-8760
        cap_shortage_dc = cap_shortage_dc.reset_index(drop=True)
                                              
            
        return cap_shortage_dc
    

        
    @property
    @memory_cache
    def cap_shortage_monthly_ts(self):
        """

        Returns the generation capacity shortage by model in a plot-ready dataframe for each model in the configuration object.

        """

        df = self.c.v.cap_shortage_ts.groupby([pd.Grouper(level='model'),
                    pd.Grouper(level='timestamp', freq='M')]).max()
        df = df.value.unstack('model')/1000
        

        return df
    
    @property
    @memory_cache
    def use_dc(self):
        """"
        TODO DOCSTRING
        """
        use_dc = self.c.v.use_ts.value.unstack('model')
        use_dc = pd.DataFrame(np.flipud(np.sort(use_dc.values, axis=0)), index=use_dc.index, columns=use_dc.columns)
        # Index = 0-8760
        use_dc = use_dc.reset_index(drop=True)
                                                                                            
            
        return use_dc
    
    @property
    @memory_cache
    def gen_cycling_pc_dly_ts(self):
        """
        The daily generation cycling by model in a plot-ready dataframe for each model in the configuration object.
        """

        gen_cycling_pc_dly_ts  = ((self.net_load_ts.groupby([pd.Grouper(level='model'), 
                                                        pd.Grouper(level='timestamp', freq='D')]).max() - \
                              self.net_load_ts.groupby([pd.Grouper(level='model'), 
                                                        pd.Grouper(level='timestamp', freq='D')]).min())/\
                                self.net_load_ts.groupby([pd.Grouper(level='model'), 
                                                        pd.Grouper(level='timestamp', freq='D')]).max())
        return gen_cycling_pc_dly_ts
    
    @property
    @memory_cache
    def gen_cycling_pc_dc(self):
        """
        The daily generation cycling duration curve based on the national aggregate.
        """

        gen_cycling_pc_dc = self.c.v.gen_cycling_pc_dly_ts.value.unstack('model')
        gen_cycling_pc_dc = pd.DataFrame(np.flipud(np.sort(gen_cycling_pc_dc.values, axis=0)), index=gen_cycling_pc_dc.index, columns=gen_cycling_pc_dc.columns)
        # Index = 0-8760
        gen_cycling_pc_dc = gen_cycling_pc_dc.reset_index(drop=True)

        return gen_cycling_pc_dc
    
    @property
    @memory_cache
    def gen_cycling_dly_ts(self):
        """
        The daily generation cycling by model in a plot-ready dataframe for each model in the configuration object.
        """

        gen_cycling_dly_ts  = (self.net_load_ts.groupby([pd.Grouper(level='model'), 
                                                        pd.Grouper(level='timestamp', freq='D')]).max() - 
                              self.net_load_ts.groupby([pd.Grouper(level='model'), 
                                                        pd.Grouper(level='timestamp', freq='D')]).min())
        return gen_cycling_dly_ts
    
    @property
    @memory_cache
    def gen_cycling_dc(self):
        """
        The daily generation cycling duration curve based on the national aggregate.
        """

        gen_cycling_dc = self.gen_cycling_dly_ts.value.unstack('model')
        gen_cycling_dc = pd.DataFrame(np.flipud(np.sort(gen_cycling_dc.values, axis=0)), index=gen_cycling_dc.index, columns=gen_cycling_dc.columns)
        # Index = 0-8760
        gen_cycling_dc = gen_cycling_dc.reset_index(drop=True)

        return gen_cycling_dc
    
    @property
    @memory_cache
    def price_by_reg_ts(self):
        """
        Nodal price by region
        """

        price_by_reg_ts = self.c.o.reg_df[self.c.o.reg_df.property == 'Price'].groupby(['model', 'timestamp', self.c.GEO_COLS[0]]).agg({'value':'mean'}).compute()
        
        return price_by_reg_ts
    
    @property
    @memory_cache
    def price_ts(self):
        """
        Average nodal price
        """

        price_ts = self.c.o.reg_df[self.c.o.reg_df.property == 'Price'].groupby(['model', 'timestamp']).agg({'value':'mean'}).compute()
        
        return price_ts
    

    @property
    @memory_cache
    def price_neighbours_ts(self):
        """
        Average nodal price for neighbouring regions. For Ukraine only, but this could be generalised for other projects.

        """
        price_all_regs_ts = self.c.o.reg_raw_df[self.c.o.reg_raw_df.property == 'Price'].compute()
        price_all_regs_ts = price_all_regs_ts.groupby(['model','name','timestamp']).agg({'value':'sum'})

        neighbour_nodes = ['ROU', 'SVK', 'POL', 'HUN']
        price_neighbours_ts = price_all_regs_ts.value.unstack('name')[neighbour_nodes]
        price_neighbours_ts = price_neighbours_ts.mean(axis=1).rename('value').to_frame()
        
        return price_neighbours_ts
            
    
    @property
    @memory_cache
    def export_cost_ts(self):
        """
        Export revenue / import cost of electricity  using average nodal price of neighbouring regions
        """

        export_cost_ts = self.c.v.exports_ts * self.c.v.price_neighbours_ts
        
        return export_cost_ts