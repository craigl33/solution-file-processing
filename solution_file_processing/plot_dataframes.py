""""
This module contains the PlotDataFrame class, which is a class that contains plot-ready
dataframes for each model in the configuration object. 

The purpose of the class is to provide a single point of access to all dataframes that 
are used for plotting. This is to ensure that the data is only loaded once and that 
the data is consistent across all functions.
"""

import pandas as pd
import numpy as np


from solution_file_processing.utils.utils import memory_cache
from solution_file_processing import log

print = log.info

class PlotDataFrames:
    """
    # TODO maybe(>>?) needs to be updated with non cached variables - not sure if this transfers
    This is class handles the complete data access to any plot=ready dataframe and works almost identically to the Variables class.
    
    The purpose of the class is to provide a single point of access to all dataframes that are used for plotting. This is to ensure that the data is only loaded once and that the data is consistent across all plots.

    """

    def __init__(self, configuration_object):
        self.c = configuration_object



    @property
    @memory_cache
    def load_by_reg(self):
        """"
        Returns the load by region in a plot-ready dataframe for each model in the configuration object.
        """
        df = self.c.v.customer_load_by_reg.groupby(["model", self.c.GEO_COLS[0]]) \
            .sum() \
            .value \
            .unstack(level=self.c.GEO_COLS[0])/ 1000
        
        units = 'TWh'
        plot_type = 'stacked'
        plot_desc = 'Annual load by region'

        return df, units, plot_type, plot_desc
    
    @property
    @memory_cache
    def pk_load_by_reg(self):
        """"
        Returns the peak load by region in a plot-ready dataframe for each model in the configuration object.
        """
        df = pd.concat(
            [ self.c.v.customer_load_reg_ts.groupby(self.c.GEO_COLS[0], axis=1)
             .sum()
             .stack(self.c.GEO_COLS[0])
             .groupby( ["model", self.c.GEO_COLS[0]])
             .max()
             .unstack(level=self.c.GEO_COLS[0]),
            self.c.v.customer_load_reg_ts.sum(axis=1)
            .groupby("model" )
            .max()
            .rename("Overall")
            .to_frame()]
            , axis=1)/1000
        
        units = 'GW'
        plot_type = 'clustered'
        plot_desc = 'Peak load by region'

        return df, units, plot_type, plot_desc
    
    @property
    @memory_cache
    def pk_netload_by_reg(self):
        """"
        Returns the peak net load by region in a plot-ready dataframe for each model in the configuration object.
        """
        df = pd.concat([ 
            self.c.v.net_load_reg_ts.groupby(self.c.GEO_COLS[0], axis=1)
                             .sum()
                             .stack(self.c.GEO_COLS[0])
                             .groupby(["model", self.c.GEO_COLS[0]])
                             .max()
                             .unstack(level=self.c.GEO_COLS[0]),
            self.c.v.net_load_reg_ts.sum(axis=1).groupby("model" )
                            .max()
                            .rename("Overall")
                            .to_frame()], axis=1) / 1000
        
        units = 'GW'
        plot_type = 'clustered'
        plot_desc = 'Peak net load by region'

        return df, units, plot_type, plot_desc
    
    @property
    @memory_cache
    def line_cap_reg(self):
        """"
        Returns the line capacity by region in a plot-ready dataframe for each model in the configuration object.
        """
        df = self.c.v.line_cap_reg.value.unstack(level="line")/ 1000
        
        units = 'GW'
        plot_type = 'clustered'
        plot_desc = 'Line capacity by region'

        return df, units, plot_type, plot_desc
    
    @property
    @memory_cache
    def line_net_exports_reg(self):
        """"
        Returns the net exports by region in a plot-ready dataframe for each model in the configuration object.
        """
        df = ( self.c.v.line_imp_exp_reg["Flow"] - 
              self.c.v.line_imp_exp_reg["Flow Back"]).unstack("line")/ 1000,
        
        units = 'TWh'
        plot_type = 'clustered'
        plot_desc = 'Net exports by region'

        return df, units, plot_type, plot_desc
    
    
    @property
    @memory_cache
    def line_exports_reg(self):
        """"
        Returns the exports by region in a plot-ready dataframe for each model in the configuration object.
        """
        df = (self.c.v.line_imp_exp_reg["Flow"]).unstack("line") / 1000
        
        units = 'TWh'
        plot_type = 'clustered'
        plot_desc = 'Annual exports by region'

        return df, units, plot_type, plot_desc
    
    @property
    @memory_cache
    def line_imports_reg(self):
        """"
        Returns the imports by region in a plot-ready dataframe for each model in the configuration object.
        """
        df = (self.c.v.line_imp_exp_reg["Flow Back"]).unstack("line") / 1000
        
        units = 'TWh'
        plot_type = 'clustered'
        plot_desc = 'Annual imports by region'

        return df, units, plot_type, plot_desc
    
    @property
    @memory_cache
    def use_by_reg(self):
        """
        Returns the unserved energy by region in a plot-ready dataframe for each model in the configuration object.
        """
        df = self.c.v.use_by_reg.groupby(level=['model', self.c.GEO_COLS[0]]) \
                        .sum() \
                        .value \
                        .unstack(self.c.GEO_COLS[0])/ 1000
        
        units = 'TWh'
        plot_type = 'stacked'
        plot_desc = 'Unserved energy by region'

        return df, units, plot_type, plot_desc
    
    @property
    @memory_cache
    def gen_by_tech(self):
        """
        Returns the generation by technology in a plot-ready dataframe for each model in the configuration object.
        """
        df = self.c.v.gen_by_tech_reg.stack(self.c.GEO_COLS) \
                       .groupby(["model", "Category"]) \
                       .sum() \
                       .unstack(level="Category") / 1000
        
        

        
        units = 'TWh'
        plot_type = 'stacked'
        plot_desc = 'Annual generation by technology'

        return df, units, plot_type, plot_desc
    
    @property
    @memory_cache
    def gen_by_reg(self):
        """
        Returns the generation by region in a plot-ready dataframe for each model in the configuration object.
        """
        df = self.c.v.gen_by_tech_reg.stack(self.c.GEO_COLS) \
                      .groupby(["model", self.c.GEO_COLS[0]]) \
                      .sum() \
                      .unstack(level=self.c.GEO_COLS[0])/1000
        
        units = 'TWh'
        plot_type = 'stacked'
        plot_desc = 'Annual generation by region'

        return df, units, plot_type, plot_desc
    

    @property
    @memory_cache
    def net_exports_by_reg(self):
        """
        Returns the net generation by region in a plot-ready dataframe for each model in the configuration object.
        """
        net_exports_by_reg = self.c.o.node_yr_df[self.c.o.node_yr_df.property == 'Net DC Export'].groupby(
            ['model', self.c.GEO_COLS[0]]).agg({'value': 'sum'}).value.unstack(self.c.GEO_COLS[0])
        
        total_exports_ic = -net_exports_by_reg.sum(axis=1).round(8).rename("External")
        net_exports_by_reg = pd.concat([net_exports_by_reg, total_exports_ic], axis=1)/1000
        
        units = 'TWh'
        plot_type = 'stacked'
        plot_desc = 'Annual net generation by region. Positive values indicate a net exporter, negative values indicate a net importer.'

        return net_exports_by_reg, units, plot_type, plot_desc
    
    @property
    @memory_cache
    def net_gen_by_reg(self):
        """
        Returns the net generation by region in a plot-ready dataframe for each model in the configuration object.
        """
        df = self.c.v.gen_by_tech_reg.stack(self.c.GEO_COLS) \
                          .groupby(["model", self.c.GEO_COLS[0]]) \
                          .sum() \
                          .unstack(level=self.c.GEO_COLS[0]) \
                          .fillna(0)/1000 - \
            self.c.v.load_by_reg.groupby(["model", self.c.GEO_COLS[0]]) \
                            .sum() \
                            .value \
                            .unstack(level=self.c.GEO_COLS[0]) / 1000
        
        units = 'TWh'
        plot_type = 'stacked'
        plot_desc = 'Annual net generation by region. Positive values indicate a net exporter, negative values indicate a net importer.'

        return df, units, plot_type, plot_desc
    
    @property
    @memory_cache
    def gen_cap_by_reg(self):
        """
        Returns the generation capacity by region in a plot-ready dataframe for each model in the configuration object.
        """

        df = (self.c.v.gen_cap_tech_reg.stack(self.c.GEO_COLS)
                          .groupby(["model", self.c.GEO_COLS[0]])
                          .sum()
                          .unstack(level=self.c.GEO_COLS[0])
        ) / 1e3
        units = 'GW'
        plot_type = 'stacked'
        plot_desc = 'Installed generation capacity by region'

        return df, units, plot_type, plot_desc

    @property
    @memory_cache
    def gen_cap_by_tech(self):
        """
        Returns the generation capacity by technology in a plot-ready dataframe for each model in the configuration object.
        """

        df = (self.c.v.gen_cap_tech_reg.stack(self.c.GEO_COLS)
                           .groupby(["model", "Category"])
                           .sum()
                           .unstack(level="Category")
        ) / 1e3
        units = 'GW'
        plot_type = 'stacked'
        plot_desc = 'Installed generation capacity by technology'

        return df, units, plot_type, plot_desc
    
    @property
    @memory_cache
    def cf_tech(self):
        """
        Returns the capacity factor by technology in a plot-ready dataframe for each model in the configuration object.
        """

        df = self.c.v.cf_tech
        units = '%'
        plot_type = 'clustered'
        plot_desc = 'Capacity factor by technology, grouped by model'

        return df, units, plot_type, plot_desc
    
    @property
    @memory_cache
    def cf_tech_trans(self):
        """
        Returns the capacity factor (transposed for )each technology in a plot-ready dataframe for each model in the configuration object.
        """

        df = self.c.v.cf_tech.T
        units = '%'
        plot_type = 'clustered'
        plot_desc = 'Capacity factor by model, grouped by technology'

        return df, units, plot_type, plot_desc

    @property
    @memory_cache
    def vre_by_reg_gen(self):
        """
        Reutrn the VRE share (%) by region by actual generation after curtailment in a plot-ready dataframe for each model in the configuration object.
        """

        df = pd.concat([self.c.v.vre_by_reg, 
                        self.c.v.vre_share.rename('Overall')], axis=1)
        units = '%'
        plot_type = 'clustered'
        plot_desc = 'VRE share by region after curtailment'

        return df, units, plot_type, plot_desc

    @property
    @memory_cache
    def vre_by_reg_av(self):
        """
        Returns the VRE share (%) by region by availability (before curtailment) in a plot-ready dataframe for each model in the configuration object.
        """

        df = pd.concat([self.c.v.vre_av_by_reg, 
                        self.c.v.vre_av_share.rename('Overall')], axis=1)
        units = '%'
        plot_type = 'clustered'
        plot_desc = 'VRE share by region before curtailment'

        return df, units, plot_type, plot_desc
    
    @property
    @memory_cache
    def re_by_reg(self):
        """
        Returns the RE share (%) by region in a plot-ready dataframe for each model in the configuration object.
        """

        df = pd.concat([self.c.v.re_by_reg, 
                        self.c.v.vre_share.rename('Overall')],axis=1)
        units = '%'
        plot_type = 'clustered'
        plot_desc = 'RE share by region (after curtailment)'

        return df, units, plot_type, plot_desc
    
    @property
    @memory_cache
    def curtailment_rate(self):
        """
        Returns the curtailment rate (%) in a plot-ready dataframe for each model in the configuration object.
        """

        df = self.c.v.curtailment_rate / 100
        units = '%'
        plot_type = 'clustered'
        plot_desc = 'Curtailment rate of VRE'

        return df, units, plot_type, plot_desc
    
    @property
    @memory_cache
    def curtailment_rate_reg(self):
        """
        Returns the curtailment rate (%) in a plot-ready dataframe for each model in the configuration object.
        """

        df = pd.concat( [self.c.v.curtailment_rate_reg, self.c.v.curtailment_rate.rename(columns={'value':'Overall'})])
        units = '%'
        plot_type = 'clustered'
        plot_desc = 'Curtailment rate of VRE by region'

        return df, units, plot_type, plot_desc
    
    @property
    @memory_cache
    def re_curtailed_by_tech(self):
        """
        Returns the RE curtailed (including spilt hydro, etc.) by technology in a plot-ready dataframe for each model in the configuration object.
        """

        df = self.c.v.re_curtailment_rate_by_tech
        units = '%'
        plot_type = 'clustered'
        plot_desc = 'RE curtailed by technology'

        return df, units, plot_type, plot_desc
    
    @property
    @memory_cache
    def fuels_by_type(self):
        """
        TODO: DOCSTRING
        """

        df = (self.c.v.fuel_by_type.groupby(["model", "Category"])
                            .sum()
                            .value
                            .unstack(level="Category")
                            .fillna(0))
        units = 'TJ'
        plot_type = 'stacked'
        plot_desc = 'Fuel consumption by type'

        return df, units, plot_type, plot_desc
    
    @property
    @memory_cache
    def co2_by_tech(self):
        """
        Returns the CO2 emissions by technology in a plot-ready dataframe for each model in the configuration object.
        """

        df = (self.c.v.co2_by_tech_reg.groupby(["model", "Category"])
                       .sum()
                       .value
                       .unstack(level="Category")
                       / 1e6)
        units = 'million tonnes'
        plot_type = 'stacked'
        plot_desc = 'CO2 emissions by generation technology'

        return df, units, plot_type, plot_desc
    
    @property
    @memory_cache
    def co2_by_fuels(self):
        """
        Returns the CO2 emissions by fuel in a plot-ready dataframe for each model in the configuration object.
        """
        df = (self.c.v.co2_by_fuel_reg.groupby(["model", "Category"])
                        .sum()
                        .value
                        .unstack("Category")
                        / 1e6)
        units = 'million tonnes'
        plot_type = 'stacked'
        plot_desc = 'CO2 emissions by fuel'

        return df, units, plot_type, plot_desc
    
    @property
    @memory_cache
    def co2_by_reg(self):
        """
        Returns the CO2 emissions by region in a plot-ready dataframe for each model in the configuration object.
        """

        df = (self.c.v.co2_by_tech_reg.groupby(["model", self.c.GEO_COLS[0]])
                      .sum()
                      .value
                      .unstack(level=self.c.GEO_COLS[0])
                      / 1e6)
        units = 'million tonnes'
        plot_type = 'stacked'
        plot_desc = 'CO2 emissions by region'

        return df, units, plot_type, plot_desc
    
    @property
    @memory_cache
    def co2_intensity_reg(self):
        """
        Returns the CO2 intensity by region in a plot-ready dataframe for each model in the configuration object.
        """

        df = (self.c.v.co2_by_reg.unstack(self.c.GEO_COLS)
                    .groupby(self.c.GEO_COLS[0], axis=1).sum() /\
              self.c.v.gen_by_tech_reg.groupby("model").sum().
                    groupby(self.c.GEO_COLS[0], axis=1).sum()
                    )
        units = 'kg/MWh'
        plot_type = 'clustered'
        plot_desc = 'CO2 intensity of electricity production by region'

        return df, units, plot_type, plot_desc
    
    @property
    @memory_cache
    def op_costs_by_prop(self):
        """
        TODO: DOCSTRING
        """

        df = (self.c.v.gen_op_costs_by_reg.groupby(["model", "property"]) \
                                    .sum()
                                    .value
                                    .unstack(level="property"))
        units = 'USDm'
        plot_type = 'stacked'
        plot_desc = 'Annual operational costs by cost component'

        return df, units, plot_type, plot_desc

    @property
    @memory_cache
    def op_and_vio_costs_by_prop(self):
        """
        TODO: DOCSTRING
        """

        df = (self.c.v.gen_op_and_vio_costs_by_prop.groupby(["model", "property"]) \
                                    .sum()
                                    .value
                                    .unstack(level="property"))
        units = 'USDm'
        plot_type = 'stacked'
        plot_desc = 'Annual operational and violation costs (e.g. min energy constraint penalties, ToP costs, etc.) by cost component'

        return df, units, plot_type, plot_desc
    

    @property
    @memory_cache
    def lcoe_by_tech(self):
        """
        Returns the LCOE by technology in a plot-ready dataframe for each model in the configuration object.
        """

        df = self.c.v.lcoe_tech.unstack(level='Category')
        units = 'USD/MWh'
        plot_type = 'clustered'
        plot_desc = 'Levelized cost of electricity of technologies, grouped by technology type'

        return df, units, plot_type, plot_desc
    
    @property
    @memory_cache
    def lcoe_by_tech_trans(self):
        """
        Returns the LCOE by technology (transposed) in a plot-ready dataframe for each model in the configuration object.
        """

        df = self.c.v.lcoe_tech.unstack(level='model')
        units = 'USD/MWh'
        plot_type = 'clustered'
        plot_desc = 'Levelized cost of electricity of technologies, grouped by model'

        return df, units, plot_type, plot_desc
    
    @property
    @memory_cache
    def ramp_pc_by_reg(self):
        """
        Reutrn the ramp rate (%) by region in a plot-ready dataframe for each model in the configuration object.
        """

        df = pd.concat(
            [
                self.c.v.ramp_reg_pc_ts.groupby(['model',self.c.GEO_COLS[0]]).max().value.unstack(self.c.GEO_COLS[0]),
                self.c.v.ramp_pc_ts.groupby(["model"]).max().rename("Overall"),
            ],
            axis=1,
            )
        units = '%/hr'
        plot_type = 'clustered'
        plot_desc = 'Hourly ramp rate by region as a percentage of daily peak net load'

        return df, units, plot_type, plot_desc
    
    @property
    @memory_cache
    def th_ramp_pc_by_reg(self):
        """
        Returns the 3-hour ramp rate (%) by region in a plot-ready dataframe for each model in the configuration object.
        """

        df =  pd.concat(
            [
                self.c.v.th_ramp_reg_pc_ts.groupby(['model',self.c.GEO_COLS[0]]).max().value.unstack(self.c.GEO_COLS[0]),
                self.c.v.th_ramp_pc_ts.groupby(["model"]).max().rename("Overall")
            ],
            axis=1,
            )
        units = '%/hr'
        plot_type = 'clustered'
        plot_desc = '3-hour ramp rate by region as a percentage of daily peak net load'

        return df, units, plot_type, plot_desc

    @property
    @memory_cache
    def ramp_by_reg(self):
        """
        Returns the ramp rate by region in a plot-ready dataframe for each model in the configuration object.
        """

        df =  pd.concat(
            [
                self.c.v.ramp_reg_ts.unstack(self.c.GEO_COLS)
                .groupby(level=self.c.GEO_COLS[0], axis=1)
                .sum()
                .groupby(["model"])
                .max(),
                self.c.v.ramp_ts.groupby(["model"]).max().value.rename("Overall"),
            ],
            axis=1,
        )

        units = 'MW/hr'
        plot_type = 'clustered'
        plot_desc = 'Hourly ramp rate by region in absolute terms'

        return df, units, plot_type, plot_desc
    
    @property
    @memory_cache
    def th_ramp_by_reg(self):
        """
        Returns the 3-hour ramp rate by region in a plot-ready dataframe for each model in the configuration object.
        """

        df =  pd.concat(
            [
                self.c.v.th_ramp_reg_ts.unstack(self.c.GEO_COLS)
                .groupby(level=self.c.GEO_COLS[0], axis=1)
                .sum()
                .groupby(["model"])
                .max(),
                self.c.v.th_ramp_ts.groupby(["model"]).max().value.rename("Overall"),
            ],
            axis=1,
        )
        units = 'Mw/hr'
        plot_type = 'clustered'
        plot_desc = '3-hour ramp rate by region in absolute terms'

        return df, units, plot_type, plot_desc
    
    @property
    @memory_cache
    def dsm_pk_contr(self):
        """
        TODO: DOCSTRING
        """

        df =  ((self.c.v.nldc_orig.iloc[:100, :] - 
                self.c.v.nldself.c.iloc[:100, :]))
        units = 'GW'
        plot_type = 'stacked'
        plot_desc = 'Peak load reduction from shiftable demand-side response (DSR)'

        return df, units, plot_type, plot_desc
    
    @property
    @memory_cache
    def av_cap_by_model(self):
        """
        TODO: DOCSTRING
        """

        df_min =  self.c.v.av_cap_by_tech_ts.sum(axis=1).groupby("model").min().rename("Min")
        df_max =  self.c.v.av_cap_by_tech_ts.sum(axis=1).groupby("model").max().rename("Max")
        df_mean = self.c.v.av_cap_by_tech_ts.sum(axis=1).groupby("model").mean().rename("Mean")
        df = pd.concat([df_min, df_max, df_mean], axis=1)

        units = 'GW'
        plot_type = 'clustered'
        plot_desc = 'Available capacity by model'

        return df, units, plot_type, plot_desc

   
    @property
    @memory_cache
    def av_cap_ts(self):
        """
        TODO: DOCSTRING
        """

        df = self.c.v.av_cap_by_tech_ts.sum(axis=1).unstack('model')/1000

        units = 'GW'
        plot_type = 'timeseries'
        plot_desc = 'Available capacity time-series by model'

        return df, units, plot_type, plot_desc

    @property
    @memory_cache
    def res_margin_ts(self):
        """
        TODO: DOCSTRING
        """

        df = self.c.v.reserve_margin_ts.value.unstack('model')

        units = '%'
        plot_type = 'timeseries'
        plot_desc = 'Available reserve margin (based on daily peak load) by model'

        return df, units, plot_type, plot_desc
    
    @property
    @memory_cache
    def av_cap_dly_ts(self):
        """
        TODO: DOCSTRING
        """

        df = self.c.v.av_cap_by_tech_ts.sum(axis=1).groupby([pd.Grouper(level='model'), pd.Grouper(level='timestamp', freq='D')]).min().unstack('model')/1000

        units = 'GW'
        plot_type = 'timeseries'
        plot_desc = 'Available capacity time-series by model'

        return df, units, plot_type, plot_desc
    
    @property
    @memory_cache
    def res_margin_dly_ts(self):
        """
        TODO: DOCSTRING
        """

        df = self.c.v.reserve_margin_ts.value.groupby([pd.Grouper(level='model'), pd.Grouper(level='timestamp', freq='D')]).min().unstack('model')

        units = '%'
        plot_type = 'timeseries'
        plot_desc = 'Available reserve margin (based on daily peak load) by model'

        return df, units, plot_type, plot_desc
    
    @property
    @memory_cache
    def gen_cap_built_tech(self):
        """

        Returns the generation capacity built by technology in a plot-ready dataframe for each model in the configuration object.

        """
        df = self.c.v.gen_built_by_tech_reg
        if df.shape[0] == 0:
            return pd.DataFrame(None), "", "", ""
       
        df = df.groupby(["model", "Category"]).sum().value.unstack("Category")/1000
        units = 'GW'
        plot_type = 'stacked'
        plot_desc = 'Generation capacity built by technology'

        return df, units, plot_type, plot_desc
    
    @property
    @memory_cache
    def gen_cap_built_reg(self):
        """
        Returns the generation capacity built by region in a plot-ready dataframe for each model in the configuration object.
        """

        df = self.c.v.gen_built_by_tech_reg
        if df.shape[0] == 0:
            return pd.DataFrame(None), "", "", ""
        
        df = df.groupby(["model", self.c.GEO_COLS[0]]).sum().value.unstack(self.c.GEO_COLS[0])/1000
        units = 'GW'
        plot_type = 'stacked'
        plot_desc = 'Generation capacity built by region'

        return df, units, plot_type, plot_desc

    @property
    @memory_cache
    def gen_build_cost_by_tech(self):
        """
        Returns the generation capacity built by region in a plot-ready dataframe for each model in the configuration object.
        """

        df = self.c.v.gen_build_cost_by_tech
        df = (df.replace(0, np.nan).dropna(how='all', axis=1)
              .dropna(how='all', axis=0)
              .replace(np.nan, 0) / 1e3
        )

        if df.shape[0] == 0:
            return pd.DataFrame(None), "", "", ""
        
        units = '$m'
        plot_type = 'stacked'
        plot_desc = 'Annualized build cost by technology'

        return df, units, plot_type, plot_desc

    @property
    @memory_cache
    def cap_shortage_by_model(self):
        """

        Returns the generation capacity shortage by model in a plot-ready dataframe for each model in the configuration object.

        """

        df = self.c.v.cap_shortage_ts.groupby('model').agg({'value':'max'})
        
        units = 'GW'
        plot_type = 'stacked'
        plot_desc = 'Generation capacity shortage by model. Based on both USE and reserve shortfalls.'

        return df, units, plot_type, plot_desc
    
    @property
    @memory_cache
    def gen_cycling_pk(self):
        """

        Returns the gen cycling peak as a percentage of the peak generation in a plot-ready dataframe for each model in the configuration object.
        """

        df = self.c.v.gen_cycling_dly_ts.groupby('model').agg({'value':'max'})
        
        units = '%'
        plot_type = 'stacked'
        plot_desc = 'Generation cycling peak as a percentage of the peak generation'
        return df, units, plot_type, plot_desc
    
    
    @property
    @memory_cache
    def gen_cycling_pc_pk(self):
        """

        Returns the gen cycling peak as a percentage of the peak generation in a plot-ready dataframe for each model in the configuration object.
        """

        df = self.c.v.gen_cycling_pc_dly_ts.groupby('model').agg({'value':'max'})
        
        units = '%'
        plot_type = 'stacked'
        plot_desc = 'Generation cycling peak as a percentage of the peak generation'
        return df, units, plot_type, plot_desc
    
    @property
    @memory_cache
    def import_cost(self):
        """

        Returns the gen cycling peak as a percentage of the peak generation in a plot-ready dataframe for each model in the configuration object.
        """

        df_costs = -self.c.v.export_cost_ts[self.c.v.export_cost_ts.value < 0].groupby('model').sum()/1e6
        df_revenues = -self.c.v.export_cost_ts[self.c.v.export_cost_ts.value > 0].groupby('model').sum()/1e6
        df = pd.concat([df_costs.value.rename('Costs'), df_revenues.value.rename('Revenues')], axis=1)

        
        units = 'USDm'
        plot_type = 'stacked'
        plot_desc = 'Costs and revenues of electricity imports and exports'
        return df, units, plot_type, plot_desc