""""
TODO Docstring
"""
import pandas as pd
import dask.dataframe as dd

from .constants import VRE_TECHS

from .utils.logger import log
from .utils.utils import caching
from .constants import PRETTY_MODEL_NAMES

print = log.info


class Objects:
    """
    TODO Docstring
    """

    def __init__(self, configuration_object):

        self.c = configuration_object

        # try:
        #     self.model_yrs = self.reg_df.groupby(['model']).first().timestamp.dt.year.values
        # except FileNotFoundError:
        #     self.model_yrs = None

    _gen_yr_df = None
    _em_gen_yr_df = None
    _node_yr_df = None
    _line_yr_df = None
    _fuelcontract_yr_df = None
    _gen_df = None
    _node_df = None
    _reg_df = None
    _res_gen_df = None
    _purch_df = None

    @property
    @caching('objects')
    def gen_yr_df(self):
        """"
        TODO Docstring
        """
        if self._gen_yr_df is None:
            _df = self.c.get_processed_object('year', 'generators')

            try:
                bat_yr_df = self.c.get_processed_object('year', 'batteries')
                _df = dd.concat([_df, bat_yr_df], axis=0)
            except ValueError:
                print("No batteries object exists. Will not be added to generators year dataframe.")

            # For WEO_tech simpl. probably should add something to soln_idx
            def _clean_weo_tech(x):
                if isinstance(x, float):
                    return x
                x = x. \
                    replace('NEW ', ''). \
                    replace(' 1', ''). \
                    replace(' 2', ''). \
                    replace(' 3', ''). \
                    replace(' 4', ''). \
                    replace(' 5', '')
                return x

            try:
                _df['WEO_Tech_simpl'] = _df['WEO tech'].map(_clean_weo_tech)
            except KeyError:
                print("No WEO tech column")

            #
            # Cofiring change: Update Category and CapacityCategory with data from Cofiring and CofiringCategory
            #

            # todo not sure if this is always working
            # todo why do we not merge Cofiring and CofiringCategory from soln_idx here similar to gen_df?
            #   Harmonization with gen_df would be nice

            # Only add cofiring columns if they exist gen_yr_df
            if 'Cofiring' in _df.columns and 'CofiringCategory' in _df.columns:
                cofiring_scens = [c for c in PRETTY_MODEL_NAMES.values() if
                                  ('2030' in c) | (c == '2025 Base') | (c == '2025 Enforced Cofiring')]

                def _update_category(df):
                    condition = (df['Cofiring'] == 'Y') & (df['model'].isin(cofiring_scens))
                    df.loc[condition, 'Category'] = df.loc[condition, 'CofiringCategory']
                    return df

                # Update Category
                _df = _df.map_partitions(_update_category)

                def _update_capacity_category(df):
                    condition = (df['Cofiring'] == 'Y') & (df['model'].isin(cofiring_scens))
                    df.loc[condition, 'CapacityCategory'] = df.loc[condition, 'CofiringCategory']
                    return df

                # Update CapacityCategory
                _df = _df.map_partitions(_update_capacity_category)
                # Drop both columns, since they are no longer needed
                _df = _df.drop(columns=['Cofiring', 'CofiringCategory'])

            else:
                print("No cofiring column in gen_yr_df. Skipping category and capacity category update.")

            self._gen_yr_df = _df
        return self._gen_yr_df

    @property
    @caching('objects')
    def em_gen_yr_df(self):
        """"
        TODO Docstring
        """
        if self._em_gen_yr_df is None:
            self._em_gen_yr_df = self.c.get_processed_object('year', 'emissions_generators')
        return self._em_gen_yr_df

    @property
    @caching('objects')
    def node_yr_df(self):
        """"
        TODO Docstring
        """
        if self._node_yr_df is None:
            self._node_yr_df = self.c.get_processed_object('year', 'nodes')
        return self._node_yr_df

    @property
    @caching('objects')
    def line_yr_df(self):
        """"
        TODO Docstring
        """
        if self._line_yr_df is None:
            self._line_yr_df = self.c.get_processed_object('year', 'lines')
        return self._line_yr_df

    @property
    @caching('objects')
    def fuelcontract_yr_df(self):
        """"
        TODO Docstring
        """
        if self._fuelcontract_yr_df is None:
            self._fuelcontract_yr_df = self.c.get_processed_object('year', 'fuelcontracts')
        return self._fuelcontract_yr_df

    @property
    @caching('objects')
    def gen_df(self):
        """"
        TODO Docstring
        """
        if self._gen_df is None:
            _df = self.c.get_processed_object('interval', 'generators')

            try:
                bat_df = self.c.get_processed_object('interval', 'batteries')
                _df = dd.concat([_df, bat_df], axis=0)
            except ValueError:
                print("No batteries object exists. Will not be added to generators interval dataframe.")

            #
            # Cofiring change: Update Category and CapacityCategory with data from Cofiring and CofiringCategory
            #

            # todo not sure if this is always working
            # Todo might should not be optional (China) and always be added to soln_idx

            _df = _df.drop(columns=['Cofiring', 'CofiringCategory'], errors='ignore')
            # Only add cofiring columns if they exist in soln_idx
            if 'Cofiring' in self.c.soln_idx.columns and 'CofiringCategory' in self.c.soln_idx.columns:
                _df = dd.merge(_df, self.c.soln_idx[['name', 'Cofiring', 'CofiringCategory']], on='name', how='left')
                cofiring_scens = [c for c in PRETTY_MODEL_NAMES.values() if
                                  ('2030' in c) | (c == '2025 Base') | (c == '2025 Enforced Cofiring')]

                def _update_category(df):
                    condition = (df['Cofiring'] == 'Y') & (df['model'].isin(cofiring_scens))
                    df.loc[condition, 'Category'] = df.loc[condition, 'CofiringCategory']
                    return df

                # Update Category
                _df = _df.map_partitions(_update_category)

                def _update_capacity_category(df):
                    condition = (df['Cofiring'] == 'Y') & (df['model'].isin(cofiring_scens))
                    df.loc[condition, 'CapacityCategory'] = df.loc[condition, 'CofiringCategory']
                    return df

                # Update CapacityCategory
                _df = _df.map_partitions(_update_capacity_category)

                # Drop both columns, since they are no longer needed
                _df = _df.drop(columns=['Cofiring', 'CofiringCategory'])

            else:
                print("No cofiring column in soln_idx. Skipping.")

            self._gen_df = _df
        return self._gen_df

    @property
    @caching('objects')
    def node_df(self):
        """"
        TODO Docstring
        """
        if self._node_df is None:
            self._node_df = self.c.get_processed_object('interval', 'nodes')
        return self._node_df

    @property
    @caching('objects')
    def reg_df(self):
        """"
        TODO Docstring
        """
        if self._reg_df is None:
            self._reg_df = self.c.get_processed_object('interval', 'regions')
        return self._reg_df

    @property
    @caching('objects')
    def res_gen_df(self):
        """"
        TODO Docstring
        """
        if self._res_gen_df is None:
            _df = self.c.get_processed_object('interval', 'reserves_generators')

            try:
                bat_df = self.c.get_processed_object('interval', 'batteries')
                _df = dd.concat([_df, bat_df], axis=0)
            except ValueError:
                print("No batteries object exists. Will not be added to reserves_generators interval dataframe.")

            self._res_gen_df = _df
        return self._res_gen_df

    @property
    @caching('objects')
    def purch_df(self):
        """"
        TODO Docstring
        """
        if self._purch_df is None:
            self._purch_df = self.c.get_processed_object('interval', 'purchasers')
        return self._purch_df


class Variables:
    """"
    TODO Docstring
    """

    def __init__(self, configuration_object):
        self.c = configuration_object

    _time_idx = None
    _gen_by_tech_reg_ts = None
    _gen_by_subtech_reg_ts = None
    _customer_load_ts = None
    _vre_av_abs_ts = None
    _net_load_ts = None
    _net_load_reg_ts = None
    _gen_inertia = None

    @property
    @caching('variables')
    def gen_by_tech_reg_ts(self):
        """"
        TODO Docstring
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
        TODO Docstring
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
        TODO Docstring
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
        TODO Docstring
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
        TODO Docstring
        """
        if self._net_load_ts is None:
            self._net_load_ts = pd.DataFrame(
                self.customer_load_ts - self.vre_av_abs_ts.fillna(0).sum(axis=1).groupby(['model', 'timestamp']).sum(),
                columns=['value'])
        return self._net_load_ts

    @property
    @caching('variables')
    def net_load_reg_ts(self):
        """"
        TODO Docstring
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
        TODO Docstring
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
