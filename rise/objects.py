""""
TODO Docstring
"""

import dask.dataframe as dd

from .solution_files import SolutionFiles
from .utils.logger import log
from .utils.utils import caching
from .constants import PRETTY_MODEL_NAMES

print = log.info


class _Objects(SolutionFiles):

    def __init__(self):
        super().__init__()

        # try:
        #     self.model_yrs = self.reg_df.groupby(['model']).first().timestamp.dt.year.values
        # except FileNotFoundError:
        #     self.model_yrs = None

    _gen_yr_df = None
    _em_gen_yr_df = None
    _node_yr_df = None
    _line_yr_df = None
    __fuelcontract_yr_df = None
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
            _df = self.get_process_object('year', 'generators')

            try:
                bat_yr_df = self.get_process_object('year', 'batteries')
                _df = dd.concat([_df, bat_yr_df], axis=0)
            except KeyError:
                print("No batteries for current scenarios")

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

            _df['WEO_Tech_simpl'] = _df['WEO tech'].map(_clean_weo_tech)

            # todo: Stuff not sure why it exists
            # Cofiring change here!
            # Due to memory allocation errors, additional columns from soln_idx are used and then discarded
            cofiring_scens = [c for c in PRETTY_MODEL_NAMES.values() if
                              ('2030' in c) | (c == '2025 Base') | (c == '2025 Enforced Cofiring')]

            # Add category
            # _df.loc[(_df.Cofiring == 'Y') & (_df.model.isin(cofiring_scens)), 'Category'] = \
            #     _df.loc[(_df.Cofiring == 'Y') & (_df.model.isin(cofiring_scens)), 'CofiringCategory']
            def _update_category(df):
                condition = (df['Cofiring'] == 'Y') & (df['model'].isin(cofiring_scens))
                df.loc[condition, 'Category'] = df.loc[condition, 'CofiringCategory']
                return df

            # Use map_partitions to apply the function to each partition
            _df = _df.map_partitions(_update_category)

            # And capacity category
            # _df.loc[(_df.Cofiring == 'Y') & (_df.model.isin(cofiring_scens)), 'CapacityCategory'] = \
            #     _df.loc[(_df.Cofiring == 'Y') & (_df.model.isin(cofiring_scens)), 'CofiringCategory']
            def _update_capacity_category(df):
                condition = (df['Cofiring'] == 'Y') & (df['model'].isin(cofiring_scens))
                df.loc[condition, 'CapacityCategory'] = df.loc[condition, 'CofiringCategory']
                return df

            # Use map_partitions to apply the function to each partition
            _df = _df.map_partitions(_update_capacity_category)

            # Drop additional columns for interval df
            _df = _df.drop(columns=['Cofiring', 'CofiringCategory'])

            self._gen_yr_df = _df
        return self._gen_yr_df

    @property
    @caching('objects')
    def em_gen_yr_df(self):
        """"
        TODO Docstring
        """
        if self._em_gen_yr_df is None:
            self._em_gen_yr_df = self.get_process_object('year', 'emissions_generators')
        return self._em_gen_yr_df

    @property
    @caching('objects')
    def node_yr_df(self):
        """"
        TODO Docstring
        """
        if self._node_yr_df is None:
            self._node_yr_df = self.get_process_object('year', 'nodes')
        return self._node_yr_df

    @property
    @caching('objects')
    def line_yr_df(self):
        """"
        TODO Docstring
        """
        if self._line_yr_df is None:
            self._line_yr_df = self.get_process_object('year', 'lines')
        return self._line_yr_df

    @property
    @caching('objects')
    def fuelcontract_yr_df(self):
        """"
        TODO Docstring
        """
        if self.__fuelcontract_yr_df is None:
            self.__fuelcontract_yr_df = self.get_process_object('year', 'fuelcontracts')
        return self.__fuelcontract_yr_df

    @property
    @caching('objects')
    def gen_df(self):
        """"
        TODO Docstring
        """
        if self._gen_df is None:
            _df = self.get_process_object('interval', 'generators')

            try:
                bat_df = self.get_process_object('interval', 'batteries')
                _df = dd.concat([_df, bat_df], axis=0)
            except KeyError:
                print("No batteries objects")

            _df = _df.drop(columns=['Cofiring', 'CofiringCategory'])
            _df = dd.merge(_df, self.soln_idx[['name', 'Cofiring', 'CofiringCategory']], on='name', how='left')

            cofiring_scens = [c for c in PRETTY_MODEL_NAMES.values() if
                              ('2030' in c) | (c == '2025 Base') | (c == '2025 Enforced Cofiring')]

            # Add category
            # _df.loc[(_df.Cofiring == 'Y') & (_df.model.isin(cofiring_scens)), 'Category'] = \
            #     _df.loc[(_df.Cofiring == 'Y') & (_df.model.isin(cofiring_scens)), 'CofiringCategory']
            def _update_category(df):
                condition = (df['Cofiring'] == 'Y') & (df['model'].isin(cofiring_scens))
                df.loc[condition, 'Category'] = df.loc[condition, 'CofiringCategory']
                return df

            # Use map_partitions to apply the function to each partition
            _df = _df.map_partitions(_update_category)

            # And capacity category
            # _df.loc[(_df.Cofiring == 'Y') & (_df.model.isin(cofiring_scens)), 'CapacityCategory'] = \
            #     _df.loc[(_df.Cofiring == 'Y') & (_df.model.isin(cofiring_scens)), 'CofiringCategory']
            def _update_capacity_category(df):
                condition = (df['Cofiring'] == 'Y') & (df['model'].isin(cofiring_scens))
                df.loc[condition, 'CapacityCategory'] = df.loc[condition, 'CofiringCategory']
                return df

            # Use map_partitions to apply the function to each partition
            _df = _df.map_partitions(_update_capacity_category)

            # Drop temp columns for interval df
            _df = _df.drop(columns=['Cofiring', 'CofiringCategory'])

            self._gen_df = _df
        return self._gen_df

    @property
    @caching('objects')
    def node_df(self):
        """"
        TODO Docstring
        """
        if self._node_df is None:
            self._node_df = self.get_process_object('interval', 'nodes')
        return self._node_df

    @property
    @caching('objects')
    def reg_df(self):
        """"
        TODO Docstring
        """
        if self._reg_df is None:
            self._reg_df = self.get_process_object('interval', 'regions')
        return self._reg_df

    @property
    @caching('objects')
    def res_gen_df(self):
        """"
        TODO Docstring
        """
        if self._res_gen_df is None:
            _df = self.get_process_object('interval', 'reserves_generators')

            try:
                bat_df = self.get_process_object('interval', 'batteries')
                _df = dd.concat([_df, bat_df], axis=0)
            except KeyError:
                print("No batteries objects")

            self._res_gen_df = _df
        return self._res_gen_df

    @property
    @caching('objects')
    def purch_df(self):
        """"
        TODO Docstring
        """
        if self._purch_df is None:
            self._purch_df = self.get_process_object('interval', 'purchases')
        return self._purch_df


objects = _Objects()
