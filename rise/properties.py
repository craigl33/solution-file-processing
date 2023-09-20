import os

import dask.dataframe as dd

from .solution_files import SolutionFileFramework
from .utils.logger import log
from .constants import PRETTY_MODEL_NAMES

print = log.info

def property_not_found_decorator(func):
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except AttributeError:
            print(f"Property does not exist for {func.__name__}.")
            return None

    return wrapper

class _Properties(SolutionFileFramework):

    def __init__(self):
        super().__init__()

        try:
            self.model_yrs = self.reg_df.groupby(['model']).first().timestamp.dt.year.values
        except FileNotFoundError:
            self.model_yrs = None

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
    @property_not_found_decorator
    def gen_yr_df(self):
        if self._gen_yr_df is None:
            _df = dd.read_parquet(os.path.join(self.DIR_04_CACHE, 'processed', 'year-generators.parquet'))

            try:
                bat_yr_df = dd.read_parquet(os.path.join(self.DIR_04_CACHE, 'processed', 'year-batteries.parquet'))
                _df = dd.concat([_df, bat_yr_df], axis=0)
            except KeyError:
                print("No batteries for current scenarios")

            # For WEO_tech simpl. probably should add something to soln_idx
            def clean_weo_tech(x):
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

            _df['WEO_Tech_simpl'] = _df['WEO tech'].map(clean_weo_tech)

            # todo: Stuff not sure why it exists
            # Cofiring change here!
            # Due to memory allocation errors, additional columns from soln_idx are used and then discarded
            cofiring_scens = [c for c in PRETTY_MODEL_NAMES.values() if
                              ('2030' in c) | (c == '2025 Base') | (c == '2025 Enforced Cofiring')]

            # Add category
            # _df.loc[(_df.Cofiring == 'Y') & (_df.model.isin(cofiring_scens)), 'Category'] = \
            #     _df.loc[(_df.Cofiring == 'Y') & (_df.model.isin(cofiring_scens)), 'CofiringCategory']
            def update_category(df):
                condition = (df['Cofiring'] == 'Y') & (df['model'].isin(cofiring_scens))
                df.loc[condition, 'Category'] = df.loc[condition, 'CofiringCategory']
                return df

            # Use map_partitions to apply the function to each partition
            _df = _df.map_partitions(update_category)

            # And capacity category
            # _df.loc[(_df.Cofiring == 'Y') & (_df.model.isin(cofiring_scens)), 'CapacityCategory'] = \
            #     _df.loc[(_df.Cofiring == 'Y') & (_df.model.isin(cofiring_scens)), 'CofiringCategory']
            def update_capacity_category(df):
                condition = (df['Cofiring'] == 'Y') & (df['model'].isin(cofiring_scens))
                df.loc[condition, 'CapacityCategory'] = df.loc[condition, 'CofiringCategory']
                return df

            # Use map_partitions to apply the function to each partition
            _df = _df.map_partitions(update_capacity_category)

            # Drop additional columns for interval df
            _df = _df.drop(columns=['Cofiring', 'CofiringCategory'])

            self._gen_yr_df = _df
        return self._gen_yr_df

    @property
    @property_not_found_decorator
    def em_gen_yr_df(self):
        if self._em_gen_yr_df is None:
            _df = dd.read_parquet(os.path.join(self.DIR_04_CACHE, 'processed', 'year-emissions_generators.parquet'))
            self._em_gen_yr_df = _df
        return self._em_gen_yr_df

    @property
    @property_not_found_decorator
    def node_yr_df(self):
        if self._node_yr_df is None:
            _df = dd.read_parquet(os.path.join(self.DIR_04_CACHE, 'processed', f'year-nodes.parquet'))
            self._node_yr_df = _df
        return self._node_yr_df

    @property
    @property_not_found_decorator
    def line_yr_df(self):
        if self._line_yr_df is None:
            _df = dd.read_parquet(os.path.join(self.DIR_04_CACHE, 'processed', 'year-lines.parquet'))
            self._line_yr_df = _df
        return self._line_yr_df

    @property
    @property_not_found_decorator
    def fuelcontract_yr_df(self):
        if self.__fuelcontract_yr_df is None:
            _df = dd.read_parquet(os.path.join(self.DIR_04_CACHE, 'processed', 'year-fuelcontracts.parquet'))
            self.__fuelcontract_yr_df = _df
        return self.__fuelcontract_yr_df

    @property
    @property_not_found_decorator
    def gen_df(self):
        if self._gen_df is None:
            _df = dd.read_parquet(os.path.join(self.DIR_04_CACHE, 'processed', 'interval-generators.parquet'))

            try:
                bat_df = dd.read_parquet(os.path.join(self.DIR_04_CACHE, 'processed', 'interval-batteries.parquet'))
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
            def update_category(df):
                condition = (df['Cofiring'] == 'Y') & (df['model'].isin(cofiring_scens))
                df.loc[condition, 'Category'] = df.loc[condition, 'CofiringCategory']
                return df

            # Use map_partitions to apply the function to each partition
            _df = _df.map_partitions(update_category)

            # And capacity category
            # _df.loc[(_df.Cofiring == 'Y') & (_df.model.isin(cofiring_scens)), 'CapacityCategory'] = \
            #     _df.loc[(_df.Cofiring == 'Y') & (_df.model.isin(cofiring_scens)), 'CofiringCategory']
            def update_capacity_category(df):
                condition = (df['Cofiring'] == 'Y') & (df['model'].isin(cofiring_scens))
                df.loc[condition, 'CapacityCategory'] = df.loc[condition, 'CofiringCategory']
                return df

            # Use map_partitions to apply the function to each partition
            _df = _df.map_partitions(update_capacity_category)

            # Drop temp columns for interval df
            _df = _df.drop(columns=['Cofiring', 'CofiringCategory'])

            self._gen_df = _df
        return self._gen_df

    @property
    @property_not_found_decorator
    def node_df(self):
        if self._node_df is None:
            _df = dd.read_parquet(os.path.join(self.DIR_04_CACHE, 'processed', 'interval-nodes.parquet'))
            self._node_df = _df
        return self._node_df

    @property
    @property_not_found_decorator
    def reg_df(self):
        if self._reg_df is None:
            _df = dd.read_parquet(os.path.join(self.DIR_04_CACHE, 'processed', 'interval-regions.parquet'))
            self._reg_df = _df
        return self._reg_df

    @property
    @property_not_found_decorator
    def res_gen_df(self):
        if self._res_gen_df is None:
            _df = dd.read_parquet(os.path.join(self.DIR_04_CACHE, 'processed', 'interval-reserves_generators.parquet'))

            try:
                bat_df = dd.read_parquet(os.path.join(self.DIR_04_CACHE, 'processed', 'interval-batteries.parquet'))
                _df = dd.concat([_df, bat_df], axis=0)
            except KeyError:
                print("No batteries objects")

            self._res_gen_df = _df
        return self._res_gen_df

    @property
    @property_not_found_decorator
    def purch_df(self):
        if self._purch_df is None:
            _df = dd.read_parquet(os.path.join(self.DIR_04_CACHE, 'processed', 'interval-purchases.parquet'))
            self._purch_df = _df
        return self._purch_df


properties = _Properties()
