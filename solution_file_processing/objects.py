""""
TODO DOCSTRING
"""
import pandas as pd
import dask.dataframe as dd

from solution_file_processing.utils.utils import drive_cache, memory_cache
from solution_file_processing.constants import PRETTY_MODEL_NAMES
from solution_file_processing import log

print = log.info


class Objects:
    """
    This is class handles the complete data access to any Object of the SolutionFile data.
    
    It provides various functionality and the only thing to be done to retrieve the relevant data is to call the
    corresponding property. To retrieve the annual generator data, just call the gen_yr_df property of the 
    class (e.g. o.gen_yr_df).

    If this is the first time the property is called, the data is loaded from the SolutionFiles (h5) based on the 
    configuration. And sometimes also processed based on the provided code below. The processed data is then stored 
    in the cache folder (04_SolutionFilesCache/<soln_choice>/objects/) and the next time the property is called, 
    the data is loaded from the cache folder and not processed again. If the data in the cache folder is deleted, 
    the data is processed again. To reprocess the data, just delete the data in the cache folder.
    
    If other or new objects are needed, just add them to the class in a similar way as the existing properties.
    There just needs to be a corresponding property in the SolutionFiles (.h5 files). Also add them to the
    list below and provide a docstring for the property.

    To get more information about the Objects, please also refer to the properties docstring.
    
    Currently, the following properties are available:

    gen_yr_df: Annual generator data
    em_gen_yr_df: Annual emissions generator data
    node_yr_df: Annual node data
    line_yr_df: Annual line data
    fuelcontract_yr_df: Annual fuel contract data
    fuel_yr_df: Annual fuel data
    gen_df: Interval generator data
    node_df: Interval node data
    reg_df: Interval region data
    res_df: Interval reserves data
    res_gen_df: Interval reserves generator data
    purch_df: Interval purchaser data
    zone_df: Zone (National vs foreign zones)

    """

    def __init__(self, configuration_object):

        self.c = configuration_object

    @property
    @memory_cache
    @drive_cache('objects')
    def gen_yr_df(self):
        """"
        TODO DOCSTRING
        """
        _df = self.c.get_processed_object('year', 'generators', return_type='pandas')

        try:
            bat_yr_df = self.batt_yr_df
            _df = pd.concat([_df, bat_yr_df], axis=0)
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
            _df['WEO_Tech_simpl'] = _df['WEO_tech'].map(_clean_weo_tech)
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
            if isinstance(_df, pd.DataFrame):
                _df = _update_category(_df)
            else:
                _df = _df.map_partitions(_update_category)

            def _update_capacity_category(df):
                condition = (df['Cofiring'] == 'Y') & (df['model'].isin(cofiring_scens))
                df.loc[condition, 'CapacityCategory'] = df.loc[condition, 'CofiringCategory']
                return df

            # Update CapacityCategory
            if isinstance(_df, dd.DataFrame):
                _df = _df.map_partitions(_update_capacity_category)
            else:
                _df = _update_capacity_category(_df)

            # Drop both columns, since they are no longer needed
            _df = _df.drop(columns=['Cofiring', 'CofiringCategory'])

        else:
            print("No cofiring column in gen_yr_df. Skipping category and capacity category update.")

        return _df

    @property
    @memory_cache
    @drive_cache('objects')
    def batt_yr_df(self):
        """"
        TODO DOCSTRING
        """

        df = self.c.get_processed_object('year', 'batteries', return_type='pandas')
        # Batteries report installed capacity per storage duration (i.e. MWh), so we need to divide by storage duration to get the total installed capacity
        df['value'] = df.value.where(df.property != 'Installed Capacity', df.value/df.StorageDuration)
        
        return df
    
    @property
    @memory_cache
    @drive_cache('objects')
    def em_gen_yr_df(self):
        """"
        TODO DOCSTRING
        """

        df = self.c.get_processed_object('year', 'emissions_generators', return_type='pandas')

        return df


    @property
    @memory_cache
    @drive_cache('objects')
    def em_fuel_yr_df(self):
        """"
        TODO DOCSTRING
        """

        df = self.c.get_processed_object('year', 'emissions_fuels', return_type='pandas')

        return df


    @property
    @memory_cache
    @drive_cache('objects')
    def node_yr_df(self):
        """"
        TODO DOCSTRING
        """

        df = self.c.get_processed_object('year', 'nodes', return_type='pandas')

        return df
    

    @property
    @memory_cache
    @drive_cache('objects')
    def line_yr_df(self):
        """"
        TODO DOCSTRING
        """

        df = self.c.get_processed_object('year', 'lines', return_type='pandas')

        return df

    @property
    @memory_cache
    @drive_cache('objects')
    def fuelcontract_yr_df(self):
        """"
        TODO DOCSTRING
        """
        df = self.c.get_processed_object('year', 'fuelcontracts', return_type='pandas')
        return df
    
    @property
    @memory_cache
    @drive_cache('objects')
    def res_yr_df(self):
        """"
        TODO DOCSTRING
        """
    

        df = self.c.get_processed_object('year', 'reserves', return_type='pandas')     

        return df
    
    @property
    @memory_cache
    @drive_cache('objects')
    def res_df(self):
        """"
        TODO DOCSTRING
        """

        df = self.c.get_processed_object('interval', 'reserves', return_type='pandas')    

        return df
        

    @property
    @memory_cache
    @drive_cache('objects')
    def gen_df(self):
        """"
        TODO DOCSTRING
        """
        _df = self.c.get_processed_object('interval', 'generators', return_type='dask')

        try:
            batt_df = self.batt_df
            _df = dd.concat([_df, batt_df], axis=0)
        except ValueError:
            print("No batteries object exists. Will not be added to generators interval dataframe.")
        ### This error could be handled better 
        except AssertionError:
            print("Merging of SolutionIndex led to empty DataFrame for batteries/interval")

        #
        # Cofiring change: Update Category and CapacityCategory with data from Cofiring and CofiringCategory
        #

        # todo not sure if this is always working
        # Todo might should not be optional (China) and always be added to soln_idx

        _df = _df.drop(columns=['Cofiring', 'CofiringCategory'], errors='ignore')
        # Only add cofiring columns if they exist in soln_idx
        if 'Cofiring' in self.c.soln_idx.columns and 'CofiringCategory' in self.c.soln_idx.columns:
            _df = dd.merge(_df, self.c.soln_idx[['PLEXOSname', 'Cofiring', 'CofiringCategory']], on='PLEXOSname', how='left')
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

        return _df
    
    @property
    @memory_cache
    @drive_cache('objects')
    def batt_df(self):
        """"
        TODO DOCSTRING
        """

        df = self.c.get_processed_object('interval', 'batteries', return_type='dask')
        # Batteries report installed capacity per storage duration (i.e. MWh), so we need to divide by storage duration to get the total installed capacity

        df['value'] = df.value.where(df.property != 'Installed Capacity', df.value/df.StorageDuration)

        return df

    @property
    @memory_cache
    @drive_cache('objects')
    def node_df(self):
        """"
        TODO DOCSTRING
        """

        _df = self.c.get_processed_object('interval', 'nodes', return_type='dask')
        return _df
    
    @property
    @memory_cache
    @drive_cache('objects')
    def reg_df(self):
        """"
        TODO DOCSTRING
        """
        _df = self.c.get_processed_object('interval', 'regions', return_type='dask')
        return _df

    @property
    @memory_cache
    @drive_cache('objects')
    def res_gen_df(self):
        """"
        TODO DOCSTRING
        """
        _df = self.c.get_processed_object('interval', 'reserves_generators', return_type='dask')

        try:
            bat_df = self.c.get_processed_object('interval', 'batteries', return_type='dask')
            _df = dd.concat([_df, bat_df], axis=0)
        except ValueError:
            print("No batteries object exists. Will not be added to reserves_generators interval dataframe.")

        # Reserve type is overwritten with the characteristic from the Reserve object as opposed to the Generator object
        # This method of mapping indices based on properties to membership-based objects could be transferred to other
        # objects such as Emissions_Generators or Emissions_Fuels, etc as well and simplify the SolutionIndex 

        # This is currently employing 2 different methods based on the slution index. However, this should be changed to
        # use a common method with a common solution index using the newly defined Type column from the model_indices table
        # in the plexos_model_setup module
        try:
            _df['ResType'] = _df['parent'].map(self.res_yr_df.groupby('PLEXOSname')['Type'].first())
        except KeyError:
            _df['ResType'] = _df['parent'].map(self.res_yr_df.groupby('PLEXOSname').first().index.str.split('_').str[0])

        return _df
    
    @property
    @memory_cache
    @drive_cache('objects')
    def res_gen_yr_df(self):
        """"
        TODO DOCSTRING
        """
        _df = self.c.get_processed_object('year', 'reserves_generators', return_type='pandas')

        try:
            bat_df = self.c.get_processed_object('year', 'batteries', return_type='pandas')
            _df = pd.concat([_df, bat_df], axis=0)
        except ValueError:
            print("No batteries object exists. Will not be added to reserves_generators interval dataframe.")

        # Reserve type is overwritten with the characteristic from the Reserve object as opposed to the Generator object
        # This is currently employing 2 different methods based on the slution index. However, this should be changed to
        # use a common method with a common solution index using the newly defined Type column from the model_indices table
        # in the plexos_model_setup module
        try:
            _df['ResType'] = _df['parent'].map(self.res_yr_df.groupby('PLEXOSname')['Type'].first())
        except KeyError:
            _df['ResType'] = _df['parent'].map(self.res_yr_df.groupby('PLEXOSname').first().index.str.split('_').str[0])

        return _df


    @property
    @memory_cache
    @drive_cache('objects')
    def purch_df(self):
        """"
        TODO DOCSTRING
        """
        _df = self.c.get_processed_object('interval', 'purchasers', return_type='dask')
        return _df
    
    @property
    @memory_cache
    @drive_cache('objects')
    def fuel_yr_df(self):
        """"
        TODO DOCSTRING
        """
        df = self.c.get_processed_object('year', 'fuels', return_type='pandas')
        return df

