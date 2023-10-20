""""
TODO docstring
"""

import os
import sys

import dask.dataframe as dd
import pandas as pd

from .logger import log

print = log.info

def caching(cache_type):
    """
    TODO docstring
    """

    def _caching_decorator(func):
        def _caching_wrapper(self, *args, **kwargs):
            if getattr(self, f'_{func.__name__}') is not None:
                return getattr(self, f'_{func.__name__}')

            path = os.path.join(self.c.DIR_04_CACHE, cache_type, f'{func.__name__}.parquet')
            if self.c.cfg['run']['variables_cache'] and os.path.exists(path):
                # Check if dask or pandas
                if os.path.isdir(path):
                    print(f"Loading from {cache_type} cache: {func.__name__}.parquet as dask dataframe.")
                    call = dd.read_parquet(path)
                else:
                    print(f"Loading from {cache_type} cache: {func.__name__}.parquet as pandas dataframe.")
                    call = pd.read_parquet(path)
            else:
                print(f"Computing {cache_type}: {func.__name__}.")
                call = func(self, *args, **kwargs)
                if self.c.cfg['run']['variables_cache']:
                    os.makedirs(os.path.dirname(path), exist_ok=True)
                    call.columns = call.columns.astype(str)  # Parquet doesn't like int column names
                    call.to_parquet(path)
                    print(f"Saved to {cache_type} cache: {func.__name__}.parquet.")

            setattr(self, f'_{func.__name__}', call)
            return call

        return _caching_wrapper

    return _caching_decorator


def silence_prints(enable: bool):
    """
    Temporarily suppresses or restores standard output (prints) based on the 'enable' flag.

    Args:
        enable (bool): If True, suppress prints. If False, restore printing.

    Usage:
        To suppress prints:
        silence_prints(True)
        # Code with prints to be silenced

        To restore prints:
        silence_prints(False)
        # Subsequent code will print to standard output
    """
    if enable:
        sys.stdout = open(os.devnull, 'w')
    else:
        sys.stdout = sys.__stdout__


def get_files(root_folder, file_type, id_text, subfolder="", return_type=0):
    """Basic function to walk through folder and return all files of a certain type containing specific text in its
    name. Can return either a list of full paths or two lkists odf directories and filenames seperately depending on
     the argument return type =0/1"""

    searched_files = []
    searched_file_paths = []
    searched_files_fullpath = []

    folder = os.path.join(root_folder, subfolder)

    for dirpath, subdirs, files in os.walk(folder):
        for file in files:
            if (file.endswith(file_type)) & (id_text in file):
                searched_files_fullpath.append(os.path.join(os.path.normpath(dirpath), file))
                searched_files.append(file)
                searched_file_paths.append(os.path.normpath(dirpath))

    if return_type == 0:
        return searched_files_fullpath
    else:
        return searched_file_paths, searched_files


def enrich_df(df, soln_idx, common_yr=None, out_type='direct', pretty_model_names={}):
    """
    TODO docstring
    """
    def _convert_to_datetime(partition, common_yr):
        partition['timestamp'] = pd.to_datetime({
            'Year': [common_yr] * len(partition),
            'Month': partition.timestamp.dt.month.values,
            'Day': partition.timestamp.dt.day.values,
            'Hour': partition.timestamp.dt.hour.values,
            'Minute': partition.timestamp.dt.minute.values
        })
        return partition

    # Output can relative type (i.e. emissions from generators) or direct type (i.e. just emissions)
    if out_type == 'rel':
        df = df.rename(columns={0: 'value'})[['parent', 'child', 'property', 'timestamp', 'model', 'value']]
        # Add soln idx
        df = dd.merge(df, soln_idx, left_on='child', right_on='name')
    else:
        df = df.rename(columns={0: 'value'})[['name', 'property', 'timestamp', 'model', 'value']]
        # Add soln idx
        df = dd.merge(df, soln_idx, left_on='name', right_on='name')

    if common_yr:
        df = df.map_partitions(_convert_to_datetime, common_yr=common_yr)

    # df.loc[:, 'model'] = df.model.apply(
    #         lambda x: pretty_model_names[x] if x in pretty_model_names.keys() else x.split('Model ')[-1]
    #         .split(' Solution.h5')[0],
    #         meta=('model', 'str'))

    def _prettify_model_names(partition):
        partition['model'] = partition.model.apply(
            lambda x: pretty_model_names[x] if x in pretty_model_names.keys() else
            x.split('Model ')[-1].split(' Solution.h5')[0])
        return partition

    df = df.map_partitions(_prettify_model_names)

    return df
