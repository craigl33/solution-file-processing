"""
This module, 'utils.py', contains a collection of functions that are used in the other modules of the solution_file_processing package.
"""

import os
import sys

import dask.dataframe as dd
import pandas as pd

from .logger import log

print = log.info

def caching(cache_type):
    """
    This is a decorator for caching the results of a function. It's specially designed for the methods in the
    Objects and Variables classes in the caching.py file. This function can just be used as a decorator for any
    of those methods (actually properties). The decorator will then cache the results of the function in a parquet
    and load the results from the cache if the function is called again, instead of running the function again.
    
    The cache can be either a pandas DataFrame or a dask DataFrame, depending on whether the cache directory 
    is a directory or a file.

    Args:
        cache_type (str): The type of cache to use. This is used to determine the subdirectory in the cache directory
         where the results are stored. This can be either 'variables' or 'objects'.

    Returns:
        function: The wrapped function.
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
                    if not isinstance(call.index, pd.MultiIndex):  # Only change for non-multiindex, for now
                        call.columns = call.columns.astype(str)  # Parquet doesn't like int column names
                    call.to_parquet(path)
                    print(f"Saved to {cache_type} cache: {func.__name__}.parquet.")

            setattr(self, f'_{func.__name__}', call)
            return call

        return _caching_wrapper

    return _caching_decorator

def catch_errors(func):
    """
    Decorator to catch errors in functions and log them instead of crashing the program. This decorator can only be
    used on functions that have a configuration object as the first argument. This is because the decorator needs to
    access the configuration object (e.g. create output and create plot functions) to check if error catching is
    enabled in the configuration file.
    """

    def _catch_errors_wrapper(*args, **kwargs):
        # Extract the configuration object "c" from the arguments
        c = args[0] if args else kwargs.get('c')

        if c.cfg['run']['catch_errors']:
            try:
                return func(*args, **kwargs)
            except Exception as e:
                log.error(f'{e.__class__.__name__} in {func.__name__}: {e}', exc_info=True)

        else:
            return func(*args, **kwargs)

    return _catch_errors_wrapper


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
    name. Can return either a list of full paths or two lists odf directories and filenames separately depending on
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
    # todo this can probably be done more efficiently and completely removed
    """

    # Output can relative type (i.e. emissions from generators) or direct type (i.e. just emissions)
    if out_type == 'rel':
        df = df.rename(columns={0: 'value'})[['parent', 'child', 'property', 'timestamp', 'model', 'value']]
        # Add soln idx
        df = dd.merge(df, soln_idx, left_on='child', right_on='name')
    else:
        df = df.rename(columns={0: 'value'})[['name', 'property', 'timestamp', 'model', 'value']]
        # Add soln idx
        df = dd.merge(df, soln_idx, left_on='name', right_on='name')

    # Replace timestamp year with common year if provided
    if common_yr:
        if isinstance(df, dd.DataFrame):
            df.timestamp = df.timestamp.apply(lambda x: x.replace(year=common_yr), meta=('timestamp', 'datetime64[ns]'))
        else:
            df.timestamp = df.timestamp.apply(lambda x: x.replace(year=common_yr))


    # df.loc[:, 'model'] = df.model.apply(
    #         lambda x: pretty_model_names[x] if x in pretty_model_names.keys() else x.split('Model ')[-1]
    #         .split(' Solution.h5')[0],
    #         meta=('model', 'str'))

    def _prettify_model_names(partition):
        partition['model'] = partition.model.apply(
            lambda x: pretty_model_names[x] if x in pretty_model_names.keys() else
            x.split('Model ')[-1].split(' Solution.h5')[0])
        return partition

    # Check if df is pandas or dask
    if isinstance(df, pd.DataFrame):
        df = _prettify_model_names(df)
    elif isinstance(df, dd.DataFrame):
        df = df.map_partitions(_prettify_model_names)

    return df
