import os

import dask.dataframe as dd
import pandas as pd

def get_files(root_folder, file_type, id_text, subfolder="", return_type=0):
    """Basic function to walk through folder and return all files of a certain type containing specific text in its name.
    Can return either a list of full paths or two lkists odf directories and filenames seperately depending on the argument
    return type =0/1"""

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


### Errors may arise from using input as output. Check back if weird behavior

from functools import wraps
from time import time

def timing(f):
    @wraps(f)
    def wrap(*args, **kw):
        ts = time()
        result = f(*args, **kw)
        te = time()
        in_min = (te-ts)/60
        print('func:%r args:[%r, %r] took: %2.4f min' % \
          (f.__name__, args, kw, in_min))
        return result
    return wrap

def add_df_column(df, column_name, value, reset_index=True):
    if type(df) == pd.Series:
        out_df = pd.DataFrame(df)
    elif type(df) == dd.Series:
        out_df = df.to_frame()
    else:
        out_df = df.copy()

    if column_name in out_df.columns:
        print('Updating /"{}/" column with new values')
        out_df.loc[:, column_name] = value
    else:
        out_df[column_name] = value

    if reset_index:
        out_df = out_df.reset_index().rename(columns={0: 'value'})

    return out_df


def enrich_df(df, soln_idx, common_yr=None, out_type='direct', pretty_model_names={}):
    def convert_to_datetime(partition, common_yr):
        partition['timestamp'] = pd.to_datetime({
            'Year': [common_yr] * len(partition),
            'Month': partition.timestamp.dt.month.values,
            'Day': partition.timestamp.dt.day.values,
            'Hour': partition.timestamp.dt.hour.values,
            'Minute': partition.timestamp.dt.minute.values
        })
        return partition


    ### Output can relative type (i.e. emissions from generators) or direct type (i.e. just emissions)
    if out_type == 'rel':
        df = df.rename(columns={0: 'value'})[['parent', 'child', 'property', 'timestamp', 'model', 'value']]
        ## Add soln idx
        df = dd.merge(df, soln_idx, left_on='child', right_on='name')
    else:
        df = df.rename(columns={0: 'value'})[['name', 'property', 'timestamp', 'model', 'value']]
        ## Add soln idx
        df = dd.merge(df, soln_idx, left_on='name', right_on='name')

    if common_yr:
        df = df.map_partitions(convert_to_datetime, common_yr=common_yr)

    # df.loc[:, 'model'] = df.model.apply(
    #         lambda x: pretty_model_names[x] if x in pretty_model_names.keys() else x.split('Model ')[-1].split(' Solution.h5')[0],
    #         meta=('model', 'str'))

    def prettify_model_names(partition):
        partition['model'] = partition.model.apply(
            lambda x: pretty_model_names[x] if x in pretty_model_names.keys() else x.split('Model ')[-1].split(' Solution.h5')[0])
        return partition

    df = df.map_partitions(prettify_model_names)

    return df
