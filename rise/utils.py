import os

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

def add_df_column(df, column_name, value, reset_index=True):
    if type(df) == pd.Series:
        out_df = pd.DataFrame(df)
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
    ### Output can relative type (i.e. emissions from generators) or direct type (i.e. just emissions)
    if out_type == 'rel':
        df = df.rename(columns={0: 'value'})[['parent', 'child', 'property', 'timestamp', 'model', 'value']]
        ## Add soln idx
        df = pd.merge(df, soln_idx, left_on='child', right_on='name')
    else:
        df = df.rename(columns={0: 'value'})[['name', 'property', 'timestamp', 'model', 'value']]
        ## Add soln idx
        df = pd.merge(df, soln_idx, left_on='name', right_on='name')

    if common_yr:
        ### Add common year date .... will in future need to add line for removing leap days
        df.loc[:, 'timestamp'] = pd.to_datetime(
            {'Year': [common_yr] * len(df), 'Month': df.timestamp.dt.month.values, 'Day': df.timestamp.dt.day.values,
             'Hour': df.timestamp.dt.hour.values, 'Minute': df.timestamp.dt.minute.values})
    else:
        df.loc[:, 'timestamp'] = df.timestamp

    try:
        df.loc[:, 'model'] = df.model.apply(lambda x: pretty_model_names[x] if x in pretty_model_names.keys() else
        x.split('Model ')[-1].split(' Solution.h5')[0])
    except:
        print('Error re-defining model names')

    return df