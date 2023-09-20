import os
import math

import pandas as pd

from .utils.logger import log
from .settings import config

print = log.info


def _list_csv_files(root_dir):
    csv_files = []

    for foldername, subfolders, filenames in os.walk(root_dir):
        for filename in filenames:
            if filename.endswith(".csv"):
                relative_path = os.path.relpath(os.path.join(foldername, filename), root_dir)
                csv_files.append(relative_path)

    return csv_files


def test_output(timescale, output_number=None, check_mode='simple'):
    if timescale == 'year':
        path = os.path.join(config['path']['model_dir'], '05_DataProcessing', config['model']['soln_choice'], 'summary_out')
        path_test = os.path.join(config['path']['model_dir_test'], '05_DataProcessing', config['model']['soln_choice'], 'summary_out')
    elif timescale == 'interval':
        path = os.path.join(config['path']['model_dir'], '05_DataProcessing', config['model']['soln_choice'], 'timeseries_out')
        path_test = os.path.join(config['path']['model_dir_test'], '05_DataProcessing', config['model']['soln_choice'], 'timeseries_out')
    else:
        raise Exception('timescale must be either "year" or "interval"')

    if output_number is None:
        print(f'Start tests for {timescale} output (check_mode={check_mode}).')
    else:
        print(f'Start tests for {timescale} output {output_number} (check_mode={check_mode}).')

    # Get all files
    files = [f for f in _list_csv_files(path_test)]
    files.sort()

    for file in files:

        if output_number is not None:
            if not file.split('\\')[-1].startswith(f'0{output_number}') and \
                    not file.split('\\')[-1].startswith(f'{output_number}'):
                continue
        try:
            df = pd.read_csv(os.path.join(path, file), low_memory=False)
        except FileNotFoundError:
            # print(f'File was not created: {file}.')
            continue
        df_test = pd.read_csv(os.path.join(path_test, file), low_memory=False)

        cols = df.columns.to_list()
        cols_test = df_test.columns.to_list()

        matched_cols = sorted(list(set(cols) & set(cols_test)))
        redundant_cols = list(set(cols) - set(cols_test))
        missing_cols = list(set(cols_test) - set(cols))

        for col in redundant_cols:
            print(f'Column {col} is redundant.')
        for col in missing_cols:
            print(f'Column {col} is missing.')

        # Make columns match
        df = df[matched_cols]
        df_test = df_test[matched_cols]

        # Round to avoid floating point errors
        df = df.round(5)
        df_test = df_test.round(5)

        test_failed = False
        if not df_test.equals(df):

            if df_test.shape != df.shape:
                print(f'Shape of {file} does not match: {df_test.shape} != {df.shape}.')
                test_failed = True

            for col in df.columns:
                if pd.to_numeric(df[col], errors='coerce').notna().all():
                    if df[col].sum() != df_test[col].sum():
                        print(f'Sum of {file} column {col} does not match: {df[col].sum()} != {df_test[col].sum()}.')
                        test_failed = True
                else:
                    if not set(df[col].unique()) == set(df_test[col].unique()):
                        print(f'Unique values of {file} column {col} do not match.')
                        test_failed = True

        if test_failed:
            print(f'Test failed: {file}.')
        else:
            print(f'Test passed: {file}.')
