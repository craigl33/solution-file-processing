import os

import pandas as pd

from .settings import MODEL_DIR, MODEL_DIR_TEST, SOLN_CHOICE


def test_output(timescale, output_number=None):
    if timescale == 'year':
        path = os.path.join(MODEL_DIR, '05_DataProcessing', SOLN_CHOICE, 'summary_out')
        path_test = os.path.join(MODEL_DIR_TEST, '05_DataProcessing', SOLN_CHOICE, 'summary_out')
    elif timescale == 'interval':
        path = os.path.join(MODEL_DIR, '05_DataProcessing', SOLN_CHOICE, 'timeseries_out')
        path_test = os.path.join(MODEL_DIR_TEST, '05_DataProcessing', SOLN_CHOICE, 'timeseries_out')
    else:
        raise Exception('timescale must be either "year" or "interval"')

    print(f'Starting tests for {timescale} output.')
    # Get all files
    files = [f for f in os.listdir(path_test)]
    files.sort()

    for file in files:
        if output_number is not None:
            if not file.startswith(f'0{output_number}') and not file.startswith(f'{output_number}'):
                continue
        try:
            df = pd.read_csv(os.path.join(path, file))
        except FileNotFoundError:
            # print(f'File was not created: {file}.')
            continue
        df_test = pd.read_csv(os.path.join(path_test, file))

        # Avoid issues with floating point precision
        df = df.round(2)
        df_test = df_test.round(2)

        # Sort dataframe in the same way
        df = df.sort_values(by=df.columns.to_list()).reset_index(drop=True)
        df_test = df_test.sort_values(by=df_test.columns.to_list()).reset_index(drop=True)

        cols = sorted(df.columns.to_list())
        cols_test = sorted(df_test.columns.to_list())

        df = df[cols]
        df_test = df_test[cols_test]

        matched_cols = list(set(cols) & set(cols_test))
        redundant_cols = list(set(cols) - set(cols_test))
        missing_cols = list(set(cols_test) - set(cols))
        for col in redundant_cols:
            print(f'Column {col} is redundant.')
        for col in missing_cols:
            print(f'Column {col} is missing.')

        df = df[matched_cols]
        df_test = df_test[matched_cols]

        test_failed = False
        if not df_test.equals(df):
            for index, row in df_test.iterrows():
                for column in df_test.columns:
                    if row[column] != df.iloc[index][column]:
                        print(f'Row {index}: {column} - {row[column]} != {df.iloc[index][column]}')
                        test_failed = True

        if test_failed:
            print(f'Test failed: {file}.')
        else:
            print(f'Test passed: {file}.')
