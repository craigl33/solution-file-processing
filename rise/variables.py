import os
from functools import wraps

import pandas as pd
import dask.dataframe as dd

from .settings import MODEL_DIR, SOLN_CHOICE, SOLN_IDX_PATH, GEO_COLS
from .solution_files import SolutionFileFramework
from .properties import properties as p
from .utils.logger import log

print = log.info
VARIABLES_CACHE = True

def variables_caching(func):
    """
    TODO docstring
    """
    def _variables_caching(self, *args, **kwargs):
        if getattr(self, f'_{func.__name__}') is not None:
            return getattr(self, f'_{func.__name__}')

        path = os.path.join(p.DIR_04_CACHE, 'variables', f'{func.__name__}.parquet')
        if VARIABLES_CACHE and os.path.exists(path):
            # Check if dask or pandas
            if os.path.isdir(path):
                print(f"Loading from cache: dd.DataFrame {func.__name__}.")
                call = dd.read_parquet(path)
            else:
                print(f"Loading from cache: pd.DataFrame {func.__name__}.")
                call = pd.read_parquet(path)
        else:
            print(f"Computing variable {func.__name__}.")
            call = func(*args, **kwargs)
            if VARIABLES_CACHE:
                print(f"Saved {func.__name__} to cache.")
                call.to_parquet(path)

        setattr(self, f'_{func.__name__}', call)
        return call

    return _variables_caching

class _Variables(SolutionFileFramework):
    def __init__(self, model_dir, soln_choice, soln_idx_path):
        super().__init__(model_dir, soln_choice, soln_idx_path)

    _time_idx = None
    _gen_by_tech_reg_ts = None
    _gen_by_subtech_reg_ts = None

    @property
    @variables_caching
    def time_idx(self):
        if self._time_idx is None:
            # todo Not sure if that always works
            # time_idx = db.region("Load").reset_index().timestamp.drop_duplicates()
            time_idx = p.reg_df.reset_index().timestamp.drop_duplicates().compute()

            self._time_idx = time_idx
        return self._time_idx

    @property
    @variables_caching
    def gen_by_tech_reg_ts(self):
        if self._gen_by_tech_reg_ts is None:
            self._gen_by_tech_reg_ts = p.gen_df[p.gen_df.property == 'Generation'] \
                .groupby(['model', 'Category'] + GEO_COLS + ['timestamp']) \
                .agg({'value': 'sum'}) \
                .compute() \
                .unstack(level='Category') \
                .fillna(0)

        return self._gen_by_tech_reg_ts

    @property
    @variables_caching
    def gen_by_subtech_reg_ts(self):
        if self._gen_by_subtech_reg_ts is None:
            self._gen_by_subtech_reg_ts = p.gen_df[p.gen_df.property == 'Generation'] \
                .groupby(['model', 'CapacityCategory'] + GEO_COLS + ['timestamp']) \
                .agg({'value': 'sum'}) \
                .compute() \
                .unstack(level='CapacityCategory') \
                .fillna(0)

        return self._gen_by_subtech_reg_ts


variables = _Variables(model_dir=MODEL_DIR, soln_choice=SOLN_CHOICE, soln_idx_path=SOLN_IDX_PATH)
