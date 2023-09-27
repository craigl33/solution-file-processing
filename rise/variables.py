""""
TODO Docstring
"""

import pandas as pd

from .solution_files import SolutionFiles
from .objects import objects as p
from .utils.logger import log
from .utils.utils import caching
from .settings import settings as s
from .constants import VRE_TECHS

print = log.info


class _Variables(SolutionFiles):
    """"
    TODO Docstring
    """
    def __init__(self):
        super().__init__()

    _time_idx = None
    _gen_by_tech_reg_ts = None
    _gen_by_subtech_reg_ts = None
    _customer_load_ts = None
    _vre_av_abs_ts = None
    _net_load_ts = None
    _net_load_reg_ts = None
    _gen_inertia = None

    @property
    @caching('variables')
    def time_idx(self):
        """"
        TODO Docstring
        """
        if self._time_idx is None:
            # todo Not sure if that always works
            # time_idx = db.region("Load").reset_index().timestamp.drop_duplicates()
            time_idx = p.reg_df.reset_index().timestamp.drop_duplicates().compute()

            self._time_idx = time_idx
        return self._time_idx

    @property
    @caching('variables')
    def gen_by_tech_reg_ts(self):
        """"
        TODO Docstring
        """
        if self._gen_by_tech_reg_ts is None:
            self._gen_by_tech_reg_ts = p.gen_df[p.gen_df.property == 'Generation'] \
                .groupby(['model', 'Category'] + s.cfg['settings']['geo_cols'] + ['timestamp']) \
                .agg({'value': 'sum'}) \
                .compute() \
                .unstack(level='Category') \
                .fillna(0)

        return self._gen_by_tech_reg_ts

    @property
    @caching('variables')
    def gen_by_subtech_reg_ts(self):
        """"
        TODO Docstring
        """
        if self._gen_by_subtech_reg_ts is None:
            self._gen_by_subtech_reg_ts = p.gen_df[p.gen_df.property == 'Generation'] \
                .groupby(['model', 'CapacityCategory'] + s.cfg['settings']['geo_cols'] + ['timestamp']) \
                .agg({'value': 'sum'}) \
                .compute() \
                .unstack(level='CapacityCategory') \
                .fillna(0)

        return self._gen_by_subtech_reg_ts

    @property
    @caching('variables')
    def customer_load_ts(self):
        """"
        TODO Docstring
        """
        if self._customer_load_ts is None:
            self._customer_load_ts = p.reg_df[(p.reg_df.property == 'Customer Load') |
                                              (p.reg_df.property == 'Unserved Energy')] \
                .groupby(['model', 'timestamp']) \
                .sum() \
                .value
        return self._customer_load_ts

    @property
    @caching('variables')
    def vre_av_abs_ts(self):
        """"
        TODO Docstring
        """
        if self._vre_av_abs_ts is None:
            self._vre_av_abs_ts = p.gen_df[(p.gen_df.property == 'Available Capacity') &
                                           (p.gen_df.Category.isin(VRE_TECHS))] \
                .groupby(['model', 'Category', 'timestamp']) \
                .sum().value.unstack(level='Category').fillna(0)

        return self._vre_av_abs_ts

    @property
    @caching('variables')
    def net_load_ts(self):
        """"
        TODO Docstring
        """
        if self._net_load_ts is None:
            self._net_load_ts = pd.DataFrame(
                self.customer_load_ts - self.vre_av_abs_ts.fillna(0).sum(axis=1).groupby(['model', 'timestamp']).sum(),
                columns=['value'])
        return self._net_load_ts

    @property
    @caching('variables')
    def net_load_reg_ts(self):
        """"
        TODO Docstring
        """
        if self._net_load_reg_ts is None:
            customer_load_reg_ts = p.node_df[(p.node_df.property == 'Customer Load') |
                                             (p.node_df.property == 'Unserved Energy')] \
                .groupby(['model'] + s.cfg['settings']['geo_cols'] + ['timestamp']) \
                .sum() \
                .value \
                .compute() \
                .unstack(level=s.cfg['settings']['geo_cols'])
            vre_av_reg_abs_ts = p.gen_df[(p.gen_df.property == 'Available Capacity') &
                                         (p.gen_df.Category.isin(VRE_TECHS))] \
                .groupby((['model'] + s.cfg['settings']['geo_cols'] + ['timestamp'])) \
                .sum() \
                .value \
                .compute() \
                .unstack(level=s.cfg['settings']['geo_cols']).fillna(0)

            self._net_load_reg_ts = customer_load_reg_ts - vre_av_reg_abs_ts

        return self._net_load_reg_ts

    @property
    @caching('variables')
    def gen_inertia(self):
        """"
        TODO Docstring
        """
        if self._gen_inertia is None:
            gen_units_gen = p.gen_df[p.gen_df.property == 'Units Generating'] \
                .groupby(['model', 'name', 'timestamp']) \
                .agg({'value': 'sum'}) \
                .compute()

            gen_units = p.gen_df[p.gen_df.property == 'Units'] \
                .groupby(['model', 'name', 'timestamp']) \
                .agg({'value': 'sum'}) \
                .compute()

            # Take only the sum to maintain the capacity value & inertia constant in the dataframe
            gen_cap = p.gen_df[p.gen_df.property == 'Installed Capacity'] \
                .groupby(['model', 'name', 'timestamp']) \
                .agg({'value': 'sum'}) \
                .compute()

            gen_cap = pd.merge(gen_cap.reset_index(),
                               p.soln_idx[['name', 'InertiaLOW', 'InertiaHI']], on='name', how='left') \
                .set_index(['model', 'name', 'timestamp'])

            #  As installed capacity is [Units] * [Max Capacity], we must calculate the unit capacity
            gen_inertia_lo = (gen_units_gen.value / gen_units.value) * (gen_cap.value * gen_cap.InertiaLOW)
            gen_inertia_hi = (gen_units_gen.value / gen_units.value) * (gen_cap.value * gen_cap.InertiaHI)

            gen_inertia = pd.merge(pd.DataFrame(gen_inertia_lo, columns=['InertiaLo']),
                                   pd.DataFrame(gen_inertia_hi, columns=['InertiaHi']),
                                   left_index=True,
                                   right_index=True)

            gen_inertia = pd.merge(gen_inertia.reset_index(),
                                   p.soln_idx[
                                       ['name', 'Island', 'Region', 'Subregion', 'Category', 'CapacityCategory']],
                                   on='name')

            self._gen_inertia = gen_inertia

        return self._gen_inertia


variables = _Variables()
