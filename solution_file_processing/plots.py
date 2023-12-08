"""
TODO Docstring
"""

import os

import pandas as pd
import numpy as np

from .utils.utils import catch_errors
from .utils.write_excel import write_xlsx_column, write_xlsx_stack, STACK_PALETTE
from .constants import VRE_TECHS
from .timeseries import create_output_11 as create_ts_output_11
from .timeseries import create_output_4 as create_timeseries_output_4
from . import log

print = log.info


def _get_plot_1_variables(c):
    # -----
    # Get: reg_ids

    load_by_reg = c.o.node_yr_df[c.o.node_yr_df.property == 'Load'] \
        .groupby(['model', 'timestamp'] + c.GEO_COLS) \
        .agg({'value': 'sum'})

    # Get gen_by_tech_reg
    gen_by_tech_reg = c.o.gen_yr_df[c.o.gen_yr_df.property == 'Generation']
    gen_techs = c.o.gen_yr_df.Category.drop_duplicates().values
    if 'Cofiring' in gen_techs:
        bio_ratio = 0.1
        gen_by_cofiring_bio = gen_by_tech_reg[gen_by_tech_reg.Category == 'Cofiring']
        gen_by_cofiring_coal = gen_by_tech_reg[gen_by_tech_reg.Category == 'Cofiring']
        gen_by_tech_reg = gen_by_tech_reg[gen_by_tech_reg.Category != 'Cofiring']

        gen_by_cofiring_bio.loc[:, 'value'] = gen_by_cofiring_bio.value * bio_ratio
        gen_by_cofiring_bio = gen_by_cofiring_bio.replace('Cofiring', 'Bioenergy')

        gen_by_cofiring_coal.loc[:, 'value'] = gen_by_cofiring_coal.value * (1 - bio_ratio)
        gen_by_cofiring_coal = gen_by_cofiring_coal.replace('Cofiring', 'Coal')

        gen_by_tech_reg = pd.concat([gen_by_tech_reg, gen_by_cofiring_bio, gen_by_cofiring_coal], axis=0)

    gen_by_tech_reg = (gen_by_tech_reg
                       .groupby(['model'] + c.GEO_COLS + ['Category'])
                       .agg({'value': 'sum'})
                       .unstack(level=c.GEO_COLS)
                       .fillna(0))

    reg_ids = list(np.unique(np.append(
        load_by_reg.unstack(c.GEO_COLS).droplevel(level=[region for region in c.GEO_COLS if region != 'Region'],
                                                  axis=1).replace(0,
                                                                  np.nan).dropna(
            how='all', axis=1).columns,
        gen_by_tech_reg.droplevel(level=[region for region in c.GEO_COLS if region != 'Region'], axis=1).replace(0,
                                                                                                                 np.nan).dropna(
            how='all', axis=1).columns)))
    # Make reg_ids flat list ('value', 'Region') -> 'Region'
    reg_ids = [x[1] for x in reg_ids]

    # -----
    # Get: doi_summary
    if not os.path.exists(os.path.join(c.DIR_05_2_TS_OUT, '11a_days_of_interest_summary.csv')):
        create_ts_output_11(c)
    doi_summary = pd.read_csv(os.path.join(c.DIR_05_2_TS_OUT, '11a_days_of_interest_summary.csv'),
                              index_col=0,
                              parse_dates=True)

    # -----
    # Get: use_reg_ts

    use_reg_ts = c.o.node_df[c.o.node_df.property == 'Unserved Energy'].groupby(
        ['model'] + c.GEO_COLS + ['timestamp']).agg({'value': 'sum'}).compute().unstack(
        level=c.GEO_COLS)

    # -----
    # Get: gen_stack_by_reg

    # Get needed variables
    load_by_reg_ts = c.o.node_df[c.o.node_df.property == 'Load'].groupby(
        ['model'] + c.GEO_COLS + ['timestamp']).sum().value.compute().unstack(
        level='timestamp').fillna(0).stack('timestamp')

    pumpload_reg_ts = (c.o.node_df[(c.o.node_df.property == 'Pump Load') |
                                   (c.o.node_df.property == 'Battery Load')]
                       .groupby(['model'] + c.GEO_COLS + ['timestamp'])
                       .sum().value.rename('Storage Load')
                       .compute())
    underlying_load_reg = (load_by_reg_ts - pumpload_reg_ts).rename('Underlying Load')
    net_load_reg_sto_ts = (c.v.net_load_reg_ts.stack(c.GEO_COLS).reorder_levels(
        ['model'] + c.GEO_COLS + ['timestamp']) + pumpload_reg_ts).rename('Net Load')

    # Get vre_curtailed_reg_ts
    ### Define model regs in multi-level format
    model_regs_multi = load_by_reg_ts.unstack(c.GEO_COLS).columns
    model_regs_multi = pd.MultiIndex.from_tuples([[i for i in x if i != 'value'] for x in model_regs_multi])

    vre_av_reg_abs_ts = (c.o.gen_df[(c.o.gen_df.property == 'Available Capacity') &
                                    (c.o.gen_df.Category.isin(VRE_TECHS))]
                         .groupby((['model'] + c.GEO_COLS + ['timestamp']))
                         .sum()
                         .value
                         .compute()
                         .unstack(level=c.GEO_COLS)
                         .fillna(0))

    vre_gen_reg_abs_ts = (c.o.gen_df[(c.o.gen_df.property == 'Generation') &
                                     (c.o.gen_df.Category.isin(VRE_TECHS))]
                          .groupby((['model'] + c.GEO_COLS + ['timestamp']))
                          .sum()
                          .value
                          .compute()
                          .unstack(level=c.GEO_COLS)
                          .fillna(0))

    vre_regs = vre_av_reg_abs_ts.columns

    ## Fill in data for regions which have no VRE (i.e. zero arrays!) to allow similar arrays for load_ts and vre_av_ts
    for reg in list(model_regs_multi):
        if reg not in vre_regs:
            vre_av_reg_abs_ts.loc[:, reg] = 0
            vre_gen_reg_abs_ts.loc[:, reg] = 0

    ### Columns in alphabetical order
    vre_av_reg_abs_ts = vre_av_reg_abs_ts[model_regs_multi]
    vre_gen_reg_abs_ts = vre_gen_reg_abs_ts[model_regs_multi]
    vre_curtailed_reg_ts = vre_av_reg_abs_ts - vre_gen_reg_abs_ts

    gen_stack_by_reg = pd.concat([c.v.gen_by_tech_reg_ts.fillna(0).droplevel(0, axis=1),
                                  net_load_reg_sto_ts,
                                  underlying_load_reg,
                                  pumpload_reg_ts,
                                  load_by_reg_ts.rename('Total Load'),
                                  load_by_reg_ts.rename('Load2'),
                                  vre_curtailed_reg_ts.stack(c.GEO_COLS)
                                 .reorder_levels(['model'] + c.GEO_COLS + ['timestamp'])
                                 .rename('Curtailment'),
                                  use_reg_ts.stack(c.GEO_COLS)
                                 .reorder_levels(['model'] + c.GEO_COLS + ['timestamp'])
                                 .rename(columns={'value': 'Unserved Energy'}),
                                  ], axis=1)

     # Add a total column for full aggregation for generation stacks at national/regional level
    gen_stack_total = gen_stack_by_reg.groupby(['model', 'timestamp']).sum().reset_index()
    gen_stack_total.loc[:, c.GEO_COLS] = 'Overall'
    gen_stack_total = gen_stack_total.set_index(['model'] + c.GEO_COLS + ['timestamp'])
    gen_stack_by_reg = pd.concat([gen_stack_by_reg, gen_stack_total], axis=0).groupby(['model'] + c.GEO_COLS + ['timestamp']).sum() 
    
    # Add summary region here too
    reg_ids = reg_ids + ['Overall']

    return reg_ids, doi_summary, use_reg_ts, gen_stack_by_reg


def _get_plot_2_variables(c):
    """
    Function to get all variables for the annual summary plots (aka PLOT 2 for now)

    customer_load_by_reg
    customer_load_reg_ts
    net_load_reg_ts
    line_cap_reg
    line_imp_exp_reg
    use_by_reg
    gen_by_tech_reg
    load_by_reg
    gen_cap_tech_reg
    cf_tech
    vre_by_reg
    vre_av_reg_abs_ts
    re_by_reg
    curtailment_rate
    re_curtailment_rate_by_tech
    fuel_by_type
    co2_by_tech_reg
    co2_by_reg
    gen_op_costs_by_reg
    gen_op_and_vio_costs_reg
    gen_total_costs_by_reg
    # gen_total_costs_by_reg_2030
    lcoe_tech
    ramp_reg_ts
    daily_pk_reg_ts
    ramp_pc_ts
    th_ramp_reg_ts
    th_ramp_pc_ts
    ramp_ts
    nldc_orig
    nldc

    """

    # load_by_reg = c.o.node_yr_df[c.o.node_yr_df.property == 'Load'] \
    #     .groupby(['model', 'timestamp'] + c.GEO_COLS) \
    #     .agg({'value': 'sum'})

    # # Get gen_by_tech_reg
    # gen_by_tech_reg = c.o.gen_yr_df[c.o.gen_yr_df.property == 'Generation']
    # gen_techs = c.o.gen_yr_df.Category.drop_duplicates().values

    # underlying_load_reg = (load_by_reg_ts - pumpload_reg_ts).rename('Underlying Load')
    # net_load_reg_sto_ts = (c.v.net_load_reg_ts.stack(c.GEO_COLS).reorder_levels(
    #     ['model'] + c.GEO_COLS + ['timestamp']) + pumpload_reg

    return reg_ids, doi_summary, use_reg_ts, gen_stack_by_reg


@catch_errors
def create_plot_1a(c):
    """
    Plot 1: Generation stacks for national days of interest
    # Todo works but has two bugs: Wrong index, Aggregation for full country is missing
    """

    print('Creating plot 1a...')
    reg_ids, doi_summary, use_reg_ts, gen_stack_by_reg = _get_plot_1_variables(c)

    model_regs = reg_ids # + ["JVB", "SUM", "IDN"] # todo, This is the reason for missing aggregation, needs generalization

    doi_periods = [doi for doi in doi_summary.index if "time" in doi]
    doi_names = [doi for doi in doi_summary.index if "time" not in doi]

    for j, p in enumerate(doi_periods):
        doi = doi_summary.loc[p]
        doi_name = doi_names[j]

        for m in c.v.model_names:
            if os.path.exists(os.path.join(c.DIR_05_3_PLOTS, m)) is False:
                os.mkdir(os.path.join(c.DIR_05_3_PLOTS, m))

            gen_stack = gen_stack_by_reg.loc[pd.IndexSlice[m, :, :], :]
            toi = pd.to_datetime(doi.loc[m])

            gen_stack_doi = gen_stack.reset_index()
            gen_stack_doi = gen_stack_doi.loc[
                (gen_stack_doi.timestamp.dt.date >= toi.date() - pd.Timedelta("3D"))
                & (gen_stack_doi.timestamp.dt.date <= toi.date() + pd.Timedelta("3D"))
                ]

            gen_stack_doi = gen_stack_doi.set_index(["model", c.GEO_COLS[0], "timestamp"])
            
        
            # gen_stack_doi = gen_stack_doi_reg.groupby(['model', 'timestamp'], as_index=False).sum()
            #         shutil.copyfile(fig_template_path, fig_path)

            file_path = os.path.join(m, f'plot1a_stack_ntl_doi_{doi_name}.xlsx')
            with pd.ExcelWriter(os.path.join(c.DIR_05_3_PLOTS, file_path),
                                engine="xlsxwriter") as writer:
                # ExcelWriter for some reason uses writer.sheets to access the sheet.
                # If you leave it empty it will not know that sheet Main is already there
                # and will create a new sheet.

                for reg in model_regs:
                    try:
                        gen_stack_doi_reg = gen_stack_doi.loc[pd.IndexSlice[:, reg, :], :].droplevel([0, 1])
                    except KeyError:
                        print(f'Cannot find {reg} in second level of index. Skipping. (Example index: '
                              f'{gen_stack_doi.index[0]})')
                        continue

                    gen_stack_doi_reg = gen_stack_doi_reg.drop(columns=['Island', 'Subregion'], errors='ignore')
                    write_xlsx_stack(df=gen_stack_doi_reg,
                                     writer=writer,
                                     sheet_name=reg,
                                     palette=STACK_PALETTE)
                    print(f'Created sheet "{reg}" in {file_path}.')

            return gen_stack_doi


def create_plot_1b(c):
    """
    Plot 1b: Generation stacks for national days of interest a specified reference model
    # Todo works but has two bugs: Wrong index, Aggregation for full country is missing
    """

    reg_ids, doi_summary, use_reg_ts, gen_stack_by_reg = _get_plot_1_variables(c)

    model_regs = reg_ids # + ["JVB", "SUM", "IDN"] # todo, This is the reason for missing aggregation, needs generalization

    doi_periods = [doi for doi in doi_summary.index if "time" in doi]
    doi_names = [doi for doi in doi_summary.index if "time" not in doi]

    ref_model = use_reg_ts.groupby('model').sum().idxmax().iloc[0]

    for i, p in enumerate(doi_periods):
        doi = doi_summary.loc[p]
        doi_name = doi_names[i]

        for m in c.v.model_names:
            save_dir_model = os.path.join(c.DIR_05_3_PLOTS, m)
            if os.path.exists(save_dir_model) is False:
                os.mkdir(save_dir_model)

            gen_stack = gen_stack_by_reg.loc[pd.IndexSlice[m, :, :], :]
            toi_ref = pd.to_datetime(doi.loc[ref_model])

            gen_stack_doi = gen_stack.reset_index()
            gen_stack_doi = gen_stack_doi.loc[
                (gen_stack_doi.timestamp.dt.date >= toi_ref.date() - pd.Timedelta("3D"))
                & (gen_stack_doi.timestamp.dt.date <= toi_ref.date() + pd.Timedelta("3D"))
                ]
            gen_stack_doi = gen_stack_doi.set_index(["model", "Region", "timestamp"])

            # gen_stack_doi = gen_stack_doi_reg.groupby(['model', 'timestamp'], as_index=False).sum()
            fig_path = os.path.join(
                save_dir_model, "plot1b_stack_ntl_ref_doi_{}.xlsx".format(doi_name)
            )

            with pd.ExcelWriter(fig_path, engine="xlsxwriter") as writer:
                ## ExcelWriter for some reason uses writer.sheets to access the sheet.
                ## If you leave it empty it will not know that sheet Main is already there
                ## and will create a new sheet.

                for reg in model_regs:
                    try:
                        gen_stack_doi_reg = gen_stack_doi.loc[pd.IndexSlice[:, reg, :], :].droplevel([0, 1])
                    except KeyError:
                        print(f'Cannot find {reg} in second level of index. Skipping. (Example index: '
                              f'{gen_stack_doi.index[0]})')
                        continue

                    gen_stack_doi_reg = gen_stack_doi_reg.drop(columns=['Island', 'Subregion'], errors='ignore')
                    write_xlsx_stack(
                        df=gen_stack_doi_reg,
                        writer=writer,
                        sheet_name=reg,
                        palette=STACK_PALETTE,
                    )


def get_plot_data(c):
    """
    # todo Not implemented at all, just copied from old jupyter notebook
    """
    plot_cols = {
        "load_by_reg":
            customer_load_by_reg.groupby(["model", "Region"]).sum().unstack(level="model") / 1000,
        "load_by_isl":
            customer_load_by_reg.groupby(["model", "Island"]).sum().unstack(level="model") / 1000,
        # 'pk_load_by_reg': load_by_reg_ts.groupby(['model', 'Region']).max().unstack(level='model'),
        "pk_load_by_isl": customer_load_reg_ts.groupby(geo_cols[0], axis=1).sum().stack(geo_cols[0]).groupby(
            ["model", geo_cols[0]]).max().unstack(level="model") / 1000,
        # 'pk_netload_by_reg': net_load_reg_ts.stack(geo_cols).groupby(['model', 'Region']).max().unstack(leel='model'),
        "pk_netload_by_isl": net_load_reg_ts.groupby(geo_cols[0], axis=1)
                             .sum()
                             .stack(geo_cols[0])
                             .groupby(["model", geo_cols[0]])
                             .max()
                             .unstack(level="model")
                             / 1000,
        "line_cap_isl": line_cap_isl["Export Limit"].rename("value").unstack(level="line")
                        / 1000,
        "line_net_exports_isl": (
                                        line_imp_exp_isl["Flow"] - line_imp_exp_isl["Flow Back"]
                                ).unstack("line")
                                / 1000,
        "line_exports_isl": (line_imp_exp_isl["Flow"]).unstack("line") / 1000,
        "line_imports_isl": (line_imp_exp_isl["Flow Back"]).unstack("line") / 1000,
        #              'use_by_reg': use_by_reg.groupby(['model','Region']).sum().unstack(level='Region'),
        "use_by_isl": use_by_reg.groupby(["model", "Island"]).sum().unstack(level="Island")
                      / 1000,
        "gen_by_tech": gen_by_tech_reg.stack(geo_cols)
                       .groupby(["model", "Category"])
                       .sum()
                       .unstack(level="Category")
                       / 1000,
        "gen_by_tech": gen_by_tech_reg.stack(geo_cols)
                       .groupby(["model", "Category"])
                       .sum()
                       .unstack(level="Category")
                       / 1000,
        "gen_by_WEOtech": gen_by_weoTech_reg.groupby(["model", "WEO_Tech_simpl"])
                          .sum()
                          .sum(axis=1)
                          .unstack(level="WEO_Tech_simpl")
                          / 1000,
        #              'gen_by_reg': gen_by_tech_reg.stack(geo_cols).groupby(['model', 'Region']).sum().unstack(level='Region'),
        "gen_by_isl": gen_by_tech_reg.stack(geo_cols)
                      .groupby(["model", "Island"])
                      .sum()
                      .unstack(level="Island")
                      / 1000,
        "net_gen_by_isl": gen_by_tech_reg.stack(geo_cols)
                          .groupby(["model", "Island"])
                          .sum()
                          .unstack(level="Island")
                          .fillna(0)
                          / 1000
                          - load_by_reg.groupby(["model", "Island"]).sum().unstack(level="Island") / 1000,
        "gen_cap_by_isl": gen_cap_tech_reg.stack(geo_cols)
                          .groupby(["model", "Island"])
                          .sum()
                          .unstack(level="Island")
                          / 1000,
        "gen_cap_by_tech": gen_cap_tech_reg.stack(geo_cols)
                           .groupby(["model", "Category"])
                           .sum()
                           .unstack(level="Category")
                           / 1000,
        "gen_cap_by_WEOtech": gen_cap_by_weoTech_reg.groupby(["model", "WEO_Tech_simpl"])
                              .sum()
                              .sum(axis=1)
                              .unstack(level="WEO_Tech_simpl")
                              / 1000,
        "cf_tech": cf_tech,
        "cf_tech_transposed": cf_tech.T,
        "vre_by_isl_byGen": vre_by_isl,
        "vre_by_isl_byAv": vre_av_reg_abs_ts.groupby("model")
                           .sum()
                           .groupby(geo_cols[0], axis=1)
                           .sum()
                           / 1000
                           / gen_by_tech_reg.groupby("model").sum().groupby(geo_cols[0], axis=1).sum(),
        "re_by_isl": re_by_isl,
        "curtailment_rate": curtailment_rate / 100,
        "re_curtailed_by_tech": re_curtailment_rate_by_tech,
        "fuels_by_type": fuel_by_type.groupby(["model", "Type"])
        .sum()
        .unstack("Type")
        .replace(0, np.nan)
        .dropna(axis=1, how="all")
        .fillna(0),
        #              'fuels_by_subtype': fuel_by_type.groupby(['model', 'Category']).sum().unstack('Category').replace(0,np.nan).dropna(axis=1,how="all").fillna(0),
        "co2_by_tech": co2_by_tech_reg.groupby(["model", "Category"])
                       .sum()
                       .unstack(level="Category")
                       / 1e6,
        "co2_by_fuels": co2_fuels_by_reg.groupby(["model", "Type"]).sum().unstack("Type")
                        / 1e6,
        #              'co2_by_subfuels': co2_fuels_by_reg.groupby(['model', 'Category']).sum().unstack('Category')/1e6,
        #              'co2_by_reg': co2_by_tech_reg.groupby(['model', 'Region']).sum().unstack(level='Region')/1e6,
        "co2_by_isl": co2_by_tech_reg.groupby(["model", "Island"])
                      .sum()
                      .unstack(level="Island")
                      / 1e6,
        #              'co2_intensity_reg': co2_by_reg.unstack(geo_cols).groupby('Region', axis=1).sum()/gen_by_tech_reg.groupby('model').sum().groupby('Region',axis=1).sum(),
        "co2_intensity_isl": co2_by_reg.unstack(geo_cols).groupby("Island", axis=1).sum()
                             / gen_by_tech_reg.groupby("model").sum().groupby("Island", axis=1).sum(),
        #              'gen_cap_by_reg': gen_cap_tech_reg.stack(geo_cols).groupby(['model', 'Region']).sum().unstack(level='Region'),
        #              'op_costs_by_tech' : gen_op_costs_by_reg.groupby(['model', 'Category']).sum().unstack(level='Category'),
        "op_costs_by_prop": gen_op_costs_by_reg.groupby(["model", "property"])
        .sum()
        .unstack(level="property"),
        "op_and_vio_costs_by_prop": gen_op_and_vio_costs_reg.groupby(["model", "property"])
        .sum()
        .unstack(level="property"),
        #              'tsc_by_tech' : gen_total_costs_by_reg.groupby(['model', 'Category']).sum().unstack(level='Category'),
        "tsc_by_prop": gen_total_costs_by_reg.groupby(["model", "property"])
        .sum()
        .unstack(level="property"),
        #              'lcoe_by_tech' : lcoe_tech.unstack(level='Category'),
        #              'lcoe_by_tech_T' : lcoe_tech.unstack(level='model'),
        "ramp_pc_by_isl": pd.concat(
            [
                (
                        ramp_reg_ts.groupby(["model", geo_cols[0], "timestamp"]).sum()
                        / daily_pk_reg_ts.stack(geo_cols)
                        .groupby(["model", geo_cols[0], "timestamp"])
                        .sum()
                )
                .groupby(["model", geo_cols[0]])
                .max()
                .unstack(level=geo_cols[0])
                * 100,
                ramp_pc_ts.groupby(["model"]).max().rename("IDN"),
            ],
            axis=1,
        ),
        "th_ramp_pc_by_isl": pd.concat(
            [
                (
                        th_ramp_reg_ts.groupby(["model", geo_cols[0], "timestamp"]).sum()
                        / daily_pk_reg_ts.stack(geo_cols)
                        .groupby(["model", geo_cols[0], "timestamp"])
                        .sum()
                )
                .groupby(["model", geo_cols[0]])
                .max()
                .unstack(level=geo_cols[0])
                * 100,
                th_ramp_pc_ts.groupby(["model"]).max().rename("IDN"),
            ],
            axis=1,
        ),
        "ramp_by_isl": pd.concat(
            [
                ramp_reg_ts.unstack(geo_cols)
                .groupby(level=geo_cols[0], axis=1)
                .sum()
                .groupby(["model"])
                .max(),
                ramp_ts.groupby(["model"]).max().value.rename("IDN"),
            ],
            axis=1,
        ),
        #              'th_ramp_by_isl' :pd.concat([th_ramp_reg_ts.unstack(geo_cols).groupby(level=geo_cols[0],axis=1).sum().groupby(['model']).max(), th_ramp_ts.groupby(['model']).max().value.rename('IDN')], axis=1)
        "dsm_pk_contr": (nldc_orig.iloc[:100, :] - nldc.iloc[:100, :])
        .mean()
        .rename("value")
        .to_frame(),
    }

    ###Java-Bali and Sumatra only
    incl_regs = ["JVB", "SUM"]

    plot_cols_JVBSUMonly = {
        "load_by_isl": customer_load_by_reg.loc[ix[:, :, incl_regs, :]]
                       .groupby(["model", "Island"])
                       .sum()
                       .unstack(level="model")
                       / 1000,
        #              'pk_load_by_reg': load_by_reg_ts.groupby(['model', 'Region']).max().unstack(level='model'),
        "pk_load_by_isl": customer_load_reg_ts.loc[:, ix[incl_regs,]]
                          .groupby(geo_cols[0], axis=1)
                          .sum()
                          .stack(geo_cols[0])
                          .groupby(["model", geo_cols[0]])
                          .max()
                          .unstack(level="model")
                          / 1000,
        #              'pk_netload_by_reg': net_load_reg_ts.stack(geo_cols).groupby(['model', 'Region']).max().unstack(leel='model'),
        "pk_netload_by_isl": net_load_reg_ts.loc[:, ix[incl_regs,]]
                             .groupby(geo_cols[0], axis=1)
                             .sum()
                             .stack(geo_cols[0])
                             .groupby(["model", geo_cols[0]])
                             .max()
                             .unstack(level="model")
                             / 1000,
        #              'use_by_reg': use_by_reg.groupby(['model','Region']).sum().unstack(level='Region'),
        "use_by_isl": use_by_reg.loc[ix[:, incl_regs, :]]
                      .groupby(["model", "Island"])
                      .sum()
                      .unstack(level="Island")
                      / 1000,
        "gen_by_tech": gen_by_tech_reg.stack(geo_cols)
                       .loc[ix[:, :, incl_regs, :]]
                       .groupby(["model", "Category"])
                       .sum()
                       .unstack(level="Category")
                       / 1000,
        "gen_by_tech": gen_by_tech_reg.stack(geo_cols)
                       .loc[ix[:, :, incl_regs, :]]
                       .groupby(["model", "Category"])
                       .sum()
                       .unstack(level="Category")
                       / 1000,
        "gen_by_isl": gen_by_tech_reg.stack(geo_cols)
                      .loc[ix[:, :, incl_regs, :]]
                      .groupby(["model", "Island"])
                      .sum()
                      .unstack(level="Island")
                      / 1000,
        "net_gen_by_isl": gen_by_tech_reg.stack(geo_cols)
                          .loc[ix[:, :, incl_regs, :]]
                          .groupby(["model", "Island"])
                          .sum()
                          .unstack(level="Island")
                          .fillna(0)
                          / 1000
                          - load_by_reg.groupby(["model", "Island"]).sum().unstack(level="Island") / 1000,
        "gen_cap_by_isl": gen_cap_tech_reg.stack(geo_cols)
                          .loc[ix[:, :, incl_regs, :]]
                          .groupby(["model", "Island"])
                          .sum()
                          .unstack(level="Island")
                          / 1000,
        "gen_cap_by_tech": gen_cap_tech_reg.stack(geo_cols)
                           .loc[ix[:, :, incl_regs, :]]
                           .groupby(["model", "Category"])
                           .sum()
                           .unstack(level="Category")
                           / 1000,
        "cf_tech": cf_tech_JVBSUMonly,
        "cf_tech_transposed": cf_tech_JVBSUMonly.T,
        "vre_by_isl_byGen": vre_by_isl_JVBSUMonly,
        "vre_by_isl_byAv": vre_av_reg_abs_ts.loc[:, ix[incl_regs,]]
                           .groupby("model")
                           .sum()
                           .groupby(geo_cols[0], axis=1)
                           .sum()
                           / 1000
                           / gen_by_tech_reg.loc[:, ix[incl_regs,]]
                           .groupby("model")
                           .sum()
                           .groupby(geo_cols[0], axis=1)
                           .sum(),
        "re_by_isl": re_by_isl_JVBSUMonly,
        "curtailment_rate": curtailment_rate_JVBSUMonly / 100,
        "op_costs_by_prop": gen_op_costs_by_reg.loc[
            ix[
            :,
            incl_regs,
            ]
        ]
        .groupby(["model", "property"])
        .sum()
        .unstack(level="property"),
        "op_and_vio_costs_by_prop": gen_op_and_vio_costs_reg.loc[
            ix[
            :,
            incl_regs,
            ]
        ]
        .groupby(["model", "property"])
        .sum()
        .unstack(level="property"),
        #              'tsc_by_tech' : gen_total_costs_by_reg.groupby(['model', 'Category']).sum().unstack(level='Category'),
        "tsc_by_prop": gen_total_costs_by_reg.loc[
            ix[
            :,
            incl_regs,
            ]
        ]
        .groupby(["model", "property"])
        .sum()
        .unstack(level="property"),
        #              'lcoe_by_tech' : lcoe_tech.unstack(level='Category'),
        #              'lcoe_by_tech_T' : lcoe_tech.unstack(level='model'),
    }

    plot_type = {
        "load_by_reg": "clustered",
        "load_by_isl": "clustered",
        "pk_load_by_reg": "clustered",
        "pk_load_by_isl": "clustered",
        "pk_netload_by_reg": "clustered",
        "pk_netload_by_isl": "clustered",
        "line_cap_isl": "clustered",
        "line_net_exports_isl": "clustered",
        "line_exports_isl": "clustered",
        "line_imports_isl": "clustered",
        "use_by_reg": "stacked",
        "use_by_isl": "stacked",
        "gen_by_tech": "stacked",
        "gen_by_WEOtech": "stacked",
        "gen_by_reg": "stacked",
        "gen_by_isl": "stacked",
        "net_gen_by_isl": "clustered",
        "vre_by_isl_byGen": "clustered",
        "vre_by_isl_byAv": "clustered",
        "re_by_isl": "clustered",
        "fuels_by_type": "stacked",
        "fuels_by_subtype": "stacked",
        "co2_by_tech": "stacked",
        "co2_by_fuels": "stacked",
        "co2_by_subfuels": "stacked",
        "co2_by_tech": "stacked",
        "co2_by_reg": "stacked",
        "co2_by_isl": "stacked",
        "co2_intensity_reg": "clustered",
        "co2_intensity_isl": "clustered",
        "curtailment_rate": "clustered",
        "re_curtailed_by_tech": "clustered",
        "gen_cap_by_reg": "stacked",
        "gen_cap_by_isl": "stacked",
        "gen_cap_by_tech": "stacked",
        "gen_cap_by_WEOtech": "stacked",
        "cf_tech": "clustered",
        "cf_tech_transposed": "clustered",
        "op_costs_by_tech": "stacked",
        "op_costs_by_prop": "stacked",
        "op_and_vio_costs_by_prop": "stacked",
        "tsc_by_tech": "stacked",
        "tsc_by_prop": "stacked",
        "lcoe_by_tech": "clustered",
        "lcoe_by_tech_T": "clustered",
        "ramp_pc_by_isl": "clustered",
        "th_ramp_pc_by_isl": "clustered",
        "ramp_by_isl": "clustered",
        "th_ramp_by_isl": "clustered",
        "dsm_pk_contr": "clustered",
    }

    plot_units = {
        "load_by_reg": "TWh",
        "load_by_isl": "TWh",
        "use_by_reg": "TWh",
        "use_by_isl": "TWh",
        "gen_by_tech": "TWh",
        "gen_by_WEOtech": "TWh",
        "gen_by_reg": "TWh",
        "gen_by_isl": "TWh",
        "net_gen_by_isl": "TWh",
        "vre_by_isl_byGen": "%",
        "vre_by_isl_byAv": "%",
        "re_by_isl": "%",
        "pk_load_by_reg": "GW",
        "pk_load_by_isl": "GW",
        "pk_netload_by_reg": "GW",
        "pk_netload_by_isl": "GW",
        "line_cap_isl": "GW",
        "line_net_exports_isl": "TWh",
        "line_exports_isl": "TWh",
        "line_imports_isl": "TWh",
        "fuels_by_type": "TJ",
        "fuels_by_subtype": "TJ",
        "co2_by_tech": "million tonnes",
        "co2_by_fuels": "million tonnes",
        "co2_by_subfuels": "million tonnes",
        "co2_by_tech": "million tonnes",
        "co2_by_reg": "million tonnes",
        "co2_by_isl": "million tonnes",
        "co2_intensity_reg": "kg/MWh",
        "co2_intensity_isl": "kg/MWh",
        "curtailment_rate": "%",
        "re_curtailed_by_tech": "%",
        "gen_cap_by_reg": "GW",
        "gen_cap_by_isl": "GW",
        "gen_cap_by_tech": "GW",
        "gen_cap_by_WEOtech": "GW",
        "cf_tech": "%",
        "cf_tech_transposed": "%",
        "op_costs_by_tech": "USDm",
        "op_costs_by_prop": "USDm",
        "op_and_vio_costs_by_prop": "USDm",
        "tsc_by_tech": "USDm",
        "tsc_by_prop": "USDm",
        "lcoe_by_tech": "USD/MWh",
        "lcoe_by_tech_T": "USD/MWh",
        "ramp_pc_by_isl": "%/hr",
        "th_ramp_pc_by_isl": "%/hr",
        "ramp_by_isl": "MW/hr",
        "th_ramp_by_isl": "MW/hr",
        "dsm_pk_contr": "GW",
    }

    return plot_cols, plot_type, plot_units, plot_cols_JVBSUMonly


def create_plot_2(c):

    

    """
    # todo Not implemented at all, just copied from old jupyter notebook
    ### Plot 2: Annual summary plots by columnn
    """
    fig_path = os.path.join(c.DIR_05_3_PLOTS, "plot2_annual_summary_plots.xlsx")

    plot_cols = {'load_by_reg': customer_load_by_reg.groupby(['model', geo_cols[0]]).sum().unstack(level=geo_cols[0])/1000,
             
             'pk_load_by_reg': pd.concat([ customer_load_reg_ts.stack(geo_cols).groupby(['model', geo_cols[0]]).max().unstack(level='model')/1000,
                                          customer_load_reg_ts.sum(axis=1).groupby(['model']).max().rename('UKR').to_frame().T/1000], axis=0),
             
             'pk_netload_by_reg': pd.concat([ net_load_reg_ts.stack(geo_cols).groupby(['model', geo_cols[0]]).max().unstack(level='model')/1000, 
                                   net_load_reg_ts.sum(axis=1).groupby(['model']).max().rename('UKR').to_frame().T/1000], axis=0),

             
#              'line_cap_isl': line_cap_isl['Export Limit'].rename('value').unstack(level = 'line')/1000,
#              'line_net_exports_isl': (line_imp_exp_isl['Flow'] - line_imp_exp_isl['Flow Back']).unstack('line')/1000,
#              'line_exports_isl': (line_imp_exp_isl['Flow']).unstack('line')/1000,
#              'line_imports_isl': (line_imp_exp_isl['Flow Back']).unstack('line')/1000,
             
             'use_by_reg': use_by_reg.groupby(['model',geo_cols[0]]).sum().unstack(level=geo_cols[0])/1000,
          
             'gen_by_tech': gen_by_tech_reg.stack(geo_cols).groupby(['model', 'Category']).sum().unstack(level='Category')/1000, 
             'gen_by_reg': gen_by_tech_reg.stack(geo_cols).groupby(['model', geo_cols[0]]).sum().unstack(level=geo_cols[0]), 

             'net_gen_by_reg': gen_by_tech_reg.stack(geo_cols).groupby(['model', geo_cols[0]]).sum().unstack(level=geo_cols[0]).fillna(0)/1000-load_by_reg.groupby(['model', geo_cols[0]]).sum().unstack(level=geo_cols[0])/1000,
             'gen_cap_by_reg': gen_cap_tech_reg.stack(geo_cols).groupby(['model', geo_cols[0]]).sum().unstack(level=geo_cols[0])/1000,
             'gen_cap_by_tech': gen_cap_tech_reg.stack(geo_cols).groupby(['model', 'Category']).sum().unstack(level='Category')/1000,  

             'cf_tech': cf_tech,
             'cf_tech_transposed': cf_tech.T,
             'vre_by_reg_byGen': vre_by_reg,
             'vre_by_reg_byAv': pd.concat([vre_av_reg_abs_ts.groupby('model').sum().groupby(geo_cols[0], axis=1).sum()/1000/gen_by_tech_reg.groupby('model').sum().groupby(geo_cols[0], axis=1).sum(), 
                                           (vre_av_reg_abs_ts.groupby('model').sum().sum(axis=1)/1000/gen_by_tech_reg.groupby('model').sum().sum(axis=1)).rename('UKR')],axis=1),
             're_by_reg': re_by_reg,
             
             'curtailment_rate': curtailment_rate/100,
             're_curtailed_by_tech': re_curtailment_rate_by_tech,
             'fuels_by_type': fuel_by_type.groupby(['model', 'Type']).sum().unstack('Type').replace(0,np.nan).dropna(axis=1,how="all").fillna(0),
             'fuels_by_subtype': fuel_by_type.groupby(['model', 'Category']).sum().unstack('Category').replace(0,np.nan).dropna(axis=1,how="all").fillna(0),
             
             'co2_by_tech': pd.concat([co2_by_tech_reg.groupby(['model', 'Category']).sum().unstack(level='Category')/1e6, co2_target_by_model],axis=1).fillna(0),
#              'co2_by_subfuels': co2_fuels_by_reg.groupby(['model', 'Category']).sum().unstack('Category')/1e6,
             'co2_by_reg': pd.concat([co2_by_tech_reg.groupby(['model', geo_cols[0]]).sum().unstack(level=geo_cols[0])/1e6, co2_target_by_model],axis=1).fillna(0),
#              'co2_by_isl': co2_by_tech_reg.groupby(['model', geo_cols[0]]).sum().unstack(level=geo_cols[0])/1e6,
             'co2_intensity_reg': co2_by_reg.unstack(geo_cols).groupby(geo_cols[0], axis=1).sum()/gen_by_tech_reg.groupby('model').sum().groupby(geo_cols[0],axis=1).sum(),
#              'co2_intensity_isl': co2_by_reg.unstack(geo_cols).groupby(geo_cols[0], axis=1).sum()/gen_by_tech_reg.groupby('model').sum().groupby(geo_cols[0],axis=1).sum(),             
             

             'op_costs_by_tech' : gen_op_costs_by_reg.groupby(['model', 'Category']).sum().unstack(level='Category'),
             'op_costs_by_prop' : gen_op_costs_by_reg.groupby(['model', 'property']).sum().unstack(level='property'),
             'op_and_vio_costs_by_prop': gen_op_and_vio_costs_reg.groupby(['model', 'property']).sum().unstack(level='property'),
             'tsc_by_tech' : gen_total_costs_by_reg.groupby(['model', 'Category']).sum().unstack(level='Category'),
             'tsc_by_prop' : gen_total_costs_by_reg.groupby(['model', 'property']).sum().unstack(level='property'),
             'tsc_by_tech_2030' : gen_total_costs_by_reg_2030.groupby(['model', 'Category']).sum().unstack(level='Category'),
             'tsc_by_prop_2030' : gen_total_costs_by_reg_2030.groupby(['model', 'property']).sum().unstack(level='property'),
             
             'lcoe_by_tech' : lcoe_tech.unstack(level='Category'),
             'lcoe_by_tech_T' : lcoe_tech.unstack(level='model'),
             'ramp_pc_by_reg' :pd.concat([(ramp_reg_ts.groupby(['model', geo_cols[0], 'timestamp']).sum()/daily_pk_reg_ts.stack(geo_cols).groupby(['model', geo_cols[0], 'timestamp']).sum()).groupby(['model', geo_cols[0]]).max().unstack(level=geo_cols[0])*100, 
                                          ramp_pc_ts.groupby(['model']).max().rename('UKR')], axis=1),
             'th_ramp_pc_by_reg' :pd.concat([(th_ramp_reg_ts.groupby(['model', geo_cols[0], 'timestamp']).sum()/daily_pk_reg_ts.stack(geo_cols).groupby(['model', geo_cols[0], 'timestamp']).sum()).groupby(['model', geo_cols[0]]).max().unstack(level=geo_cols[0])*100, 
                                          th_ramp_pc_ts.groupby(['model']).max().rename('UKR')], axis=1),
             'ramp_by_reg' : pd.concat([ramp_reg_ts.unstack(geo_cols).groupby(level=geo_cols[0],axis=1).sum().groupby(['model']).max(), ramp_ts.groupby(['model']).max().value.rename('UKR')], axis=1),
             
             'dsm_pk_contr': (nldc_orig.iloc[:100,:] - nldc.iloc[:100,:]).mean().rename('value').to_frame()
            }
 




    plot_type = {'load_by_reg': 'stacked',

                          
             'pk_load_by_reg':  'clustered',

             'pk_netload_by_reg': 'clustered',

             
#              'line_cap_isl': 'clustered',
#              'line_net_exports_isl': 'clustered',
#              'line_exports_isl': 'clustered',
#              'line_imports_isl': 'clustered',
             
             'use_by_reg': 'stacked',

             'gen_by_tech': 'stacked', 
             'gen_by_WEOtech': 'stacked', 
             'gen_by_reg': 'stacked', 
             'net_gen_by_reg': 'clustered', 
             'vre_by_reg_byGen': 'clustered',
             'vre_by_reg_byAv': 'clustered',
             're_by_reg': 'clustered',
             
             'fuels_by_type': 'stacked',
             'fuels_by_subtype': 'stacked',
             'co2_by_tech': 'stacked',
             'co2_by_fuels': 'stacked',
             'co2_by_subfuels': 'stacked',
             
             'co2_by_tech':'stacked',
             'co2_by_reg': 'stacked',

             'co2_intensity_reg': 'clustered',
    
             'curtailment_rate': 'clustered',
             're_curtailed_by_tech':'clustered',
             'gen_cap_by_reg': 'stacked',

             'gen_cap_by_tech': 'stacked',
             'gen_cap_by_WEOtech': 'stacked',
             'cf_tech': 'clustered',
             'cf_tech_transposed': 'clustered',
             'op_costs_by_tech' : 'stacked',
             'op_costs_by_prop' :'stacked',
             'op_and_vio_costs_by_prop' :'stacked',
             'tsc_by_tech' : 'stacked',
             'tsc_by_prop' : 'stacked',
             'tsc_by_tech_2030' : 'stacked',
             'tsc_by_prop_2030' : 'stacked',
             
             'lcoe_by_tech' : 'clustered',
             'lcoe_by_tech_T' : 'clustered',
             'ramp_pc_by_reg' :'clustered',
             'th_ramp_pc_by_reg' :'clustered',
             'ramp_by_reg' :'clustered',
             'th_ramp_by_reg' :'clustered',
             'dsm_pk_contr':'clustered'
            }
               


    plot_units = {'load_by_reg': 'TWh',

             'use_by_reg': 'TWh',

             'gen_by_tech': 'TWh', 
             'gen_by_WEOtech': 'TWh', 
             'gen_by_reg': 'TWh', 

             'net_gen_by_reg':'TWh',
             'vre_by_reg_byGen': '%',
             'vre_by_reg_byAv': '%',
             're_by_reg': '%',       
              
             'pk_load_by_reg':  'GW',

             'pk_netload_by_reg': 'GW',

              
             'fuels_by_type': 'TJ',
             'fuels_by_subtype': 'TJ',
             'co2_by_tech': 'million tonnes',
             'co2_by_fuels': 'million tonnes',
             'co2_by_subfuels': 'million tonnes',
              
             'co2_by_tech':'million tonnes',
             'co2_by_reg': 'million tonnes',

             'co2_intensity_reg': 'kg/MWh',

             'curtailment_rate': '%',
              're_curtailed_by_tech':'%',
             'gen_cap_by_reg': 'GW',

             'gen_cap_by_tech': 'GW',
             'gen_cap_by_WEOtech': 'GW',
             'cf_tech': '%',
             'cf_tech_transposed': '%',
             'op_costs_by_tech' : 'USDm',
             'op_costs_by_prop' :'USDm',
            'op_and_vio_costs_by_prop' :'USDm',
             'tsc_by_tech' : 'USDm',
             'tsc_by_prop' : 'USDm',
             'tsc_by_tech_2030' : 'USDm',
             'tsc_by_prop_2030' : 'USDm',
             'lcoe_by_tech' : 'USD/MWh',
             'lcoe_by_tech_T' : 'USD/MWh',
             'ramp_pc_by_reg' :'%/hr',
             'th_ramp_pc_by_reg' :'%/hr',
             'ramp_by_reg' :'MW/hr',
             'th_ramp_by_reg' :'MW/hr',
             'dsm_pk_contr': 'GW'}



    with pd.ExcelWriter(fig_path, engine="xlsxwriter") as writer:
        for i in plot_cols.keys():
            if plot_cols[i].shape[0] == 0:
                print("Empty dataframe for: {}".format(i))
            else:
                write_xlsx_column(
                    df=plot_cols[i],
                    writer=writer,
                    sheet_name=i,
                    subtype=plot_type[i],
                    units=plot_units[i],
                    palette=combined_palette,
                )

    fig_path = os.path.join(c.DIR_05_3_PLOTS, "plot2_annual_summary_plots_JVBSUMonly.xlsx")

    with pd.ExcelWriter(fig_path, engine="xlsxwriter") as writer:
        for i in plot_cols_JVBSUMonly.keys():
            if plot_cols_JVBSUMonly[i].shape[0] == 0:
                print("Empty dataframe for: {}".format(i))
            else:
                write_xlsx_column(
                    df=plot_cols_JVBSUMonly[i],
                    writer=writer,
                    sheet_name=i,
                    subtype=plot_type[i],
                    units=plot_units[i],
                    palette=combined_palette,
                )


def create_plot_3(c):
    """
    # todo Not implemented at all, just copied from old jupyter notebook
    Status: Could work, but can't be run because 03 year output is missing
    """
    ### Gen by tech/reg plots per model
    for ref_m in c.v.model_names:
        save_dir_model = os.path.join(save_dir_plots, ref_m)
        if os.path.exists(save_dir_model) is False:
            os.mkdir(save_dir_model)

        fig_path = os.path.join(
            save_dir_model, "plot3_gen_by_tech_reg_{}.xlsx".format(ref_m)
        )
        with pd.ExcelWriter(fig_path, engine="xlsxwriter") as writer:
            gen_tech_reg_m = (
                    gen_by_tech_reg.loc[pd.IndexSlice[ref_m, :]].groupby(c.GEO_COLS[0], axis=1).sum().T
                    / 1000
            )
            gen_cap_tech_reg_m = (
                    gen_cap_tech_reg.loc[pd.IndexSlice[ref_m, :]].groupby(c.GEO_COLS[0], axis=1).sum().T
                    / 1000
            )

            write_xlsx_column(
                df=gen_tech_reg_m,
                writer=writer,
                sheet_name="gen_tech_reg",
                subtype="stacked",
                units="TWh",
                palette=combined_palette,
            )
            write_xlsx_column(
                df=gen_cap_tech_reg_m,
                writer=writer,
                sheet_name="gen_cap_tech_reg",
                subtype="stacked",
                units="GW",
                palette=combined_palette,
            )


def create_plot_6(c):
    """
    # todo Could work, but can't be run because implementation of 04 ts output is missing
    """
    ### Plot 6: Cost savings plots by reference model

    ### Get rid of cofiring if any for the purpose of comparison
    if not os.path.exists(os.path.join(c.DIR_05_2_TS_OUT, '04a_gen_op_costs_reg.csv')):
        # todo implement this
        pass

    gen_op_costs_by_reg = pd.read_csv(os.path.join(c.DIR_05_2_TS_OUT, '04a_gen_op_costs_reg.csv'))
    gen_op_costs_by_tech = (
        gen_op_costs_by_reg.unstack("Category")
        .rename(columns={"Cofiring": "Coal"})
        .stack()
        .groupby(["model", "Category"])
        .sum()
        .unstack("model")
        .fillna(0)
    )
    gen_total_costs_by_reg = pd.read_csv(os.path.join(c.DIR_05_2_TS_OUT, '04c_gen_total_costs_reg.csv'))
    gen_total_costs_by_tech = (
        gen_total_costs_by_reg.unstack("Category")
        .rename(columns={"Cofiring": "Coal"})
        .stack()
        .groupby(["model", "Category"])
        .sum()
        .unstack("model")
        .fillna(0)
    )

    gen_op_costs_by_prop = (
        gen_op_costs_by_reg.groupby(["model", "property"]).sum().unstack("model").fillna(0)
    )
    gen_total_costs_by_prop = (
        gen_total_costs_by_reg.groupby(["model", "property"])
        .sum()
        .unstack("model")
        .fillna(0)
    )
    gen_op_and_vio_costs_reg = pd.read_csv(os.path.join(c.DIR_05_2_TS_OUT, '04b_gen_op_and_vio_costs_reg.csv'))
    gen_op_vio_costs_by_prop = (
        gen_op_and_vio_costs_reg.groupby(["model", "property"])
        .sum()
        .unstack("model")
        .fillna(0)
    )

    for ref_m in c.v.model_names:
        save_dir_model = os.path.join(c.DIR_05_3_PLOTS, ref_m)
        if os.path.exists(save_dir_model) is False:
            os.mkdir(save_dir_model)

        fig_path = os.path.join(
            save_dir_model, "plot6_cost_savings_ref_{}.xlsx".format(ref_m)
        )
        with pd.ExcelWriter(fig_path, engine="xlsxwriter") as writer:
            ref_op_prop = gen_op_costs_by_prop[ref_m]
            ref_op_vio_prop = gen_op_vio_costs_by_prop[ref_m]
            ref_tsc_prop = gen_total_costs_by_prop[ref_m]
            ref_op_tech = gen_op_costs_by_tech[ref_m]
            ref_tsc_tech = gen_total_costs_by_tech[ref_m]

            savings_op_by_prop = (
                (-gen_op_costs_by_prop).drop(columns=ref_m).subtract(-ref_op_prop, axis=0).T
            )
            savings_op_vio_by_prop = (
                (-gen_op_vio_costs_by_prop)
                .drop(columns=ref_m)
                .subtract(-ref_op_vio_prop, axis=0)
                .T
            )
            savings_tsc_by_prop = (
                (-gen_total_costs_by_prop)
                .drop(columns=ref_m)
                .subtract(-ref_tsc_prop, axis=0)
                .T
            )
            savings_op_by_tech = (
                (-gen_op_costs_by_tech).drop(columns=ref_m).subtract(-ref_op_tech, axis=0).T
            )
            savings_tsc_by_tech = (
                (-gen_total_costs_by_tech)
                .drop(columns=ref_m)
                .subtract(-ref_tsc_tech, axis=0)
                .T
            )

            savings_op_by_prop_pc = savings_op_by_prop / ref_op_prop.sum()
            savings_op_vio_by_prop_pc = savings_op_vio_by_prop / ref_op_vio_prop.sum()
            savings_tsc_by_prop_pc = savings_tsc_by_prop / ref_tsc_prop.sum()
            savings_op_by_tech_pc = savings_op_by_tech / ref_op_tech.sum()
            savings_tsc_by_tech_pc = savings_tsc_by_tech / ref_tsc_tech.sum()

            write_xlsx_column(
                df=savings_op_by_prop,
                writer=writer,
                sheet_name="savings_op_by_prop",
                subtype="stacked",
                units="USDm",
                total_scatter_col="Total savings",
            )
            write_xlsx_column(
                df=savings_op_vio_by_prop,
                writer=writer,
                sheet_name="savings_op_vio_by_prop",
                subtype="stacked",
                units="USDm",
                total_scatter_col="Total savings",
            )
            write_xlsx_column(
                df=savings_tsc_by_prop,
                writer=writer,
                sheet_name="savings_tsc_by_prop",
                subtype="stacked",
                units="USDm",
                total_scatter_col="Total savings",
            )
            write_xlsx_column(
                df=savings_op_by_tech,
                writer=writer,
                sheet_name="savings_op_by_tech",
                subtype="stacked",
                units="USDm",
                total_scatter_col="Total savings",
            )
            write_xlsx_column(
                df=savings_tsc_by_tech,
                writer=writer,
                sheet_name="savings_tsc_by_tech",
                subtype="stacked",
                units="USDm",
                total_scatter_col="Total savings",
            )
            write_xlsx_column(
                df=savings_op_by_prop_pc,
                writer=writer,
                sheet_name="savings_op_by_prop_pc",
                subtype="stacked",
                units="",
                total_scatter_col="Relative savings",
            )
            write_xlsx_column(
                df=savings_op_vio_by_prop_pc,
                writer=writer,
                sheet_name="savings_op_vio_by_prop_pc",
                subtype="stacked",
                units="",
                total_scatter_col="Relative savings",
            )
            write_xlsx_column(
                df=savings_tsc_by_prop_pc,
                writer=writer,
                sheet_name="savings_tsc_by_prop_pc",
                subtype="stacked",
                units="",
                total_scatter_col="Relative savings",
            )
            write_xlsx_column(
                df=savings_op_by_tech_pc,
                writer=writer,
                sheet_name="savings_op_by_tech_pc",
                subtype="stacked",
                units="",
                total_scatter_col="Relative savings",
            )
            write_xlsx_column(
                df=savings_tsc_by_tech_pc,
                writer=writer,
                sheet_name="savings_tsc_by_tech_pc",
                subtype="stacked",
                units="",
                total_scatter_col="Relative savings",
            )


def create_plot_7(c):
    """
    Status:
    Plot7: CO2 savings plots by reference model

    Creates following output files:
    - for each model in /{model}/:
        - plot7_co2_savings_ref_{model}.xlsx
    """
    print("Creating plot 7...")
    # Get rid of cofiring if any for the purpose of comparison
    co2_by_tech_reg = c.o.em_gen_yr_df[c.o.em_gen_yr_df.parent.str.contains('CO2') &
                                       (c.o.em_gen_yr_df.property == 'Production')] \
        .groupby(['model'] + c.GEO_COLS + ['Category']) \
        .agg({'value': 'sum'})
    co2_by_tech = (
            co2_by_tech_reg
            .unstack("Category")
            .rename(columns={"Cofiring": "Coal"})
            .stack()
            .groupby(["model", "Category"])
            .sum()
            .unstack("model")
            .fillna(0)
            .droplevel(0, axis=1)  # Drop not needed multiindex ('value' column)
            / 1e6
    )
    co2_by_reg_plt = (
            co2_by_tech_reg
            .groupby(["model", "Region"])
            .sum()
            .unstack("model")
            .fillna(0)
            .droplevel(0, axis=1)  # Drop not needed multiindex ('value' column)
            / 1e6
    )
    co2_by_isl_plt = (
            co2_by_tech_reg
            .groupby(["model", "Island"])
            .sum()
            .unstack("model")
            .fillna(0)
            .droplevel(0, axis=1)  # Drop not needed multiindex ('value' column)
            / 1e6
    )

    em_by_type_tech_reg = c.o.em_gen_yr_df[(c.o.em_gen_yr_df.property == 'Production')].groupby(
        ['model', 'parent'] + c.GEO_COLS + ['Category']).agg({'value': 'sum'}).reset_index()

    def get_parent(x):
        return x if '_' not in x else x.split('_')[0]

    em_by_type_tech_reg.parent = em_by_type_tech_reg.parent.apply(get_parent)

    em_by_type = (
            em_by_type_tech_reg.groupby(["model", "parent"]).sum().value.unstack("model") / 1e6
    )

    for ref_m in c.v.model_names:
        save_dir_model = os.path.join(c.DIR_05_3_PLOTS, ref_m)
        if os.path.exists(save_dir_model) is False:
            os.mkdir(save_dir_model)

        fig_path = os.path.join(
            save_dir_model, "plot7_co2_savings_ref_{}.xlsx".format(ref_m)
        )
        with pd.ExcelWriter(fig_path, engine="xlsxwriter") as writer:
            ref_co2_tech = co2_by_tech[ref_m]
            ref_co2_reg = co2_by_reg_plt[ref_m]
            ref_co2_isl = co2_by_isl_plt[ref_m]
            ref_em_type = em_by_type[ref_m]

            co2_savings_by_tech = (
                (-co2_by_tech).drop(columns=ref_m).subtract(-ref_co2_tech, axis=0).T
            )
            co2_savings_by_reg = (
                (-co2_by_reg_plt).drop(columns=ref_m).subtract(-ref_co2_reg, axis=0).T
            )
            co2_savings_by_isl = (
                (-co2_by_isl_plt).drop(columns=ref_m).subtract(-ref_co2_isl, axis=0).T
            )

            em_savings_by_reg = (
                (-em_by_type).drop(columns=ref_m).subtract(-ref_em_type, axis=0).T
            )

            co2_savings_by_tech_pc = (-co2_by_tech).drop(columns=ref_m).subtract(
                -ref_co2_tech, axis=0
            ).T / ref_co2_tech.sum()
            co2_savings_by_reg_pc = (-co2_by_reg_plt).drop(columns=ref_m).subtract(
                -ref_co2_reg, axis=0
            ).T / ref_co2_reg.sum()
            co2_savings_by_isl_pc = (-co2_by_isl_plt).drop(columns=ref_m).subtract(
                -ref_co2_isl, axis=0
            ).T / ref_co2_isl.sum()
            em_savings_by_reg_pc = (-em_by_type).drop(columns=ref_m).subtract(
                -ref_em_type, axis=0
            ).T / ref_em_type

            write_xlsx_column(
                df=co2_savings_by_tech,
                writer=writer,
                sheet_name="co2_savings_by_tech_abs",
                subtype="stacked",
                units="million tonnes",
                total_scatter_col="Total reduction",
            )
            write_xlsx_column(
                df=co2_savings_by_reg,
                writer=writer,
                sheet_name="co2_savings_by_reg_abs",
                subtype="stacked",
                units="million tonnes",
                total_scatter_col="Total reduction",
            )
            write_xlsx_column(
                df=co2_savings_by_isl,
                writer=writer,
                sheet_name="co2_savings_by_isl_abs",
                subtype="stacked",
                units="million tonnes",
                total_scatter_col="Total reduction",
            )

            write_xlsx_column(
                df=em_savings_by_reg,
                writer=writer,
                sheet_name="em_savings_by_type_abs",
                subtype="stacked",
                units="million tonnes",
                total_scatter_col="Total reduction",
            )

            write_xlsx_column(
                df=co2_savings_by_tech_pc,
                writer=writer,
                sheet_name="co2_savings_by_tech_pc",
                subtype="stacked",
                units="",
                total_scatter_col="Relative reduction",
            )
            write_xlsx_column(
                df=co2_savings_by_reg_pc,
                writer=writer,
                sheet_name="co2_savings_by_reg_pc",
                subtype="stacked",
                units="",
                total_scatter_col="Relative reduction",
            )
            write_xlsx_column(
                df=co2_savings_by_isl_pc,
                writer=writer,
                sheet_name="co2_savings_by_isl_pc",
                subtype="stacked",
                units="",
                total_scatter_col="Relative reduction",
            )

            write_xlsx_column(
                df=em_savings_by_reg_pc,
                writer=writer,
                sheet_name="em_savings_by_type_pc",
                subtype="clustered",
                units="",
            )
    print("Done.")
