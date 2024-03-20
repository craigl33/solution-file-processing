"""
TODO Docstring
"""

import os

import pandas as pd
import numpy as np

from .utils.utils import catch_errors
from .utils.write_excel import write_xlsx_column, write_xlsx_stack, STACK_PALETTE, IEA_PALETTE_16
# from .utils.write_excel import IEA_CMAP_14, IEA_CMAP_16, IEA_CMAP_D8, IEA_CMAP_L8
from .constants import VRE_TECHS
from .timeseries import create_output_11 as create_ts_output_11
# from .timeseries import create_output_4 as create_timeseries_output_4
from . import log

print = log.info





def _get_plot_1_variables(c):
    # -----
    # Get: reg_ids

    reg_ids = c.v.reg_ids

    # -----
    # Get: doi_summary
    if not os.path.exists(os.path.join(c.DIR_05_2_TS_OUT, '11a_days_of_interest_summary.csv')):
        create_ts_output_11(c)
    doi_summary = pd.read_csv(os.path.join(c.DIR_05_2_TS_OUT, '11a_days_of_interest_summary.csv'),
                              index_col=0,
                              parse_dates=True)

    # -----
    # Get: use_reg_ts

    # -----
    # Get: gen_stack_by_reg

    gen_stack_by_reg = c.v.gen_stack_by_reg

     # Add a total column for full aggregation for generation stacks at national/regional level
    gen_stack_total = gen_stack_by_reg.groupby(['model', 'timestamp']).sum()
    exports_total = c.v.exports_ts.where(c.v.exports_ts > 0).fillna(0)
    imports_total = c.v.exports_ts.where(c.v.exports_ts < 0).fillna(0).abs()
    gen_stack_total.loc[:,'Imports'] = imports_total.value.rename('Imports')
    gen_stack_total.loc[:,'Exports'] = exports_total.value.rename('Exports')

    gen_stack_total.loc[:,'Net Load w/ Exports'] = gen_stack_total['Net Load'].rename('Net Load w/ Exports') + exports_total.value.rename('Net Load w/ Exports')
    gen_stack_total = gen_stack_total.reset_index()
    
    gen_stack_total[c.GEO_COLS] = 'Overall'
    gen_stack_total = gen_stack_total.set_index(['model'] + c.GEO_COLS + ['timestamp'])
    gen_stack_by_reg = pd.concat([gen_stack_by_reg, gen_stack_total], axis=0).groupby(['model'] + c.GEO_COLS + ['timestamp']).sum()
    
    # Add summary region here too
    reg_ids = reg_ids + ['Overall']

    # probably to delete
    use_reg_ts = c.v.use_reg_ts

    return reg_ids, doi_summary, use_reg_ts, gen_stack_by_reg

def create_plot_1a(c):
    """
    Plot 1b: Generation stacks for national days of interest a specified reference model
    # Todo works but has two bugs: Wrong index, Aggregation for full country is missing
    """

    reg_ids, doi_summary, use_reg_ts, gen_stack_by_reg = _get_plot_1_variables(c)

    model_regs = reg_ids # + ["JVB", "SUM", "IDN"] # todo, This is the reason for missing aggregation, needs generalization

    doi_periods = [doi for doi in doi_summary.index if "time" in doi]
    doi_names = [doi for doi in doi_summary.index if "time" not in doi]

    for i, p in enumerate(doi_periods):
        doi = doi_summary.loc[p]
        doi_name = doi_names[i]

        for m in c.v.model_names:
            save_dir_model = os.path.join(c.DIR_05_3_PLOTS, m)
            if os.path.exists(save_dir_model) is False:
                os.mkdir(save_dir_model)

            gen_stack = gen_stack_by_reg.loc[pd.IndexSlice[m, :, :], :]
            toi_ref = pd.to_datetime(doi.loc[m])

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

            with pd.ExcelWriter(fig_path, engine="xlsxwriter") as writer: # pylint: disable=abstract-class-instantiated
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

            with pd.ExcelWriter(fig_path, engine="xlsxwriter") as writer: # pylint: disable=abstract-class-instantiated
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


def create_plot_1c(c, toi=None):
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
            gen_stack_doi = gen_stack.reset_index()
            yoi = gen_stack_doi.timestamp.dt.year.unique()[0]


            if toi != None:
                doi = pd.to_datetime(f"{toi}-{yoi}").date()
                gen_stack_doi = gen_stack_doi.loc[
                    (gen_stack_doi.timestamp.dt.date >= doi - pd.Timedelta("3D"))
                    & (gen_stack_doi.timestamp.dt.date <= doi + pd.Timedelta("3D"))
                    ]
            else:
                gen_stack_doi = gen_stack

            gen_stack_doi = gen_stack_doi.set_index(["model", c.GEO_COLS[0], "timestamp"])
            
        
            # gen_stack_doi = gen_stack_doi_reg.groupby(['model', 'timestamp'], as_index=False).sum()
            #         shutil.copyfile(fig_template_path, fig_path)

            if toi != None:
                fig_path = os.path.join(m, 'plot1c_stack_custom_date_{}.xlsx'.format(str(toi)))
            else:
                fig_path = os.path.join(m, 'plot1c_stack_entire_year.xlsx')

            with pd.ExcelWriter(fig_path, engine="xlsxwriter") as writer: # pylint: disable=abstract-class-instantiated
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


def create_plot_2(c):
    
    """
    ### Plot 2: Annual summary plots by columnn

    ### TODO: It would be a great idea to add some of the VRE phase classification metrics here too.
    """

    fig_path = os.path.join(c.DIR_05_3_PLOTS, "plot2_annual_summary_plots.xlsx")    

    plot_cols = {
        "load_by_reg": c.v.customer_load_by_reg.groupby(["model", c.GEO_COLS[0]]).sum().value.unstack(level="model") / 1000,
        "pk_load_by_reg": c.v.customer_load_reg_ts.groupby(c.GEO_COLS[0], axis=1).sum().stack(c.GEO_COLS[0]).groupby(
            ["model", c.GEO_COLS[0]]).max().unstack(level="model") / 1000,
        "pk_netload_by_reg": c.v.net_load_reg_ts.groupby(c.GEO_COLS[0], axis=1)
                             .sum()
                             .stack(c.GEO_COLS[0])
                             .groupby(["model", c.GEO_COLS[0]])
                             .max()
                             .unstack(level="model")
                             / 1000,
        "line_cap_reg": c.v.line_cap_reg.value.unstack(level="line")/ 1000,
        "line_net_exports_reg": ( c.v.line_imp_exp_reg["Flow"] - c.v.line_imp_exp_reg["Flow Back"]).unstack("line")/ 1000,
        "line_exports_reg": (c.v.line_imp_exp_reg["Flow"]).unstack("line") / 1000,
        "line_imports_reg": (c.v.line_imp_exp_reg["Flow Back"]).unstack("line") / 1000,
        #              'use_by_reg': use_by_reg.groupby(['model','Region']).sum().unstack(level='Region'),
        "use_by_reg": c.v.use_by_reg.groupby(level=['model', c.GEO_COLS[0]]).sum()/ 1000,
        "gen_by_tech": c.v.gen_by_tech_reg.stack(c.GEO_COLS)
                       .groupby(["model", "Category"])
                       .sum()
                       .unstack(level="Category")
                       / 1000,
        "gen_by_reg": c.v.gen_by_tech_reg.stack(c.GEO_COLS)
                      .groupby(["model", c.GEO_COLS[0]])
                      .sum()
                      .unstack(level=c.GEO_COLS[0])
                      / 1000,
        "net_gen_by_reg": c.v.gen_by_tech_reg.stack(c.GEO_COLS)
                          .groupby(["model", c.GEO_COLS[0]])
                          .sum()
                          .unstack(level=c.GEO_COLS[0])
                          .fillna(0)
                          / 1000
                          - c.v.load_by_reg.groupby(["model", c.GEO_COLS[0]]).sum().value.unstack(level=c.GEO_COLS[0]) / 1000,
        "gen_cap_by_reg": c.v.gen_cap_tech_reg.stack(c.GEO_COLS)
                          .groupby(["model", c.GEO_COLS[0]])
                          .sum()
                          .unstack(level=c.GEO_COLS[0])
                          / 1000,
        "gen_cap_by_tech": c.v.gen_cap_tech_reg.stack(c.GEO_COLS)
                           .groupby(["model", "Category"])
                           .sum()
                           .unstack(level="Category")
                           / 1000,
        "cf_tech": c.v.cf_tech,
        "cf_tech_transposed": c.v.cf_tech.T,
        "vre_by_reg_byGen": c.v.vre_by_reg,
        "vre_by_reg_byAv": c.v.vre_av_reg_abs_ts.groupby("model")
                           .sum()
                           .groupby(c.GEO_COLS[0], axis=1)
                           .sum()
                           / 1000,
        "re_by_reg": c.v.re_by_reg,
        "curtailment_rate": c.v.curtailment_rate / 100,
        "re_curtailed_by_tech": c.v.re_curtailment_rate_by_tech,
        ### fuels by type shouldnt be 
        "fuels_by_type": c.v.fuel_by_type.groupby(["model", c.GEO_COLS[0], "Type"])
                            .sum()
                            .value
                            .unstack(level="Type")
                            .fillna(0),
        #              'fuels_by_subtype': fuel_by_type.groupby(['model', 'Category']).sum().unstack('Category').replace(0,np.nan).dropna(axis=1,how="all").fillna(0),
        "co2_by_tech": c.v.co2_by_tech_reg.groupby(["model", "Category"])
                       .sum()
                       .unstack(level="Category")
                       / 1e6,
        "co2_by_fuels": c.v.co2_by_fuel_reg.groupby(["model", "Type"]).sum().unstack("Type")
                        / 1e6,
        "co2_by_reg": c.v.co2_by_tech_reg.groupby(["model", c.GEO_COLS[0]])
                      .sum()
                      .unstack(level=c.GEO_COLS[0])
                      / 1e6,
        "co2_intensity_reg": c.v.co2_by_reg.unstack(c.GEO_COLS).groupby(c.GEO_COLS[0], axis=1).sum()
                             / c.v.gen_by_tech_reg.groupby("model").sum().groupby(c.GEO_COLS[0], axis=1).sum(),

        "op_costs_by_prop": c.v.gen_op_costs_by_reg.groupby(["model", "property"]) \
                                    .sum()
                                    .unstack(level="property"),
        #'lcoe_by_tech' : lcoe_tech.unstack(level='Category'),
        #              'lcoe_by_tech_T' : lcoe_tech.unstack(level='model'),
        "ramp_pc_by_reg": pd.concat(
            [
                c.v.ramp_reg_pc_ts.groupby(['model',c.GEO_COLS[0]]).max().value.unstack(c.GEO_COLS[0]),
                c.v.ramp_pc_ts.groupby(["model"]).max().rename("Overall"),
            ],
            axis=1,
            ),
        "th_ramp_pc_by_reg": pd.concat(
            [
                c.v.th_ramp_reg_pc_ts.groupby(['model',c.GEO_COLS[0]]).max().value.unstack(c.GEO_COLS[0]),
                c.v.th_ramp_pc_ts.groupby(["model"]).max().rename("Overall")
            ],
            axis=1,
            ),

        "ramp_by_reg": pd.concat(
            [
                c.v.ramp_reg_ts.unstack(c.GEO_COLS)
                .groupby(level=c.GEO_COLS[0], axis=1)
                .sum()
                .groupby(["model"])
                .max(),
                c.v.ramp_ts.groupby(["model"]).max().value.rename("Overall"),
            ],
            axis=1,
        ),
        "th_ramp_by_reg": pd.concat(
            [
                c.v.th_ramp_reg_ts.unstack(c.GEO_COLS)
                .groupby(level=c.GEO_COLS[0], axis=1)
                .sum()
                .groupby(["model"])
                .max(),
                c.v.th_ramp_ts.groupby(["model"]).max().value.rename("Overall"),
            ],
            axis=1,
        ),
        #"dsm_pk_contr": (c.v.nldc_orig.iloc[:100, :] - c.v.nldc.iloc[:100, :])
 
    }

    plot_type = {
        "load_by_reg": "clustered",
        "pk_load_by_reg": "clustered",
        "pk_netload_by_reg": "clustered",
        "line_cap_reg": "clustered",
        "line_net_exports_reg": "clustered",
        "line_exports_reg": "clustered",
        "line_imports_reg": "clustered",
        "use_by_reg": "stacked",
        "gen_by_tech": "stacked",
        "gen_by_WEOtech": "stacked",
        "gen_by_reg": "stacked",
        "net_gen_by_reg": "clustered",
        "vre_by_reg_byGen": "clustered",
        "vre_by_reg_byAv": "clustered",
        "re_by_reg": "clustered",
        "fuels_by_type": "stacked",
        "fuels_by_subtype": "stacked",
        "co2_by_tech": "stacked",
        "co2_by_fuels": "stacked",
        "co2_by_subfuels": "stacked",
        "co2_by_reg": "stacked",
        "co2_intensity_reg": "clustered",
        "curtailment_rate": "clustered",
        "re_curtailed_by_tech": "clustered",
        "gen_cap_by_reg": "stacked",
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
        "ramp_pc_by_reg": "clustered",
        "th_ramp_pc_by_reg": "clustered",
        "ramp_by_reg": "clustered",
        "th_ramp_by_reg": "clustered",
        "dsm_pk_contr": "clustered",
    }

    plot_units = {
        "load_by_reg": "TWh",
        "use_by_reg": "TWh",
        "gen_by_tech": "TWh",
        "gen_by_WEOtech": "TWh",
        "gen_by_reg": "TWh",
        "net_gen_by_reg": "TWh",
        "vre_by_reg_byGen": "%",
        "vre_by_reg_byAv": "%",
        "re_by_reg": "%",
        "pk_load_by_reg": "GW",
        "pk_netload_by_reg": "GW",
        "line_cap_reg": "GW",
        "line_net_exports_reg": "TWh",
        "line_exports_reg": "TWh",
        "line_imports_reg": "TWh",
        "fuels_by_type": "TJ",
        "fuels_by_subtype": "TJ",
        "co2_by_tech": "million tonnes",
        "co2_by_fuels": "million tonnes",
        "co2_by_subfuels": "million tonnes",
        "co2_by_reg": "million tonnes",
        "co2_intensity_reg": "kg/MWh",
        "curtailment_rate": "%",
        "re_curtailed_by_tech": "%",
        "gen_cap_by_reg": "GW",
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
        "ramp_pc_by_reg": "%/hr",
        "th_ramp_pc_by_reg": "%/hr",
        "ramp_by_reg": "MW/hr",
        "th_ramp_by_reg": "MW/hr",
        "dsm_pk_contr": "GW",
    }

    # would be good to make palettes etc based on the regions for consistency purposes
    reg_ids = list(set(c.v.load_by_reg.reset_index()[c.GEO_COLS[0]].values))
    reg_palette = {reg_ids[i]: IEA_PALETTE_16[i] for i in range(len(reg_ids))}

    # Regions and technologies will always be consistent this way. May need to be copied to other parts of the code
    combined_palette = dict(STACK_PALETTE, **reg_palette)

    with pd.ExcelWriter(fig_path, engine="xlsxwriter") as writer: # pylint: disable=abstract-class-instantiated
        for i, df in plot_cols.items():
            if df.shape[0] == 0:
                print(f"Empty dataframe for: {i}")
            else:
                write_xlsx_column(
                    df=df,
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
        save_dir_model = os.path.join(c.DIR_05_3_PLOTS, ref_m)
        if os.path.exists(save_dir_model) is False:
            os.mkdir(save_dir_model)

        fig_path = os.path.join(
            save_dir_model, f"plot3_gen_by_tech_reg_{ref_m}.xlsx"
        )
        with pd.ExcelWriter(fig_path, engine="xlsxwriter") as writer: # pylint: disable=abstract-class-instantiated
            gen_tech_reg_m = (
                    c.v.gen_by_tech_reg.loc[pd.IndexSlice[ref_m, :]].groupby(c.GEO_COLS[0], axis=1).sum().T
                    / 1000
            )
            gen_cap_tech_reg_m = (
                    c.v.gen_cap_tech_reg.loc[pd.IndexSlice[ref_m, :]].groupby(c.GEO_COLS[0], axis=1).sum().T
                    / 1000
            )

            write_xlsx_column(
                df=gen_tech_reg_m,
                writer=writer,
                sheet_name="gen_tech_reg",
                subtype="stacked",
                units="TWh",
                palette=STACK_PALETTE,
            )
            write_xlsx_column(
                df=gen_cap_tech_reg_m,
                writer=writer,
                sheet_name="gen_cap_tech_reg",
                subtype="stacked",
                units="GW",
                palette=STACK_PALETTE,
            )


def create_plot_6(c):
    """
    # todo Could work, but can't be run because implementation of 04 ts output is missing
    """
    ### Plot 6: Cost savings plots by reference model

    ### Cofiring stuff that was built in is now removed


    gen_op_costs_by_tech = c.v.gen_op_costs_by_reg.groupby(["model", "Category"]) \
        .sum() \
        .unstack("model") \
        .fillna(0)
    
    gen_total_costs_by_reg = c.v.gen_total_costs_by_reg
    gen_total_costs_by_tech = gen_total_costs_by_reg.groupby(["model", "Category"]) \
        .sum() \
        .unstack("model") \
        .fillna(0)

    gen_op_costs_by_prop = (
        c.v.gen_op_costs_by_reg.groupby(["model", "property"]).sum().unstack("model").fillna(0)
    )
    gen_total_costs_by_prop = (
        c.v.gen_total_costs_by_reg.groupby(["model", "property"])
        .sum()
        .unstack("model")
        .fillna(0)
    )

    ### TODO: Implement gen_op_and_vio_costs_reg in the variables!
    # gen_op_and_vio_costs_reg = c.v.gen_op_and_vio_costs_reg
    # gen_op_vio_costs_by_prop = (
    #     gen_op_and_vio_costs_reg.groupby(["model", "property"])
    #     .sum()
    #     .unstack("model")
    #     .fillna(0)
    # )

    for ref_m in c.v.model_names:
        save_dir_model = os.path.join(c.DIR_05_3_PLOTS, ref_m)
        if os.path.exists(save_dir_model) is False:
            os.mkdir(save_dir_model)

        fig_path = os.path.join(
            save_dir_model, "plot6_cost_savings_ref_{}.xlsx".format(ref_m)
        )
        with pd.ExcelWriter(fig_path, engine="xlsxwriter") as writer: # pylint: disable=abstract-class-instantiated
            ref_op_prop = gen_op_costs_by_prop[ref_m]
            # ref_op_vio_prop = gen_op_vio_costs_by_prop[ref_m]
            ref_tsc_prop = gen_total_costs_by_prop[ref_m]
            ref_op_tech = gen_op_costs_by_tech[ref_m]
            ref_tsc_tech = gen_total_costs_by_tech[ref_m]

            savings_op_by_prop = (
                (-gen_op_costs_by_prop).drop(columns=ref_m).subtract(-ref_op_prop, axis=0).T
            )
            # savings_op_vio_by_prop = (
            #     (-gen_op_vio_costs_by_prop)
            #     .drop(columns=ref_m)
            #     .subtract(-ref_op_vio_prop, axis=0)
            #     .T
            # )
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
            # savings_op_vio_by_prop_pc = savings_op_vio_by_prop / ref_op_vio_prop.sum()
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
            # write_xlsx_column(
            #     df=savings_op_vio_by_prop,
            #     writer=writer,
            #     sheet_name="savings_op_vio_by_prop",
            #     subtype="stacked",
            #     units="USDm",
            #     total_scatter_col="Total savings",
            # )
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
            # write_xlsx_column(
            #     df=savings_op_vio_by_prop_pc,
            #     writer=writer,
            #     sheet_name="savings_op_vio_by_prop_pc",
            #     subtype="stacked",
            #     units="",
            #     total_scatter_col="Relative savings",
            # )
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
    co2_by_reg_plt = (
            co2_by_tech_reg
            .groupby(["model", c.GEO_COLS[0]])
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
        with pd.ExcelWriter(fig_path, engine="xlsxwriter") as writer: # pylint: disable=abstract-class-instantiated
            ref_co2_tech = co2_by_tech[ref_m]
            ref_co2_reg = co2_by_reg_plt[ref_m]
            ref_co2_reg = co2_by_reg_plt[ref_m]
            ref_em_type = em_by_type[ref_m]

            co2_savings_by_tech = (
                (-co2_by_tech).drop(columns=ref_m).subtract(-ref_co2_tech, axis=0).T
            )
            co2_savings_by_reg = (
                (-co2_by_reg_plt).drop(columns=ref_m).subtract(-ref_co2_reg, axis=0).T
            )
            co2_savings_by_reg = (
                (-co2_by_reg_plt).drop(columns=ref_m).subtract(-ref_co2_reg, axis=0).T
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
            co2_savings_by_reg_pc = (-co2_by_reg_plt).drop(columns=ref_m).subtract(
                -ref_co2_reg, axis=0
            ).T / ref_co2_reg.sum()
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
                df=co2_savings_by_reg,
                writer=writer,
                sheet_name="co2_savings_by_reg_abs",
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
                df=co2_savings_by_reg_pc,
                writer=writer,
                sheet_name="co2_savings_by_reg_pc",
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
