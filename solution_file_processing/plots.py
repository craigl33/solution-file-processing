"""
TODO Docstring
"""

import os

import pandas as pd
import numpy as np

from .utils.utils import catch_errors
from .utils.write_excel import STACK_PALETTE, IEA_PALETTE_16, EXTENDED_PALETTE, SERVICES_PALETTE
from .constants import VRE_TECHS, PRETTY_MODEL_NAMES, SERVICE_TECH_IDX
from .utils.write_excel import write_xlsx_column, write_xlsx_stack, write_xlsx_line, write_xlsx_scatter
# from .utils.write_excel import IEA_CMAP_14, IEA_CMAP_16, IEA_CMAP_D8, IEA_CMAP_L8

from .timeseries import create_output_11 as create_ts_output_11
# from .timeseries import create_output_4 as create_timeseries_output_4
from . import log

print = log.info


def _get_plot_2_variables(c, cols):
    """
    Function to access plot variables for plot 2 based on input columns
    
    """
    plot_cols, plot_units, plot_type, plot_desc = {col : getattr(c.p, col)[0] for col in cols}, {col : getattr(c.p, col)[1] for col in cols}, {col : getattr(c.p, col)[2] for col in cols}, {col : getattr(c.p, col)[3] for col in cols} 

    return plot_cols, plot_units, plot_type, plot_desc

def _get_plot_1_variables(c):
    # -----
    # Get: reg_ids

    reg_ids = c.v.reg_ids

    # -----
    # Get: doi_summary

    doi_summary = c.v.doi_summary

    # -----
    # Get: gen_stack_by_reg

    gen_stack_by_reg = c.v.gen_stack_by_reg
    net_exports_ts  = c.v.exports_by_reg_ts.groupby(['model','timestamp']).sum()

     # Add a total column for full aggregation for generation stacks at national/regional level
    gen_stack_total = gen_stack_by_reg.groupby(['model', 'timestamp']).sum()
    exports_total = net_exports_ts.where(net_exports_ts > 0).fillna(0)
    imports_total = net_exports_ts.where(net_exports_ts < 0).fillna(0).abs()
    gen_stack_total.loc[:,'Imports'] = imports_total.value.rename('Imports')
    gen_stack_total.loc[:,'Exports'] = exports_total.value.rename('Exports')

    gen_stack_total.loc[:,'Net Load w/ Exports'] = gen_stack_total['Net Load'].rename('Net Load w/ Exports') + exports_total.value.rename('Net Load w/ Exports')
    gen_stack_total = gen_stack_total.reset_index()
    
    gen_stack_total[c.GEO_COLS] = 'Overall'
    gen_stack_total = gen_stack_total.set_index(['model'] + c.GEO_COLS + ['timestamp'])
    ### Combine the total with the rest of the data and scale to GW
    gen_stack_by_reg = pd.concat([gen_stack_by_reg, gen_stack_total], axis=0).groupby(['model'] + c.GEO_COLS + ['timestamp']).sum()/1000
    
    # Add summary region here too
    reg_ids = reg_ids + ['Overall']

    return reg_ids, doi_summary, gen_stack_by_reg

def create_plot_1a(c):
    """
    Plot 1a: Generation stacks for national days of interest a specified reference model
    # Todo works but has two bugs: Wrong index, Aggregation for full country is missing
    """

    print("Creating plot 1a...")

    reg_ids, doi_summary, gen_stack_by_reg = _get_plot_1_variables(c)

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
                save_dir_model, "plot1a_stack_doi_{}.xlsx".format(doi_name)
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
    print("Done.")


def create_plot_1b(c, ref_m=None):
    """
    Plot 1b: Generation stacks for national days of interest a specified reference model
    # Todo works but has two bugs: Wrong index, Aggregation for full country is missing
    """

    print("Creating plot 1b...")

    reg_ids, doi_summary, gen_stack_by_reg = _get_plot_1_variables(c)

    model_regs = reg_ids # + ["JVB", "SUM", "IDN"] # todo, This is the reason for missing aggregation, needs generalization

    doi_periods = [doi for doi in doi_summary.index if "time" in doi]
    doi_names = [doi for doi in doi_summary.index if "time" not in doi]

    if ref_m is None:
    ### Defaults to the model with the highest load. This is kinda arbirtary.
        ref_model = c.v.model_names[0]
    elif ref_m == 'USE':
        ref_model = c.v.use_reg_ts.groupby('model').sum().idxmax().iloc[0]
    else:
        ref_model = ref_m

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
                save_dir_model, f"plot1b_stack_doi_{doi_name}_ref_{ref_m}.xlsx"
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
    print("Done.")


def create_plot_1c(c, toi=None):
    """
    Plot 1b: Generation stacks for national days of interest a specified reference model
    # Todo works but has two bugs: Wrong index, Aggregation for full country is missing
    """

    print("Creating plot 1c...")
    
    reg_ids, doi_summary, gen_stack_by_reg = _get_plot_1_variables(c)

    model_regs = reg_ids # + ["JVB", "SUM", "IDN"] # todo, This is the reason for missing aggregation, needs generalization

    doi_periods = [doi for doi in doi_summary.index if "time" in doi]
    doi_names = [doi for doi in doi_summary.index if "time" not in doi]

    ref_model = c.v.use_reg_ts.groupby('model').sum().idxmax().iloc[0]

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
    print("Done.")
        
        
def create_plot_2(c, plot_vars=None):
    
    """
    ### Plot 2: Annual summary plots by columnn

    ### TODO: It would be a great idea to add some of the VRE phase classification metrics here too.
    """

    print("Creating plot 2...")

    fig_path = os.path.join(c.DIR_05_3_PLOTS, "plot2_annual_summary_plots.xlsx")    
    default_vars = c.LOAD_PLOTS + c.GEN_PLOTS + c.OTHER_PLOTS

    if plot_vars is None:
        plot_cols, plot_units, plot_type, plot_desc = _get_plot_2_variables(c, default_vars)
    else:
        plot_cols, plot_units, plot_type, plot_desc = _get_plot_2_variables(c, plot_vars)

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
                    palette=c.v.combined_palette,
                    desc=plot_desc[i]
                )
        
    print("Done.")


def create_plot_2b_ref_plots(c, plot_vars=None, ref_m=None):
    """
    Status:
    Plot2a: Annual summary plots by column relative to a reference model

    Creates following output files:
    - for each model in /{model}/:
        - plot2b_annual_relative_ref_{model}.xlsx
    """
    
    # Get rid of cofiring if any for the purpose of comparison

    default_vars = c.LOAD_PLOTS + c.GEN_PLOTS + c.OTHER_PLOTS

    if plot_vars is None:
        plot_cols, plot_units, plot_type, plot_desc = _get_plot_2_variables(c, default_vars)
    else:
        plot_cols, plot_units, plot_type, plot_desc = _get_plot_2_variables(c, plot_vars)

    if ref_m is None:
        ref_m = c.v.model_names[0]

    if ref_m not in c.v.model_names:
        print(f"Error: {ref_m} is not a valid model name.")
        return

    print(f"Creating plot 2b with reference to {ref_m} model")

    fig_path = os.path.join(c.DIR_05_3_PLOTS, f"plot2b_annual_plots_relative_ref_{ref_m}.xlsx")    

    with pd.ExcelWriter(fig_path, engine="xlsxwriter") as writer: # pylint: disable=abstract-class-instantiated
        for i, df in plot_cols.items():
            if df.shape[0] == 0:
                print(f"Empty dataframe for: {i}")
            else:


                if df.axes[0].name == "model":
                    # Adds in a total column for the comparison of totals between models
                    df.loc[:,'Overall'] = df.sum(axis=1)
                    ref_srs = df.loc[ref_m, :]
                    df = df.sub(ref_srs, axis=1)
                    df_pc = df.div(ref_srs, axis=1).replace(np.inf, np.nan).replace(-np.inf, np.nan).fillna(0)
                elif df.axes[1].name == "model":
                    ref_srs = df.loc[:, ref_m]
                    df = df.sub(ref_srs, axis=0) # subtract ref_srs from each row
                    df_pc = df.div(ref_srs, axis=0).replace(np.inf, np.nan).replace(-np.inf, np.nan).fillna(0) # divide each row by ref_srs
                else:
                    print(f"Error: {i} has no model axis.")
                    continue                

                write_xlsx_column(
                    df=df,
                    writer=writer,
                    sheet_name=i,
                    subtype=plot_type[i],
                    units=plot_units[i],
                    palette=c.v.combined_palette,
                    desc=plot_desc[i]
                )

                write_xlsx_column(
                    df=df_pc,
                    writer=writer,
                    sheet_name=f"{i}_pc",
                    subtype='clustered',
                    units='%',
                    palette=c.v.combined_palette,
                    desc=plot_desc[i]
                )
    print("Done.")



def create_plot_3(c):
    """
    # Annual generation by tech/reg plots per model
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
    print("Done.")


def create_plot_4_costs(c):
    """
    # todo Could work, but can't be run because implementation of 04 ts output is missing
    """
    ### Plot 4: Cost savings plots by reference model

    ### Cofiring stuff that was built in is now removed

    print("Creating plot 4...")

    gen_op_costs_by_tech = c.v.gen_op_costs_by_reg.groupby(["model", "Category"]) \
        .sum() \
        .value \
        .unstack("model") \
        .fillna(0)
    
    gen_total_costs_by_reg = c.v.gen_total_costs_by_reg
    gen_total_costs_by_tech = gen_total_costs_by_reg.groupby(["model", "Category"]) \
        .sum() \
        .value \
        .unstack("model") \
        .fillna(0)

    gen_op_costs_by_prop = (
        c.v.gen_op_costs_by_reg.groupby(["model", "property"]).sum().value.unstack("model").fillna(0)
    )
    gen_total_costs_by_prop = (
        c.v.gen_total_costs_by_reg.groupby(["model", "property"])
        .sum()
        .value
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
                units="%",
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
                units="%",
                total_scatter_col="Relative savings",
            )
            write_xlsx_column(
                df=savings_op_by_tech_pc,
                writer=writer,
                sheet_name="savings_op_by_tech_pc",
                subtype="stacked",
                units="%",
                total_scatter_col="Relative savings",
            )
            write_xlsx_column(
                df=savings_tsc_by_tech_pc,
                writer=writer,
                sheet_name="savings_tsc_by_tech_pc",
                subtype="stacked",
                units="%",
                total_scatter_col="Relative savings",
            )
        print("Done.")


@catch_errors
def create_plot_5_undispatched_tech(c):
        """
        Output 5: to plot undispatched capacity for USE (and maybe other metrics)

        """

        #### Output 5: to plot undispatched capacity for USE (and maybe other metrics)
        ix = pd.IndexSlice

        print('Creating output 5 for undispatched capacity during USE...')

        if c.cfg['settings']['reg_ts']:
            for m in c.v.model_names:
                save_dir_model = os.path.join(c.DIR_05_3_PLOTS, m)
                if os.path.exists(save_dir_model) is False:
                    os.mkdir(save_dir_model)
                    
                model_use_ts = c.v.use_ts.loc[ix[m,:]].reset_index()
                use_periods = model_use_ts[model_use_ts.value>0]
                model_use_reg_ts = c.v.use_reg_ts.loc[ix[m,:]].reset_index()
                
                if len(use_periods) == 0:
                    print(f"No USE for {m}. No output created for undispatched capacity during USE.")
                    use_days = None
                    continue
                else:
                    use_days = np.unique(use_periods.timestamp.dt.date)

                fig_path = os.path.join(save_dir_model, f"plot5_undisp_cap_during_USE_{m}.xlsx")


                with pd.ExcelWriter(fig_path, engine="xlsxwriter") as writer: # pylint: disable=abstract-class-instantiated
                ## ExcelWriter for some reason uses writer.sheets to access the sheet.
                ## If you leave it empty it will not know that sheet Main is already there
                ## and will create a new sheet.

                    for reg in c.v.reg_ids:

                        undisp_cap_by_reg = c.v.undisp_cap_by_tech_reg_ts.groupby(
                            ['model', c.GEO_COLS[0],'timestamp']).sum().loc[m,reg,:].reset_index()
                        undisp_cap_by_reg = undisp_cap_by_reg.loc[undisp_cap_by_reg.timestamp.dt.date.isin(use_days)].set_index('timestamp')
        
                        use_reg = model_use_reg_ts.loc[model_use_reg_ts.timestamp.dt.date.isin(use_days)].set_index('timestamp').groupby(
                            level=c.GEO_COLS[0], axis=1).sum()[reg].to_frame().rename(columns={reg:'Unserved Energy'})
                        undisp_cap_by_reg = pd.concat([undisp_cap_by_reg, use_reg], axis=1)



                        write_xlsx_stack(
                            df=undisp_cap_by_reg,
                            writer=writer,
                            sheet_name=reg,
                            palette=STACK_PALETTE,
                        )

                    

        print('Done.')


def create_plot_6_ldc_and_line_plots(c):
    """
    Status:
    Plot 6: LDC and nLDC plots

    Creates following output files:
    - for all models in a single file:
        - plot6_ldc_plots.xlsx
    """

    print("Creating plot 6...")

    plot_lines = {'ldc':c.v.ldc/1000,
                'nldc':c.v.nldc/1000,
                'nldc_curtail':c.v.nldc_curtail/1000,
                'nldc_sto':c.v.nldc_sto/1000,
                'nldc_sto_curtail':c.v.nldc_sto_curtail/1000,
                'curtailment_dc':c.v.curtailment_dc/1000,
                'ramp_pc_dc':c.v.ramp_pc_dc,
                'th_ramp_pc_dc':c.v.th_ramp_pc_dc,
                'ramp_abs_dc':c.v.ramp_dc,
                'th_ramp_abs_dc':c.v.th_ramp_dc,
                'srmc_dc':c.v.srmc_dc
                }      

    ln_plot_type = {'ldc':'ldc',
                'nldc':'ldc',
                'nldc_curtail':'ldc',
                'nldc_sto':'ldc',
                'nldc_sto_curtail':'ldc',
                'curtailment_dc':'ldc',
                'ramp_pc_dc':'ldc',
                'th_ramp_pc_dc':'ldc',
                'ramp_abs_dc':'ldc',
                'th_ramp_abs_dc':'ldc',
                'srmc_dc':'ldc'
                }      
    

    ln_plot_units = {'ldc':'GW',
                'nldc':'GW',
                'nldc_curtail':'GW',
                'nldc_sto':'GW',
                'nldc_sto_curtail':'GW',
                'curtailment_dc':'GW',
                'ramp_pc_dc':'%/hr',
                'th_ramp_pc_dc':'%/3-hr',
                'ramp_abs_dc':'MW/hr',
                'th_ramp_abs_dc':'MW/3-hr',
                'srmc_dc':'$/MWh'

                
                }      


    fig_path = os.path.join(c.DIR_05_3_PLOTS,'plot6_ldc_and_line_plots.xlsx')

    with pd.ExcelWriter(fig_path, engine='xlsxwriter') as writer: # pylint: disable=abstract-class-instantiated
    
        for i in plot_lines.keys():
            write_xlsx_line(df=plot_lines[i], writer=writer, sheet_name=i,subtype=ln_plot_type[i], units=ln_plot_units[i], line_width=1.5)   

    print("Done.")


def create_plot_7_co2_savings(c):
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


def create_plot_8_services(c, ref_model=None):
    """
    Status:
    Plot 8 Services figure output

    Creates following output files:
    - for each model in /{model}/:
        - plot8_services_{model}.xlsx

    Services are as follows:

    Generation
    InertiaContribution
    PeakContribution
    PeakContribution_actual
    Regulating reserve
    Spinning reserve
    UpRampContribution
    """	

    ix = pd.IndexSlice

    # service_tech_idx = pd.read_excel('/templates/fig00_services_template.xlsx', sheet_name='tech_idx')
    subtechs = c.o.gen_yr_df[c.o.gen_yr_df.property == 'Generation'].CapacityCategory.unique()

    if ref_model is None:
        ref_model = c.v.model_names[0]

    fig_path = os.path.join(c.DIR_05_3_PLOTS,'plot8_services_fig.xlsx')

    with pd.ExcelWriter(fig_path, engine="xlsxwriter") as writer: # pylint: disable=abstract-class-instantiated
        for m in c.v.model_names:
            
            ### All subtechs for consistent solution size/columns
            
            services_out = pd.DataFrame(index=subtechs).rename_axis('Technology')
            save_dir_model = os.path.join(c.DIR_05_3_PLOTS, m)
            
        ## A. Energy contribution
            energy_contr = c.o.gen_yr_df[(c.o.gen_yr_df.property == 'Generation')&(c.o.gen_yr_df.model == m)].groupby('CapacityCategory') \
                                .agg({'value':'sum'}).value

        ## B. Inertia
            ### Calc. lowest 100 inertia periods

            total_inertia = c.v.inertia_by_reg.loc[ix[m,]].groupby(level='timestamp') \
                                .agg({'value':'sum'}).value
            inertia_100 = total_inertia.nsmallest(n=100)
            # stability_100 = total_inertia.nsmallest(n=100)

            inertia_by_tech100 = c.v.gen_inertia.groupby(['model','CapacityCategory','timestamp']) \
                                .agg({'InertiaLo':'sum'}).InertiaLo.loc[ix[m,]].unstack(
                'CapacityCategory').loc[inertia_100.index]
            inertia_contr = inertia_by_tech100.sum(axis=0)
            
        ### C. Peak contribution
            ## Calc 100 top periods
            # CapacityCategory and other indices are not consistent across projects
            # This is a temporary fix
            vre_techs = [ c for c in c.v.gen_by_subtech_ts.columns if c in  ['Solar', 'SolarPV' 'Wind'] ]
        #     gen_by_subtech_ts = gen_df[gen_df.property == 'Generation'].groupby(['model','CapacityCategory','timestamp']).sum().loc[ix[m,]]
            non_vre_techs = [c for c in c.v.gen_by_subtech_ts.columns if c not in vre_techs ]
            load_100 = c.v.customer_load_orig_ts.loc[ix[m,:]].value.nlargest(100)
            netload_100 = c.v.net_load_orig_ts.loc[ix[m,:]].value.nlargest(100)
            

            ### NA should be filled up top instead
            pk_contr = c.v.gen_by_subtech_ts.fillna(0).loc[ix[m,netload_100.index],:].reset_index(drop=True)
            pk_contr = pk_contr.mean()
            pk_contr.loc[vre_techs] = 0
            vre_pk_contr = pd.Series(data=np.mean(load_100.values-netload_100.values), index = ['VRE'])
            dsm_pk_contr = pd.Series(data=np.mean(c.v.net_load_ts.loc[ix[m,:]].value.nlargest(100) - c.v.net_load_orig_ts.loc[ix[m,:]].value.nlargest(100)), index=['DSMshift'])
            pk_contr = pd.concat([pk_contr, vre_pk_contr, dsm_pk_contr])
        
        #     ### DSM would go here too!
        #     pk_contr = pd.merge(non_vre_pk_contr, vre_pk_contr, left_index=True, right_index=True)
        #     pk_contr = pk_contr.sum()/pk_contr.sum().sum()
            
        ### D. Reserves
            spin_res_contr = c.v.spinres_prov_ts.groupby(['model','CapacityCategory']).agg({'value':'sum'}).value.loc[ix[m,],]
            spin_res_av_ann_contr = c.v.spinres_av_ts.groupby(['model','CapacityCategory']).agg({'value':'sum'}).value.loc[ix[m,],]
            spin_res_contr = spin_res_contr.mask(spin_res_contr<0).fillna(0)
            spin_res_av_ann_contr = spin_res_av_ann_contr.mask(spin_res_av_ann_contr<0).fillna(0)
                
            ## 100 most difficult periods for stability/reserves = highest net load ==> systen is the most strained 
            spinres_av100 = c.v.spinres_av_ts.loc[ix[:,:,:,netload_100.index]]
            spinres_av100 = spinres_av100.mask(spinres_av100<0).fillna(0)
            spin_res_av_contr = spinres_av100.groupby(['model','CapacityCategory']).agg({'value':'sum'}).value.loc[ix[m,],]



            ### Some models have not have reg reserves so, this avoids this from breaking
            try:
                reg_res_contr = c.v.regres_prov_ts.groupby(['model','CapacityCategory']).agg({'value':'sum'}).value.loc[ix[m,],]
                reg_res_av_ann_contr = c.v.regres_av_ts.groupby(['model','CapacityCategory']).agg({'value':'sum'}).value.loc[ix[m,],]
                ### Some bug?
                reg_res_contr = reg_res_contr.mask(reg_res_contr<0).fillna(0)
                reg_res_av_ann_contr = reg_res_av_ann_contr.mask(reg_res_av_ann_contr<0).fillna(0)
                
                ## 100 most difficult periods for stability/reserves = lowest inertia periods    
                regres_av100 = c.v.regres_av_ts.loc[ix[:,:,:,netload_100.index]]
                regres_av100 = regres_av100.mask(regres_av100<0).fillna(0)
                reg_res_av_contr = regres_av100.groupby(['model','CapacityCategory']).agg({'value':'sum'}).value.loc[ix[m,],]
            except KeyError:
                reg_res_contr = pd.Series(data=np.zeros(len(spin_res_contr)), index = spin_res_contr.index)
                reg_res_av_ann_contr = pd.Series(data=np.zeros(len(spin_res_av_contr)), index = spin_res_av_contr.index)
                reg_res_av_contr = pd.Series(data=np.zeros(len(spin_res_av_contr)), index = spin_res_av_contr.index)
                
            
        ### E. Upward ramp contribution
        ### TODO: Ramp contirbution from VRE should be zeroed/ignored
            ramp_100 = c.v.ramp_ts.loc[ix[m,:],:].droplevel(0).value.nlargest(int(0.1*8760*c.v.hour_corr))
            ramp_contr = c.v.ramp_by_gen_subtech_ts.loc[ix[m,ramp_100.index],].droplevel(0)
            ramp_contr = ramp_contr.mask(ramp_contr < 0).fillna(0).mean()
            dsm_ramp_contr = pd.Series(data=np.mean(c.v.ramp_orig_ts.loc[ix[m,:],:].droplevel(0).value.nlargest(int(0.1*8760*c.v.hour_corr)) - ramp_100), index=['DSMshift'])
            ramp_contr = pd.concat([ramp_contr, dsm_ramp_contr])

        ### Out

            services_out = pd.concat([services_out, energy_contr.rename('Energy'), pk_contr.rename('Peak'), ramp_contr.rename('UpwardRamps'),
                                spin_res_contr.rename('SpinRes'), reg_res_contr.rename('RegRes'), inertia_contr.rename('Inertia'),
                                    spin_res_av_contr.rename('SpinResAv'), reg_res_av_contr.rename('RegResAv'), ], axis=1).fillna(0)
            

            services_out.index = [SERVICE_TECH_IDX[i] for i in services_out.index]
            services_out = services_out.groupby(services_out.index).sum().T

            ## All model names should be shortened to 31 characters for sheet_names
            if len(m) >= 31:
                m = m[:15] + m[-15:]

            # TODO: Need to fix the dimensions of the chart area, legend, etc.
            write_xlsx_column(
                df=services_out,
                writer=writer,
                sheet_name= m,
                type="bar",
                subtype="percent_stacked",
                units="",
                palette=SERVICES_PALETTE,
                to_combine=True
            )

    
            # This should be an EXCEL output using some sort of aggregation from indices
            services_out.reset_index().rename(columns={'index':'property'}).to_csv(os.path.join(save_dir_model, 'plot8_services_fig.csv'), index=False)

        print("Creating plot 8...")


def create_plot_9_av_cap(c):
    """
    Status:
    Plot 6: LDC and nLDC plots

    Creates following output files:
    - for all models in a single file:
        - plot6_ldc_plots.xlsx
    """

    print("Creating plot 6...")

    plot_lines = {'av_cap':c.p.av_cap_ts[0],
                 'res_margin':c.p.res_margin_ts[0],
                 'av_cap_dly':c.p.av_cap_dly_ts[0],
                 'res_margin_dly':c.p.res_margin_dly_ts[0]
                }
    
    ln_plot_type = {'av_cap':'timeseries',
                'res_margin':'timeseries',
                'av_cap_dly':'timeseries',
                'res_margin_dly':'timeseries'
                }

    ln_plot_units = {'av_cap':'GW',
                'res_margin':'%',
                'av_cap_dly':'GW',
                'res_margin_dly':'%'
                }


    fig_path = os.path.join(c.DIR_05_3_PLOTS,'plot9_av_cap_plots.xlsx')

    with pd.ExcelWriter(fig_path, engine='xlsxwriter') as writer: # pylint: disable=abstract-class-instantiated
    
        for i in plot_lines.keys():
            print(i)
            write_xlsx_line(df=plot_lines[i], writer=writer, sheet_name=i,subtype=ln_plot_type[i], units=ln_plot_units[i], line_width=1.5)   

    print("Done.")

def create_plot_10_ts_by_model(c):
    """
    Status:
    Plot 10: TS plots by model

    Creates following output files:
    - for all models in a single file:
        - plot10_ts_by_model_plots.xlsx
    """

    vre_gen_monthly_ts = c.v.vre_gen_monthly_ts
    fig_path = os.path.join(c.DIR_05_3_PLOTS,'plot10_ts_by_model_plots.xlsx')
    
    with pd.ExcelWriter(fig_path, engine='xlsxwriter') as writer: # pylint: disable=abstract-class-instantiated
    
        for m in c.v.model_names:
            df = vre_gen_monthly_ts.loc[pd.IndexSlice[m,]]
            
            if len(m) >= 31:
                sheet_m = m[:15] + m[-15:]
            else:
                sheet_m = m

            write_xlsx_line(df=df, writer=writer, sheet_name=sheet_m, subtype='timeseries', units='GWh', line_width=1)
    print("Done.")