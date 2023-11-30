"""
TODO Docstring
"""

import os

import matplotlib.pyplot as plt
import matplotlib
import seaborn as sns
import calmap
import pandas as pd
import numpy as np

from matplotlib.patches import Patch
from matplotlib.lines import Line2D
from matplotlib import colors

from .utils.utils import catch_errors
from .constants import VRE_TECHS
from . import log

print = log.info

np.random.seed(sum(map(ord, 'calplot')))

iea_palette = {'grey5': '#f2f2f2', 'grey10': '#e6e6e6', 'pl': '#b187ef', 'bl': '#49d3ff', 'tl': '#00e0e0',
               'gl': '#68f394', 'yl': '#fff45a',
               'ol': '#ffb743', 'rl': '#ff684d', 'gl': '#68f394', 'yl': '#fff45a', 'grey40': '#949494',
               'grey50': '#6f6f6f',
               'p': '#af6ab1', 'b': '#3e7ad3', 't': '#00ada1', 'g': '#1dbe62', 'y': '#fed324',
               'o': '#f1a800', 'r': '#e34946', 'grey20': '#afafaf', 'black': '#000000', 'white': '#ffffff',
               'iea_b': '#0044ff', 'iea_b50': '#80a2ff'}

extended_palette = dict(
    {'{}'.format(i): plt.matplotlib.colors.rgb2hex(plt.cm.get_cmap('tab20b').colors[i]) for i in np.arange(0, 20)},
    **{'{}'.format(i + 20): plt.matplotlib.colors.rgb2hex(plt.cm.get_cmap('tab20c').colors[i]) for i in
       np.arange(0, 20)})

### For overflow, i.e. things that go beyond the 16 or so colors of IEA palette.
iea_palette_plus = dict(iea_palette, **extended_palette)

tech_palette = {'Coal': 'grey20', 'Abated coal': 'grey10', 'Cofiring': 'grey10', 'Gas': 'p', 'Abated gas': 'p',
                'Oil': 'grey50', 'Hydro': 'bl', 'Geothermal': 'r', 'Bioenergy': 'gl', 'Solar': 'y', 'Wind': 'g',
                'Fuel Cell': 't', 'Other': 't', 'Battery': 'b', 'Storage': 'b'}

# model_palette = {'2019':'rl', '2025 Base':'o', '2025 SolarPlus':'bl', '2025 SolarPlus Lite':'pl',  '2025 SolarPlus Extra':'gl' }


### We should add load vs customer load (to see charging effect),
### Similarly could add load + exports to get effective load
stack_palette = {'Geothermal': 'o', 'Bioenergy': 'gl', 'Coal': 'grey20', 'Cofiring': 'grey5', 'Abated coal': 'grey5',
                 'Gas': 'p', 'Abated gas': 'pl', 'Hydro': 'bl', 'Oil': 'grey50', 'Imports': 't', 'Other': 't',
                 'Fuel Cell': 'tl', 'Storage': 'b',
                 'Solar': 'y', 'Wind': 'g', 'Total Load': 'black', 'Load2': 'white', 'Exports': 'p', 'Net Load': 'r',
                 'Curtailment': 'yl', 'Unserved Energy': 'iea_b', 'Underlying Load': 'p', 'Storage Load': 'grey50',
                 'Nuclear': 'r'}

reg_palette = {'APB_BALI': 'bl', 'APB_JBR': 'b', 'APB_JKB': 'pl', 'APB_JTD': 't', 'APB_JTM': 'y'}
subreg_palette = {'BAL': 'bl', 'BNT': 'b', 'DIY': 'gl', 'JBR': 't', 'JKT': 'ol', 'JTE': 'pl', 'JTM': 'grey20',
                  'SMN': 'r', 'SMS': 'tl'}
isl_palette = {'JVB': 'bl', 'SUM': 'gl', 'KLM': 'rl', 'SLW': 'ol', 'MPN': 'pl', 'IDN': 'yl'}

iea_palette_l8 = ['rl', 'ol', 'gl', 'bl', 'pl', 'grey10', 'yl',
                  'tl']  ### got rid of light yellow as its a poor choice for plots.
iea_palette_d8 = ['r', 'o', 'y', 'g', 't', 'b', 'p', 'grey50']
iea_palette_16 = iea_palette_l8 + iea_palette_d8

iea_palette_14 = ['rl', 'ol', 'bl', 'gl', 'pl', 'grey10', 'y', 'tl', 'g', 't', 'b', 'grey50', 'yl', 'r', 'p']
iea_cmap_l8 = colors.ListedColormap([iea_palette[c] for c in iea_palette_l8])
iea_cmap_d8 = colors.ListedColormap([iea_palette[c] for c in iea_palette_d8])
iea_cmap_16 = colors.ListedColormap([iea_palette[c] for c in iea_palette_16])
iea_cmap_14 = colors.ListedColormap([iea_palette[c] for c in iea_palette_14])

tab20bc = colors.ListedColormap([extended_palette[i] for i in extended_palette.keys()])

# model_palette = dict(zip([m for m in pretty_model_names.values() if m in model_names], iea_palette_14[:len(model_names)]))
combined_palette = dict(tech_palette, **subreg_palette, **reg_palette,
                        **isl_palette)  # , **model_palette, **weo_Tech_palette)


def write_xlsx_column(
        df,
        writer,
        excel_file=None,
        sheet_name="Sheet1",
        palette=combined_palette,
        subtype="stacked",
        units="",
        total_scatter_col=None,
        to_combine=False,
        right_ax=None,
):
    cm_to_pixel = 37.7953

    ## Sort columns by order in palettes described above
    sort_cols = [c for c in palette.keys() if c in df.columns] + [
        c for c in df.columns if c not in palette.keys()
    ]
    sort_index = [i for i in palette.keys() if i in df.index] + [
        i for i in df.index if i not in palette.keys()
    ]

    if type(df.index == pd.Index):
        df = df.loc[sort_index, sort_cols]
    else:
        df = df.loc[:, sort_cols]

    if excel_file:
        writer = pd.ExcelWriter(excel_file, engine="xlsxwriter")

    ### Whether we caluclate the scatter col or not. Should probably rename the variable from total_col, as its not always a total
    if (total_scatter_col != None) & (total_scatter_col not in df.columns):
        df.loc[:, total_scatter_col] = df.sum(axis=1)

    df.to_excel(writer, sheet_name=sheet_name)

    # Access the XlsxWriter workbook and worksheet objects from the dataframe.
    workbook = writer.book
    worksheet = writer.sheets[sheet_name]

    if units == "%":
        num_fmt = "0%"
        units = ""
    else:
        num_fmt = "# ###"

    # Create a chart object.
    chart = workbook.add_chart({"type": "column", "subtype": subtype})
    chart.set_size({"width": 15 * cm_to_pixel, "height": 7 * cm_to_pixel})

    if to_combine:
        chart.set_plotarea(
            {
                "layout": {
                    "x": 0.1,
                    "y": 0,
                    "width": 0.9,
                    "height": 0.8,
                },
                "fill": {"none": True},
            }
        )
    else:
        chart.set_plotarea(
            {
                #         'layout': {
                #             'x':      0.1,
                #             'y':      0,
                #             'width':  0.9,
                #             'height': 0.8,
                #         },
                "fill": {"none": True}
            }
        )

    chart.set_chartarea(
        {
            "fill": {"none": True},
            "border": {"none": True},
        }
    )

    if total_scatter_col != None:
        chart2 = workbook.add_chart({"type": "scatter"})

    for col_num in np.arange(df.index.nlevels, df.shape[1] + df.index.nlevels):
        if df.columns[col_num - df.index.nlevels] != total_scatter_col:
            # Configure the series of the chart from the dataframe data.
            ## Col_num iterates from first data column, which varies if it is multiindex columns or not

            try:
                fill_colour = iea_palette_plus[
                    palette[df.columns[col_num - df.index.nlevels]]
                ]
            except KeyError:
                fill_colour = iea_cmap_16.colors[col_num - df.index.nlevels]

            # fill_colour = matplotlib.colors.rgb2hex(plt.cm.get_cmap('tab20c').colors[20-col_num-df.index.nlevels])

            # Or using a list of values instead of category/value formulas:
            #     [sheetname, first_row, first_col, last_row, last_col]

            # Or using a list of values instead of category/value formulas:
            #     [sheetname, first_row, first_col, last_row, last_col]
            chart.add_series(
                {
                    "name": [sheet_name, 0, col_num],
                    "categories": [sheet_name, 1, 0, df.shape[0], df.index.nlevels - 1],
                    "values": [sheet_name, 1, col_num, df.shape[0], col_num],
                    "gap": 75,
                    "fill": {"color": fill_colour, "border": "#000000"},
                    "border": {"color": "#000000"},
                }
            )
        else:
            if right_ax != None:
                chart2.add_series(
                    {
                        "name": [sheet_name, 0, col_num],
                        "categories": [
                            sheet_name,
                            1,
                            0,
                            df.shape[0],
                            df.index.nlevels - 1,
                        ],
                        "values": [sheet_name, 1, col_num, df.shape[0], col_num],
                        "marker": {
                            "type": "circle",
                            "size": 8,
                            "border": {"color": "#000000"},
                            "fill": {"color": "#ffffff", "transparency": 30},
                        },
                        "y2_axis": True,
                    }
                )
            else:
                chart2.add_series(
                    {
                        "name": [sheet_name, 0, col_num],
                        "categories": [
                            sheet_name,
                            1,
                            0,
                            df.shape[0],
                            df.index.nlevels - 1,
                        ],
                        "values": [sheet_name, 1, col_num, df.shape[0], col_num],
                        "marker": {
                            "type": "circle",
                            "size": 8,
                            "border": {"color": "#000000"},
                            "fill": {"color": "#ffffff", "transparency": 30},
                        },
                    }
                )

            ### This is the total column and will always be last, so we can just combine here and save an if/else loop
            chart.combine(chart2)

    ### Set label_position to low if there are negative values
    if (df < 0).sum().sum() > 0:
        label_position = "low"
    else:
        label_position = "next_to"

    chart.set_x_axis(
        {
            "num_font": {"name": "Arial", "size": 10},
            "line": {"color": "black"},
            "label_position": label_position,
        }
    )

    chart.set_y_axis(
        {
            "major_gridlines": {"visible": False},
            "num_font": {"name": "Arial", "size": 10},
            "num_format": num_fmt,
            "name": units,
            "name_font": {
                "name": "Arial",
                "size": 10,
                "bold": False,
                "text_rotation": -90,
            },
            "name_layout": {"x": 0.02, "y": 0.02},
            "line": {"none": True},
            "major_gridlines": {
                "visible": True,
                "line": {"width": 1, "color": "#d9d9d9"},
            },
        }
    )

    if right_ax != None:
        if df.shape[1] > 1:
            max_chars = np.max([len(c) for c in df.columns])
            min_width = 0.075
            width = min_width + max_chars * 0.01
        else:
            width = 0

        chart2.set_y2_axis(
            {
                "major_gridlines": {"visible": False},
                "num_font": {"name": "Arial", "size": 10},
                "num_format": num_fmt,
                "name": right_ax,
                "name_font": {
                    "name": "Arial",
                    "size": 10,
                    "bold": False,
                    "text_rotation": -90,
                },
                "name_layout": {"x": 0.98 - width, "y": 0.02},
                "line": {"none": True},
                "major_gridlines": {
                    "visible": True,
                    "line": {"width": 1, "color": "#d9d9d9"},
                },
            }
        )

    chart.set_title({"none": True})

    if df.shape[1] > 1:
        max_chars = np.max([len(c) for c in df.columns])

        ### Legend should not exceed 16chars and should always be more than 8chars

        if max_chars < 8:
            max_chars = 8
        elif max_chars > 16:
            max_chars = 16

        min_width = 0.075
        width = min_width + max_chars * 0.01

        chart.set_legend(
            {
                "font": {"name": "Arial", "size": 10},
                "layout": {"x": 1 - width, "y": 0, "height": 1, "width": width},
            }
        )
    else:
        chart.set_legend({"visble": False})
        chart.set_legend({"position": "none"})

    # Insert the chart into the worksheet....this probably should depend on the size of the dataframe
    if to_combine:
        worksheet.insert_chart("K22", chart)
    else:
        worksheet.insert_chart("K2", chart)


def write_xlsx_stack(
        df,
        writer,
        excel_file=None,
        sheet_name="Sheet1",
        palette=stack_palette,
        units="MW",
        to_combine=False,
):
    cm_to_pixel = 37.7953

    ## Sort columns by order in palettes described above
    sort_cols = [c for c in palette.keys() if c in df.columns] + [
        c for c in df.columns if c not in palette.keys()
    ]
    df = df.loc[:, sort_cols]

    if excel_file:
        writer = pd.ExcelWriter(excel_file, engine="xlsxwriter")

    df.to_excel(writer, sheet_name=sheet_name)

    # Access the XlsxWriter workbook and worksheet objects from the dataframe.
    workbook = writer.book
    worksheet = writer.sheets[sheet_name]

    # Create a chart object.
    chart = workbook.add_chart({"type": "area", "subtype": "stacked"})
    chart2 = workbook.add_chart({"type": "line"})

    sec_axis_vars = ["Load2", "Curtailment"]
    write_xlsx_stack = ["Net Load", "Load"]

    # Configure the series of the chart from the dataframe data.
    ## Col_num iterates from first data column, which varies if it is multiindex columns or not
    for col_num in np.arange(df.index.nlevels, df.shape[1] + df.index.nlevels):
        try:
            fill_colour = iea_palette_plus[palette[df.columns[col_num - 1]]]
        except KeyError:
            print("Non-specified colour for: {}".format(df.columns[col_num - 1]))
            fill_colour = iea_cmap_16.colors[col_num - 1]

        if df.columns[col_num - 1] == "Load2":
            chart.add_series(
                {
                    "name": [sheet_name, 0, col_num],
                    "categories": [sheet_name, 1, 0, df.shape[0], 0],
                    "values": [sheet_name, 1, col_num, df.shape[0], col_num],
                    "fill": {"none": True},
                    "border": {"none": True},
                    "y2_axis": True,
                }
            )

            leg_del_idx = [int(col_num - 1)]

        elif df.columns[col_num - 1] == "Curtailment":
            chart.add_series(
                {
                    "name": [sheet_name, 0, col_num],
                    "categories": [sheet_name, 1, 0, df.shape[0], 0],
                    "values": [sheet_name, 1, col_num, df.shape[0], col_num],
                    "pattern": {
                        "pattern": "light_upward_diagonal",
                        "fg_color": iea_palette["y"],
                        "bg_color": iea_palette["r"],
                    },
                    "border": {"none": True},
                    "y2_axis": True,
                }
            )
        elif df.columns[col_num - 1] == "Total Load":
            chart2.add_series(
                {
                    "name": [sheet_name, 0, col_num],
                    "categories": [sheet_name, 1, 0, df.shape[0], 0],
                    "values": [sheet_name, 1, col_num, df.shape[0], col_num],
                    "line": {"width": 0.25, "color": "black", "dash_type": "solid"},
                }
            )
        elif df.columns[col_num - 1] == "Underlying Load":
            continue
        elif df.columns[col_num - 1] == "Storage Load":
            continue
        #             chart2.add_series({
        #                 'name':       [sheet_name, 0, col_num],
        #                 'categories': [sheet_name, 1, 0, df.shape[0], 0],
        #                 'values':     [sheet_name, 1, col_num, df.shape[0], col_num],
        #                 'line': {'width': 1.00, 'color':iea_palette['p'], 'dash_type': 'dash'},
        #             })
        elif df.columns[col_num - 1] == "Net Load":
            chart2.add_series(
                {
                    "name": [sheet_name, 0, col_num],
                    "categories": [sheet_name, 1, 0, df.shape[0], 0],
                    "values": [sheet_name, 1, col_num, df.shape[0], col_num],
                    "line": {
                        "width": 1.00,
                        "color": iea_palette["r"],
                        "dash_type": "dash",
                    },
                }
            )
        else:
            chart.add_series(
                {
                    "name": [sheet_name, 0, col_num],
                    "categories": [sheet_name, 1, 0, df.shape[0], 0],
                    "values": [sheet_name, 1, col_num, df.shape[0], col_num],
                    "fill": {"color": fill_colour},
                    "border": {"none": True},
                }
            )

    # Configure the chart axes.
    num_fmt = "# ###"

    chart.combine(chart2)

    chart.set_size({"width": 15 * cm_to_pixel, "height": 9.5 * cm_to_pixel})

    if to_combine:
        chart.set_plotarea(
            {
                "layout": {
                    "x": 0.1,
                    "y": 0,
                    "width": 0.9,
                    "height": 0.8,
                },
                "fill": {"none": True},
            }
        )
    else:
        chart.set_plotarea(
            {
                #         'layout': {
                #             'x':      0.1,
                #             'y':      0,
                #             'width':  0.9,
                #             'height': 0.8,
                #         },
                "fill": {"none": True}
            }
        )

    chart.set_chartarea(
        {
            "fill": {"none": True},
            "border": {"none": True},
        }
    )

    ### Set label_position to low if there are negative values
    if (df < 0).sum().sum() > 0:
        label_position = "low"
    else:
        label_position = "next_to"

    chart.set_x_axis(
        {
            "num_font": {"name": "Arial", "size": 10},
            "num_format": "dd mmm hh:mm",
            "major_unit": 24,
            "interval_unit": 24,
            "interval_tick": 12,
            "line": {"color": "black"},
            "text_axis": True,
            "label_position": label_position,
        }
    )

    chart.set_y_axis(
        {
            "major_gridlines": {"visible": False},
            "num_font": {"name": "Arial", "size": 10},
            "num_format": num_fmt,
            "name": units,
            "name_font": {
                "name": "Arial",
                "size": 10,
                "bold": False,
                "text_rotation": -90,
            },
            "name_layout": {"x": 0.02, "y": 0.02},
            "line": {"none": True},
            "major_gridlines": {
                "visible": True,
                "line": {"width": 1, "color": "#d9d9d9"},
            },
        }
    )

    chart.set_y2_axis({"visible": False})

    #     leg_del_idx = df.shape[1]

    if "Load2" in df.columns:
        chart.set_legend(
            {
                "font": {"name": "Arial", "size": 10},
                "position": "bottom",
                "layout": {"x": 0, "y": 0.7, "width": 1, "height": 0.25},
                "delete_series": leg_del_idx,
            }
        )
    else:
        chart.set_legend(
            {
                "font": {"name": "Arial", "size": 10},
                "position": "bottom",
                "layout": {"x": 0, "y": 0.7, "width": 1, "height": 0.25},
            }
        )

    # Insert the chart into the worksheet.
    worksheet.insert_chart("S2", chart)


def write_xlsx_scatter(
        df,
        writer,
        excel_file=None,
        sheet_name="Sheet1",
        colour=None,
        palette=combined_palette,
        units="",
        alpha=80,
        to_combine=False,
        markersize=4,
        common_yr=2041,
):
    cm_to_pixel = 37.7953

    if excel_file:
        writer = pd.ExcelWriter(excel_file, engine="xlsxwriter")

    df.to_excel(writer, sheet_name=sheet_name)

    # Access the XlsxWriter workbook and worksheet objects from the dataframe.
    workbook = writer.book
    worksheet = writer.sheets[sheet_name]

    if units == "":
        num_fmt = "0%"
        units = ""
    else:
        num_fmt = "# ###"

    # Create a chart object.
    chart = workbook.add_chart({"type": "scatter"})
    chart.set_size({"width": 15 * cm_to_pixel, "height": 8.5 * cm_to_pixel})

    if to_combine:
        chart.set_plotarea(
            {
                "layout": {
                    "x": 0.1,
                    "y": 0,
                    "width": 0.9,
                    "height": 0.8,
                },
                "fill": {"none": True},
            }
        )
    else:
        chart.set_plotarea(
            {
                #         'layout': {
                #             'x':      0.1,
                #             'y':      0,
                #             'width':  0.9,
                #             'height': 0.8,
                #         },
                "fill": {"none": True}
            }
        )

    chart.set_chartarea(
        {
            "fill": {"none": True},
            "border": {"none": True},
        }
    )

    # Configure the series of the chart from the dataframe data.
    ## Col_num iterates from first data column, which varies if it is multiindex columns or not
    for col_num in np.arange(1, df.shape[1] + 1):
        if colour != None:
            fill_colour = colour
        else:
            try:
                fill_colour = iea_palette_plus[
                    palette[df.columns[col_num - df.index.nlevels]]
                ]
            except KeyError:
                fill_colour = iea_cmap_16.colors[col_num - df.index.nlevels]

        chart.add_series(
            {
                "name": [sheet_name, 0, col_num],
                "categories": [sheet_name, 1, 0, df.shape[0], df.index.nlevels - 1],
                "values": [sheet_name, 1, col_num, df.shape[0], col_num],
                "marker": {
                    "type": "circle",
                    "size": markersize,
                    "border": {"color": fill_colour},
                    "fill": {"color": fill_colour, "transparency": alpha},
                },
            }
        )

    # Configure the chart axes.
    ### Set label_position to low if there are negative values
    if (df < 0).sum().sum() > 0:
        label_position = "low"
    else:
        label_position = "next_to"

    chart.set_x_axis(
        {
            "num_font": {"name": "Arial", "size": 10},
            "num_format": "mmm",
            "date_axis": True,
            "min": pd.to_datetime("01-01-{}".format(common_yr)),
            "max": pd.to_datetime("31-12-{}".format(common_yr)),
            "major_unit": 31,
            "interval_unit": 31,
            "interval_tick": 31,
            "label_position": label_position,
        }
    )

    chart.set_y_axis(
        {
            "major_gridlines": {"visible": False},
            "num_font": {"name": "Arial", "size": 10},
            "num_format": num_fmt,
            "name": units,
            "name_font": {
                "name": "Arial",
                "size": 10,
                "bold": False,
                "text_rotation": -90,
            },
            "name_layout": {"x": 0.02, "y": 0.02},
            "line": {"none": True},
            "major_gridlines": {
                "visible": True,
                "line": {"width": 1, "color": "#d9d9d9"},
            },
        }
    )

    chart.set_title({"none": True})

    chart.set_legend(
        {
            "font": {"name": "Arial", "size": 10},
            "position": "bottom",
            #                       'layout': {'x':      0,
            #                                 'y':      0.7,
            #                                 'width':  1,
            #                                 'height': 0.25
            #                                },
            #                      'delete_series': [leg_del_idx]
        }
    )

    #     chart.set_legend({'num_font':  {'name': 'Arial', 'size': 10}})

    # Insert the chart into the worksheet....this probably should depend on the size of the dataframe
    if to_combine:
        worksheet.insert_chart("K22", chart)
    else:
        worksheet.insert_chart("K2", chart)


def write_xlsx_line(
        df,
        writer,
        excel_file=None,
        sheet_name="Sheet1",
        subtype="timeseries",
        palette=combined_palette,
        units="",
        ldc_idx=None,
        label_position="next_to",
        to_combine=False,
        line_width=1.5,
):
    cm_to_pixel = 37.7953

    ## Sort columns by order in palettes described above
    sort_cols = [c for c in palette.keys() if c in df.columns] + [
        c for c in df.columns if c not in palette.keys()
    ]
    sort_index = [i for i in palette.keys() if i in df.index] + [
        i for i in df.index if i not in palette.keys()
    ]

    if type(df.index == pd.Index):
        df = df.loc[sort_index, sort_cols]
    else:
        df = df.loc[:, sort_cols]

    if subtype == "ldc":
        if ldc_idx is None:
            df.index = (np.arange(0, df.shape[0]) + 1) / df.shape[0]
        else:
            df.index = ldc_idx

    if excel_file:
        writer = pd.ExcelWriter(excel_file, engine="xlsxwriter")

    df.to_excel(writer, sheet_name=sheet_name)

    # Access the XlsxWriter workbook and worksheet objects from the dataframe.
    workbook = writer.book
    worksheet = writer.sheets[sheet_name]

    if units == "":
        num_fmt = "0%"
        units = ""
    else:
        num_fmt = "# ###"

    if subtype == "timeseries":
        chart = workbook.add_chart({"type": "line"})
    else:
        chart = workbook.add_chart({"type": "scatter", "subtype": "line"})

    # Create a chart object.
    if to_combine:
        chart.set_size({"width": 7.5 * cm_to_pixel, "height": 7 * cm_to_pixel})
    else:
        chart.set_size({"width": 15 * cm_to_pixel, "height": 7 * cm_to_pixel})

    if to_combine:
        chart.set_plotarea(
            {
                "layout": {
                    "x": 0.25,
                    "y": 0,
                    "width": 0.75,
                    "height": 0.65,
                },
                "fill": {"none": True},
            }
        )
    else:
        chart.set_plotarea(
            {
                "layout": {
                    "x": 0.25,
                    "y": 0,
                    "width": 0.75,
                    "height": 0.65,
                },
                "fill": {"none": True},
            }
        )

    chart.set_chartarea(
        {
            "fill": {"none": True},
            "border": {"none": True},
        }
    )

    # Configure the series of the chart from the dataframe data.
    ## Col_num iterates from first data column, which varies if it is multiindex columns or not
    for col_num in np.arange(df.index.nlevels, df.shape[1] + df.index.nlevels):
        try:
            line_colour = iea_palette_plus[
                palette[df.columns[col_num - df.index.nlevels]]
            ]
        except KeyError:
            line_colour = iea_cmap_16.colors[col_num - df.index.nlevels]

        chart.add_series(
            {
                "name": [sheet_name, 0, col_num],
                "categories": [sheet_name, 1, 0, df.shape[0], df.index.nlevels - 1],
                "values": [sheet_name, 1, col_num, df.shape[0], col_num],
                "line": {"color": line_colour, "width": line_width},
                "marker": {"type": "none"},
            }
        )

    ### Set label_position to low if there are negative values
    if (df < 0).sum().sum() > 0:
        label_position = "low"
    else:
        label_position = "next_to"

    chart.set_x_axis(
        {
            "num_font": {"name": "Arial", "size": 10},
            "num_format": "dd mmm hh:mm",
            "major_unit": 24,
            "interval_unit": 24,
            "interval_tick": 12,
            "line": {"color": "black"},
            "text_axis": True,
            "label_position": label_position,
        }
    )

    if subtype == "ldc":
        if np.round(np.max(df.index) / 5, -1) < 1:
            chart.set_x_axis(
                {
                    "num_font": {"name": "Arial", "size": 10},
                    "line": {"color": "black"},
                    "major_unit": np.round(np.max(df.index) / 5, -1),
                    "min": 0,
                    "max": np.max(df.index),
                    "num_format": "0.0%",
                    "label_position": label_position,
                }
            )
        else:
            chart.set_x_axis(
                {
                    "num_font": {"name": "Arial", "size": 10},
                    "line": {"color": "black"},
                    "major_unit": np.round(np.max(df.index) / 5, -1),
                    "min": 0,
                    "max": np.max(df.index),
                    "num_format": "0%",
                    "label_position": label_position,
                }
            )
    else:
        chart.set_x_axis(
            {
                "num_font": {"name": "Arial", "size": 10},
                "line": {"color": "black"},
                "num_format": "mmm",
                "major_unit": 30,
                "label_position": label_position,
            }
        )

    chart.set_y_axis(
        {
            "major_gridlines": {"visible": False},
            "num_font": {"name": "Arial", "size": 10},
            "num_format": num_fmt,
            "name": units,
            "name_font": {
                "name": "Arial",
                "size": 10,
                "bold": False,
                "text_rotation": -90,
            },
            "name_layout": {"x": 0.02, "y": 0.02},
            "line": {"none": True},
            "major_gridlines": {
                "visible": True,
                "line": {"width": 1, "color": "#d9d9d9"},
            },
        }
    )

    chart.set_title({"none": True})

    if df.shape[1] > 1:
        chart.set_legend(
            {
                "font": {"name": "Arial", "size": 10},
                "position": "bottom",
                "layout": {"x": 0, "y": 0.85, "height": 0.15, "width": 1},
            }
        )

    #         max_chars = np.max([len(c) for c in df.columns])
    #         min_width = 0.075
    #         width = min_width + max_chars*0.01

    #         chart.set_legend({'font':  {'name': 'Arial', 'size': 10},
    #                           'layout': {'x': 1- width,
    #                                     'y':      0,
    #                                     'height': 1,
    #                                     'width':width
    #                                    }})
    else:
        chart.set_legend({"visble": False})
        chart.set_legend({"position": "none"})

    # Insert the chart into the worksheet....this probably should depend on the size of the dataframe

    if to_combine:
        worksheet.insert_chart("K22", chart)
    else:
        worksheet.insert_chart("K2", chart)


@catch_errors
def create_plot_1a(c):
    ### Plot 1: Generation stacks for national days of interest

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
    model_regs = reg_ids + ["JVB", "SUM", "IDN"]

    doi_summary = pd.read_csv(os.path.join(c.DIR_05_2_TS_OUT, '11a_days_of_interest_summary.csv'),
                              index_col=0,
                              parse_dates=True)

    doi_periods = [doi for doi in doi_summary.index if "time" in doi]
    doi_names = [doi for doi in doi_summary.index if "time" not in doi]

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

    use_reg_ts = c.o.node_df[c.o.node_df.property == 'Unserved Energy'].groupby(
        ['model'] + c.GEO_COLS + ['timestamp']).agg({'value': 'sum'}).compute().unstack(
        level=c.GEO_COLS)
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
            gen_stack_doi = gen_stack_doi.set_index(["model", "Region", "timestamp"])

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

                    gen_stack_doi_reg = gen_stack_doi_reg.drop(columns=['Island', 'Subregion'])
                    write_xlsx_stack(df=gen_stack_doi_reg,
                                     writer=writer,
                                     sheet_name=reg,
                                     palette=stack_palette)
                    print(f'Created sheet "{reg}" in {file_path}.')


def create_plot_1b(c):
    ### Plot 1b: Generation stacks for national days of interest a specified reference model

    load_by_reg = c.o.node_yr_df[c.o.node_yr_df.property == 'Load'].groupby(
        ['model', 'timestamp'] + c.GEO_COLS).sum().value

    reg_ids = list(np.unique(np.append(
        load_by_reg.unstack(c.GEO_COLS).droplevel(level=[c for c in c.GEO_COLS if c != 'Region'], axis=1).replace(0,
                                                                                                                  np.nan).dropna(
            how='all', axis=1).columns,
        gen_by_tech_reg.droplevel(level=[c for c in c.GEO_COLS if c != 'Region'], axis=1).replace(0, np.nan).dropna(
            how='all', axis=1).columns)))

    model_regs = reg_ids + ["JVB", "SUM", "IDN"]

    ### Ref model is based on highest USE....this can be changed/even selected via drop-down menu, etc.
    ref_model = use_reg_ts.groupby("model").sum().idxmax().iloc[0]
    ####|
    doi_periods = [doi for doi in doi_summary.index if "time" in doi]
    doi_names = [doi for doi in doi_summary.index if "time" not in doi]

    for i, p in enumerate(doi_periods):
        doi = doi_summary.loc[p]
        doi_name = doi_names[i]

        for m in model_names:
            save_dir_model = os.path.join(save_dir_plots, m)
            if os.path.exists(save_dir_model) is False:
                os.mkdir(save_dir_model)

            gen_stack = gen_stack_by_reg.loc[ix[m, :, :], :]
            toi_ref = doi.loc[ref_model]

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
            #         shutil.copyfile(fig_template_path, fig_path)

            with pd.ExcelWriter(fig_path, engine="xlsxwriter") as writer:
                ## ExcelWriter for some reason uses writer.sheets to access the sheet.
                ## If you leave it empty it will not know that sheet Main is already there
                ## and will create a new sheet.

                for reg in model_regs:
                    gen_stack_doi_reg = gen_stack_doi.loc[ix[:, reg, :], :].droplevel(
                        [0, 1]
                    )
                    write_xlsx_stack(
                        df=gen_stack_doi_reg,
                        writer=writer,
                        sheet_name=reg,
                        palette=stack_palette,
                    )


def get_plot_data(c):
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
    ### Plot 2: Annual summary plots by columnn

    fig_path = os.path.join(c.DIR_05_3_PLOTS, "plot2_annual_summary_plots.xlsx")

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
    Status: Could work, but can't be run because 04 interval output is missing
    """
    ### Plot 6: Cost savings plots by reference model

    ### Get rid of cofiring if any for the purpose of comparison
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


def plot_7(c):
    """
    Status:
    Plot7: CO2 savings plots by reference model
    """
    # Get rid of cofiring if any for the purpose of comparison
    co2_by_tech_reg = c.o.em_gen_yr_df[c.o.em_gen_yr_df.parent.str.contains('CO2') &
                                       (c.o.em_gen_yr_df.property == 'Production')] \
        .groupby(['model'] + c.GEO_COLS + ['Category']) \
        .agg({'value': 'sum'}).compute()
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
        ['model', 'parent'] + c.GEO_COLS + ['Category']).agg({'value': 'sum'}).reset_index().compute()

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
