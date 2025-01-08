"""
TODO DOCSTRING
"""
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np

from matplotlib import colors

from solution_file_processing import log

print = log.info

np.random.seed(sum(map(ord, 'calplot')))

IEA_PALETTE = {'grey5': '#f2f2f2', 
               'grey10': '#e6e6e6', 
               'pl': '#b187ef', 
               'bl': '#49d3ff', 
               'tl': '#00e0e0',
               'gl': '#68f394', 
               'yl': '#fff45a',
               'ol': '#ffb743', 
               'rl': '#ff684d', 
               'grey40': '#949494',
               'grey50': '#6f6f6f',
               'p': '#af6ab1', 
               'b':'#3e7ad3', 
               't': '#00ada1', 
               'g': '#1dbe62', 
               'y': '#fed324',
               'o': '#f1a800', 
               'r': '#e34946', 
               'grey20': '#afafaf', 
               'black': '#000000', 
               'white': '#ffffff',
               'iea_b': '#0044ff', 
               'iea_b50': '#80a2ff', 
               'brown':'#6E4F00'}

EXTENDED_PALETTE = dict(
    {'{}'.format(i): plt.matplotlib.colors.rgb2hex(plt.cm.get_cmap('tab20b').colors[i]) for i in np.arange(0, 20)},
    **{'{}'.format(i + 20): plt.matplotlib.colors.rgb2hex(plt.cm.get_cmap('tab20c').colors[i]) for i in
       np.arange(0, 20)})

### For overflow, i.e. things that go beyond the 16 or so colors of IEA palette.
IEA_PALETTE_PLUS = dict(IEA_PALETTE, **EXTENDED_PALETTE)

# model_palette = {'2019':'rl', '2025 Base':'o', '2025 SolarPlus':'bl', '2025 SolarPlus Lite':'pl',  '2025 SolarPlus Extra':'gl' }


### We should add load vs customer load (to see charging effect),
### Similarly could add load + exports to get effective load

# reg_palette = {'APB_BALI': 'bl', 'APB_JBR': 'b', 'APB_JKB': 'pl', 'APB_JTD': 't', 'APB_JTM': 'y'}
# subreg_palette = {'BAL': 'bl', 'BNT': 'b', 'DIY': 'gl', 'JBR': 't', 'JKT': 'ol', 'JTE': 'pl', 'JTM': 'grey20',
#                   'SMN': 'r', 'SMS': 'tl'}
# isl_palette = {'JVB': 'bl', 'SUM': 'gl', 'KLM': 'rl', 'SLW': 'ol', 'MPN': 'pl', 'IDN': 'yl'}

TECH_PALETTE = {'Coal': 'grey20', 'Abated coal': 'grey10', 'Cofiring': 'grey10', 'Gas': 'p', 'Abated gas': 'p',
                'Oil': 'grey50', 'Hydro': 'bl', 'Geothermal': 'r', 'Bioenergy': 'gl', 'Solar': 'y', 'Wind': 'g',
                'Fuel Cell': 't', 'Other': 't', 'Battery': 'b', 'Storage': 'b'}



STACK_PALETTE = { 'Imports':'white','Nuclear': 'p', 'Geothermal': 'r', 'Bioenergy': 'gl', 'Coal': 'brown', 'Cofiring': 'grey5', 'Abated coal': 'rl',
                 'Gas': 'grey20', 'Abated gas': 'pl', 'Hydro': 'bl', 'Oil': 'grey50', 'Exports': 'black', 'Other RE': 'tl', 'Other': 't',
                 'Fuel Cell': 'tl', 'Storage': 'b', 'Battery': 'b', 'DSM': 'black', 
                  'Wind': 'g', 'Solar': 'y','Load w/ Exports':'iea_b50','Total Load': 'black', 'Load2': 'white', 'Net Load w/ Exports':'iea_b',
                 'Net Load': 'r', 
                 'Curtailment': 'yl', 'Unserved Energy': 'r', 'Underlying Load': 'p', 'Storage Load': 'grey50', 
                 }

SERVICES_PALETTE = {'Coal':'brown','Gas':'grey20','Oil':'grey50','Nuclear':'p','Bioenergy & other renewables':'gl','Geothermal':'o','Variable renewables':'t',
'Hydro':'bl','Storage':'b','Demand response':'white'}


IEA_PALETTE_L8 =  ['rl', 'ol', 'bl',  'pl', 'gl', 'grey10', 'yl',  'tl' ]
IEA_PALETTE_D8 = ['r', 'o', 'b',  'p', 'g', 'grey50', 'y',  't' ]
IEA_PALETTE_16a = IEA_PALETTE_L8 + IEA_PALETTE_D8 
IEA_PALETTE_14 = ['rl', 'ol', 'bl', 'gl', 'pl', 'grey10', 'y', 'tl', 'g', 't', 'b', 'grey50', 'yl', 'r', 'p']
IEA_PALETTE_16b = [ 'r', 'o', 'b',  'p', 'g', 'y',  't', 'bl', 'gl', 'grey10', 'yl', 'rl', 'ol',   'pl', 'tl', 'grey50']
IEA_PALETTE_16 = [ 'bl', 'gl','rl', 'ol', 'pl', 'grey10', 'yl',  'b', 'r', 'o','t', 'p', 'g', 'y', 'tl',   'grey50']


IEA_CMAP_L8 = colors.ListedColormap([IEA_PALETTE[c] for c in IEA_PALETTE_L8])
IEA_CMAP_D8 = colors.ListedColormap([IEA_PALETTE[c] for c in IEA_PALETTE_D8])
IEA_CMAP_16 = colors.ListedColormap([IEA_PALETTE[c] for c in IEA_PALETTE_16])
IEA_CMAP_14 = colors.ListedColormap([IEA_PALETTE[c] for c in IEA_PALETTE_14])

tab20bc = colors.ListedColormap([EXTENDED_PALETTE[i] for i in EXTENDED_PALETTE.keys()])


def write_xlsx_bar(
        df,
        writer,
        excel_file=None,
        sheet_name="Sheet1",
        palette=IEA_PALETTE_16,
        subtype="stacked",
        units="",
        total_scatter_col=None,
        to_combine=False,
        right_ax=None,
):
    cm_to_pixel = 37.7953

    if type(palette) == dict:
        ## Sort columns by order in palettes described above
        sort_cols = [c for c in palette.keys() if c in df.columns] + [
            c for c in df.columns if c not in palette.keys()
        ]
        sort_index = [i for i in palette.keys() if i in df.index] + [
            i for i in df.index if i not in palette.keys()
        ]
    else:
        ## The use of palette as a list is poor practice and should
        ## be avoided. This is a temporary fix to allow for the code to run.
        ## This should be fixed in all plot functions
        palette = {c: palette[i] for i, c in enumerate(df.columns)}
        sort_cols = df.columns
        sort_index = [i for i in df.index]

    if type(df.index == pd.Index):
        df = df.loc[sort_index, sort_cols]
    else:
        df = df.loc[:, sort_cols]

    if excel_file:
        writer = pd.ExcelWriter(excel_file, engine="xlsxwriter") # pylint: disable=abstract-class-instantiated

    ### Whether we caluclate the scatter col or not. Should probably rename the variable from total_col, as its not always a total
    if (total_scatter_col is not None) & (total_scatter_col not in df.columns):
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
                fill_colour = IEA_PALETTE_PLUS[
                    palette[df.columns[col_num - df.index.nlevels]]
                ]
            except KeyError:
                fill_colour = IEA_CMAP_16.colors[col_num - df.index.nlevels]

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

    chart.set_x_axis(
        {
            "num_font": {"name": "Arial", "size": 10},
            "line": {"color": "black"},
            "label_position": label_position,
        }
    )

    chart.set_y_axis(
        {
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


def write_xlsx_column(
        df: pd.DataFrame,
        writer: pd.ExcelWriter,
        excel_file: str = None,
        sheet_name: str = "Sheet1",
        palette: dict = IEA_PALETTE_16,
        type: str = "column",
        subtype: str = "stacked",
        units: str = "",
        total_scatter_col: str = None,
        to_combine: bool = False,
        right_ax: str = None,
        desc: str = None,
):
    cm_to_pixel = 37.7953

    if isinstance(palette, dict):
        ## Sort columns by order in palettes described above
        sort_cols = [c for c in palette.keys() if c in df.columns] + [
            c for c in df.columns if c not in palette.keys()
        ]
        sort_index = [i for i in palette.keys() if i in df.index] + [
            i for i in df.index if i not in palette.keys()
        ]
    else:
        ## The use of palette as a list is poor practice and should
        ## be avoided. This is a temporary fix to allow for the code to run.
        ## This should be fixed in all plot functions
        palette = {c: palette[i] for i, c in enumerate(df.columns)}
        sort_cols = df.columns
        sort_index = [i for i in df.index]

    if isinstance(df.index, pd.Index):
        df = df.loc[sort_index, sort_cols]
    else:
        df = df.loc[:, sort_cols]

    if excel_file:
        writer = pd.ExcelWriter(excel_file, engine="xlsxwriter") # pylint: disable=abstract-class-instantiated

    ### Whether we caluclate the scatter col or not. Should probably rename the variable from total_col, as its not always a total
    if (subtype == "stacked") & (('Total' in df.columns)|('Overall' in df.columns)):
        if 'Total' in df.columns:
            total_scatter_col = 'Total'
        else:
            total_scatter_col = 'Overall'

    if (total_scatter_col is not None) & (total_scatter_col not in df.columns):
        df.loc[:, total_scatter_col] = df.sum(axis=1)

    df.to_excel(writer, sheet_name=sheet_name)
    pd.DataFrame(data=[desc],index=['Description:']).to_excel(writer, sheet_name=sheet_name, startrow=df.shape[0] + 2)


    # Access the XlsxWriter workbook and worksheet objects from the dataframe.
    workbook = writer.book
    worksheet = writer.sheets[sheet_name]

    if units == "%":
        num_fmt = "0%"
        units = ""
    else:
        num_fmt = "# ###"

    # Create a chart object.
    chart = workbook.add_chart({"type": type, "subtype": subtype})
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
                fill_colour = IEA_PALETTE_PLUS[
                    palette[df.columns[col_num - df.index.nlevels]]
                ]
            except KeyError:
                fill_colour = IEA_CMAP_16.colors[col_num - df.index.nlevels]

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

    if type=="bar":
        y_gridlines = {"visible": False}
    else:
        y_gridlines = {"visible": True, "line": {"width": 1, "color": "#d9d9d9"}}

    chart.set_y_axis(
        {
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
            "major_gridlines": y_gridlines,
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
                "major_gridlines": y_gridlines,
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

        if type == "column":
            legend_layout = {"x": 1 - width, "y": 0, "height": 1, "width": width}
        else:
            legend_layout = {"x": 0, "y": 0, "height": 0.1, "width": 1}


        chart.set_legend(
            {
                "font": {"name": "Arial", "size": 10},
                "layout": legend_layout,
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

    chart.set_x_axis(
        {
            "num_font": {"name": "Arial", "size": 10},
            "line": {"color": "black"},
            "label_position": label_position,
        }
    )

    chart.set_y_axis(
        {
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
        palette=STACK_PALETTE,
        units="GW",
        to_combine=False,
):
    cm_to_pixel = 37.7953

    ## Sort columns by order in palettes described above
    sort_cols = [i for i in palette.keys() if i in df.columns] + \
                 [j for j in df.columns if j not in palette.keys()]
    df = df.loc[:, sort_cols]

    if excel_file:
        writer = pd.ExcelWriter(excel_file, engine="xlsxwriter") # pylint: disable=abstract-class-instantiated

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
    # Col_num iterates from first data column, which varies if it is multiindex columns or not
    # By iterating from df.index.nlevels, we avoid adding any part of the index while ensuring we had all data from the dataframe 
    # Note this is because df.shape ignores index columns, but the items added to the chart are from the Excel columns, which are included!!
    for col_num in np.arange(0, df.shape[1]):

        # As Excel-indexed columns include the index columns, we need to have two column variables
        # col_num is used for reference to DF while excel_col_num is for reference to columns written in the Excel sheet
        excel_col_num = col_num + df.index.nlevels

        try:
            fill_colour = IEA_PALETTE_PLUS[palette[df.columns[col_num]]]
        except KeyError:
            print("Non-specified colour for: {}".format(df.columns[col_num]))
            try:
                fill_colour = IEA_CMAP_16.colors[col_num]
            except IndexError:
                print("Too many columns for colour palette, starts repeating.")
                fill_colour = IEA_CMAP_16.colors[col_num - 16]

        if df.columns[col_num] == "Load2":
            chart.add_series(
                {
                    "name": [sheet_name, 0, excel_col_num],
                    "categories": [sheet_name, 1, 0, df.shape[0], 0],
                    "values": [sheet_name, 1, excel_col_num, df.shape[0], excel_col_num],
                    "fill": {"none": True},
                    "border": {"none": True},
                    "y2_axis": True,
                }
            )

            leg_del_idx = [int(excel_col_num - 1)] #Not working as we dont have all the variables in the graph itself

        elif df.columns[col_num] == "Curtailment":
            chart.add_series(
                {
                    "name": [sheet_name, 0, excel_col_num],
                    "categories": [sheet_name, 1, 0, df.shape[0], 0],
                    "values": [sheet_name, 1, excel_col_num, df.shape[0], excel_col_num],
                    "pattern": {
                        "pattern": "light_upward_diagonal",
                        "fg_color": IEA_PALETTE["y"],
                        "bg_color": IEA_PALETTE["r"],
                    },
                    "border": {"none": True},
                    "y2_axis": True,
                }
            )

        
        elif df.columns[col_num] == "Imports":
            chart.add_series(
                {
                    "name": [sheet_name, 0, excel_col_num],
                    "categories": [sheet_name, 1, 0, df.shape[0], 0],
                    "values": [sheet_name, 1, excel_col_num, df.shape[0], excel_col_num],
                    "pattern": {
                        "pattern": "percent_5",
                        "fg_color": IEA_PALETTE["black"],
                        "bg_color": IEA_PALETTE["white"],
                    },
                    "border": {"none": True},
                    "y2_axis": False,
                }
            )
        
        elif df.columns[col_num] == "Exports":
            continue

        elif df.columns[col_num] == "Total Load":
            chart2.add_series(
                {
                    "name": [sheet_name, 0, excel_col_num],
                    "categories": [sheet_name, 1, 0, df.shape[0], 0],
                    "values": [sheet_name, 1, excel_col_num, df.shape[0], excel_col_num],
                    "line": {"width": 0.25, "color": STACK_PALETTE[df.columns[col_num]], "dash_type": "solid"},
                }
            )
        elif df.columns[col_num] == "Underlying Load":
            continue
        elif df.columns[col_num] == "Storage Load":
            continue
        #             chart2.add_series({
        #                 'name':       [sheet_name, 0, col_num],
        #                 'categories': [sheet_name, 1, 0, df.shape[0], 0],
        #                 'values':     [sheet_name, 1, col_num, df.shape[0], col_num],
        #                 'line': {'width': 1.00, 'color':iea_palette['p'], 'dash_type': 'dash'},
        #             })
        elif df.columns[col_num] == "Net Load":
            chart2.add_series(
                {
                    "name": [sheet_name, 0, excel_col_num],
                    "categories": [sheet_name, 1, 0, df.shape[0], 0],
                    "values": [sheet_name, 1, excel_col_num, df.shape[0], excel_col_num],
                    "line": {
                        "width": 1.00,
                        "color": fill_colour,
                        "dash_type": "dash",
                    },
                }
            )
        elif df.columns[col_num] == "Net Load w/ Exports":
            continue
            # chart2.add_series(
            #     {
            #         "name": [sheet_name, 0, excel_col_num],
            #         "categories": [sheet_name, 1, 0, df.shape[0], 0],
            #         "values": [sheet_name, 1, excel_col_num, df.shape[0], excel_col_num],
            #         "line": {
            #             "width": 1.5,
            #             "color": fill_colour,
            #             "dash_type": "dash",
            #         },
            #     }
            # )
        elif df.columns[col_num] == "Load w/ Exports":
            chart2.add_series(
                {
                    "name": [sheet_name, 0, excel_col_num],
                    "categories": [sheet_name, 1, 0, df.shape[0], 0],
                    "values": [sheet_name, 1, excel_col_num, df.shape[0], excel_col_num],
                    "line": {
                        "width": 1.00,
                        "color": fill_colour,
                        "dash_type": "dash",
                    },
                }
            )
        else:
            chart.add_series(
                {
                    "name": [sheet_name, 0, excel_col_num],
                    "categories": [sheet_name, 1, 0, df.shape[0], 0],
                    "values": [sheet_name, 1, excel_col_num, df.shape[0], excel_col_num],
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
                # "delete_series": leg_del_idx,
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
        palette=IEA_PALETTE_16,
        units="",
        alpha=80,
        to_combine=False,
        markersize=4,
        common_yr=2041,
):
    cm_to_pixel = 37.7953

    if excel_file:
        writer = pd.ExcelWriter(excel_file, engine="xlsxwriter") # pylint: disable=abstract-class-instantiated

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
                fill_colour = IEA_PALETTE_PLUS[
                    palette[df.columns[col_num - df.index.nlevels]]
                ]
            except KeyError:
                fill_colour = IEA_CMAP_16.colors[col_num - df.index.nlevels]

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
        palette=IEA_PALETTE_16,
        units="",
        ldc_idx=None,
        label_position="next_to",
        to_combine=False,
        line_width=1.5,
):
    cm_to_pixel = 37.7953

    if type(palette) == dict:
        ## Sort columns by order in palettes described above
        sort_cols = [c for c in palette.keys() if c in df.columns] + [
            c for c in df.columns if c not in palette.keys()
        ]
        sort_index = [i for i in palette.keys() if i in df.index] + [
            i for i in df.index if i not in palette.keys()
        ]
    else:
        ## The use of palette as a list is poor practice and should
        ## be avoided. This is a temporary fix to allow for the code to run.
        ## This should be fixed in all plot functions
        palette = {c: palette[i] for i, c in enumerate(df.columns)}
        sort_cols = df.columns
        sort_index = [i for i in df.index]

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
        writer = pd.ExcelWriter(excel_file, engine="xlsxwriter") # pylint: disable=abstract-class-instantiated

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
            line_colour = IEA_PALETTE_PLUS[
                palette[df.columns[col_num - df.index.nlevels]]
            ]
        except KeyError:
            line_colour = IEA_CMAP_16.colors[col_num - df.index.nlevels]

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

def write_double_xlsx_column(
        df,
        writer,
        excel_file=None,
        sheet_name="Sheet1",
        palette=IEA_PALETTE_16,
        type="column",
        subtype="stacked",
        units="",
        total_scatter_col=None,
        to_combine=False,
        right_ax=None,
):
    cm_to_pixel = 37.7953


    # Access the XlsxWriter workbook and worksheet objects from the dataframe.
    workbook = writer.book
    worksheet = writer.sheets[sheet_name]

    if units == "%":
        num_fmt = "0%"
        units = ""
    else:
        num_fmt = "# ###"

    # Create a chart object.
    chart = workbook.add_chart({"type": type, "subtype": subtype})
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
                fill_colour = IEA_PALETTE_PLUS[
                    palette[df.columns[col_num - df.index.nlevels]]
                ]
            except KeyError:
                fill_colour = IEA_CMAP_16.colors[col_num - df.index.nlevels]

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