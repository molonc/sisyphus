import os
import base64
import pandas as pd


def encode_as_base64(filepath):
    '''
    Encode image in base64.
    '''
    with open(filepath, "rb") as image_file:
        encoded_string = base64.b64encode(image_file.read())
    return encoded_string.decode("utf-8")


def pretty():
    '''
    Style table in HTML format.
    '''
    def hover(hover_color="#e8e8e8"):
        '''
        Change row background color when hovered.
        '''
        return dict(selector="tr:hover", props=[("background-color", "%s" % hover_color)])

    return [
        hover(),
        dict(selector="th",
             props=[
                 ('font-size', '14pt'),
                 ("font-family", "Helvetica"),
                 ("color", 'black'),
                 ('background-color', 'rgb(232, 232, 232)'),
                 ("text-align", "right"),
                 ("font-weight", "normal"),
                 ("padding", "10px"),
             ]),
        dict(selector="td",
             props=[
                 ("font-family", "Helvetica"),
                 ("text-align", "right"),
                 ('font-size', '14pt'),
                 ("padding", "10px"),
             ]),
        dict(selector="tr", props=[
            ("line-height", "12px"),
            ("padding", "10px"),
        ]),
    ]


def rename_metrics(df, rowNames, metrics):
    '''
    Rename the rows of dataframe.

    :params
        df:         dataframe object
        rowNames:   original row names
        metrics:    new row names
    :return
        modified df
    '''
    rename_pairs = {original: new for original, new in zip(rowNames, metrics)}
    df = df.rename(index=rename_pairs)
    return df


def generate_html(library_id, output_dir):
    '''
    Generate html report for a tenx library.

    :params
        library_id: library id of the library of interest
        output_dir: path to output
    :return
        None
    '''
    print("Generating html report...")

    # Paths to pngs and csvs
    csv = output_dir + "/libraries/" + library_id + "/QC_" + library_id + ".csv"

    plots = output_dir + "/libraries/" + library_id + "/QC_plots_" + library_id + ".png",
    scatter = output_dir + "/libraries/" + library_id + "/QC_scatter_" + library_id + ".png"
    pngs = [plots, scatter]

    pngs = [
        os.path.join(
            output_dir,
            "libraries",
            library_id,
            f"QC_plots_{library_id}.png",
        ),
        os.path.join(
            output_dir,
            "libraries",
            library_id,
            f"QC_scatter_{library_id}.png",
        )
    ]

    html_file = output_dir + "/libraries/" + library_id + "/QC_report_" + library_id + ".html"

    # fetch library id and number of cells before truncating index column
    temp_df = pd.read_csv(csv)
    num_cells = temp_df["total_num_cells"].iloc[0]
    del temp_df

    # define how to split up tables. by default, the csv is split into 4 tables for ease of viewing
    # define number of rows you wish to see in each table
    len1 = 3
    len2 = 6
    len3 = 4
    len4 = 4

    usecols1 = [_ for _ in range(len1 + 2)]
    usecols2 = [0] + [_ for _ in range(len1 + 1, len1 + len2 + 2)]
    usecols3 = [0] + [_ for _ in range(len1 + len2 + 1, len1 + len2 + len3 + 2)]
    usecols4 = [0] + [_ for _ in range(len1 + len2 + len3 + 1, len1 + len2 + len3 + len4 + 2)]

    # work-around to truncate index column
    df1 = pd.read_csv(csv, usecols=usecols1, index_col=0)
    df1 = df1.transpose().iloc[1:]
    print(df1)

    df2 = pd.read_csv(csv, usecols=usecols2, index_col=0)
    df2 = df2.transpose().iloc[1:]
    print(df2)

    df3 = pd.read_csv(csv, usecols=usecols3, index_col=0)
    df3 = df3.transpose().iloc[1:]
    print(df3)

    df4 = pd.read_csv(csv, usecols=usecols4, index_col=0)
    df4 = df4.transpose().iloc[1:]
    print(df4)

    html_elements = []

    # define style for the report
    html_elements.append("<style>")
    html_elements.append(".all {font-family:'Arial'}")
    html_elements.append("</style>")

    # header of each file
    html_elements.append("<h3 class='all'>Library ID: " + library_id + "</h3>")
    html_elements.append("<p class='all'>Total number of cells: " + str(num_cells) + "</p>")
    html_elements.append(
        "<p class='all'>* Filtering threshold: cells has good quality if (1) % mito genes <= 20%; (2) % ribo genes <= 60%; (3) number of features (genes) >= 1000.</p>"
    )
    html_elements.append("<p class='all'>** Values shown with 2 decimal places.</p>")
    html_elements.append("<p class='all'>*** Values shown floored to integer.</p>")

    # content
    metrics = [
        "Total number of cells in whole dataset", "* Total number of filtered cells",
        "* Percentage of good quality cells", "Number of cells with mito <= 20%",
        "Percentage of cells with mito <= 20 ", "Number of cells with mito > 20%",
        "Percentage of cells with mito > 20%", "Number of cells with ribo <= 60%",
        "Percentage of cells with ribo <= 60%", "*** Median number of genes per cell in whole dataset",
        "*  *** Median number of genes per cell in filtered dataset",
        "*** Mean number of genes per cell in whole dataset",
        "*  *** Mean number of genes per cell in filtered dataset", "*** Mean reads per cell in whole dataset",
        "*  *** Mean reads per cell in filtered dataset", "Max reads per cell in whole dataset",
        "* Max reads per cell in filtered dataset"
    ]
    df1 = rename_metrics(df1, list(df1.index.values), metrics[:len1])
    df2 = rename_metrics(df2, list(df2.index.values), metrics[len1:len1 + len2])
    df3 = rename_metrics(df3, list(df3.index.values), metrics[len1 + len2:len1 + len2 + len3])
    df4 = rename_metrics(df4, list(df4.index.values), metrics[len1 + len2 + len3:])

    dfs = [df1, df2, df3, df4]
    headers = [
        "Summary", "Mito and Ribo cell details", "Genes per cell details", "Reads per cell details", "QC Violin Plots",
        "QC Scatter Plot"
    ]
    for df, header in zip(dfs, headers[0:4]):
        html_elements.append("<h3 class='all'>{}</h3>\n".format(header))
        df_table = df.style.set_precision(2).set_table_styles(pretty()).render()
        html_elements.append(df_table)

    for header, png in zip(headers[1:], pngs):
        png = encode_as_base64(png)
        html_elements.append("<h3 class='all'>{}</h3>\n".format(header))
        html_elements.append('<img src="data:image/png;base64,{}" alt="qc.png" scale="0">'.format(png))

    # write to html
    with open(html_file, 'w') as html_out:
        for element in html_elements:
            html_out.write(element)
            html_out.write('\n')


pngs = [[('/datadrive/qc/libraries/SCRNA10X_SA_CHIP0201_004/QC_plots_SCRNA10X_SA_CHIP0201_004.png', ),
         '/datadrive/qc/libraries/SCRNA10X_SA_CHIP0201_004/QC_scatter_SCRNA10X_SA_CHIP0201_004.png']]
