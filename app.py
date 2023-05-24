import polars as pl
import numpy as np


import dash
from plotly import graph_objects as go

from dash import dcc, html, Input, Output, State, ALL, ctx, no_update
from dash.exceptions import PreventUpdate
from dash.long_callback import DiskcacheLongCallbackManager

import dash_ag_grid as dag
import dash_mantine_components as dmc

from utils.data_utils import (
    scan_ldf,
    aggregate_on_trip_distance_time,
    aggregate_on_pay_tip,
)
from utils.chart_utils import generate_scatter_go, generate_data_shader_plot
from utils.schema_utils import VISIBLE_COLUMNS, FILTER_TYPE_DICT
from utils import layout_utils
from utils import style_utils


import diskcache

cache = diskcache.Cache("./cache")
long_callback_manager = DiskcacheLongCallbackManager(cache)


app = dash.Dash(
    __name__,
    external_stylesheets=[
        "assets/styles.css",
    ],
)
app.title = "Dash app"
server = app.server  # expose server variable for Procfile


app.layout = html.Div(
    style=style_utils.APP_STYLE,
    children=[
        dmc.Group(
            style={'border-bottom': 'solid', 'bottom-border-color': '#FFFFFF',},
            children=[
                dmc.Image(src=app.get_asset_url("plotly-dark.avif"), width=100),
                dmc.Title("NYC Taxi Data", style={"margin-top": "1em"}),
            ],
        ),
        dmc.Center(
            style={'margin-top': "2em"},
            children=dmc.Title(
                "Browse Data",
            ),
        ),
        dmc.Menu(
            id="columns-selection-menu",
            clickOutsideEvents=False,
            closeDelay=False,
            closeOnClickOutside=False,
            closeOnEscape=False,
            closeOnItemClick=False,
            styles={
                "dropdown": {
                    "background-color": "#2d3038",
                    "z-index": 500,
                }
            },
            children=[
                dmc.MenuTarget(
                    dmc.Button(
                        "select columns",
                        id="open-modal-bttn",
                        variant="outline",
                        style={
                            "border-color": "#FFD15F",
                            "color": "#FFD15F",
                            "margin-bottom": "1em",
                        },
                    ),
                ),
                dmc.MenuDropdown(
                    children=[
                        dmc.MenuItem(
                            style={
                                "padding": "0",
                            },
                            children=layout_utils.render_columns_modal(),
                        ),
                    ],
                ),
            ],
        ),
        dmc.LoadingOverlay(
            overlayColor="black",
            loaderProps={"variant": "dots"},
            children=[
                dag.AgGrid(
                    id="infinite-grid",
                    rowModelType="infinite",
                    enableEnterpriseModules=True,
                    columnDefs=layout_utils.generate_column_defintions(),
                    pagination=True,
                    paginationPageSize=100,
                    className="ag-theme-alpine-dark",
                    defaultColDef={"filter": True},
                ),
                dcc.Store(id="filter-model"),
            ],
        ),
        dmc.Center(
            dmc.Title(
                "Plots",
            ),
            style={"margin-bottom": "1em", "margin-top": "2em",},
        ),
        dmc.Group(
            [
                dmc.Button(
                    "re-run vizualizations",
                    id="viz-bttn",
                    variant="outline",
                    style={
                        "border-color": "#FFD15F",
                        "color": "#FFD15F",
                        "margin-bottom": "1em",
                    },
                ),
            ],
            style={"margin-top": "1em"},
        ),
        dmc.LoadingOverlay(
            overlayColor="black",
            loaderProps={"variant": "dots"},
            children=[
                dmc.Group(
                    [
                        html.Div(
                            dcc.Graph(id="mileage-time-graph", style={"height": "400px"}),
                        ),
                        html.Div(
                            dcc.Graph(
                                id="request-dropoff-graph",
                                style={"height": "400px"},
                            ),
                        ),
                    ]
                )
            ],
        ),
    ],
)


@app.callback(
    Output("infinite-grid", "columnDefs"),
    Output("columns-selection-menu", "opened"),
    Input("open-modal-bttn", "n_clicks"),
    Input("apply-bttn", "n_clicks"),
    State("columns-selection-menu", "opened"),
    State({"type": "checkbox", "field": ALL}, "checked"),
    State({"type": "checkbox", "field": ALL}, "label"),
    prevent_initial_call=True,
    manager=long_callback_manager,
)
def update_columns(
    click1,
    click2,
    opened,
    columns_checked,
    columns_label,
):
    if ctx.triggered_id == "open-modal-bttn":
        return no_update, not opened
    column_list = []
    for i, col in enumerate(columns_label):
        if columns_checked[i]:
            column_list.append(col)
    columnDefs = layout_utils.generate_column_defintions(column_list)
    return columnDefs, False


@app.callback(
    Output("infinite-grid", "getRowsResponse"),
    Output("filter-model", "data"),
    Input("infinite-grid", "getRowsRequest"),
    Input("infinite-grid", "columnDefs"),
    manager=long_callback_manager,
)
def infinite_scroll(request, columnDefs):
    if request is None:
        raise PreventUpdate
    columns = [col["field"] for col in columnDefs]
    ldf = scan_ldf(filter_model=request["filterModel"], columns=columns)
    df = ldf.collect()
    partial = df.slice(request["startRow"], request["endRow"]).to_pandas()

    return {
        "rowData": partial.to_dict("records"),
        "rowCount": len(df),
    }, request["filterModel"]


@app.callback(
    Output("mileage-time-graph", "figure"),
    Output("request-dropoff-graph", "figure"),
    State("filter-model", "data"),
    Input("viz-bttn", "n_clicks"),
    State("infinite-grid", "columnDefs"),
    manager=long_callback_manager,
)
def visualize(filter_model, n_clicks, columnDefs):
    columns = ["trip_time", "trip_miles", "driver_pay", "tips"]
    if filter_model:
        columns_to_filter = [col for col in filter_model]
        columns = list(set([*columns_to_filter, *columns]))
    ldf = scan_ldf(filter_model=filter_model, columns=columns)
    df = ldf.collect()
    if len(df) > 20000:
        agg1 = aggregate_on_trip_distance_time(ldf)
        if len(agg1) < 20000:
            fig1 = generate_scatter_go(agg1, "rounded_time", "rounded_miles")
        else:
            fig1 = generate_data_shader_plot(agg1, "rounded_time", "rounded_miles")

        agg2 = aggregate_on_pay_tip(ldf)
        if len(agg2) < 25000:
            fig2 = generate_scatter_go(agg2, "rounded_driver_pay", "rounded_tips")
        else:
            fig2 = generate_data_shader_plot(agg2, "rounded_driver_pay", "rounded_tips")
        return fig1, fig2
    else:
        fig1 = generate_scatter_go(df, "trip_time", "trip_miles")
        fig2 = generate_scatter_go(df, "driver_pay", "tips")
        return fig1, fig2


if __name__ == "__main__":
    app.run_server(debug=True, threaded=False)
