import polars as pl
import datashader as ds
import numpy as np
from plotly import graph_objects as go
from plotly import express as px


def generate_scatter_go(df, x_label, y_label):
    x_l = df.select(pl.col(x_label)).get_columns()[0].to_list()
    y_l = df.select(pl.col(y_label)).get_columns()[0].to_list()

    fig = go.Figure(
        data=go.Scatter(
            x=x_l,
            y=y_l,
            mode="markers",
            marker=dict(line=dict(width=1), color="#FFD15F"),
        )
    )

    fig.update_layout(
        xaxis_title=x_label,
        yaxis_title=y_label,
        paper_bgcolor="#23262E",
        plot_bgcolor="#23262E",
        font=dict(color="#FFFFFF"),
    )
    return fig


def generate_data_shader_plot(df, x_label, y_label):
    cvs = ds.Canvas(plot_width=100, plot_height=100)
    agg = cvs.points(df.to_pandas(), x=x_label, y=y_label)
    zero_mask = agg.values == 0
    agg.values = np.log10(agg.values, where=np.logical_not(zero_mask))
    agg.values[zero_mask] = np.nan
    fig = px.imshow(agg, origin="lower", labels={"color": "Log10(count)"}, color_continuous_scale='solar')
    fig.update_traces(hoverongaps=False)
    fig.update_layout(coloraxis_colorbar=dict(title="Count", tickprefix="1.e",), font=dict(color="#FFFFFF"), paper_bgcolor="#23262E", plot_bgcolor="#23262E",)
    return fig
