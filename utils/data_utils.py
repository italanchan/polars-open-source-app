import polars as pl
import os

if "DASH_APP_NAME" in os.environ:
    filepath = os.path.join(os.sep, "mount", "fhvhv_data.arrow")
    DATA_SOURCE = pl.scan_ipc(filepath)
else:
    DATA_SOURCE = pl.scan_ipc("data/fhvhv_data_mini.arrow")


column_names = DATA_SOURCE.columns


def scan_ldf(
    filter_model=None,
    columns=None,
    sort_model=None,
):
    ldf = DATA_SOURCE
    ldf = ldf.with_columns(pl.col("request_datetime").cast(pl.Date).alias("event_date"))
    if columns:
        ldf = ldf.select(columns)
    if filter_model:
        expression_list = make_filter_expr_list(filter_model)
        if expression_list:
            filter_query = None
            for expr in expression_list:
                if filter_query is None:
                    filter_query = expr
                else:
                    filter_query &= expr
            ldf = ldf.filter(filter_query)
    return ldf


def make_filter_expr_list(filter_model):
    expression_list = []
    for col_name in filter_model:
        if "operator" in filter_model[col_name]:
            if filter_model[col_name]["operator"] == "AND":
                expr1 = parse_column_filter(
                    filter_model[col_name]["condition1"], col_name
                )
                expr2 = parse_column_filter(
                    filter_model[col_name]["condition2"], col_name
                )
                expr = expr1 & expr2
            else:
                expr1 = parse_column_filter(
                    filter_model[col_name]["condition1"], col_name
                )
                expr2 = parse_column_filter(
                    filter_model[col_name]["condition2"], col_name
                )
                expr = expr | expr2
        else:
            expr = parse_column_filter(filter_model[col_name], col_name)
        expression_list.append(expr)

    return expression_list


def get_filter_values(col_name):
    return (
        DATA_SOURCE.select(pl.col(col_name))
        .collect()
        .unique()
        .get_columns()[0]
        .to_list()
    )


def parse_column_filter(filter_obj, col_name):
    """Build a polars filter expression based on the filter object"""
    if filter_obj["filterType"] == "set":
        expr = None
        for val in filter_obj["values"]:
            expr |= pl.col(col_name).cast(pl.Utf8).cast(pl.Categorical) == val
    else:
        if filter_obj["filterType"] == "date":
            crit1 = filter_obj["dateFrom"]

            if "dateTo" in filter_obj:
                crit2 = filter_obj["dateTo"]

        else:
            if "filter" in filter_obj:
                crit1 = filter_obj["filter"]
            if "filterTo" in filter_obj:
                crit2 = filter_obj["filterTo"]

        if filter_obj["type"] == "contains":
            lower = (crit1).lower()
            expr = pl.col(col_name).str.to_lowercase().str.contains(lower)

        elif filter_obj["type"] == "notContains":
            lower = (crit1).lower()
            expr = ~pl.col(col_name).str.to_lowercase().str.contains(lower)
        elif filter_obj["type"] == "startsWith":
            lower = (crit1).lower()
            expr = pl.col(col_name).str.starts_with(lower)

        elif filter_obj["type"] == "notStartsWith":
            lower = (crit1).lower()
            expr = ~pl.col(col_name).str.starts_with(lower)

        elif filter_obj["type"] == "endsWith":
            lower = (crit1).lower()
            expr = pl.col(col_name).str.ends_with(lower)

        elif filter_obj["type"] == "notEndsWith":
            lower = (crit1).lower()
            expr = ~pl.col(col_name).str.ends_with(lower)

        elif filter_obj["type"] == "blank":
            expr = pl.col(col_name).is_null()

        elif filter_obj["type"] == "notBlank":
            expr = ~pl.col(col_name).is_null()

        elif filter_obj["type"] == "equals":
            expr = pl.col(col_name) == crit1

        elif filter_obj["type"] == "notEqual":
            expr = pl.col(col_name) != crit1

        elif filter_obj["type"] == "lessThan":
            expr = pl.col(col_name) < crit1

        elif filter_obj["type"] == "lessThanOrEqual":
            expr = pl.col(col_name) <= crit1

        elif filter_obj["type"] == "greaterThan":
            expr = pl.col(col_name) > crit1

        elif filter_obj["type"] == "greaterThanOrEqual":
            expr = pl.col(col_name) >= crit1

        elif filter_obj["type"] == "inRange":
            if filter_obj["filterType"] == "date":
                expr = (pl.col(col_name) >= crit1) & (pl.col(col_name) <= crit2)
            else:
                expr = (pl.col(col_name) >= crit1) & (pl.col(col_name) <= crit2)
        else:
            None

    return expr


def aggregate_on_trip_distance_time(ldf):
    results = (
        ldf.with_columns(
            [
                pl.col("trip_miles").round(1).alias("rounded_miles"),
                pl.col("trip_time")
                .apply(lambda x: round(x / 100, 0) * 100)
                .alias("rounded_time"),
            ]
        )
        .collect()
        .groupby(["rounded_miles", "rounded_time"])
        .count()
    )
    return results


def aggregate_on_pay_tip(ldf):
    results = (
        ldf.with_columns(
            [
                pl.col("driver_pay").round(0).alias("rounded_driver_pay"),
                pl.col("tips").round(0).alias("rounded_tips"),
            ]
        )
        .collect()
        .groupby(["rounded_driver_pay", "rounded_tips"])
        .count()
    )
    return results
