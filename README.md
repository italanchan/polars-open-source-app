# Polars Dash App

This Dash app was created to showcase how to use the Polars library in a Dash Application with large a dataset constructed fomr the NYC taxi data.

Useful links:
- [Dash Python User Guide](https://dash.plotly.com/)
- [Polars python API](https://pola-rs.github.io/polars/py-polars/html/reference/)
- [NYC taxi data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
  
## The Data



## Development

Install development-specific requirements by running

```
pip install -r requirements-dev.txt
```

## Running this application



1. Install the Python dependencies

    ```
    pip install -r requirements.txt 
    ```

2. Convert one or multiple of the parquet files named fhvhv_tripdata_YYYY-MM.parquet from the [High Volume for Hire NYC Taxi Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) to an arrow file called fhvhv_data.arrow and add to data folder in project directory


    ```python
    import polars as pl
    df = pl.read_parquet("fhvhv_tripdata_YYYY-MM.parquet")
    df.write_ipc("data/fhvhv_data.arrow")
    ```

3. Run the following command:

    ```python
    python app.py
    ```





