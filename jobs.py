import pandas as pd
from dagster import job, op, Field, String, execute_job
from main import query_to_dataframe, getYFData, getFREDData
from db import get_engine
from sqlalchemy import MetaData, Table

@op
def get_symbols() -> pd.DataFrame:
    symbols = query_to_dataframe()
    return symbols

@op
def get_yf_data(symbols) -> pd.DataFrame:
    yf_data = getYFData(symbols)
    return yf_data

@op
def get_fred_data() -> pd.DataFrame:
    fred_data = getFREDData()
    return fred_data

@op(config_schema={"table_name": Field(String)})
def load_to_db(context, df: pd.DataFrame):
    table_name = context.op_config["table_name"]
    if df.empty:
        context.log.info(f"Skipping... Empty {table_name}")
        return
    try:
        engine = get_engine()
        with engine.begin() as conn:
            # Reflect the existing table structure
            metadata = MetaData()
            table = Table(table_name, metadata, autoload_with=engine)
            
            # Delete old data
            conn.execute(table.delete())
            # Replace with new data
            conn.execute(
                table.insert(),
                df.to_dict('records')
            )
        context.log.info(f"Successfully loaded data to {table_name}")
    except Exception as e:
        context.log.error(f"Error loading data to {table_name}: {str(e)}")
        raise

load_yf = load_to_db.alias("load_yf")
load_fred = load_to_db.alias("load_fred")

@job
def pipeline():
    symbols = get_symbols()
    yf_data = get_yf_data(symbols)
    fred_data = get_fred_data()
    load_yf(yf_data)
    load_fred(fred_data)

if __name__ == "__main__":
    result = pipeline.execute_in_process(
        run_config={
            "ops": {
                "load_yf": {"config": {"table_name": "yf_data"}},
                "load_fred": {"config": {"table_name": "fred_data"}},
            }
        }
    )