import os
import pandas as pd
import duckdb
import logging
from prefect import flow, task
import great_expectations as ge

# Configurar logs
os.makedirs("data", exist_ok=True)
logging.basicConfig(
    filename="data/pipeline_log.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Crear carpetas del data lake
for layer in ["bronze", "silver", "gold"]:
    os.makedirs(f"data/{layer}", exist_ok=True)

URL = "https://raw.githubusercontent.com/datasets/covid-19/master/data/countries-aggregated.csv"

@task
def extract_data():
    logging.info("Extrayendo datos desde la fuente.")
    df = pd.read_csv(URL)
    df.to_parquet("data/bronze/covid_bronze.parquet")
    logging.info(f"Datos extraídos: {len(df)} filas.")
    return df

@task
def transform_data(df):
    logging.info("Iniciando transformación.")
    df["Date"] = pd.to_datetime(df["Date"])
    df["Active"] = df["Confirmed"] - df["Recovered"] - df["Deaths"]
    df.to_parquet("data/silver/covid_silver.parquet")
    logging.info("Transformación completada.")
    return df

@task
def validate_data():
    logging.info("Validando datos con Great Expectations.")
    df = ge.read_csv(URL)
    df.expect_column_values_to_not_be_null("Country")
    df.expect_column_values_to_be_between("Confirmed", min_value=0)
    validation_result = df.validate()
    logging.info(f"Resultado de validación: {validation_result.success}")
    return validation_result.success

@task
def aggregate_gold(df):
    logging.info("Generando capa GOLD (resumen por país).")
    con = duckdb.connect()
    result = con.execute("""
        SELECT Country, 
               MAX(Confirmed) AS MaxConfirmed,
               MAX(Deaths) AS MaxDeaths,
               MAX(Recovered) AS MaxRecovered
        FROM df
        GROUP BY Country
        ORDER BY MaxConfirmed DESC
        LIMIT 10
    """).df()
    result.to_parquet("data/gold/covid_gold.parquet")
    logging.info("Capa GOLD generada correctamente.")
    return result

@flow(name="pipeline_validado")
def main_flow():
    df = extract_data()
    df = transform_data(df)
    if validate_data():
        aggregate_gold(df)
        logging.info("Pipeline finalizado exitosamente ✅")
    else:
        logging.error("❌ Falló la validación de datos.")

if __name__ == "__main__":
    main_flow()
