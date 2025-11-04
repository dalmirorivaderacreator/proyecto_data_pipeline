from datetime import datetime
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
import duckdb
import os
import logging
from prefect import flow, task

# Configuración de logs
logging.basicConfig(
    filename='data/pipeline_log.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

URL = "https://raw.githubusercontent.com/datasets/covid-19/master/data/countries-aggregated.csv"

# --- TAREAS -----------------------------------------------------

@task
def extract_data():
    logging.info("Descargando datos...")
    df = pd.read_csv(URL)
    logging.info(f"Datos descargados: {len(df)} filas.")
    return df

@task
def transform_data(df):
    logging.info("Iniciando transformación...")
    df['Date'] = pd.to_datetime(df['Date'])
    df = df.groupby('Country', as_index=False).agg({
        'Confirmed': 'max',
        'Recovered': 'max',
        'Deaths': 'max'
    })
    df['FatalityRate'] = (df['Deaths'] / df['Confirmed']).fillna(0).round(3)
    logging.info("Transformación completada.")
    return df

@task
def save_data(df, stage='silver'):
    file_path = f"data/{stage}/covid_{stage}.parquet"
    table = pa.Table.from_pandas(df)
    pq.write_table(table, file_path)
    logging.info(f"Archivo guardado en: {file_path}")
    return file_path

@task
def validate_with_duckdb(path):
    con = duckdb.connect()
    result = con.execute(f"SELECT COUNT(*) FROM read_parquet('{path}')").fetchone()
    logging.info(f"Validación: {result[0]} registros en el archivo.")
    return result[0]

# --- FLOW PRINCIPAL ---------------------------------------------

@flow(name="pipeline_mejorado")
def main_flow():
    logging.info("🚀 Iniciando pipeline mejorado...")
    df_raw = extract_data()
    save_data(df_raw, 'bronze')

    df_transformed = transform_data(df_raw)
    path_silver = save_data(df_transformed, 'silver')
    validate_with_duckdb(path_silver)

    logging.info("✅ Pipeline mejorado completado correctamente.")

if __name__ == "__main__":
    main_flow()