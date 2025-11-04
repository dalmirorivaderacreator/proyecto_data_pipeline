from prefect import flow, task
import duckdb
import pandas as pd
import os

# ==============================
# CONFIGURACIÓN INICIAL
# ==============================
DATA_DIR = "data"
os.makedirs(DATA_DIR, exist_ok=True)

@task
def extract_data():
    """Descarga un dataset público desde una URL."""
    url = "https://raw.githubusercontent.com/datasets/covid-19/master/data/countries-aggregated.csv"
    print("📥 Descargando datos desde:", url)
    df = pd.read_csv(url)
    print(f"✅ Datos descargados: {len(df)} filas")
    return df

@task
def transform_data(df: pd.DataFrame):
    """Transforma los datos: agrega columna 'month'."""
    df["Date"] = pd.to_datetime(df["Date"])
    df["month"] = df["Date"].dt.to_period("M")
    print("🔄 Transformación completada.")
    return df

@task
def load_data(df: pd.DataFrame):
    """Guarda los datos como Parquet en formato tipo 'bronze'."""
    parquet_path = os.path.join(DATA_DIR, "covid_bronze.parquet")
    df.to_parquet(parquet_path, index=False)
    print(f"💾 Archivo guardado en: {parquet_path}")
    return parquet_path

@task
def validate_with_duckdb(parquet_path: str):
    """Verifica los datos usando DuckDB."""
    con = duckdb.connect()
    result = con.execute(f"SELECT COUNT(*) FROM read_parquet('{parquet_path}')").fetchone()
    print(f"🔍 Validación: {result[0]} registros en el parquet.")
    con.close()

@flow(name="pipeline_basico")
def main_flow():
    df = extract_data()
    df_t = transform_data(df)
    path = load_data(df_t)
    validate_with_duckdb(path)
    print("✅ Pipeline completo!")

if __name__ == "__main__":
    main_flow()
