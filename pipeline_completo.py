# pipeline_completo.py
import pandas as pd
from prefect import task, flow
from database import DatabaseManager

@task
def cargar_datos_procesados():
    """Carga el archivo Parquet que ya tienes procesado"""
    print("📁 Cargando datos desde data/gold/covid_gold.parquet...")
    
    try:
        # LEEMOS EL PARQUET REAL en lugar de CSV
        df = pd.read_parquet("data/gold/covid_gold.parquet")
        
        print(f"✅ Datos cargados: {len(df)} filas")
        print(f"   Columnas: {list(df.columns)}")
        print("   Primeras 3 filas:")
        print(df.head(3))
        return df
    except Exception as e:
        print(f"❌ Error cargando datos: {e}")
        return None

@task
def guardar_en_sql(df):
    """Guarda los datos en la base de datos SQL"""
    print("💾 Guardando en base de datos...")
    
    try:
        # Conectamos a la base de datos
        db = DatabaseManager()
        
        # Nos aseguramos que la tabla existe
        db.create_table()
        
        # Guardamos el DataFrame en SQL
        conn = db.get_connection()
        df.to_sql("processed_data", conn, if_exists='replace', index=False)
        conn.close()
        
        print(f"✅ Datos guardados en SQL: {len(df)} filas")
        return True
    except Exception as e:
        print(f"❌ Error guardando en SQL: {e}")
        return False

@task
def verificar_datos():
    """Verifica que los datos se guardaron bien"""
    print("🔍 Verificando datos en la base de datos...")
    
    try:
        db = DatabaseManager()
        conn = db.get_connection()
        
        # Contamos cuántas filas hay
        resultado = pd.read_sql("SELECT COUNT(*) as total FROM processed_data", conn)
        total_filas = resultado.iloc[0]['total']
        
        # Mostramos información de las columnas
        column_info = pd.read_sql("PRAGMA table_info(processed_data)", conn)
        print("📋 Estructura de la tabla:")
        print(column_info[['name', 'type']])
        
        # Mostramos las primeras filas
        primeras_filas = pd.read_sql("SELECT * FROM processed_data LIMIT 3", conn)
        print("📊 Primeras 3 filas en la base de datos:")
        print(primeras_filas)
        
        conn.close()
        
        print(f"✅ Verificación exitosa: {total_filas} filas en la base de datos")
        return total_filas > 0
    except Exception as e:
        print(f"❌ Error en verificación: {e}")
        return False

@flow(name="pipeline-completo-sql")
def pipeline_principal():
    """Este es nuestro flujo principal"""
    print("=" * 50)
    print("🚀 INICIANDO PIPELINE COMPLETO")
    print("=" * 50)
    
    # 1. Cargar datos procesados (tu Parquet real)
    datos = cargar_datos_procesados()
    
    if datos is not None:
        # 2. Guardar en base de datos SQL
        exito = guardar_en_sql(datos)
        
        if exito:
            # 3. Verificar que todo salió bien
            verificacion = verificar_datos()
            
            if verificacion:
                print("=" * 50)
                print("🎉 ¡PIPELINE COMPLETADO CON ÉXITO!")
                print("¡Usando tus datos REALES de COVID!")
                print("=" * 50)
            else:
                print("❌ Error en la verificación")
        else:
            print("❌ Error guardando en SQL")
    else:
        print("❌ Error cargando datos")

# Ejecutar el pipeline
if __name__ == "__main__":
    pipeline_principal()