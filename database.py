# database.py
import sqlite3
import pandas as pd
import os

class DatabaseManager:
    def __init__(self, db_path="data/processed_data.db"):
        self.db_path = db_path
        
    def get_connection(self):
        """Crea la conexión a la base de datos"""
        return sqlite3.connect(self.db_path)
    
    def create_table(self):
        """Crea la tabla donde guardaremos los datos"""
        create_table_query = """
        CREATE TABLE IF NOT EXISTS processed_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT,
            category TEXT,
            value REAL,
            processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        
        conn = self.get_connection()
        conn.execute(create_table_query)
        conn.commit()
        conn.close()
        print("✅ Tabla 'processed_data' creada exitosamente")

# Prueba de funcionamiento 
if __name__ == "__main__":
    db = DatabaseManager()
    db.create_table()
    print("🎉 Base de datos lista!")