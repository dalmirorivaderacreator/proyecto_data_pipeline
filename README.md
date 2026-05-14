# 🌍 COVID Data Engineering Pipeline

[![Python](https://img.shields.io/badge/Python-3.8%2B-blue)](https://python.org)
[![Prefect](https://img.shields.io/badge/Prefect-2.0-orange)](https://prefect.io)
[![Streamlit](https://img.shields.io/badge/Streamlit-1.28-red)](https://streamlit.io)

Un proyecto completo de Data Engineering que procesa estadísticas de COVID-19 y proporciona un dashboard interactivo para análisis.

## 📊 Descripción del Proyecto

Este proyecto demuestra un flujo completo de trabajo en Data Engineering:
- **Ingesta de Datos**: Carga y procesamiento de datos de COVID-19
- **Pipeline ETL**: Transformación y limpieza usando Prefect
- **Almacenamiento**: Datos procesados en base de datos SQLite
- **Visualización**: Dashboard interactivo con Streamlit y Plotly

## 📌 Sobre este proyecto

Este proyecto lo hice siguiendo paso a paso lo que me indicaba la IA. Fue mi primer contacto con herramientas como Prefect, Streamlit y la arquitectura Medallion, pero la realidad es que solo copiaba y pegaba. No tomé decisiones técnicas ni resolví problemas por mi cuenta.

Hoy sé que un proyecto que no puedo explicar no tiene valor real para mi aprendizaje. Por eso estoy hoy estoy construyendo las cosas diferente. Mi proyecto actual es UGCAnalitica , donde cada línea la escribo yo y cada error lo resuelvo yo.

👉 Te invito a conocer UGCA. https://github.com/dalmirorivaderacreator/UGCAnalitica

 desde cero, escribiendo cada línea y aprendiendo realmente.



## 🏗️ Arquitectura

```
Datos Crudos → Capa Bronze → Capa Silver → Capa Gold → Base SQL → Dashboard Streamlit
```

## 🚀 Características Principales

- **Orquestación**: Prefect para gestión de workflows
- **Procesamiento**: Pandas para operaciones ETL
- **Base de Datos**: SQLite para almacenamiento estructurado
- **Visualización**: Streamlit + Plotly para dashboards interactivos
- **Calidad de Datos**: Validación y manejo de errores

## 📁 Estructura del Proyecto

```
proyecto_data_pipeline/
├── data/
│   ├── bronze/           # Capa de datos crudos
│   ├── silver/           # Capa de datos limpiados
│   ├── gold/             # Capa de datos procesados
│   └── processed_data.db # Base de datos SQLite
├── dashboard.py          # Dashboard Streamlit
├── database.py           # Gestión de base de datos
├── pipeline_completo.py  # Pipeline ETL principal
├── requirements.txt      # Dependencias
└── README.md
```

## 🛠️ Instalación y Uso

### 1. Clonar el repositorio
```bash
git clone https://github.com/dalmirorivaderacreator/proyecto_data_pipeline.git
cd proyecto_data_pipeline
```

### 2. Instalar dependencias
```bash
pip install -r requirements.txt
```

### 3. Ejecutar el pipeline completo
```bash
python pipeline_completo.py
```

### 4. Lanzar el dashboard
```bash
streamlit run dashboard.py
```

## 📊 Características del Dashboard

- **Métricas Globales**: Total de casos, muertes, recuperaciones
- **Comparación entre Países**: Gráficos de barras interactivos
- **Análisis de Mortalidad**: Relación muertes vs recuperaciones
- **Tablas de Datos**: Datos detallados a nivel país
- **Diseño Responsive**: Funciona en desktop y móvil

## 🔧 Tecnologías Utilizadas

- **Prefect 2.0**: Orquestación de workflows
- **Pandas**: Manipulación y análisis de datos
- **SQLite**: Base de datos relacional
- **Streamlit**: Framework de aplicación web
- **Plotly**: Visualizaciones interactivas
- **PyArrow**: Manejo de archivos Parquet

## 👨‍💻 Autor

dalmirorivaderacreator - [GitHub](https://github.com/dalmirorivaderacreator)

## 📄 Licencia

Este proyecto está bajo la Licencia MIT.
