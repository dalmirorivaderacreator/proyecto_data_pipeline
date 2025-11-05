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

## 🎓 Resultados de Aprendizaje

Este proyecto demuestra:
- Desarrollo de pipelines de datos end-to-end
- Mejores prácticas ETL con Prefect
- Diseño y gestión de bases de datos
- Técnicas de visualización de datos
- Estructura de código lista para producción

## 👨‍💻 Autor

Dalmirorivaderacreator - [GitHub](https://github.com/dalmirorivaderacreator)

## 📄 Licencia

Este proyecto está bajo la Licencia MIT.
