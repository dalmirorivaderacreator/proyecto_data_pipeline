# dashboard.py
import streamlit as st
import pandas as pd
import sqlite3
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime

# Configurar la página
st.set_page_config(
    page_title="Dashboard COVID - Data Engineering",
    page_icon="🌍",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Título principal
st.title("🌍 Dashboard COVID - Data Engineering Project")
st.markdown("---")

# Función para cargar datos desde SQLite
@st.cache_data
def load_data():
    try:
        conn = sqlite3.connect('data/processed_data.db')
        df = pd.read_sql("SELECT * FROM processed_data", conn)
        conn.close()
        return df
    except Exception as e:
        st.error(f"Error cargando datos: {e}")
        return pd.DataFrame()

# Cargar datos
df = load_data()

if df.empty:
    st.warning("No se encontraron datos. Ejecuta primero: python pipeline_completo.py")
else:
    # Sidebar con información del proyecto
    st.sidebar.title("📊 Información del Proyecto")
    st.sidebar.markdown("""
    **Tecnologías utilizadas:**
    - Prefect (Orquestación)
    - Python + Pandas (ETL)
    - SQLite (Almacenamiento)
    - Streamlit (Visualización)
    """)
    
    st.sidebar.markdown("---")
    st.sidebar.markdown(f"**Última actualización:** {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    st.sidebar.markdown(f"**Total de países:** {len(df)}")
    
    # SECCIÓN 1: MÉTRICAS PRINCIPALES
    st.header("📈 Métricas Globales COVID-19")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        total_confirmed = df['MaxConfirmed'].sum()
        st.metric(
            label="Total Confirmados", 
            value=f"{total_confirmed:,}",
            delta="Datos globales"
        )
    
    with col2:
        total_deaths = df['MaxDeaths'].sum()
        st.metric(
            label="Total Muertes", 
            value=f"{total_deaths:,}",
            delta_color="inverse"
        )
    
    with col3:
        total_recovered = df['MaxRecovered'].sum()
        st.metric(
            label="Total Recuperados", 
            value=f"{total_recovered:,}",
            delta="Casos recuperados"
        )
    
    with col4:
        avg_mortality = (df['MaxDeaths'].sum() / df['MaxConfirmed'].sum()) * 100
        st.metric(
            label="Tasa de Mortalidad", 
            value=f"{avg_mortality:.2f}%"
        )
    
    st.markdown("---")
    
    # SECCIÓN 2: GRÁFICOS PRINCIPALES
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("📊 Top 10 Países - Casos Confirmados")
        
        # Ordenar datos para el gráfico
        top_countries = df.nlargest(10, 'MaxConfirmed')
        
        fig_barras = px.bar(
            top_countries,
            x='Country',
            y='MaxConfirmed',
            color='MaxConfirmed',
            color_continuous_scale='viridis',
            title="Top 10 Países con Más Casos Confirmados"
        )
        fig_barras.update_layout(
            xaxis_title="País",
            yaxis_title="Casos Confirmados",
            showlegend=False
        )
        st.plotly_chart(fig_barras, use_container_width=True)
    
    with col2:
        st.subheader("🔍 Relación: Muertes vs Recuperaciones")
        
        fig_dispersion = px.scatter(
            df,
            x='MaxDeaths',
            y='MaxRecovered',
            size='MaxConfirmed',
            color='Country',
            hover_name='Country',
            size_max=60,
            title="Muertes vs Recuperaciones por País"
        )
        fig_dispersion.update_layout(
            xaxis_title="Total Muertes",
            yaxis_title="Total Recuperados"
        )
        st.plotly_chart(fig_dispersion, use_container_width=True)
    
    # SECCIÓN 3: GRÁFICO DE TASA DE MORTALIDAD
    st.subheader("📉 Tasa de Mortalidad por País")
    
    # Calcular tasa de mortalidad para cada país
    df_mortality = df.copy()
    df_mortality['MortalityRate'] = (df_mortality['MaxDeaths'] / df_mortality['MaxConfirmed']) * 100
    df_mortality = df_mortality.nlargest(10, 'MortalityRate')
    
    fig_mortalidad = px.bar(
        df_mortality,
        x='Country',
        y='MortalityRate',
        color='MortalityRate',
        color_continuous_scale='reds',
        title="Top 10 Países con Mayor Tasa de Mortalidad (%)"
    )
    fig_mortalidad.update_layout(
        yaxis_title="Tasa de Mortalidad (%)",
        showlegend=False
    )
    st.plotly_chart(fig_mortalidad, use_container_width=True)
    
    # SECCIÓN 4: DATOS DETALLADOS
    st.markdown("---")
    st.subheader("📋 Datos Detallados - Todos los Países")
    
    # Mostrar dataframe con opciones
    col1, col2 = st.columns([3, 1])
    
    with col2:
        st.markdown("**Opciones de visualización:**")
        show_raw_data = st.checkbox("Mostrar datos sin formato", value=False)
    
    if show_raw_data:
        st.dataframe(df)
    else:
        # DataFrame formateado
        df_display = df.copy()
        df_display['MaxConfirmed'] = df_display['MaxConfirmed'].apply(lambda x: f"{x:,}")
        df_display['MaxDeaths'] = df_display['MaxDeaths'].apply(lambda x: f"{x:,}")
        df_display['MaxRecovered'] = df_display['MaxRecovered'].apply(lambda x: f"{x:,}")
        
        st.dataframe(df_display, use_container_width=True)
    
    # SECCIÓN 5: INFORMACIÓN TÉCNICA
    st.markdown("---")
    st.subheader("🔧 Información Técnica del Pipeline")
    
    tech_col1, tech_col2, tech_col3 = st.columns(3)
    
    with tech_col1:
        st.markdown("**📊 Datos:**")
        st.markdown(f"- Países procesados: {len(df)}")
        st.markdown(f"- Total de registros: {len(df)}")
        st.markdown(f"- Columnas: {len(df.columns)}")
    
    with tech_col2:
        st.markdown("**⚙️ Pipeline:**")
        st.markdown("- Prefect: Orquestación")
        st.markdown("- Pandas: Transformación")
        st.markdown("- SQLite: Almacenamiento")
    
    with tech_col3:
        st.markdown("**📈 Visualización:**")
        st.markdown("- Streamlit: Dashboard")
        st.markdown("- Plotly: Gráficos")
        st.markdown("- SQL: Consultas")

# Footer
st.markdown("---")
st.markdown(
    "**🎓 Proyecto de Data Engineering** | "
    "Pipeline completo: ETL → SQL → Dashboard | "
    "Creado con Python 🐍"
)