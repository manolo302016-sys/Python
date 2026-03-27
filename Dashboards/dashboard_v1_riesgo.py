"""
dashboard_v1_riesgo.py — Dashboard V1: Resultados de Riesgo Psicosocial

Visualiza los resultados de la batería de riesgo psicosocial según
Resolución 2764/2022 (MinTrabajo Colombia).

Layout V1-Paso21:
  - Semáforo general por factor (Intralaboral, Extralaboral, Estrés)
  - Distribución por nivel de riesgo (5 niveles)
  - Heatmap de dominios y dimensiones
  - Top 10 preguntas críticas
  - Comparativo vs benchmark Colombia/Sector

Tecnología: Streamlit + Plotly
Tokens de diseño: AVANTUM (obligatorio)

Fuente documental: Visualizador 1, Paso 21
Versión: 1.0 | Pipeline MentalPRO | Modelo AVANTUM
"""

import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd
import numpy as np
from pathlib import Path
from typing import Dict, List, Optional

# ===========================================================================
# CONFIGURACIÓN Y TOKENS AVANTUM (OBLIGATORIO - NO MODIFICAR)
# ===========================================================================

# Paleta de colores AVANTUM
AVANTUM_COLORS = {
    "primary": "#0A1628",      # Navy oscuro - fondos principales
    "secondary": "#1E3A5F",    # Navy medio - cards
    "accent": "#C9952A",       # Dorado - highlights, CTAs
    "success": "#28A745",      # Verde - bajo riesgo
    "warning": "#FFC107",      # Amarillo - riesgo medio
    "danger": "#DC3545",       # Rojo - alto riesgo
    "critical": "#8B0000",     # Rojo oscuro - muy alto riesgo
    "info": "#17A2B8",         # Cyan - informativo
    "light": "#F8F9FA",        # Gris claro - textos secundarios
    "dark": "#343A40",         # Gris oscuro - textos
}

# Colores por nivel de riesgo (5 niveles - BINDING D1)
COLORES_NIVEL_RIESGO = {
    "Muy alto": "#8B0000",     # Rojo oscuro
    "Alto": "#DC3545",         # Rojo
    "Medio": "#FFC107",        # Amarillo
    "Bajo": "#28A745",         # Verde
}

# Tipografía
FONT_FAMILY = "Inter, sans-serif"

# Configuración de página
st.set_page_config(
    page_title="MentalPRO - Riesgo Psicosocial",
    page_icon="🧠",
    layout="wide",
    initial_sidebar_state="expanded"
)


# ===========================================================================
# ESTILOS CSS AVANTUM
# ===========================================================================

def aplicar_estilos_avantum():
    """Aplica estilos CSS según tokens AVANTUM."""
    st.markdown(f"""
    <style>
        /* Fondo general */
        .stApp {{
            background-color: {AVANTUM_COLORS["light"]};
        }}
        
        /* Sidebar */
        [data-testid="stSidebar"] {{
            background-color: {AVANTUM_COLORS["primary"]};
        }}
        [data-testid="stSidebar"] * {{
            color: {AVANTUM_COLORS["light"]} !important;
        }}
        
        /* Headers */
        h1, h2, h3 {{
            color: {AVANTUM_COLORS["primary"]};
            font-family: {FONT_FAMILY};
        }}
        
        /* Cards métricas */
        [data-testid="metric-container"] {{
            background-color: white;
            border: 1px solid #E0E0E0;
            border-radius: 8px;
            padding: 16px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.05);
        }}
        
        /* Botones */
        .stButton > button {{
            background-color: {AVANTUM_COLORS["accent"]};
            color: white;
            border: none;
            border-radius: 4px;
        }}
        .stButton > button:hover {{
            background-color: #B8860B;
        }}
        
        /* Tabs */
        .stTabs [data-baseweb="tab-list"] {{
            gap: 8px;
        }}
        .stTabs [data-baseweb="tab"] {{
            background-color: white;
            border-radius: 4px;
        }}
        .stTabs [aria-selected="true"] {{
            background-color: {AVANTUM_COLORS["accent"]};
        }}
    </style>
    """, unsafe_allow_html=True)


# ===========================================================================
# CARGA DE DATOS
# ===========================================================================

@st.cache_data
def cargar_datos(ruta_base: str = "data") -> Dict[str, pd.DataFrame]:
    """Carga todos los datasets necesarios para el dashboard."""
    path = Path(ruta_base)
    
    datos = {}
    
    # Cargar datasets
    archivos = {
        "benchmark": "processed/fact_benchmark.parquet",
        "frecuencias": "processed/fact_frecuencias.parquet",
        "criticas": "processed/fact_preguntas_criticas.parquet",
        "resumen": "final/resumen_ejecutivo.parquet",
        "kpi_empresa": "final/kpi_empresa.parquet",
    }
    
    for key, archivo in archivos.items():
        ruta = path / archivo
        if ruta.exists():
            datos[key] = pd.read_parquet(ruta)
        else:
            st.warning(f"Archivo no encontrado: {archivo}")
            datos[key] = pd.DataFrame()
    
    return datos


# ===========================================================================
# COMPONENTES DE VISUALIZACIÓN
# ===========================================================================

def crear_semaforo_factor(df: pd.DataFrame, factor: str) -> go.Figure:
    """Crea indicador de semáforo para un factor."""
    df_factor = df[df["factor"] == factor]
    
    if len(df_factor) == 0:
        return go.Figure()
    
    score = df_factor["score_promedio"].mean()
    nivel = df_factor["nivel_predominante"].mode().iloc[0] if len(df_factor) > 0 else "N/A"
    
    # Determinar color según nivel
    color = COLORES_NIVEL_RIESGO.get(nivel, AVANTUM_COLORS["info"])
    
    fig = go.Figure(go.Indicator(
        mode="gauge+number",
        value=score,
        title={"text": factor.capitalize(), "font": {"size": 16, "color": AVANTUM_COLORS["primary"]}},
        number={"suffix": "", "font": {"size": 24}},
        gauge={
            "axis": {"range": [0, 100], "tickwidth": 1},
            "bar": {"color": color},
            "bgcolor": "white",
            "borderwidth": 2,
            "bordercolor": AVANTUM_COLORS["primary"],
            "steps": [
                {"range": [0, 20], "color": "#28A745"},
                {"range": [20, 40], "color": "#90EE90"},
                {"range": [40, 60], "color": "#FFC107"},
                {"range": [60, 80], "color": "#DC3545"},
                {"range": [80, 100], "color": "#8B0000"},
            ],
        }
    ))
    
    fig.update_layout(
        height=200,
        margin=dict(l=20, r=20, t=40, b=20),
        paper_bgcolor="white",
        font={"family": FONT_FAMILY}
    )
    
    return fig


def crear_distribucion_niveles(df: pd.DataFrame, empresa: str = None) -> go.Figure:
    """Crea gráfico de barras con distribución por nivel de riesgo."""
    if empresa:
        df = df[df["empresa"] == empresa]
    
    # Contar por nivel
    niveles_orden = ["Muy alto", "Alto", "Medio", "Bajo"]
    
    if "nivel_riesgo_colombia" in df.columns:
        conteo = df["nivel_riesgo_colombia"].value_counts()
    else:
        conteo = pd.Series(dtype=int)
    
    # Asegurar todos los niveles
    for nivel in niveles_orden:
        if nivel not in conteo.index:
            conteo[nivel] = 0
    
    conteo = conteo[niveles_orden]
    colores = [COLORES_NIVEL_RIESGO.get(n, "#999") for n in niveles_orden]
    
    fig = go.Figure(go.Bar(
        x=niveles_orden,
        y=conteo.values,
        marker_color=colores,
        text=conteo.values,
        textposition="auto",
    ))
    
    fig.update_layout(
        title="Distribución por Nivel de Riesgo",
        xaxis_title="Nivel",
        yaxis_title="Cantidad",
        height=350,
        margin=dict(l=40, r=40, t=60, b=40),
        paper_bgcolor="white",
        plot_bgcolor="white",
        font={"family": FONT_FAMILY}
    )
    
    return fig


def crear_heatmap_dimensiones(df: pd.DataFrame, empresa: str = None) -> go.Figure:
    """Crea heatmap de scores por dimensión."""
    if empresa:
        df = df[df["empresa"] == empresa]
    
    if "dimension" not in df.columns or len(df) == 0:
        return go.Figure()
    
    # Filtrar solo registros con dimensión
    df_dim = df[df["dimension"].notna() & (df["dimension"] != "")]
    
    if len(df_dim) == 0:
        return go.Figure()
    
    # Pivot para heatmap
    pivot = df_dim.groupby(["dominio", "dimension"])["score_transformado"].mean().unstack()
    
    fig = go.Figure(go.Heatmap(
        z=pivot.values,
        x=pivot.columns.tolist(),
        y=pivot.index.tolist(),
        colorscale=[
            [0, "#28A745"],
            [0.25, "#90EE90"],
            [0.5, "#FFC107"],
            [0.75, "#DC3545"],
            [1, "#8B0000"]
        ],
        colorbar={"title": "Score"},
    ))
    
    fig.update_layout(
        title="Heatmap: Score por Dimensión",
        height=400,
        margin=dict(l=150, r=40, t=60, b=100),
        paper_bgcolor="white",
        font={"family": FONT_FAMILY}
    )
    
    return fig


def crear_tabla_preguntas_criticas(df: pd.DataFrame, top_n: int = 10) -> pd.DataFrame:
    """Retorna tabla formateada de preguntas críticas."""
    if len(df) == 0:
        return pd.DataFrame()
    
    df_top = df.nlargest(top_n, "pct_negativo")[
        ["id_pregunta", "dimension", "pct_negativo", "texto_pregunta"]
    ].copy()
    
    df_top.columns = ["ID", "Dimensión", "% Negativo", "Pregunta"]
    df_top["% Negativo"] = df_top["% Negativo"].apply(lambda x: f"{x:.1f}%")
    
    return df_top


def crear_comparativo_benchmark(df: pd.DataFrame, empresa: str) -> go.Figure:
    """Crea gráfico comparativo vs benchmark Colombia y Sector."""
    df_emp = df[df["empresa"] == empresa]
    
    if len(df_emp) == 0 or "nivel_orden_colombia" not in df_emp.columns:
        return go.Figure()
    
    # Agrupar por factor
    comparativo = df_emp.groupby("factor").agg({
        "score_transformado": "mean",
        "nivel_orden_colombia": "mean",
    }).reset_index()
    
    fig = go.Figure()
    
    fig.add_trace(go.Bar(
        name="Score Empresa",
        x=comparativo["factor"],
        y=comparativo["score_transformado"],
        marker_color=AVANTUM_COLORS["accent"],
    ))
    
    # Línea de referencia (benchmark medio = nivel 3)
    fig.add_hline(
        y=50, 
        line_dash="dash", 
        line_color=AVANTUM_COLORS["info"],
        annotation_text="Benchmark Colombia (medio)"
    )
    
    fig.update_layout(
        title="Comparativo vs Benchmark",
        xaxis_title="Factor",
        yaxis_title="Score",
        height=350,
        paper_bgcolor="white",
        plot_bgcolor="white",
        font={"family": FONT_FAMILY}
    )
    
    return fig


# ===========================================================================
# APLICACIÓN PRINCIPAL
# ===========================================================================

def main():
    """Función principal del dashboard."""
    aplicar_estilos_avantum()
    
    # Header
    st.title("🧠 MentalPRO - Resultados de Riesgo Psicosocial")
    st.markdown("**Resolución 2764/2022 - MinTrabajo Colombia**")
    st.markdown("---")
    
    # Cargar datos
    datos = cargar_datos()
    
    if len(datos.get("benchmark", pd.DataFrame())) == 0:
        st.error("No se encontraron datos. Ejecute primero el pipeline de procesamiento.")
        return
    
    df_benchmark = datos["benchmark"]
    df_resumen = datos.get("resumen", pd.DataFrame())
    df_criticas = datos.get("criticas", pd.DataFrame())
    df_kpi = datos.get("kpi_empresa", pd.DataFrame())
    
    # Sidebar - Filtros
    st.sidebar.image("assets/logo_avantum.png", width=200) if Path("assets/logo_avantum.png").exists() else None
    st.sidebar.title("Filtros")
    
    empresas = ["Todas"] + sorted(df_benchmark["empresa"].unique().tolist())
    empresa_sel = st.sidebar.selectbox("Empresa", empresas)
    
    if empresa_sel != "Todas":
        df_benchmark = df_benchmark[df_benchmark["empresa"] == empresa_sel]
        if len(df_criticas) > 0:
            df_criticas = df_criticas[df_criticas["empresa"] == empresa_sel]
    
    # Métricas principales
    col1, col2, col3, col4 = st.columns(4)
    
    n_trabajadores = df_benchmark["cedula"].nunique()
    n_empresas = df_benchmark["empresa"].nunique()
    pct_alto = len(df_benchmark[df_benchmark["nivel_riesgo_colombia"].isin(["Alto", "Muy alto"])]) / len(df_benchmark) * 100 if len(df_benchmark) > 0 else 0
    
    col1.metric("Trabajadores", f"{n_trabajadores:,}")
    col2.metric("Empresas", n_empresas)
    col3.metric("% Riesgo Alto/Muy Alto", f"{pct_alto:.1f}%")
    col4.metric("Preguntas Críticas", len(df_criticas) if len(df_criticas) > 0 else 0)
    
    st.markdown("---")
    
    # Tabs principales
    tab1, tab2, tab3, tab4 = st.tabs(["📊 Semáforos", "📈 Distribución", "🗺️ Heatmap", "⚠️ Críticas"])
    
    with tab1:
        st.subheader("Semáforo por Factor")
        col1, col2, col3 = st.columns(3)
        
        with col1:
            fig = crear_semaforo_factor(df_kpi, "intralaboral")
            st.plotly_chart(fig, use_container_width=True)
        with col2:
            fig = crear_semaforo_factor(df_kpi, "extralaboral")
            st.plotly_chart(fig, use_container_width=True)
        with col3:
            fig = crear_semaforo_factor(df_kpi, "estres")
            st.plotly_chart(fig, use_container_width=True)
    
    with tab2:
        st.subheader("Distribución por Nivel de Riesgo")
        fig = crear_distribucion_niveles(df_benchmark, empresa_sel if empresa_sel != "Todas" else None)
        st.plotly_chart(fig, use_container_width=True)
        
        if empresa_sel != "Todas":
            fig_bench = crear_comparativo_benchmark(datos["benchmark"], empresa_sel)
            st.plotly_chart(fig_bench, use_container_width=True)
    
    with tab3:
        st.subheader("Heatmap de Dimensiones")
        fig = crear_heatmap_dimensiones(df_benchmark, empresa_sel if empresa_sel != "Todas" else None)
        st.plotly_chart(fig, use_container_width=True)
    
    with tab4:
        st.subheader("Top 10 Preguntas Críticas")
        if len(df_criticas) > 0:
            df_tabla = crear_tabla_preguntas_criticas(df_criticas)
            st.dataframe(df_tabla, use_container_width=True, hide_index=True)
        else:
            st.info("No se identificaron preguntas críticas (ninguna supera el umbral de 40%)")
    
    # Footer
    st.markdown("---")
    st.markdown(
        f"<div style='text-align: center; color: {AVANTUM_COLORS['dark']};'>"
        "Dashboard MentalPRO v1.0 | Modelo AVANTUM | Resolución 2764/2022"
        "</div>",
        unsafe_allow_html=True
    )


if __name__ == "__main__":
    main()