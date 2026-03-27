"""
dashboard_v2_gestion.py — Dashboard V2: Gestión del Riesgo Psicosocial

Visualiza el plan de gestión y priorización de protocolos según
la metodología de intervención AVANTUM.

Layout V2-Paso8:
  - Matriz de priorización (urgencia vs impacto)
  - Protocolos activos por empresa
  - Roadmap de intervención
  - KPIs de gestión por eje
  - Seguimiento de avance

Tecnología: Streamlit + Plotly
Tokens de diseño: AVANTUM (obligatorio)

Fuente documental: Visualizador 2, Paso 8
Versión: 1.0 | Pipeline MentalPRO | Modelo AVANTUM
"""

import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import numpy as np
from pathlib import Path
from typing import Dict, List

# Importar configuración AVANTUM del dashboard V1
AVANTUM_COLORS = {
    "primary": "#0A1628",
    "secondary": "#1E3A5F",
    "accent": "#C9952A",
    "success": "#28A745",
    "warning": "#FFC107",
    "danger": "#DC3545",
    "critical": "#8B0000",
    "info": "#17A2B8",
    "light": "#F8F9FA",
    "dark": "#343A40",
}

COLORES_PRIORIDAD = {
    "Crítica": "#8B0000",
    "Alta": "#DC3545",
    "Media": "#FFC107",
    "Baja": "#28A745",
}

COLORES_NIVEL_GESTION = {
    5: "#8B0000",  # Urgente
    4: "#DC3545",  # Correctiva
    3: "#FFC107",  # Preventiva
    2: "#90EE90",  # Mantenimiento
    1: "#28A745",  # Prorrogable
}

FONT_FAMILY = "Inter, sans-serif"

st.set_page_config(
    page_title="MentalPRO - Gestión del Riesgo",
    page_icon="📋",
    layout="wide",
    initial_sidebar_state="expanded"
)


# ===========================================================================
# CARGA DE DATOS
# ===========================================================================

@st.cache_data
def cargar_datos(ruta_base: str = "data") -> Dict[str, pd.DataFrame]:
    """Carga datasets de gestión."""
    path = Path(ruta_base)
    datos = {}
    
    archivos = {
        "prioridades": "processed/fact_prioridades.parquet",
        "gestion": "processed/fact_gestion_scores.parquet",
        "resumen": "final/resumen_ejecutivo.parquet",
    }
    
    for key, archivo in archivos.items():
        ruta = path / archivo
        if ruta.exists():
            datos[key] = pd.read_parquet(ruta)
        else:
            datos[key] = pd.DataFrame()
    
    return datos


# ===========================================================================
# COMPONENTES DE VISUALIZACIÓN
# ===========================================================================

def crear_matriz_priorizacion(df: pd.DataFrame) -> go.Figure:
    """Crea matriz de priorización urgencia vs impacto."""
    if len(df) == 0:
        return go.Figure()
    
    # Mapear prioridad a valores numéricos
    prioridad_map = {"Crítica": 4, "Alta": 3, "Media": 2, "Baja": 1}
    df = df.copy()
    df["prioridad_num"] = df["prioridad_calculada"].map(prioridad_map).fillna(2)
    
    fig = go.Figure()
    
    for prioridad in ["Crítica", "Alta", "Media", "Baja"]:
        df_p = df[df["prioridad_calculada"] == prioridad]
        if len(df_p) > 0:
            fig.add_trace(go.Scatter(
                x=df_p["pct_urgente_correctiva"],
                y=df_p["score_linea"],
                mode="markers+text",
                name=prioridad,
                marker=dict(
                    size=15,
                    color=COLORES_PRIORIDAD.get(prioridad, "#999"),
                ),
                text=df_p["prot_id"],
                textposition="top center",
                hovertemplate="<b>%{text}</b><br>% Urgente: %{x:.1f}%<br>Score: %{y:.2f}<extra></extra>"
            ))
    
    fig.update_layout(
        title="Matriz de Priorización de Protocolos",
        xaxis_title="% Población en Riesgo Alto/Muy Alto",
        yaxis_title="Score de Línea (0-1)",
        height=450,
        paper_bgcolor="white",
        plot_bgcolor="white",
        font={"family": FONT_FAMILY},
        legend=dict(orientation="h", yanchor="bottom", y=1.02),
    )
    
    # Cuadrantes
    fig.add_hline(y=0.5, line_dash="dot", line_color="gray")
    fig.add_vline(x=30, line_dash="dot", line_color="gray")
    
    return fig


def crear_roadmap_protocolos(df: pd.DataFrame) -> go.Figure:
    """Crea timeline de implementación de protocolos."""
    if len(df) == 0:
        return go.Figure()
    
    # Ordenar por prioridad
    orden_prioridad = {"Crítica": 0, "Alta": 1, "Media": 2, "Baja": 3}
    df = df.copy()
    df["orden"] = df["prioridad_calculada"].map(orden_prioridad).fillna(4)
    df = df.sort_values("orden")
    
    # Asignar trimestres ficticios basados en prioridad
    df["trimestre"] = df["orden"].apply(lambda x: f"Q{int(x)+1} 2025")
    
    fig = px.timeline(
        df.head(10),
        x_start="trimestre",
        x_end="trimestre",
        y="prot_id",
        color="prioridad_calculada",
        color_discrete_map=COLORES_PRIORIDAD,
        title="Roadmap de Implementación (Top 10)",
    )
    
    fig.update_layout(
        height=400,
        paper_bgcolor="white",
        font={"family": FONT_FAMILY},
    )
    
    return fig


def crear_kpis_por_eje(df: pd.DataFrame) -> go.Figure:
    """Crea gráfico de KPIs agrupados por eje de gestión."""
    if len(df) == 0 or "linea" not in df.columns:
        return go.Figure()
    
    # Agrupar por línea
    df_agg = df.groupby("linea").agg({
        "score_linea": "mean",
        "activacion_flag": "sum",
        "n_trabajadores": "sum",
    }).reset_index()
    
    df_agg = df_agg.sort_values("score_linea")
    
    fig = go.Figure(go.Bar(
        x=df_agg["score_linea"],
        y=df_agg["linea"],
        orientation="h",
        marker_color=[
            COLORES_NIVEL_GESTION.get(5 if s < 0.3 else 4 if s < 0.5 else 3 if s < 0.7 else 2, "#999")
            for s in df_agg["score_linea"]
        ],
        text=df_agg["score_linea"].apply(lambda x: f"{x:.2f}"),
        textposition="auto",
    ))
    
    fig.update_layout(
        title="Score por Línea de Gestión",
        xaxis_title="Score (0-1)",
        yaxis_title="",
        height=500,
        margin=dict(l=250),
        paper_bgcolor="white",
        plot_bgcolor="white",
        font={"family": FONT_FAMILY},
    )
    
    return fig


# ===========================================================================
# APLICACIÓN PRINCIPAL
# ===========================================================================

def main():
    """Función principal del dashboard V2."""
    st.title("📋 MentalPRO - Gestión del Riesgo Psicosocial")
    st.markdown("**Plan de Intervención y Priorización de Protocolos**")
    st.markdown("---")
    
    datos = cargar_datos()
    
    if len(datos.get("prioridades", pd.DataFrame())) == 0:
        st.error("No se encontraron datos de priorización. Ejecute primero el pipeline.")
        return
    
    df_prio = datos["prioridades"]
    df_gestion = datos.get("gestion", pd.DataFrame())
    df_resumen = datos.get("resumen", pd.DataFrame())
    
    # Sidebar
    st.sidebar.title("Filtros")
    empresas = ["Todas"] + sorted(df_prio["empresa"].unique().tolist())
    empresa_sel = st.sidebar.selectbox("Empresa", empresas)
    
    if empresa_sel != "Todas":
        df_prio = df_prio[df_prio["empresa"] == empresa_sel]
    
    # Métricas
    col1, col2, col3, col4 = st.columns(4)
    
    n_protocolos = df_prio["prot_id"].nunique()
    n_activos = df_prio[df_prio["activacion_flag"] == True]["prot_id"].nunique()
    n_criticos = len(df_prio[df_prio["prioridad_calculada"] == "Crítica"])
    pct_cobertura = n_activos / n_protocolos * 100 if n_protocolos > 0 else 0
    
    col1.metric("Protocolos Totales", n_protocolos)
    col2.metric("Protocolos Activos", n_activos)
    col3.metric("Prioridad Crítica", n_criticos)
    col4.metric("Cobertura", f"{pct_cobertura:.0f}%")
    
    st.markdown("---")
    
    # Tabs
    tab1, tab2, tab3 = st.tabs(["🎯 Matriz", "📅 Roadmap", "📊 KPIs"])
    
    with tab1:
        st.subheader("Matriz de Priorización")
        fig = crear_matriz_priorizacion(df_prio)
        st.plotly_chart(fig, use_container_width=True)
        
        st.subheader("Detalle de Protocolos")
        st.dataframe(
            df_prio[["prot_id", "linea", "prioridad_calculada", "score_linea", "pct_urgente_correctiva", "activacion_flag"]]
            .sort_values("prioridad_calculada"),
            use_container_width=True,
            hide_index=True
        )
    
    with tab2:
        st.subheader("Roadmap de Implementación")
        st.info("Los protocolos se ordenan por prioridad. Q1=Crítica, Q2=Alta, Q3=Media, Q4=Baja")
        fig = crear_roadmap_protocolos(df_prio)
        st.plotly_chart(fig, use_container_width=True)
    
    with tab3:
        st.subheader("KPIs por Línea de Gestión")
        fig = crear_kpis_por_eje(df_prio)
        st.plotly_chart(fig, use_container_width=True)
    
    # Footer
    st.markdown("---")
    st.markdown(
        f"<div style='text-align: center; color: {AVANTUM_COLORS['dark']};'>"
        "Dashboard MentalPRO v2.0 | Gestión del Riesgo | Modelo AVANTUM"
        "</div>",
        unsafe_allow_html=True
    )


if __name__ == "__main__":
    main()