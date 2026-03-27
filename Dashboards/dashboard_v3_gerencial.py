"""
dashboard_v3_gerencial.py — Dashboard V3: Vista Ejecutiva / Gerencial

Dashboard de alto nivel para presentación a Comité de Dirección.
Muestra KPIs consolidados, semáforo general y tendencias.

Layout V3-Paso1:
  - Tarjetas de KPIs principales (19 indicadores)
  - Semáforo ejecutivo por empresa
  - Tendencia histórica (si hay datos)
  - Benchmark sectorial
  - Top 3 acciones prioritarias

Tecnología: Streamlit + Plotly
Tokens de diseño: AVANTUM (obligatorio)

Fuente documental: Visualizador 3, Paso 1
Versión: 1.0 | Pipeline MentalPRO | Modelo AVANTUM
"""

import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import numpy as np
from pathlib import Path
from typing import Dict

AVANTUM_COLORS = {
    "primary": "#0A1628",
    "secondary": "#1E3A5F",
    "accent": "#C9952A",
    "success": "#28A745",
    "warning": "#FFC107",
    "danger": "#DC3545",
    "critical": "#8B0000",
    "light": "#F8F9FA",
    "dark": "#343A40",
}

COLORES_SEMAFORO = {
    "VERDE": "#28A745",
    "AMARILLO": "#FFC107",
    "ROJO": "#DC3545",
}

FONT_FAMILY = "Inter, sans-serif"

st.set_page_config(
    page_title="MentalPRO - Vista Ejecutiva",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="collapsed"
)


@st.cache_data
def cargar_datos(ruta_base: str = "data") -> Dict[str, pd.DataFrame]:
    """Carga datasets para vista ejecutiva."""
    path = Path(ruta_base)
    datos = {}
    
    archivos = {
        "resumen": "final/resumen_ejecutivo.parquet",
        "kpi_empresa": "final/kpi_empresa.parquet",
        "prioridades": "processed/fact_prioridades.parquet",
    }
    
    for key, archivo in archivos.items():
        ruta = path / archivo
        if ruta.exists():
            datos[key] = pd.read_parquet(ruta)
        else:
            datos[key] = pd.DataFrame()
    
    return datos


def crear_tarjeta_kpi(titulo: str, valor: str, delta: str = None, color: str = None):
    """Crea una tarjeta de KPI estilizada."""
    delta_html = f"<span style='color: {'green' if delta and delta.startswith('+') else 'red'};'>{delta}</span>" if delta else ""
    color_borde = color or AVANTUM_COLORS["accent"]
    
    st.markdown(f"""
    <div style="
        background: white;
        border-left: 4px solid {color_borde};
        padding: 20px;
        border-radius: 8px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        margin-bottom: 10px;
    ">
        <h4 style="margin: 0; color: {AVANTUM_COLORS['dark']}; font-size: 14px;">{titulo}</h4>
        <p style="margin: 5px 0 0 0; font-size: 28px; font-weight: bold; color: {AVANTUM_COLORS['primary']};">
            {valor} {delta_html}
        </p>
    </div>
    """, unsafe_allow_html=True)


def crear_semaforo_empresas(df: pd.DataFrame) -> go.Figure:
    """Crea visualización de semáforo por empresa."""
    if len(df) == 0 or "semaforo" not in df.columns:
        return go.Figure()
    
    # Contar por semáforo
    conteo = df["semaforo"].value_counts()
    
    fig = go.Figure(go.Pie(
        labels=conteo.index.tolist(),
        values=conteo.values,
        marker_colors=[COLORES_SEMAFORO.get(s, "#999") for s in conteo.index],
        hole=0.5,
        textinfo="label+value",
        textfont={"size": 14},
    ))
    
    fig.update_layout(
        title="Semáforo General por Empresa",
        height=350,
        paper_bgcolor="white",
        font={"family": FONT_FAMILY},
        showlegend=False,
        annotations=[dict(text="Empresas", x=0.5, y=0.5, font_size=16, showarrow=False)]
    )
    
    return fig


def crear_ranking_empresas(df: pd.DataFrame) -> go.Figure:
    """Crea ranking de empresas por % riesgo crítico."""
    if len(df) == 0:
        return go.Figure()
    
    df_sorted = df.sort_values("pct_riesgo_critico", ascending=True).head(10)
    
    colores = [
        COLORES_SEMAFORO.get(s, "#999") 
        for s in df_sorted["semaforo"]
    ]
    
    fig = go.Figure(go.Bar(
        x=df_sorted["pct_riesgo_critico"],
        y=df_sorted["empresa"],
        orientation="h",
        marker_color=colores,
        text=df_sorted["pct_riesgo_critico"].apply(lambda x: f"{x:.1f}%"),
        textposition="auto",
    ))
    
    fig.update_layout(
        title="Top 10 Empresas - % Riesgo Crítico (menor = mejor)",
        xaxis_title="% Riesgo Crítico",
        yaxis_title="",
        height=400,
        margin=dict(l=200),
        paper_bgcolor="white",
        plot_bgcolor="white",
        font={"family": FONT_FAMILY},
    )
    
    return fig


def crear_resumen_sector(df: pd.DataFrame) -> go.Figure:
    """Crea comparativo por sector económico."""
    if len(df) == 0 or "sector_rag" not in df.columns:
        return go.Figure()
    
    df_sector = df.groupby("sector_rag").agg({
        "pct_riesgo_critico": "mean",
        "n_trabajadores": "sum",
        "empresa": "count",
    }).reset_index()
    df_sector.columns = ["Sector", "% Riesgo", "Trabajadores", "Empresas"]
    
    fig = px.bar(
        df_sector.sort_values("% Riesgo"),
        x="% Riesgo",
        y="Sector",
        orientation="h",
        color="% Riesgo",
        color_continuous_scale=["#28A745", "#FFC107", "#DC3545"],
        text="% Riesgo",
    )
    
    fig.update_traces(texttemplate="%{text:.1f}%", textposition="auto")
    fig.update_layout(
        title="Promedio % Riesgo Crítico por Sector",
        height=350,
        paper_bgcolor="white",
        font={"family": FONT_FAMILY},
        coloraxis_showscale=False,
    )
    
    return fig


def main():
    """Función principal del dashboard ejecutivo."""
    # Header ejecutivo
    col_logo, col_titulo = st.columns([1, 5])
    with col_titulo:
        st.title("📊 MentalPRO - Vista Ejecutiva")
        st.markdown("**Resumen para Comité de Dirección**")
    
    st.markdown("---")
    
    datos = cargar_datos()
    
    if len(datos.get("resumen", pd.DataFrame())) == 0:
        st.error("No se encontraron datos. Ejecute primero el pipeline completo.")
        return
    
    df_resumen = datos["resumen"]
    df_kpi = datos.get("kpi_empresa", pd.DataFrame())
    df_prio = datos.get("prioridades", pd.DataFrame())
    
    # KPIs principales en tarjetas
    st.subheader("📈 KPIs Principales")
    
    col1, col2, col3, col4, col5 = st.columns(5)
    
    total_empresas = df_resumen["empresa"].nunique()
    total_trabajadores = df_resumen["n_trabajadores"].sum()
    pct_verde = len(df_resumen[df_resumen["semaforo"] == "VERDE"]) / total_empresas * 100 if total_empresas > 0 else 0
    pct_rojo = len(df_resumen[df_resumen["semaforo"] == "ROJO"]) / total_empresas * 100 if total_empresas > 0 else 0
    protocolos_activos = df_prio[df_prio["activacion_flag"] == True]["prot_id"].nunique() if len(df_prio) > 0 else 0
    
    with col1:
        crear_tarjeta_kpi("Empresas", f"{total_empresas:,}")
    with col2:
        crear_tarjeta_kpi("Trabajadores", f"{total_trabajadores:,}")
    with col3:
        crear_tarjeta_kpi("% Semáforo Verde", f"{pct_verde:.0f}%", color=COLORES_SEMAFORO["VERDE"])
    with col4:
        crear_tarjeta_kpi("% Semáforo Rojo", f"{pct_rojo:.0f}%", color=COLORES_SEMAFORO["ROJO"])
    with col5:
        crear_tarjeta_kpi("Protocolos Activos", f"{protocolos_activos}")
    
    st.markdown("---")
    
    # Gráficos principales
    col_izq, col_der = st.columns(2)
    
    with col_izq:
        fig = crear_semaforo_empresas(df_resumen)
        st.plotly_chart(fig, use_container_width=True)
    
    with col_der:
        fig = crear_resumen_sector(df_resumen)
        st.plotly_chart(fig, use_container_width=True)
    
    st.markdown("---")
    
    # Ranking y acciones
    col_rank, col_acciones = st.columns([2, 1])
    
    with col_rank:
        fig = crear_ranking_empresas(df_resumen)
        st.plotly_chart(fig, use_container_width=True)
    
    with col_acciones:
        st.subheader("🎯 Top 3 Acciones Prioritarias")
        
        if len(df_prio) > 0:
            top_prio = df_prio[df_prio["prioridad_calculada"] == "Crítica"].head(3)
            if len(top_prio) == 0:
                top_prio = df_prio.nsmallest(3, "score_linea")
            
            for i, (_, row) in enumerate(top_prio.iterrows(), 1):
                st.markdown(f"""
                <div style="
                    background: white;
                    border-left: 4px solid {AVANTUM_COLORS['danger']};
                    padding: 15px;
                    margin-bottom: 10px;
                    border-radius: 4px;
                ">
                    <strong>{i}. {row['prot_id']}</strong><br>
                    <small>{row.get('linea', 'N/A')}</small><br>
                    <span style="color: {AVANTUM_COLORS['danger']};">Score: {row['score_linea']:.2f}</span>
                </div>
                """, unsafe_allow_html=True)
        else:
            st.info("No hay datos de priorización disponibles")
    
    # Footer
    st.markdown("---")
    st.markdown(
        f"<div style='text-align: center; color: {AVANTUM_COLORS['dark']};'>"
        "Dashboard MentalPRO v3.0 | Vista Ejecutiva | Modelo AVANTUM"
        "</div>",
        unsafe_allow_html=True
    )


if __name__ == "__main__":
    main()