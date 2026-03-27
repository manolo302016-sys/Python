"""
dashboard_v4_asis.py — Dashboard V4: Análisis de Costos (ASIS)

Visualiza el impacto económico del ausentismo asociado a riesgo
psicosocial, incluyendo factor de presentismo.

Layout V4:
  - Costo total por empresa (directo + presentismo)
  - Distribución por causa de ausentismo
  - Correlación nivel de riesgo vs costo
  - Proyección anual
  - Simulador ROI de intervención

BINDING D5: Factor presentismo = 1.40 (40% adicional)

Tecnología: Streamlit + Plotly
Tokens de diseño: AVANTUM (obligatorio)

Fuente documental: Visualizador 4
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
    "light": "#F8F9FA",
    "dark": "#343A40",
}

# BINDING D5: Factor presentismo
FACTOR_PRESENTISMO = 1.40

FONT_FAMILY = "Inter, sans-serif"

st.set_page_config(
    page_title="MentalPRO - Análisis de Costos",
    page_icon="💰",
    layout="wide",
    initial_sidebar_state="expanded"
)


@st.cache_data
def cargar_datos(ruta_base: str = "data") -> Dict[str, pd.DataFrame]:
    """Carga datasets de costos."""
    path = Path(ruta_base)
    datos = {}
    archivos = {
        "costos": "final/costo_ausentismo.parquet",
        "costos_causa": "final/costo_por_causa.parquet",
        "costos_nivel": "final/costo_por_nivel_riesgo.parquet",
        "resumen": "final/resumen_ejecutivo.parquet",
    }
    for key, archivo in archivos.items():
        ruta = path / archivo
        if ruta.exists():
            datos[key] = pd.read_parquet(ruta)
        else:
            datos[key] = pd.DataFrame()
    return datos


def formatear_moneda(valor: float) -> str:
    """Formatea valor como moneda colombiana."""
    if valor >= 1_000_000_000:
        return f"${valor/1_000_000_000:.1f}B"
    elif valor >= 1_000_000:
        return f"${valor/1_000_000:.1f}M"
    elif valor >= 1_000:
        return f"${valor/1_000:.1f}K"
    else:
        return f"${valor:,.0f}"


def crear_waterfall_costos(df: pd.DataFrame) -> go.Figure:
    """Crea gráfico waterfall de componentes del costo."""
    if len(df) == 0:
        return go.Figure()
    
    costo_directo = df["costo_directo"].sum()
    costo_presentismo = df["costo_presentismo"].sum()
    costo_total = df["costo_total"].sum()
    
    fig = go.Figure(go.Waterfall(
        name="Costo",
        orientation="v",
        x=["Costo Directo", "Presentismo (+40%)", "COSTO TOTAL"],
        y=[costo_directo, costo_presentismo, costo_total],
        textposition="outside",
        text=[formatear_moneda(costo_directo), formatear_moneda(costo_presentismo), formatear_moneda(costo_total)],
        connector={"line": {"color": AVANTUM_COLORS["primary"]}},
        decreasing={"marker": {"color": AVANTUM_COLORS["success"]}},
        increasing={"marker": {"color": AVANTUM_COLORS["danger"]}},
        totals={"marker": {"color": AVANTUM_COLORS["accent"]}},
    ))
    
    fig.update_layout(
        title=f"Composición del Costo (Factor Presentismo: {FACTOR_PRESENTISMO}x)",
        yaxis_title="Costo ($)",
        height=400,
        paper_bgcolor="white",
        font={"family": FONT_FAMILY},
    )
    return fig


def crear_costos_por_causa(df: pd.DataFrame) -> go.Figure:
    """Crea gráfico de costos por causa de ausentismo."""
    if len(df) == 0:
        return go.Figure()
    
    df_sorted = df.sort_values("costo_total", ascending=True).tail(10)
    
    colores = [
        AVANTUM_COLORS["danger"] if psico else AVANTUM_COLORS["secondary"]
        for psico in df_sorted["es_psicosocial"]
    ]
    
    fig = go.Figure(go.Bar(
        x=df_sorted["costo_total"],
        y=df_sorted["causa"],
        orientation="h",
        marker_color=colores,
        text=df_sorted["costo_total"].apply(formatear_moneda),
        textposition="auto",
    ))
    
    fig.update_layout(
        title="Top 10 Causas de Ausentismo por Costo",
        xaxis_title="Costo Total ($)",
        yaxis_title="",
        height=400,
        margin=dict(l=200),
        paper_bgcolor="white",
        plot_bgcolor="white",
        font={"family": FONT_FAMILY},
    )
    return fig


def crear_correlacion_riesgo_costo(df_costo: pd.DataFrame, df_resumen: pd.DataFrame) -> go.Figure:
    """Crea scatter de correlación nivel riesgo vs costo."""
    if len(df_costo) == 0 or len(df_resumen) == 0:
        return go.Figure()
    
    df = df_costo.merge(
        df_resumen[["empresa", "pct_riesgo_critico", "semaforo"]],
        on="empresa", how="left"
    )
    if len(df) == 0:
        return go.Figure()
    
    fig = px.scatter(
        df, x="pct_riesgo_critico", y="costo_total",
        size="n_trabajadores_ausentes", color="semaforo",
        color_discrete_map={"VERDE": "#28A745", "AMARILLO": "#FFC107", "ROJO": "#DC3545"},
        hover_name="empresa",
        title="Correlación: % Riesgo Crítico vs Costo de Ausentismo",
    )
    fig.update_layout(
        xaxis_title="% Trabajadores en Riesgo Crítico",
        yaxis_title="Costo Total ($)",
        height=400, paper_bgcolor="white", font={"family": FONT_FAMILY},
    )
    return fig


def simulador_roi():
    """Componente de simulación de ROI."""
    st.subheader("🧮 Simulador de ROI")
    col1, col2 = st.columns(2)
    with col1:
        costo_actual = st.number_input("Costo actual de ausentismo ($)",
            min_value=0, value=100_000_000, step=1_000_000, format="%d")
        reduccion_esperada = st.slider("Reducción esperada con intervención (%)",
            min_value=5, max_value=50, value=20, step=5)
    with col2:
        costo_intervencion = st.number_input("Costo de la intervención ($)",
            min_value=0, value=20_000_000, step=1_000_000, format="%d")
    
    ahorro = costo_actual * (reduccion_esperada / 100)
    roi = ((ahorro - costo_intervencion) / costo_intervencion * 100) if costo_intervencion > 0 else 0
    payback = (costo_intervencion / (ahorro / 12)) if ahorro > 0 else float("inf")
    
    st.markdown("---")
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("💰 Ahorro Esperado", formatear_moneda(ahorro))
    with col2:
        st.metric("📈 ROI", f"{roi:.0f}%")
    with col3:
        st.metric("⏱️ Payback", f"{payback:.1f} meses" if payback < 100 else "N/A")
    
    if roi > 100:
        st.success("✅ Intervención altamente rentable (ROI > 100%)")
    elif roi > 0:
        st.warning("⚠️ Intervención rentable pero con ROI moderado")
    else:
        st.error("❌ Intervención no rentable con los parámetros actuales")


def main():
    """Función principal del dashboard V4."""
    st.title("💰 MentalPRO - Análisis de Costos de Ausentismo")
    st.markdown(f"**Factor de Presentismo: {FACTOR_PRESENTISMO}x** (BINDING D5)")
    st.markdown("---")
    
    datos = cargar_datos()
    df_costos = datos.get("costos", pd.DataFrame())
    df_causa = datos.get("costos_causa", pd.DataFrame())
    df_resumen = datos.get("resumen", pd.DataFrame())
    
    if len(df_costos) == 0:
        st.warning("No hay datos de costos disponibles.")
        st.info("El dashboard requiere dim_ausentismo.parquet con datos de días perdidos.")
        st.markdown("---")
        simulador_roi()
        return
    
    # Sidebar filtros
    st.sidebar.title("Filtros")
    empresas = ["Todas"] + sorted(df_costos["empresa"].unique().tolist())
    empresa_sel = st.sidebar.selectbox("Empresa", empresas)
    
    if empresa_sel != "Todas":
        df_costos = df_costos[df_costos["empresa"] == empresa_sel]
        if len(df_causa) > 0:
            df_causa = df_causa[df_causa["empresa"] == empresa_sel]
    
    # KPIs principales
    col1, col2, col3, col4 = st.columns(4)
    costo_total = df_costos["costo_total"].sum()
    costo_directo = df_costos["costo_directo"].sum()
    dias_perdidos = df_costos["dias_perdidos"].sum()
    costo_psico = df_causa[df_causa["es_psicosocial"]]["costo_total"].sum() if len(df_causa) > 0 else 0
    
    col1.metric("Costo Total", formatear_moneda(costo_total))
    col2.metric("Costo Directo", formatear_moneda(costo_directo))
    col3.metric("Días Perdidos", f"{dias_perdidos:,.0f}")
    col4.metric("Costo Psicosocial", formatear_moneda(costo_psico))
    
    st.markdown("---")
    
    # Tabs
    tab1, tab2, tab3, tab4 = st.tabs(["📊 Composición", "🏷️ Por Causa", "📈 Correlación", "🧮 Simulador"])
    
    with tab1:
        fig = crear_waterfall_costos(df_costos)
        st.plotly_chart(fig, use_container_width=True)
        if "costo_anual_proyectado" in df_costos.columns:
            st.subheader("Proyección Anual")
            proy = df_costos["costo_anual_proyectado"].sum()
            st.metric("Costo Proyectado 12 meses", formatear_moneda(proy))
    
    with tab2:
        if len(df_causa) > 0:
            fig = crear_costos_por_causa(df_causa)
            st.plotly_chart(fig, use_container_width=True)
            st.subheader("Detalle por Causa")
            st.dataframe(
                df_causa[["causa", "categoria_causa", "n_casos", "dias_perdidos", "costo_total", "es_psicosocial"]]
                .sort_values("costo_total", ascending=False),
                use_container_width=True, hide_index=True
            )
        else:
            st.info("No hay datos de causas disponibles")
    
    with tab3:
        fig = crear_correlacion_riesgo_costo(df_costos, df_resumen)
        st.plotly_chart(fig, use_container_width=True)
    
    with tab4:
        simulador_roi()
    
    # Footer
    st.markdown("---")
    st.markdown(
        f"<div style='text-align: center; color: {AVANTUM_COLORS["dark"]};'>"
        f"Dashboard MentalPRO v4.0 | Análisis ASIS | Factor Presentismo: {FACTOR_PRESENTISMO}x"
        "</div>", unsafe_allow_html=True
    )


if __name__ == "__main__":
    main()