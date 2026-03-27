"""
08_consolidacion.py — Implementa V3-Paso 1: KPIs Consolidados

Consolida todos los outputs anteriores en tablas finales para dashboards.
Genera KPIs agregados por empresa, área, cargo, demografía.

Input:
  - data/processed/fact_benchmark.parquet (scores + niveles)
  - data/processed/fact_prioridades.parquet (protocolos)
  - data/processed/fact_gestion_scores.parquet (gestión)

Output:
  - data/final/kpi_empresa.parquet
  - data/final/kpi_area.parquet
  - data/final/kpi_demografico.parquet
  - data/final/resumen_ejecutivo.parquet

Fuente documental: Visualizador 3, Paso 1
Versión: 1.0 | Pipeline MentalPRO | Modelo AVANTUM
"""

import logging
import sys
from pathlib import Path
from typing import Tuple, Dict, List
import numpy as np
import pandas as pd
import yaml

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("07_consolidacion")


def cargar_config(config_path: str = "config/config.yaml") -> dict:
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


# ===========================================================================
# CONSTANTES Y UMBRALES
# ===========================================================================

# Niveles de riesgo para cálculos
NIVELES_RIESGO = ["Bajo", "Medio", "Alto", "Muy alto"]
NIVELES_CRITICOS = ["Alto", "Muy alto"]  # Para % población en riesgo

# KPIs por protocolo (de 05_prioridades)
KPI_POR_PROTOCOLO = {
    "PROT-01": "IAEE",   # Índice Afrontamiento Eficaz del Estrés
    "PROT-02": "IBC",    # Índice Bienestar Cognitivo
    "PROT-03": "IBET",   # Índice Bienestar Emocional y Trascendente
    "PROT-07": "IBFT",   # Índice Bienestar Financiero del Trabajador
    "PROT-08": "IBFHS",  # Índice Bienestar Físico y Hábitos Saludables
    "PROT-09": "ICVT",   # Índice Conciliación Vida-Trabajo
    "PROT-13": "IGDE",   # Índice Gestión Demandas Emocionales
    "PROT-15": "ICTS",   # Índice Carga de Trabajo Saludable
    "PROT-16": "IJTS",   # Índice Jornadas de Trabajo Saludables
    "PROT-18": "TCLC",   # Tasa Convivencia y Respeto
    "PROT-19": "ILIPP",  # Índice Liderazgo Impacto Psicosocial Positivo
    "PROT-20": "IEO",    # Índice Engagement Organizacional
}


# ===========================================================================
# FUNCIONES DE CÁLCULO DE KPIs
# ===========================================================================

def calcular_kpis_basicos(df: pd.DataFrame, grupo_cols: List[str]) -> pd.DataFrame:
    """
    Calcula KPIs básicos agregados por grupo.
    
    KPIs calculados:
    - n_trabajadores: Conteo único de cédulas
    - score_promedio: Media del score_transformado
    - pct_riesgo_alto: % trabajadores en Alto + Muy alto
    - pct_riesgo_critico: % trabajadores en Muy alto solamente
    - nivel_predominante: Moda del nivel de riesgo
    """
    aggs = {
        "cedula": "nunique",
        "score_transformado": ["mean", "std", "min", "max"],
    }
    
    df_agg = df.groupby(grupo_cols).agg(aggs).reset_index()
    df_agg.columns = grupo_cols + ["n_trabajadores", "score_promedio", "score_std", "score_min", "score_max"]
    
    # Calcular % en riesgo alto/muy alto
    def pct_riesgo_alto(group):
        total = len(group)
        if total == 0:
            return 0
        en_riesgo = len(group[group["nivel_riesgo_colombia"].isin(NIVELES_CRITICOS)])
        return round(100 * en_riesgo / total, 1)
    
    df_pct = df.groupby(grupo_cols).apply(pct_riesgo_alto).reset_index(name="pct_riesgo_alto")
    
    # Calcular nivel predominante (moda)
    df_moda = df.groupby(grupo_cols)["nivel_riesgo_colombia"].agg(
        lambda x: x.mode().iloc[0] if len(x.mode()) > 0 else "N/A"
    ).reset_index(name="nivel_predominante")
    
    # Merge todo
    df_result = df_agg.merge(df_pct, on=grupo_cols).merge(df_moda, on=grupo_cols)
    
    return df_result


def calcular_distribucion_niveles(df: pd.DataFrame, grupo_cols: List[str]) -> pd.DataFrame:
    """Calcula distribución porcentual por nivel de riesgo."""
    # Pivot para obtener conteos por nivel
    df_dist = df.groupby(grupo_cols + ["nivel_riesgo_colombia"]).size().unstack(fill_value=0)
    
    # Normalizar a porcentajes
    df_dist = df_dist.div(df_dist.sum(axis=1), axis=0) * 100
    df_dist = df_dist.round(1)
    
    # Renombrar columnas
    df_dist.columns = [f"pct_{col.lower().replace(' ', '_')}" for col in df_dist.columns]
    df_dist = df_dist.reset_index()
    
    return df_dist


def calcular_kpi_protocolo(df_prioridades: pd.DataFrame, empresa: str) -> pd.DataFrame:
    """Calcula KPIs específicos por protocolo para una empresa."""
    df_emp = df_prioridades[df_prioridades["empresa"] == empresa].copy()
    
    if len(df_emp) == 0:
        return pd.DataFrame()
    
    resultados = []
    for _, row in df_emp.iterrows():
        prot_id = row["prot_id"]
        kpi_nombre = KPI_POR_PROTOCOLO.get(prot_id, "")
        
        resultados.append({
            "empresa": empresa,
            "prot_id": prot_id,
            "linea": row["linea"],
            "kpi_codigo": row.get("kpi", kpi_nombre),
            "score_linea": row["score_linea"],
            "pct_urgente": row["pct_urgente_correctiva"],
            "prioridad": row["prioridad_calculada"],
            "activacion": row["activacion_flag"],
        })
    
    return pd.DataFrame(resultados)


# ===========================================================================
# CONSOLIDACIÓN POR DIMENSIÓN
# ===========================================================================

def consolidar_por_empresa(df_benchmark: pd.DataFrame) -> pd.DataFrame:
    """Consolida KPIs a nivel empresa."""
    log.info("Consolidando KPIs por empresa...")
    
    # Filtrar solo registros de factor total (no dimensiones/dominios)
    df_factor = df_benchmark[
        (df_benchmark["dimension"].isna() | (df_benchmark["dimension"] == "")) &
        (df_benchmark["dominio"].isna() | (df_benchmark["dominio"] == ""))
    ].copy()
    
    grupo = ["empresa", "sector_rag", "factor"]
    df_kpis = calcular_kpis_basicos(df_factor, grupo)
    df_dist = calcular_distribucion_niveles(df_factor, grupo)
    
    df_result = df_kpis.merge(df_dist, on=grupo, how="left")
    
    log.info("  Generados %d registros empresa-factor", len(df_result))
    return df_result


def consolidar_por_area(df_benchmark: pd.DataFrame) -> pd.DataFrame:
    """Consolida KPIs a nivel área/dependencia."""
    log.info("Consolidando KPIs por área...")
    
    if "area" not in df_benchmark.columns:
        log.warning("  Columna 'area' no encontrada, usando 'cargo' como alternativa")
        col_area = "cargo" if "cargo" in df_benchmark.columns else None
        if col_area is None:
            return pd.DataFrame()
    else:
        col_area = "area"
    
    df_factor = df_benchmark[
        (df_benchmark["dimension"].isna() | (df_benchmark["dimension"] == "")) &
        (df_benchmark["dominio"].isna() | (df_benchmark["dominio"] == ""))
    ].copy()
    
    grupo = ["empresa", col_area, "factor"]
    df_kpis = calcular_kpis_basicos(df_factor, grupo)
    
    log.info("  Generados %d registros área-factor", len(df_kpis))
    return df_kpis


def consolidar_demografico(df_benchmark: pd.DataFrame) -> pd.DataFrame:
    """Consolida KPIs por variables demográficas."""
    log.info("Consolidando KPIs demográficos...")
    
    # Variables demográficas disponibles
    demo_vars = ["genero", "rango_edad", "antiguedad", "nivel_educativo", "tipo_contrato"]
    available_vars = [v for v in demo_vars if v in df_benchmark.columns]
    
    if not available_vars:
        log.warning("  No se encontraron variables demográficas")
        return pd.DataFrame()
    
    df_factor = df_benchmark[
        (df_benchmark["dimension"].isna() | (df_benchmark["dimension"] == "")) &
        (df_benchmark["dominio"].isna() | (df_benchmark["dominio"] == ""))
    ].copy()
    
    all_results = []
    for var in available_vars:
        grupo = ["empresa", var, "factor"]
        df_kpis = calcular_kpis_basicos(df_factor, grupo)
        df_kpis["dimension_demografica"] = var
        df_kpis = df_kpis.rename(columns={var: "valor_demografico"})
        all_results.append(df_kpis)
    
    if not all_results:
        return pd.DataFrame()
    
    df_result = pd.concat(all_results, ignore_index=True)
    log.info("  Generados %d registros demográficos", len(df_result))
    return df_result


# ===========================================================================
# RESUMEN EJECUTIVO
# ===========================================================================

def generar_resumen_ejecutivo(
    df_benchmark: pd.DataFrame,
    df_prioridades: pd.DataFrame,
    df_gestion: pd.DataFrame
) -> pd.DataFrame:
    """
    Genera resumen ejecutivo por empresa con métricas clave.
    
    Incluye:
    - Score general por factor
    - Nivel de riesgo predominante
    - Top 3 protocolos prioritarios
    - % población en riesgo crítico
    - Indicador de alerta (semáforo)
    """
    log.info("Generando resumen ejecutivo...")
    
    empresas = df_benchmark["empresa"].unique()
    resumenes = []
    
    for empresa in empresas:
        df_emp = df_benchmark[df_benchmark["empresa"] == empresa]
        sector = df_emp["sector_rag"].iloc[0] if "sector_rag" in df_emp.columns else "N/A"
        
        # Scores por factor
        scores = {}
        for factor in ["intralaboral", "extralaboral", "estres"]:
            df_f = df_emp[
                (df_emp["factor"] == factor) &
                (df_emp["dimension"].isna() | (df_emp["dimension"] == "")) &
                (df_emp["dominio"].isna() | (df_emp["dominio"] == ""))
            ]
            if len(df_f) > 0:
                scores[f"score_{factor}"] = round(df_f["score_transformado"].mean(), 1)
                # Nivel predominante
                nivel = df_f["nivel_riesgo_colombia"].mode()
                scores[f"nivel_{factor}"] = nivel.iloc[0] if len(nivel) > 0 else "N/A"
            else:
                scores[f"score_{factor}"] = None
                scores[f"nivel_{factor}"] = "N/A"
        
        # % en riesgo crítico (Alto + Muy alto)
        total = len(df_emp["cedula"].unique())
        df_critico = df_emp[df_emp["nivel_riesgo_colombia"].isin(NIVELES_CRITICOS)]
        pct_critico = round(100 * len(df_critico["cedula"].unique()) / total, 1) if total > 0 else 0
        
        # Top 3 protocolos prioritarios
        df_prio = df_prioridades[df_prioridades["empresa"] == empresa]
        top_prots = df_prio[df_prio["activacion_flag"] == True].nsmallest(3, "score_linea")
        top_3 = ", ".join(top_prots["prot_id"].tolist()) if len(top_prots) > 0 else "Ninguno"
        
        # Indicador semáforo
        if pct_critico > 40:
            semaforo = "ROJO"
        elif pct_critico > 20:
            semaforo = "AMARILLO"
        else:
            semaforo = "VERDE"
        
        resumen = {
            "empresa": empresa,
            "sector_rag": sector,
            "n_trabajadores": total,
            **scores,
            "pct_riesgo_critico": pct_critico,
            "top_3_protocolos": top_3,
            "n_protocolos_activos": len(df_prio[df_prio["activacion_flag"] == True]),
            "semaforo": semaforo,
        }
        resumenes.append(resumen)
    
    df_result = pd.DataFrame(resumenes)
    log.info("  Generados %d resúmenes ejecutivos", len(df_result))
    return df_result


# ===========================================================================
# VALIDACIÓN
# ===========================================================================

def validar_consolidacion(df_empresa: pd.DataFrame, df_resumen: pd.DataFrame) -> Tuple[bool, List[str]]:
    """Valida outputs de consolidación."""
    log.info("Validando consolidación...")
    errores = []
    
    # 1. Todas las empresas tienen resumen
    empresas_kpi = set(df_empresa["empresa"].unique())
    empresas_resumen = set(df_resumen["empresa"].unique())
    faltantes = empresas_kpi - empresas_resumen
    if faltantes:
        errores.append(f"Empresas sin resumen: {faltantes}")
    
    # 2. Scores en rango válido
    if (df_empresa["score_promedio"] < 0).any() or (df_empresa["score_promedio"] > 100).any():
        errores.append("Scores fuera de rango 0-100")
    
    # 3. Porcentajes válidos
    if (df_empresa["pct_riesgo_alto"] < 0).any() or (df_empresa["pct_riesgo_alto"] > 100).any():
        errores.append("Porcentajes fuera de rango 0-100")
    
    es_valido = len(errores) == 0
    if es_valido:
        log.info("Validación EXITOSA")
    else:
        log.error("Validación FALLIDA: %s", errores)
    return es_valido, errores


# ===========================================================================
# PIPELINE PRINCIPAL
# ===========================================================================

def main(config_path: str = "config/config.yaml") -> None:
    """
    Pipeline principal 07.
    1. Carga fact_benchmark, fact_prioridades, fact_gestion_scores
    2. Consolida KPIs por empresa, área, demografía
    3. Genera resumen ejecutivo
    4. Guarda outputs en data/final/
    """
    config = cargar_config(config_path)
    proc_path = Path(config["paths"]["processed"])
    final_path = Path(config["paths"]["final"])
    final_path.mkdir(parents=True, exist_ok=True)
    
    log.info("=" * 60)
    log.info("07_consolidacion.py — Iniciando pipeline")
    log.info("=" * 60)
    
    # --- Carga
    ruta_benchmark = proc_path / "fact_benchmark.parquet"
    ruta_prioridades = proc_path / "fact_prioridades.parquet"
    ruta_gestion = proc_path / "fact_gestion_scores.parquet"
    
    for ruta in [ruta_benchmark, ruta_prioridades, ruta_gestion]:
        if not ruta.exists():
            log.error("Archivo no encontrado: %s", ruta)
            sys.exit(1)
    
    df_benchmark = pd.read_parquet(ruta_benchmark)
    df_prioridades = pd.read_parquet(ruta_prioridades)
    df_gestion = pd.read_parquet(ruta_gestion)
    
    log.info("Cargados: benchmark=%d, prioridades=%d, gestion=%d",
             len(df_benchmark), len(df_prioridades), len(df_gestion))
    
    # --- Consolidación
    df_empresa = consolidar_por_empresa(df_benchmark)
    df_area = consolidar_por_area(df_benchmark)
    df_demo = consolidar_demografico(df_benchmark)
    df_resumen = generar_resumen_ejecutivo(df_benchmark, df_prioridades, df_gestion)
    
    # --- Validación
    ok, _ = validar_consolidacion(df_empresa, df_resumen)
    if not ok:
        log.warning("Pipeline 07 con advertencias")
    
    # --- Guardar
    df_empresa.to_parquet(final_path / "kpi_empresa.parquet", index=False)
    df_resumen.to_parquet(final_path / "resumen_ejecutivo.parquet", index=False)
    
    if len(df_area) > 0:
        df_area.to_parquet(final_path / "kpi_area.parquet", index=False)
    if len(df_demo) > 0:
        df_demo.to_parquet(final_path / "kpi_demografico.parquet", index=False)
    
    log.info("Guardados en %s", final_path)
    
    # --- Resumen
    log.info("-" * 40)
    log.info("RESUMEN:")
    log.info("  Empresas: %d", df_resumen["empresa"].nunique())
    log.info("  KPIs empresa: %d registros", len(df_empresa))
    log.info("  KPIs área: %d registros", len(df_area))
    log.info("  KPIs demográficos: %d registros", len(df_demo))
    
    # Distribución semáforo
    if "semaforo" in df_resumen.columns:
        dist = df_resumen["semaforo"].value_counts()
        log.info("  Semáforo empresas:")
        for sem, n in dist.items():
            log.info("    %s: %d", sem, n)
    
    log.info("-" * 40)
    log.info("07 completado exitosamente.")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="07_consolidacion.py — V3 Paso 1")
    parser.add_argument("--config", default="config/config.yaml")
    args = parser.parse_args()
    main(config_path=args.config)