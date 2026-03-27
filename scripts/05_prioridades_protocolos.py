"""
05_prioridades_protocolos.py — Implementa V2-Pasos 6 y 7
Pasos:
  V2-Paso6 : Jerarquía por lesividad + KPIs por línea de gestión + asignación PROT_ID
  V2-Paso7 : Prioridades de protocolos por sector económico

Input:  data/processed/fact_gestion_scores.parquet
Output: data/processed/fact_prioridades.parquet

Contenido:
  - Mapeo línea de gestión → PROT_ID (PROT-01 a PROT-20)
  - Prioridad de impacto (Alta/Media/Baja) por indicador
  - Protocolos prioritarios por sector económico
  - Fórmula KPI general: (N trabajadores score≥umbral / Total) × 100

Fuente documental: Visualizador 2, Pasos 6-7 (filas 739-902)
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
log = logging.getLogger("05_prioridades_protocolos")


def cargar_config(config_path: str = "config/config.yaml") -> dict:
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


# ===========================================================================
# V2-PASO 6: MAPEO LÍNEA → PROTOCOLO + PRIORIDAD
# Fuente: Visualizador 2, Paso 6 (filas 739-762)
# ===========================================================================

# Mapeo: línea de gestión → (PROT_ID, nombre_protocolo, prioridad, KPI)
LINEA_A_PROTOCOLO = {
    # Bienestar biopsicosocial
    "Afrontamiento del estrés y recursos psicológicos": {
        "prot_id": "PROT-01",
        "nombre": "Afrontamiento del estrés y recursos psicológicos",
        "prioridad": "Alta",
        "kpi": "IAEE",
        "kpi_nombre": "Índice de Afrontamiento Eficaz del Estrés",
    },
    "Bienestar cognitivo": {
        "prot_id": "PROT-02",
        "nombre": "Bienestar cognitivo",
        "prioridad": "Baja",
        "kpi": "IBC",
        "kpi_nombre": "Índice de Bienestar Cognitivo",
    },
    "Bienestar emocional y trascendente": {
        "prot_id": "PROT-03",  # También PROT-04, PROT-05
        "nombre": "Bienestar emocional y trascendente",
        "prioridad": "Alta",
        "kpi": "IBET",
        "kpi_nombre": "Índice de Bienestar Emocional y Trascendente",
    },
    "Bienestar extralaboral": {
        "prot_id": "PROT-06",
        "nombre": "Bienestar extralaboral",
        "prioridad": "Media",
        "kpi": "IBE",
        "kpi_nombre": "Índice de Bienestar Extralaboral",
    },
    "Bienestar financiero": {
        "prot_id": "PROT-07",
        "nombre": "Bienestar financiero",
        "prioridad": "Alta",
        "kpi": "IBFT",
        "kpi_nombre": "Índice de Bienestar Financiero del Trabajador",
    },
    "Bienestar físico": {
        "prot_id": "PROT-08",
        "nombre": "Bienestar físico y hábitos saludables",
        "prioridad": "Alta",
        "kpi": "IBFHS",
        "kpi_nombre": "Índice de Bienestar Físico y Hábitos Saludables",
    },
    "Bienestar vida-trabajo": {
        "prot_id": "PROT-09",
        "nombre": "Bienestar vida-trabajo (conciliación)",
        "prioridad": "Media",
        "kpi": "ICVT",
        "kpi_nombre": "Índice de Conciliación Vida-Trabajo",
    },
    "Comportamientos seguros": {
        "prot_id": "PROT-10",
        "nombre": "Prevención de conductas de riesgo",
        "prioridad": "Baja",
        "kpi": "TCR",
        "kpi_nombre": "Tasa de Comportamientos Seguros",
    },
    "Motivación laboral": {
        "prot_id": "PROT-20",
        "nombre": "Gestión del Engagement organizacional",
        "prioridad": "Media",
        "kpi": "IML",
        "kpi_nombre": "Índice de Motivación Laboral",
    },
    # Condiciones de trabajo saludable
    "Arquitectura de roles y responsabilidades": {
        "prot_id": "PROT-11",
        "nombre": "Arquitectura de roles y responsabilidades",
        "prioridad": "Media",
        "kpi": "ICRR",
        "kpi_nombre": "Índice de Claridad de Roles y Responsabilidades",
    },
    "Aprendizaje y desarrollo (L&D Strategy)": {
        "prot_id": "PROT-12",
        "nombre": "Estrategia de aprendizaje y desarrollo",
        "prioridad": "Baja",
        "kpi": "IDCA",
        "kpi_nombre": "Índice de Desarrollo de Competencias y Aprendizaje",
    },
    "Condiciones emocionales saludables": {
        "prot_id": "PROT-13",
        "nombre": "Gestión de condiciones emocionales",
        "prioridad": "Alta",
        "kpi": "IGDE",
        "kpi_nombre": "Índice de Gestión de Demandas Emocionales",
    },
    "Condiciones físicas saludables": {
        "prot_id": "PROT-14",
        "nombre": "Gestión de condiciones físicas y ergonomía",
        "prioridad": "Media",
        "kpi": "ICFS",
        "kpi_nombre": "Índice de Condiciones Físicas Saludables",
    },
    "Carga de trabajo saludable": {
        "prot_id": "PROT-15",
        "nombre": "Gestión de la carga de trabajo",
        "prioridad": "Alta",
        "kpi": "ICTS",
        "kpi_nombre": "Índice de Carga de Trabajo Saludable",
    },
    "Jornadas de trabajo saludables": {
        "prot_id": "PROT-16",
        "nombre": "Gestión de jornadas de trabajo",
        "prioridad": "Alta",
        "kpi": "IJTS",
        "kpi_nombre": "Índice de Jornadas de Trabajo Saludables",
    },
    "Cambio organizacional": {
        "prot_id": "PROT-17",
        "nombre": "Gestión del cambio organizacional",
        "prioridad": "Baja",
        "kpi": "IGSCO",
        "kpi_nombre": "Índice de Gestión Saludable del Cambio Organizacional",
    },
    "Engagement organizacional": {
        "prot_id": "PROT-20",
        "nombre": "Gestión del Engagement organizacional",
        "prioridad": "Media",
        "kpi": "IEO",
        "kpi_nombre": "Índice de Engagement Organizacional",
    },
    # Entorno y clima de trabajo saludable
    "Convivencia y relacionamiento": {
        "prot_id": "PROT-18",
        "nombre": "Convivencia, relacionamiento y prevención de violencia",
        "prioridad": "Alta",
        "kpi": "TCLC",
        "kpi_nombre": "Tasa de Convivencia y Respeto",
    },
    "Ecosistema de liderazgo con impacto psicosocial": {
        "prot_id": "PROT-19",
        "nombre": "Ecosistema de liderazgo con impacto psicosocial",
        "prioridad": "Alta",
        "kpi": "ILIPP",
        "kpi_nombre": "Índice de Liderazgo con Impacto Psicosocial Positivo",
    },
}


# ===========================================================================
# V2-PASO 7: PRIORIDADES POR SECTOR ECONÓMICO
# Fuente: Visualizador 2, Paso 7 + Contexto_RAG
# ===========================================================================

# Protocolos prioritarios por sector (orden de implementación)
PROTOCOLOS_POR_SECTOR = {
    "Agricultura": ["PROT-08", "PROT-06", "PROT-07", "PROT-16"],
    "Comercio/financiero": ["PROT-09", "PROT-15", "PROT-20", "PROT-07"],
    "Construcción": ["PROT-08", "PROT-14", "PROT-16", "PROT-10"],
    "Manufactura": ["PROT-14", "PROT-08", "PROT-15", "PROT-16"],
    "Servicios": ["PROT-09", "PROT-18", "PROT-13", "PROT-03"],
    "Transporte": ["PROT-16", "PROT-08", "PROT-14", "PROT-09"],
    "Salud": ["PROT-04", "PROT-05", "PROT-13", "PROT-10"],
    "Educación": ["PROT-18", "PROT-13", "PROT-02", "PROT-11"],
    "Administración Pública": ["PROT-17", "PROT-11", "PROT-19", "PROT-20"],
    "Minas/energía": ["PROT-08", "PROT-14", "PROT-10", "PROT-05"],
}

# Umbrales para activación de protocolos
UMBRAL_CRITICO_PCT = 40      # >40% población en urgente+correctiva → crítico
UMBRAL_PRIORITARIO_PCT = 20  # 20-39% → prioritario


# ===========================================================================
# FUNCIONES DE CÁLCULO
# ===========================================================================

def calcular_prioridades_empresa(df_gestion: pd.DataFrame, empresa: str) -> pd.DataFrame:
    """
    Calcula prioridades de protocolos para una empresa.
    
    Para cada línea de gestión:
      1. Obtiene score promedio de la línea
      2. Calcula % de trabajadores en niveles 4+5 (correctiva+urgente)
      3. Asigna prioridad según umbral
      4. Determina flag de activación
    
    Returns:
        DataFrame con una fila por línea: prot_id, linea, score_linea,
        pct_urgente, prioridad_calculada, activacion_flag
    """
    df_emp = df_gestion[df_gestion["empresa"] == empresa].copy()
    if len(df_emp) == 0:
        return pd.DataFrame()
    
    sector = df_emp["sector_rag"].iloc[0]
    resultados = []
    
    # Filtrar solo líneas (excluir _TOTAL_EJE)
    df_lineas = df_emp[~df_emp["linea"].str.startswith("_")]
    
    for linea in df_lineas["linea"].unique():
        df_linea = df_lineas[df_lineas["linea"] == linea]
        
        # Score promedio de la línea
        score_linea = df_linea["score_0a1"].mean()
        
        # % en niveles 4 y 5 (intervención correctiva + urgente)
        if "nivel_gestion" in df_linea.columns:
            n_urgente = len(df_linea[df_linea["nivel_gestion"] >= 4])
            pct_urgente = 100 * n_urgente / len(df_linea) if len(df_linea) > 0 else 0
        else:
            pct_urgente = 0
        
        # Obtener info del protocolo
        info_prot = LINEA_A_PROTOCOLO.get(linea, {})
        prot_id = info_prot.get("prot_id", "N/A")
        prioridad_base = info_prot.get("prioridad", "Media")
        kpi = info_prot.get("kpi", "")
        
        # Determinar prioridad calculada según umbrales
        if pct_urgente > UMBRAL_CRITICO_PCT:
            prioridad_calculada = "Crítica"
            activacion_flag = True
        elif pct_urgente > UMBRAL_PRIORITARIO_PCT:
            prioridad_calculada = "Alta"
            activacion_flag = True
        elif prioridad_base == "Alta":
            prioridad_calculada = "Alta"
            activacion_flag = score_linea < 0.45
        else:
            prioridad_calculada = prioridad_base
            activacion_flag = score_linea < 0.29
        
        # Verificar si es protocolo prioritario del sector
        prots_sector = PROTOCOLOS_POR_SECTOR.get(sector, [])
        es_prioritario_sector = prot_id in prots_sector
        
        resultados.append({
            "empresa": empresa,
            "sector_rag": sector,
            "linea": linea,
            "prot_id": prot_id,
            "kpi": kpi,
            "score_linea": round(score_linea, 4),
            "n_trabajadores": len(df_linea["cedula"].unique()),
            "pct_urgente_correctiva": round(pct_urgente, 1),
            "prioridad_base": prioridad_base,
            "prioridad_calculada": prioridad_calculada,
            "es_prioritario_sector": es_prioritario_sector,
            "activacion_flag": activacion_flag,
        })
    
    return pd.DataFrame(resultados)


def procesar_todas_empresas(df_gestion: pd.DataFrame) -> pd.DataFrame:
    """Procesa prioridades para todas las empresas."""
    log.info("Calculando prioridades de protocolos...")
    
    empresas = df_gestion["empresa"].unique()
    log.info("  Procesando %d empresas", len(empresas))
    
    all_prioridades = []
    for empresa in empresas:
        df_prio = calcular_prioridades_empresa(df_gestion, empresa)
        if len(df_prio) > 0:
            all_prioridades.append(df_prio)
    
    if not all_prioridades:
        return pd.DataFrame()
    
    df_result = pd.concat(all_prioridades, ignore_index=True)
    log.info("  Generados %d registros de prioridades", len(df_result))
    return df_result


# ===========================================================================
# VALIDACIÓN
# ===========================================================================

def validar_prioridades(df: pd.DataFrame) -> Tuple[bool, pd.DataFrame]:
    """Valida fact_prioridades."""
    log.info("Validando fact_prioridades...")
    errores = []
    
    # 1. PK sin duplicados
    pk = ["empresa", "linea"]
    dupes = df[df.duplicated(subset=pk, keep=False)]
    if len(dupes) > 0:
        errores.append({"check": "PK_duplicada", "n": len(dupes)})
    
    # 2. PROT_ID válidos
    prot_invalidos = df[~df["prot_id"].str.startswith("PROT-") & (df["prot_id"] != "N/A")]
    if len(prot_invalidos) > 0:
        errores.append({"check": "prot_id_invalido", "n": len(prot_invalidos)})
    
    # 3. Scores en rango
    fuera_rango = df[(df["score_linea"] < 0) | (df["score_linea"] > 1)]
    if len(fuera_rango) > 0:
        errores.append({"check": "score_fuera_rango", "n": len(fuera_rango)})
    
    df_errores = pd.DataFrame(errores)
    es_valido = len(errores) == 0
    if es_valido:
        log.info("Validación EXITOSA")
    else:
        log.error("Validación FALLIDA")
    return es_valido, df_errores


# ===========================================================================
# PIPELINE PRINCIPAL
# ===========================================================================

def main(config_path: str = "config/config.yaml") -> None:
    """
    Pipeline principal 05.
    1. Carga fact_gestion_scores (con categorías del paso 04)
    2. V2-Paso6: Asigna PROT_ID y calcula % urgente
    3. V2-Paso7: Determina prioridad según sector
    4. Guarda fact_prioridades.parquet
    """
    config = cargar_config(config_path)
    ruta_gestion = Path(config["paths"]["processed"]) / "fact_gestion_scores.parquet"
    ruta_out = Path(config["paths"]["processed"]) / "fact_prioridades.parquet"
    
    log.info("=" * 60)
    log.info("05_prioridades_protocolos.py — Iniciando pipeline")
    log.info("Input:  %s", ruta_gestion)
    log.info("Output: %s", ruta_out)
    log.info("=" * 60)
    
    # --- Carga
    if not ruta_gestion.exists():
        log.error("Archivo no encontrado: %s", ruta_gestion)
        sys.exit(1)
    
    df_gestion = pd.read_parquet(ruta_gestion)
    log.info("Cargados %d registros de gestión", len(df_gestion))
    
    # --- Procesar prioridades
    df_prioridades = procesar_todas_empresas(df_gestion)
    
    if len(df_prioridades) == 0:
        log.error("No se generaron prioridades")
        sys.exit(1)
    
    # --- Validación
    ok, _ = validar_prioridades(df_prioridades)
    if not ok:
        log.error("Pipeline 05 con ERRORES")
        sys.exit(1)
    
    # --- Guardar
    ruta_out.parent.mkdir(parents=True, exist_ok=True)
    df_prioridades.to_parquet(ruta_out, index=False)
    log.info("Guardado: %s (%d filas)", ruta_out, len(df_prioridades))
    
    # --- Resumen
    log.info("-" * 40)
    log.info("RESUMEN:")
    log.info("  Empresas: %d", df_prioridades["empresa"].nunique())
    log.info("  Líneas únicas: %d", df_prioridades["linea"].nunique())
    log.info("  Protocolos a activar: %d", df_prioridades["activacion_flag"].sum())
    
    # Distribución por prioridad calculada
    dist = df_prioridades["prioridad_calculada"].value_counts()
    log.info("  Por prioridad:")
    for prio, n in dist.items():
        log.info("    %s: %d", prio, n)
    
    log.info("-" * 40)
    log.info("05 completado exitosamente.")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="05_prioridades_protocolos.py — V2 Pasos 6-7")
    parser.add_argument("--config", default="config/config.yaml")
    args = parser.parse_args()
    main(config_path=args.config)