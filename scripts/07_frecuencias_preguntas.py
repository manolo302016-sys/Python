"""
07_frecuencias_preguntas.py — Implementa V1-Paso 20: Frecuencias por Pregunta

Calcula la distribución de respuestas (frecuencias) para cada pregunta
de los cuestionarios, permitiendo análisis detallado a nivel de ítem.

Input:
  - data/processed/fact_respuestas_clean.parquet (respuestas codificadas)
  - data/raw/dim_preguntas.parquet (catálogo de preguntas)

Output:
  - data/processed/fact_frecuencias.parquet

Contenido del output:
  - Por cada pregunta: conteo y % de cada opción de respuesta
  - Agrupado por empresa, factor, dimensión
  - Identificación de preguntas críticas (alto % en respuestas negativas)

Fuente documental: Visualizador 1, Paso 20
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
log = logging.getLogger("07_frecuencias_preguntas")


def cargar_config(config_path: str = "config/config.yaml") -> dict:
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


# ===========================================================================
# CONSTANTES
# ===========================================================================

# Opciones de respuesta por tipo de escala
OPCIONES_LIKERT = ["Siempre", "Casi siempre", "Algunas veces", "Casi nunca", "Nunca"]
OPCIONES_DICOTOMICA = ["Sí", "No"]
OPCIONES_AFRONTAMIENTO = ["Siempre", "Frecuentemente", "A veces", "Nunca"]

# Umbral para marcar pregunta como crítica (% en opciones negativas)
UMBRAL_CRITICO_PCT = 40

# Opciones consideradas "negativas" por tipo de escala
# (depende de si el ítem es invertido o no, pero por defecto)
OPCIONES_NEGATIVAS_LIKERT = ["Siempre", "Casi siempre"]  # Para ítems de riesgo
OPCIONES_NEGATIVAS_LIKERT_INV = ["Nunca", "Casi nunca"]  # Para ítems protectores


# ===========================================================================
# FUNCIONES DE CÁLCULO DE FRECUENCIAS
# ===========================================================================

def calcular_frecuencias_pregunta(
    df: pd.DataFrame,
    id_pregunta: str,
    grupo_cols: List[str] = None
) -> pd.DataFrame:
    """
    Calcula frecuencias de respuesta para una pregunta.
    
    Args:
        df: DataFrame con respuestas (debe tener columna 'respuesta_texto')
        id_pregunta: ID de la pregunta a analizar
        grupo_cols: Columnas para agrupar (ej: ['empresa', 'factor'])
    
    Returns:
        DataFrame con conteo y % por opción de respuesta
    """
    df_preg = df[df["id_pregunta"] == id_pregunta].copy()
    
    if len(df_preg) == 0:
        return pd.DataFrame()
    
    if grupo_cols is None:
        grupo_cols = ["empresa"]
    
    # Contar respuestas por grupo y opción
    df_freq = df_preg.groupby(grupo_cols + ["respuesta_texto"]).size().reset_index(name="conteo")
    
    # Calcular total por grupo para obtener porcentajes
    df_total = df_preg.groupby(grupo_cols).size().reset_index(name="total")
    df_freq = df_freq.merge(df_total, on=grupo_cols)
    
    # Calcular porcentaje
    df_freq["porcentaje"] = (df_freq["conteo"] / df_freq["total"] * 100).round(1)
    
    # Agregar metadata de la pregunta
    df_freq["id_pregunta"] = id_pregunta
    
    return df_freq


def calcular_frecuencias_todas(
    df_respuestas: pd.DataFrame,
    df_preguntas: pd.DataFrame,
    grupo_cols: List[str] = None
) -> pd.DataFrame:
    """
    Calcula frecuencias para todas las preguntas.
    
    Args:
        df_respuestas: DataFrame con todas las respuestas
        df_preguntas: Catálogo de preguntas con metadata
        grupo_cols: Columnas para agrupar
    
    Returns:
        DataFrame consolidado con frecuencias de todas las preguntas
    """
    log.info("Calculando frecuencias por pregunta...")
    
    if grupo_cols is None:
        grupo_cols = ["empresa", "factor"]
    
    preguntas = df_respuestas["id_pregunta"].unique()
    log.info("  Procesando %d preguntas", len(preguntas))
    
    all_freqs = []
    for id_preg in preguntas:
        df_freq = calcular_frecuencias_pregunta(df_respuestas, id_preg, grupo_cols)
        if len(df_freq) > 0:
            all_freqs.append(df_freq)
    
    if not all_freqs:
        return pd.DataFrame()
    
    df_result = pd.concat(all_freqs, ignore_index=True)
    
    # Enriquecer con metadata de preguntas
    if len(df_preguntas) > 0:
        cols_meta = ["id_pregunta", "texto_pregunta", "dimension", "dominio", "es_invertido"]
        cols_disponibles = [c for c in cols_meta if c in df_preguntas.columns]
        df_result = df_result.merge(
            df_preguntas[cols_disponibles].drop_duplicates(),
            on="id_pregunta",
            how="left"
        )
    
    log.info("  Generadas %d filas de frecuencias", len(df_result))
    return df_result


# ===========================================================================
# IDENTIFICACIÓN DE PREGUNTAS CRÍTICAS
# ===========================================================================

def identificar_preguntas_criticas(
    df_freq: pd.DataFrame,
    umbral_pct: float = UMBRAL_CRITICO_PCT
) -> pd.DataFrame:
    """
    Identifica preguntas con alto % de respuestas negativas.
    
    Una pregunta es crítica si:
    - Para ítems de riesgo: >umbral% en "Siempre" + "Casi siempre"
    - Para ítems protectores (invertidos): >umbral% en "Nunca" + "Casi nunca"
    
    Args:
        df_freq: DataFrame con frecuencias calculadas
        umbral_pct: Umbral porcentual para marcar como crítico
    
    Returns:
        DataFrame con preguntas críticas y su % en opciones negativas
    """
    log.info("Identificando preguntas críticas (umbral: %.0f%%)...", umbral_pct)
    
    # Determinar opciones negativas según si es invertido
    def es_respuesta_negativa(row):
        if row.get("es_invertido", False):
            return row["respuesta_texto"] in OPCIONES_NEGATIVAS_LIKERT_INV
        else:
            return row["respuesta_texto"] in OPCIONES_NEGATIVAS_LIKERT
    
    df = df_freq.copy()
    df["es_negativa"] = df.apply(es_respuesta_negativa, axis=1)
    
    # Sumar % de opciones negativas por pregunta y grupo
    grupo_cols = ["empresa", "factor", "id_pregunta"]
    cols_presentes = [c for c in grupo_cols if c in df.columns]
    
    df_neg = df[df["es_negativa"]].groupby(cols_presentes)["porcentaje"].sum().reset_index()
    df_neg = df_neg.rename(columns={"porcentaje": "pct_negativo"})
    
    # Filtrar críticas
    df_criticas = df_neg[df_neg["pct_negativo"] >= umbral_pct].copy()
    df_criticas["es_critica"] = True
    
    # Enriquecer con texto de pregunta
    if "texto_pregunta" in df_freq.columns:
        df_texto = df_freq[["id_pregunta", "texto_pregunta", "dimension", "dominio"]].drop_duplicates()
        df_criticas = df_criticas.merge(df_texto, on="id_pregunta", how="left")
    
    log.info("  Encontradas %d preguntas críticas", len(df_criticas))
    return df_criticas


def generar_resumen_por_dimension(df_freq: pd.DataFrame) -> pd.DataFrame:
    """
    Genera resumen de frecuencias agregado por dimensión.
    
    Para cada dimensión calcula:
    - Promedio de % en opciones negativas
    - Número de preguntas críticas
    - Pregunta más crítica
    """
    if "dimension" not in df_freq.columns:
        return pd.DataFrame()
    
    # Calcular % negativo por pregunta
    def es_respuesta_negativa(row):
        if row.get("es_invertido", False):
            return row["respuesta_texto"] in OPCIONES_NEGATIVAS_LIKERT_INV
        else:
            return row["respuesta_texto"] in OPCIONES_NEGATIVAS_LIKERT
    
    df = df_freq.copy()
    df["es_negativa"] = df.apply(es_respuesta_negativa, axis=1)
    
    grupo_cols = ["empresa", "dimension", "id_pregunta"]
    cols_presentes = [c for c in grupo_cols if c in df.columns]
    
    df_neg = df[df["es_negativa"]].groupby(cols_presentes)["porcentaje"].sum().reset_index()
    df_neg = df_neg.rename(columns={"porcentaje": "pct_negativo"})
    
    # Agregar por dimensión
    df_dim = df_neg.groupby(["empresa", "dimension"]).agg({
        "pct_negativo": ["mean", "max"],
        "id_pregunta": "count"
    }).reset_index()
    df_dim.columns = ["empresa", "dimension", "pct_negativo_promedio", "pct_negativo_max", "n_preguntas"]
    
    # Contar críticas
    df_criticas = df_neg[df_neg["pct_negativo"] >= UMBRAL_CRITICO_PCT]
    df_count_crit = df_criticas.groupby(["empresa", "dimension"]).size().reset_index(name="n_criticas")
    
    df_dim = df_dim.merge(df_count_crit, on=["empresa", "dimension"], how="left")
    df_dim["n_criticas"] = df_dim["n_criticas"].fillna(0).astype(int)
    
    return df_dim


# ===========================================================================
# VALIDACIÓN
# ===========================================================================

def validar_frecuencias(df: pd.DataFrame) -> Tuple[bool, List[str]]:
    """Valida cálculos de frecuencias."""
    log.info("Validando frecuencias...")
    errores = []
    
    # 1. Porcentajes suman ~100 por pregunta y grupo
    grupo_cols = ["empresa", "factor", "id_pregunta"]
    cols_presentes = [c for c in grupo_cols if c in df.columns]
    
    df_sum = df.groupby(cols_presentes)["porcentaje"].sum()
    fuera_rango = df_sum[(df_sum < 99) | (df_sum > 101)]
    if len(fuera_rango) > 0:
        errores.append(f"Porcentajes no suman 100 en {len(fuera_rango)} grupos")
    
    # 2. No hay porcentajes negativos
    if (df["porcentaje"] < 0).any():
        errores.append("Porcentajes negativos encontrados")
    
    # 3. Todas las preguntas tienen al menos una respuesta
    preguntas_vacias = df.groupby("id_pregunta")["conteo"].sum()
    if (preguntas_vacias == 0).any():
        errores.append("Preguntas sin respuestas encontradas")
    
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
    1. Carga fact_respuestas_clean y dim_preguntas
    2. Calcula frecuencias por pregunta y grupo
    3. Identifica preguntas críticas
    4. Genera resumen por dimensión
    5. Guarda fact_frecuencias.parquet
    """
    config = cargar_config(config_path)
    raw_path = Path(config["paths"]["raw"])
    proc_path = Path(config["paths"]["processed"])
    
    log.info("=" * 60)
    log.info("07_frecuencias_preguntas.py — Iniciando pipeline (V1-Paso20)")
    log.info("=" * 60)
    
    # --- Carga
    ruta_respuestas = proc_path / "fact_respuestas_clean.parquet"
    ruta_preguntas = raw_path / "dim_preguntas.parquet"
    
    if not ruta_respuestas.exists():
        log.error("Archivo no encontrado: %s", ruta_respuestas)
        sys.exit(1)
    
    df_respuestas = pd.read_parquet(ruta_respuestas)
    log.info("Cargadas %d respuestas", len(df_respuestas))
    
    # Cargar catálogo de preguntas si existe
    if ruta_preguntas.exists():
        df_preguntas = pd.read_parquet(ruta_preguntas)
        log.info("Cargadas %d preguntas del catálogo", len(df_preguntas))
    else:
        log.warning("Catálogo de preguntas no encontrado, continuando sin metadata")
        df_preguntas = pd.DataFrame()
    
    # --- Calcular frecuencias
    df_freq = calcular_frecuencias_todas(
        df_respuestas,
        df_preguntas,
        grupo_cols=["empresa", "factor"]
    )
    
    if len(df_freq) == 0:
        log.error("No se generaron frecuencias")
        sys.exit(1)
    
    # --- Identificar críticas
    df_criticas = identificar_preguntas_criticas(df_freq)
    
    # --- Resumen por dimensión
    df_dim = generar_resumen_por_dimension(df_freq)
    
    # --- Validación
    ok, _ = validar_frecuencias(df_freq)
    if not ok:
        log.warning("Pipeline 07 con advertencias")
    
    # --- Agregar flag de crítica al dataset principal
    if len(df_criticas) > 0:
        criticas_set = set(df_criticas["id_pregunta"].unique())
        df_freq["es_critica"] = df_freq["id_pregunta"].isin(criticas_set)
    else:
        df_freq["es_critica"] = False
    
    # --- Guardar
    df_freq.to_parquet(proc_path / "fact_frecuencias.parquet", index=False)
    log.info("Guardado: %s (%d filas)", proc_path / "fact_frecuencias.parquet", len(df_freq))
    
    if len(df_criticas) > 0:
        df_criticas.to_parquet(proc_path / "fact_preguntas_criticas.parquet", index=False)
        log.info("Guardado: fact_preguntas_criticas.parquet (%d filas)", len(df_criticas))
    
    if len(df_dim) > 0:
        df_dim.to_parquet(proc_path / "fact_frecuencias_dimension.parquet", index=False)
    
    # --- Resumen
    log.info("-" * 40)
    log.info("RESUMEN:")
    log.info("  Preguntas analizadas: %d", df_freq["id_pregunta"].nunique())
    log.info("  Empresas: %d", df_freq["empresa"].nunique())
    log.info("  Preguntas críticas: %d", len(df_criticas))
    
    if len(df_criticas) > 0:
        log.info("  Top 5 preguntas más críticas:")
        top5 = df_criticas.nlargest(5, "pct_negativo")
        for _, row in top5.iterrows():
            log.info("    %s: %.1f%% negativo", row["id_pregunta"], row["pct_negativo"])
    
    log.info("-" * 40)
    log.info("07 completado exitosamente.")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="07_frecuencias_preguntas.py — V1-Paso 20")
    parser.add_argument("--config", default="config/config.yaml")
    args = parser.parse_args()
    main(config_path=args.config)