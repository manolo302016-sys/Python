"""
04_categorias_gestion.py — Implementa V2-Pasos 4 y 5
Pasos:
  V2-Paso4 : Clasificación categórica de ejes y líneas de gestión (5 niveles)
  V2-Paso5 : Asignación de enfoque de gestión (texto interpretativo)

Input:  data/processed/fact_gestion_scores.parquet
Output: data/processed/fact_gestion_scores.parquet (actualizado con columnas adicionales)

Puntos de corte (aplican igual para todos los ejes y líneas):
  score > 0.79  → Gestión prorrogable
  score > 0.65  → Gestión preventiva
  score > 0.45  → Gestión de mejora selectiva
  score > 0.29  → Intervención correctiva
  score ≤ 0.29  → Intervención urgente

Fuente documental: Visualizador 2, Pasos 4-5
Versión: 1.0 | Pipeline MentalPRO | Modelo AVANTUM
"""

import logging
import sys
from pathlib import Path
from typing import Tuple
import numpy as np
import pandas as pd
import yaml

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("04_categorias_gestion")


def cargar_config(config_path: str = "config/config.yaml") -> dict:
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


# ===========================================================================
# V2-PASO 4: PUNTOS DE CORTE PARA CLASIFICACIÓN CATEGÓRICA
# Fuente: Visualizador 2, Paso 4 (filas 697-726)
# Aplica igual para TODOS los ejes y líneas, formas A y B
# ===========================================================================

# Puntos de corte (límites superiores de cada nivel)
# Nivel 1 = mejor (Gestión prorrogable), Nivel 5 = peor (Intervención urgente)
PUNTOS_CORTE = {
    "gestion_prorrogable":     {"min": 0.79, "max": 1.00, "nivel": 1},
    "gestion_preventiva":      {"min": 0.65, "max": 0.79, "nivel": 2},
    "mejora_selectiva":        {"min": 0.45, "max": 0.65, "nivel": 3},
    "intervencion_correctiva": {"min": 0.29, "max": 0.45, "nivel": 4},
    "intervencion_urgente":    {"min": 0.00, "max": 0.29, "nivel": 5},
}

# Mapeo nivel → categoría
NIVEL_A_CATEGORIA = {
    1: "Gestión prorrogable",
    2: "Gestión preventiva",
    3: "Gestión de mejora selectiva",
    4: "Intervención correctiva",
    5: "Intervención urgente",
}


# ===========================================================================
# V2-PASO 5: ENFOQUE DE GESTIÓN (Texto interpretativo)
# Fuente: Visualizador 2, Paso 5 (filas 728-737)
# ===========================================================================

ENFOQUE_GESTION = {
    1: (
        "Promoción: Reforzar y mantener factores protectores y controles actuales. "
        "Implementar actividades de promoción de la salud y el bienestar de manera "
        "transversal a la organización, para reconocer las fortalezas y mantener las acciones actuales."
    ),
    2: (
        "Educación y prevención: Actuar desde actividades de formación y capacitación "
        "que permitan desarrollar pautas de autocuidado y reconocimiento de acciones tempranas "
        "para prevenir los riesgos y peligros desde las personas y la organización. "
        "Prevenir retos futuros y generar seguimiento proactivo."
    ),
    3: (
        "Ajuste y mejora: Intervención en indicadores y dimensiones específicas que "
        "puntuaron en niveles altos con el fin de mejorar e impactar focos de riesgo "
        "y evitar que la magnitud aumente en ausencia de controles."
    ),
    4: (
        "Control correctivo: Implementación de intervenciones mediante protocolos "
        "estructurados para mitigar o mejorar los factores de riesgo identificados. "
        "Articular estas intervenciones en el SVE psicosocial (Sistemas de Vigilancia Epidemiológica)."
    ),
    5: (
        "Intervención urgente: Intervención que debería implementarse dentro de los "
        "6 meses siguientes, la cual aborda la implementación de los protocolos sobre "
        "las líneas de alta prioridad, desde una perspectiva organizacional, grupal e "
        "individual y con conexión directa al PVE de la empresa."
    ),
}


# ===========================================================================
# FUNCIONES DE CLASIFICACIÓN
# ===========================================================================

def clasificar_nivel_gestion(score: float) -> int:
    """
    V2-Paso4: Clasifica un score 0-1 en nivel de gestión 1-5.
    
    Puntos de corte:
      score > 0.79  → 1 (Gestión prorrogable)
      score > 0.65  → 2 (Gestión preventiva)
      score > 0.45  → 3 (Mejora selectiva)
      score > 0.29  → 4 (Intervención correctiva)
      score ≤ 0.29  → 5 (Intervención urgente)
    
    Returns:
        int 1-5 (1=mejor, 5=peor)
    """
    if pd.isna(score):
        return np.nan
    
    if score > 0.79:
        return 1  # Gestión prorrogable
    elif score > 0.65:
        return 2  # Gestión preventiva
    elif score > 0.45:
        return 3  # Mejora selectiva
    elif score > 0.29:
        return 4  # Intervención correctiva
    else:
        return 5  # Intervención urgente


def obtener_categoria_gestion(nivel: int) -> str:
    """Convierte nivel numérico (1-5) a etiqueta categórica."""
    if pd.isna(nivel):
        return np.nan
    return NIVEL_A_CATEGORIA.get(int(nivel), "Desconocido")


def obtener_enfoque_gestion(nivel: int) -> str:
    """V2-Paso5: Obtiene texto interpretativo según nivel."""
    if pd.isna(nivel):
        return np.nan
    return ENFOQUE_GESTION.get(int(nivel), "")


def aplicar_clasificacion(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aplica clasificación categórica a fact_gestion_scores.
    
    Agrega columnas:
      - nivel_gestion: int 1-5
      - categoria_gestion: str (etiqueta)
      - enfoque_gestion: str (texto interpretativo)
    """
    log.info("Aplicando clasificación categórica V2-Paso4...")
    df = df.copy()
    
    # Clasificar nivel
    df["nivel_gestion"] = df["score_0a1"].apply(clasificar_nivel_gestion)
    
    # Obtener categoría
    df["categoria_gestion"] = df["nivel_gestion"].apply(obtener_categoria_gestion)
    
    # Obtener enfoque
    log.info("Asignando enfoque de gestión V2-Paso5...")
    df["enfoque_gestion"] = df["nivel_gestion"].apply(obtener_enfoque_gestion)
    
    # Estadísticas
    dist = df["categoria_gestion"].value_counts()
    log.info("Distribución de categorías:")
    for cat, n in dist.items():
        pct = 100 * n / len(df)
        log.info("  %s: %d (%.1f%%)", cat, n, pct)
    
    return df


# ===========================================================================
# VALIDACIÓN
# ===========================================================================

def validar_categorias(df: pd.DataFrame) -> Tuple[bool, pd.DataFrame]:
    """Valida que las categorías estén correctamente asignadas."""
    log.info("Validando categorías de gestión...")
    errores = []
    
    # 1. Niveles válidos (1-5)
    niveles_invalidos = df[(df["nivel_gestion"] < 1) | (df["nivel_gestion"] > 5)]
    if len(niveles_invalidos) > 0:
        errores.append({"check": "nivel_invalido", "n": len(niveles_invalidos)})
    
    # 2. Categorías no nulas donde hay score
    scores_sin_cat = df[df["score_0a1"].notna() & df["categoria_gestion"].isna()]
    if len(scores_sin_cat) > 0:
        errores.append({"check": "score_sin_categoria", "n": len(scores_sin_cat)})
    
    # 3. Coherencia nivel vs score
    # Nivel 1 debe tener score > 0.79
    incoherentes = df[(df["nivel_gestion"] == 1) & (df["score_0a1"] <= 0.79)]
    if len(incoherentes) > 0:
        errores.append({"check": "nivel_score_incoherente", "n": len(incoherentes)})
    
    # 4. Todas las categorías esperadas presentes
    cats_esperadas = set(NIVEL_A_CATEGORIA.values())
    cats_encontradas = set(df["categoria_gestion"].dropna().unique())
    faltantes = cats_esperadas - cats_encontradas
    if faltantes:
        log.warning("Categorías sin registros: %s (puede ser normal)", faltantes)
    
    df_errores = pd.DataFrame(errores)
    es_valido = len(errores) == 0
    if es_valido:
        log.info("Validación EXITOSA")
    else:
        log.error("Validación FALLIDA — %d checks con errores", len(errores))
    return es_valido, df_errores


# ===========================================================================
# PIPELINE PRINCIPAL
# ===========================================================================

def main(config_path: str = "config/config.yaml") -> None:
    """
    Pipeline principal 04.
    1. Carga fact_gestion_scores
    2. V2-Paso4: Clasifica en nivel 1-5
    3. V2-Paso5: Asigna enfoque de gestión
    4. Guarda fact_gestion_scores actualizado
    """
    config = cargar_config(config_path)
    ruta_gestion = Path(config["paths"]["processed"]) / "fact_gestion_scores.parquet"
    
    log.info("=" * 60)
    log.info("04_categorias_gestion.py — Iniciando pipeline")
    log.info("Input/Output: %s", ruta_gestion)
    log.info("=" * 60)
    
    # --- Carga
    if not ruta_gestion.exists():
        log.error("Archivo no encontrado: %s", ruta_gestion)
        log.error("Ejecutar primero: 03_scoring_gestion.py")
        sys.exit(1)
    
    df = pd.read_parquet(ruta_gestion)
    log.info("Cargados %d registros", len(df))
    
    # --- Verificar columna score_0a1
    if "score_0a1" not in df.columns:
        log.error("Columna score_0a1 no encontrada")
        sys.exit(1)
    
    # --- Aplicar clasificación
    df = aplicar_clasificacion(df)
    
    # --- Validación
    ok, _ = validar_categorias(df)
    if not ok:
        log.error("Pipeline 04 completado con ERRORES")
        sys.exit(1)
    
    # --- Guardar
    df.to_parquet(ruta_gestion, index=False)
    log.info("Guardado: %s (%d filas × %d cols)", ruta_gestion, len(df), df.shape[1])
    
    # --- Resumen
    log.info("-" * 40)
    log.info("RESUMEN:")
    log.info("  Columnas agregadas: nivel_gestion, categoria_gestion, enfoque_gestion")
    log.info("  Score promedio: %.3f", df["score_0a1"].mean())
    log.info("  Nivel promedio: %.2f", df["nivel_gestion"].mean())
    
    # Distribución por eje
    for eje in df["eje"].unique():
        df_eje = df[df["eje"] == eje]
        log.info("  %s — score prom: %.3f, nivel prom: %.2f",
                 eje, df_eje["score_0a1"].mean(), df_eje["nivel_gestion"].mean())
    
    log.info("-" * 40)
    log.info("04 completado exitosamente.")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="04_categorias_gestion.py — V2 Pasos 4-5")
    parser.add_argument("--config", default="config/config.yaml")
    args = parser.parse_args()
    main(config_path=args.config)