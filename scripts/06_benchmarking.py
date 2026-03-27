"""
06_benchmarking.py — Implementa V1-Pasos 16-19

IMPORTANTE AL COPIAR: Buscar y reemplazar en tu IDE:
  "sin_riesgo" -> "sin_riesgo"  
  "Sin riesgo" -> "Sin riesgo"
  "Sin__" -> "Sin "

Input:  data/processed/fact_scores_baremo.parquet
Output: data/processed/fact_benchmark.parquet
"""

import logging
import sys
from pathlib import Path
from typing import Tuple, Dict, Optional
import numpy as np
import pandas as pd
import yaml

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("06_benchmarking")


def cargar_config(config_path: str = "config/config.yaml") -> dict:
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


# V1-PASO 16: BAREMOS COLOMBIA
BAREMO_COLOMBIA_INTRA_A = {
    "intralaboral_total": {"sin_riesgo": (0, 19.4), "bajo": (19.5, 25.8), "medio": (25.9, 31.5), "alto": (31.6, 39.6), "muy_alto": (39.7, 100)},
    "liderazgo_relaciones_sociales": {"sin_riesgo": (0, 13.8), "bajo": (13.9, 22.2), "medio": (22.3, 29.2), "alto": (29.3, 40.3), "muy_alto": (40.4, 100)},
    "control_sobre_trabajo": {"sin_riesgo": (0, 25.0), "bajo": (25.1, 33.3), "medio": (33.4, 43.8), "alto": (43.9, 56.3), "muy_alto": (56.4, 100)},
    "demandas_trabajo": {"sin_riesgo": (0, 25.0), "bajo": (25.1, 30.6), "medio": (30.7, 36.1), "alto": (36.2, 43.1), "muy_alto": (43.2, 100)},
    "recompensas": {"sin_riesgo": (0, 4.2), "bajo": (4.3, 12.5), "medio": (12.6, 20.8), "alto": (20.9, 31.3), "muy_alto": (31.4, 100)},
    "caracteristicas_liderazgo": {"sin_riesgo": (0, 9.6), "bajo": (9.7, 21.2), "medio": (21.3, 30.8), "alto": (30.9, 46.2), "muy_alto": (46.3, 100)},
    "relaciones_sociales_trabajo": {"sin_riesgo": (0, 10.0), "bajo": (10.1, 20.0), "medio": (20.1, 27.5), "alto": (27.6, 40.0), "muy_alto": (40.1, 100)},
    "retroalimentacion_desempeno": {"sin_riesgo": (0, 20.0), "bajo": (20.1, 30.0), "medio": (30.1, 45.0), "alto": (45.1, 60.0), "muy_alto": (60.1, 100)},
    "relacion_colaboradores": {"sin_riesgo": (0, 16.7), "bajo": (16.8, 29.2), "medio": (29.3, 41.7), "alto": (41.8, 54.2), "muy_alto": (54.3, 100)},
    "claridad_rol": {"sin_riesgo": (0, 3.6), "bajo": (3.7, 14.3), "medio": (14.4, 25.0), "alto": (25.1, 39.3), "muy_alto": (39.4, 100)},
    "capacitacion": {"sin_riesgo": (0, 16.7), "bajo": (16.8, 33.3), "medio": (33.4, 50.0), "alto": (50.1, 66.7), "muy_alto": (66.8, 100)},
    "participacion_cambio": {"sin_riesgo": (0, 31.3), "bajo": (31.4, 43.8), "medio": (43.9, 56.3), "alto": (56.4, 68.8), "muy_alto": (68.9, 100)},
    "oportunidades_desarrollo": {"sin_riesgo": (0, 12.5), "bajo": (12.6, 25.0), "medio": (25.1, 37.5), "alto": (37.6, 56.3), "muy_alto": (56.4, 100)},
    "control_autonomia": {"sin_riesgo": (0, 50.0), "bajo": (50.1, 62.5), "medio": (62.6, 75.0), "alto": (75.1, 87.5), "muy_alto": (87.6, 100)},
    "demandas_cuantitativas": {"sin_riesgo": (0, 25.0), "bajo": (25.1, 33.3), "medio": (33.4, 45.8), "alto": (45.9, 58.3), "muy_alto": (58.4, 100)},
    "demandas_carga_mental": {"sin_riesgo": (0, 60.0), "bajo": (60.1, 70.0), "medio": (70.1, 80.0), "alto": (80.1, 90.0), "muy_alto": (90.1, 100)},
    "demandas_emocionales": {"sin_riesgo": (0, 25.0), "bajo": (25.1, 36.1), "medio": (36.2, 47.2), "alto": (47.3, 58.3), "muy_alto": (58.4, 100)},
    "exigencias_responsabilidad": {"sin_riesgo": (0, 47.9), "bajo": (48.0, 60.4), "medio": (60.5, 70.8), "alto": (70.9, 83.3), "muy_alto": (83.4, 100)},
    "demandas_ambientales": {"sin_riesgo": (0, 18.2), "bajo": (18.3, 27.3), "medio": (27.4, 40.9), "alto": (41.0, 54.6), "muy_alto": (54.7, 100)},
    "demandas_jornada": {"sin_riesgo": (0, 18.8), "bajo": (18.9, 31.3), "medio": (31.4, 43.8), "alto": (43.9, 56.3), "muy_alto": (56.4, 100)},
    "consistencia_rol": {"sin_riesgo": (0, 20.0), "bajo": (20.1, 35.0), "medio": (35.1, 45.0), "alto": (45.1, 60.0), "muy_alto": (60.1, 100)},
    "influencia_ambiente": {"sin_riesgo": (0, 25.0), "bajo": (25.1, 37.5), "medio": (37.6, 50.0), "alto": (50.1, 62.5), "muy_alto": (62.6, 100)},
    "reconocimiento_compensacion": {"sin_riesgo": (0, 8.3), "bajo": (8.4, 16.7), "medio": (16.8, 29.2), "alto": (29.3, 41.7), "muy_alto": (41.8, 100)},
    "pertenencia_organizacion": {"sin_riesgo": (0, 0.1), "bajo": (0.2, 6.3), "medio": (6.4, 12.5), "alto": (12.6, 25.0), "muy_alto": (25.1, 100)},
}

BAREMO_COLOMBIA_INTRA_B = {
    "intralaboral_total": {"sin_riesgo": (0, 20.6), "bajo": (20.7, 26.0), "medio": (26.1, 31.8), "alto": (31.9, 40.0), "muy_alto": (40.1, 100)},
    "liderazgo_relaciones_sociales": {"sin_riesgo": (0, 9.1), "bajo": (9.2, 18.2), "medio": (18.3, 27.3), "alto": (27.4, 40.9), "muy_alto": (41.0, 100)},
    "control_sobre_trabajo": {"sin_riesgo": (0, 33.3), "bajo": (33.4, 45.8), "medio": (45.9, 56.3), "alto": (56.4, 68.8), "muy_alto": (68.9, 100)},
    "demandas_trabajo": {"sin_riesgo": (0, 24.3), "bajo": (24.4, 29.4), "medio": (29.5, 35.3), "alto": (35.4, 41.2), "muy_alto": (41.3, 100)},
    "recompensas": {"sin_riesgo": (0, 4.2), "bajo": (4.3, 12.5), "medio": (12.6, 20.8), "alto": (20.9, 33.3), "muy_alto": (33.4, 100)},
    "caracteristicas_liderazgo": {"sin_riesgo": (0, 7.7), "bajo": (7.8, 17.3), "medio": (17.4, 28.9), "alto": (29.0, 46.2), "muy_alto": (46.3, 100)},
    "relaciones_sociales_trabajo": {"sin_riesgo": (0, 5.0), "bajo": (5.1, 15.0), "medio": (15.1, 25.0), "alto": (25.1, 37.5), "muy_alto": (37.6, 100)},
    "retroalimentacion_desempeno": {"sin_riesgo": (0, 15.0), "bajo": (15.1, 30.0), "medio": (30.1, 45.0), "alto": (45.1, 60.0), "muy_alto": (60.1, 100)},
    "claridad_rol": {"sin_riesgo": (0, 3.6), "bajo": (3.7, 14.3), "medio": (14.4, 28.6), "alto": (28.7, 42.9), "muy_alto": (43.0, 100)},
    "capacitacion": {"sin_riesgo": (0, 25.0), "bajo": (25.1, 50.0), "medio": (50.1, 62.5), "alto": (62.6, 75.0), "muy_alto": (75.1, 100)},
    "participacion_cambio": {"sin_riesgo": (0, 25.0), "bajo": (25.1, 43.8), "medio": (43.9, 56.3), "alto": (56.4, 75.0), "muy_alto": (75.1, 100)},
    "oportunidades_desarrollo": {"sin_riesgo": (0, 25.0), "bajo": (25.1, 37.5), "medio": (37.6, 50.0), "alto": (50.1, 62.5), "muy_alto": (62.6, 100)},
    "control_autonomia": {"sin_riesgo": (0, 66.7), "bajo": (66.8, 77.8), "medio": (77.9, 88.9), "alto": (89.0, 94.4), "muy_alto": (94.5, 100)},
    "demandas_cuantitativas": {"sin_riesgo": (0, 25.0), "bajo": (25.1, 37.5), "medio": (37.6, 50.0), "alto": (50.1, 62.5), "muy_alto": (62.6, 100)},
    "demandas_carga_mental": {"sin_riesgo": (0, 50.0), "bajo": (50.1, 65.0), "medio": (65.1, 75.0), "alto": (75.1, 85.0), "muy_alto": (85.1, 100)},
    "demandas_emocionales": {"sin_riesgo": (0, 16.7), "bajo": (16.8, 30.6), "medio": (30.7, 44.4), "alto": (44.5, 58.3), "muy_alto": (58.4, 100)},
    "demandas_ambientales": {"sin_riesgo": (0, 18.2), "bajo": (18.3, 31.8), "medio": (31.9, 45.5), "alto": (45.6, 59.1), "muy_alto": (59.2, 100)},
    "demandas_jornada": {"sin_riesgo": (0, 18.8), "bajo": (18.9, 31.3), "medio": (31.4, 50.0), "alto": (50.1, 62.5), "muy_alto": (62.6, 100)},
    "influencia_ambiente": {"sin_riesgo": (0, 25.0), "bajo": (25.1, 37.5), "medio": (37.6, 50.0), "alto": (50.1, 62.5), "muy_alto": (62.6, 100)},
    "reconocimiento_compensacion": {"sin_riesgo": (0, 8.3), "bajo": (8.4, 20.8), "medio": (20.9, 33.3), "alto": (33.4, 45.8), "muy_alto": (45.9, 100)},
    "pertenencia_organizacion": {"sin_riesgo": (0, 0.1), "bajo": (0.2, 6.3), "medio": (6.4, 12.5), "alto": (12.6, 25.0), "muy_alto": (25.1, 100)},
}

BAREMO_COLOMBIA_EXTRA = {
    "extralaboral_total": {"sin_riesgo": (0, 12.9), "bajo": (13.0, 17.7), "medio": (17.8, 24.2), "alto": (24.3, 32.3), "muy_alto": (32.4, 100)},
    "tiempo_fuera_trabajo": {"sin_riesgo": (0, 6.3), "bajo": (6.4, 18.8), "medio": (18.9, 31.3), "alto": (31.4, 50.0), "muy_alto": (50.1, 100)},
    "relaciones_familiares": {"sin_riesgo": (0, 8.3), "bajo": (8.4, 16.7), "medio": (16.8, 25.0), "alto": (25.1, 41.7), "muy_alto": (41.8, 100)},
    "comunicacion_relaciones": {"sin_riesgo": (0, 5.0), "bajo": (5.1, 15.0), "medio": (15.1, 25.0), "alto": (25.1, 40.0), "muy_alto": (40.1, 100)},
    "situacion_economica": {"sin_riesgo": (0, 16.7), "bajo": (16.8, 33.3), "medio": (33.4, 50.0), "alto": (50.1, 66.7), "muy_alto": (66.8, 100)},
    "vivienda_entorno": {"sin_riesgo": (0, 8.3), "bajo": (8.4, 13.9), "medio": (14.0, 22.2), "alto": (22.3, 33.3), "muy_alto": (33.4, 100)},
    "influencia_extralaboral": {"sin_riesgo": (0, 12.5), "bajo": (12.6, 18.8), "medio": (18.9, 25.0), "alto": (25.1, 37.5), "muy_alto": (37.6, 100)},
    "desplazamiento": {"sin_riesgo": (0, 0.1), "bajo": (0.2, 8.3), "medio": (8.4, 25.0), "alto": (25.1, 41.7), "muy_alto": (41.8, 100)},
}

BAREMO_COLOMBIA_ESTRES = {
    "estres_total": {"sin_riesgo": (0, 7.8), "bajo": (7.9, 12.6), "medio": (12.7, 17.7), "alto": (17.8, 25.0), "muy_alto": (25.1, 100)},
}

# V1-PASO 17: BAREMOS SECTORIALES (SOLO INTRALABORAL TOTAL)
BAREMO_SECTORIAL = {
    "Agricultura": {
        "A": {"sin_riesgo": (0, 22.1), "bajo": (22.2, 28.3), "medio": (28.4, 34.2), "alto": (34.3, 42.1), "muy_alto": (42.2, 100)},
        "B": {"sin_riesgo": (0, 23.4), "bajo": (23.5, 29.1), "medio": (29.2, 35.0), "alto": (35.1, 43.2), "muy_alto": (43.3, 100)},
    },
    "Comercio/financiero": {
        "A": {"sin_riesgo": (0, 18.7), "bajo": (18.8, 24.9), "medio": (25.0, 30.8), "alto": (30.9, 38.7), "muy_alto": (38.8, 100)},
        "B": {"sin_riesgo": (0, 19.8), "bajo": (19.9, 25.3), "medio": (25.4, 31.2), "alto": (31.3, 39.1), "muy_alto": (39.2, 100)},
    },
    "Construcción": {
        "A": {"sin_riesgo": (0, 21.3), "bajo": (21.4, 27.6), "medio": (27.7, 33.5), "alto": (33.6, 41.4), "muy_alto": (41.5, 100)},
        "B": {"sin_riesgo": (0, 22.7), "bajo": (22.8, 28.4), "medio": (28.5, 34.3), "alto": (34.4, 42.3), "muy_alto": (42.4, 100)},
    },
    "Manufactura": {
        "A": {"sin_riesgo": (0, 20.2), "bajo": (20.3, 26.5), "medio": (26.6, 32.4), "alto": (32.5, 40.3), "muy_alto": (40.4, 100)},
        "B": {"sin_riesgo": (0, 21.5), "bajo": (21.6, 27.2), "medio": (27.3, 33.1), "alto": (33.2, 41.1), "muy_alto": (41.2, 100)},
    },
    "Servicios": {
        "A": {"sin_riesgo": (0, 18.2), "bajo": (18.3, 24.4), "medio": (24.5, 30.3), "alto": (30.4, 38.2), "muy_alto": (38.3, 100)},
        "B": {"sin_riesgo": (0, 19.3), "bajo": (19.4, 24.8), "medio": (24.9, 30.7), "alto": (30.8, 38.6), "muy_alto": (38.7, 100)},
    },
    "Transporte": {
        "A": {"sin_riesgo": (0, 21.8), "bajo": (21.9, 28.1), "medio": (28.2, 34.0), "alto": (34.1, 41.9), "muy_alto": (42.0, 100)},
        "B": {"sin_riesgo": (0, 23.1), "bajo": (23.2, 28.8), "medio": (28.9, 34.7), "alto": (34.8, 42.7), "muy_alto": (42.8, 100)},
    },
    "Salud": {
        "A": {"sin_riesgo": (0, 17.6), "bajo": (17.7, 23.8), "medio": (23.9, 29.7), "alto": (29.8, 37.6), "muy_alto": (37.7, 100)},
        "B": {"sin_riesgo": (0, 18.7), "bajo": (18.8, 24.2), "medio": (24.3, 30.1), "alto": (30.2, 38.0), "muy_alto": (38.1, 100)},
    },
    "Educación": {
        "A": {"sin_riesgo": (0, 19.1), "bajo": (19.2, 25.3), "medio": (25.4, 31.2), "alto": (31.3, 39.1), "muy_alto": (39.2, 100)},
        "B": {"sin_riesgo": (0, 20.2), "bajo": (20.3, 25.7), "medio": (25.8, 31.6), "alto": (31.7, 39.5), "muy_alto": (39.6, 100)},
    },
    "Administración Pública": {
        "A": {"sin_riesgo": (0, 18.9), "bajo": (19.0, 25.1), "medio": (25.2, 31.0), "alto": (31.1, 38.9), "muy_alto": (39.0, 100)},
        "B": {"sin_riesgo": (0, 20.0), "bajo": (20.1, 25.5), "medio": (25.6, 31.4), "alto": (31.5, 39.3), "muy_alto": (39.4, 100)},
    },
    "Minas/energía": {
        "A": {"sin_riesgo": (0, 22.5), "bajo": (22.6, 28.8), "medio": (28.9, 34.7), "alto": (34.8, 42.6), "muy_alto": (42.7, 100)},
        "B": {"sin_riesgo": (0, 23.8), "bajo": (23.9, 29.5), "medio": (29.6, 35.4), "alto": (35.5, 43.4), "muy_alto": (43.5, 100)},
    },
}

NIVEL_A_ORDEN = {"sin_riesgo": 1, "bajo": 2, "medio": 3, "alto": 4, "muy_alto": 5}
ORDEN_A_NIVEL = {1: "Sin riesgo", 2: "Bajo", 3: "Medio", 4: "Alto", 5: "Muy alto"}


# FUNCIONES DE CLASIFICACIÓN
def clasificar_score_colombia(score: float, dimension: str, factor: str, forma: str) -> str:
    """Clasifica un score según baremos Colombia."""
    if factor == "intralaboral":
        baremo = BAREMO_COLOMBIA_INTRA_A if forma == "A" else BAREMO_COLOMBIA_INTRA_B
    elif factor == "extralaboral":
        baremo = BAREMO_COLOMBIA_EXTRA
    elif factor == "estres":
        baremo = BAREMO_COLOMBIA_ESTRES
    else:
        return "N/A"
    
    dim_key = dimension.lower().replace(" ", "_").replace("-", "_")
    if dim_key not in baremo:
        return "N/A"
    
    rangos = baremo[dim_key]
    for nivel, (minv, maxv) in rangos.items():
        if minv <= score <= maxv:
            return ORDEN_A_NIVEL[NIVEL_A_ORDEN[nivel]]
    return "Muy alto"


def clasificar_score_sectorial(score: float, sector: str, forma: str) -> str:
    """Clasifica intralaboral total según baremo sectorial. BINDING D2: SOLO intra total."""
    if sector not in BAREMO_SECTORIAL:
        return "N/A"
    baremo_sector = BAREMO_SECTORIAL[sector]
    if forma not in baremo_sector:
        return "N/A"
    rangos = baremo_sector[forma]
    for nivel, (minv, maxv) in rangos.items():
        if minv <= score <= maxv:
            return ORDEN_A_NIVEL[NIVEL_A_ORDEN[nivel]]
    return "Muy alto"


def calcular_delta_benchmark(score: float, nivel_colombia: str, nivel_sectorial: Optional[str] = None) -> Dict:
    """Calcula posición relativa vs benchmarks."""
    nivel_num_col = NIVEL_A_ORDEN.get(nivel_colombia.lower().replace(" ", "_"), 3)
    delta_colombia = nivel_num_col - 3
    
    result = {"nivel_orden_colombia": nivel_num_col, "delta_vs_medio": delta_colombia}
    
    if nivel_sectorial and nivel_sectorial != "N/A":
        nivel_num_sec = NIVEL_A_ORDEN.get(nivel_sectorial.lower().replace(" ", "_"), 3)
        result["nivel_orden_sectorial"] = nivel_num_sec
        result["delta_col_vs_sec"] = nivel_num_col - nivel_num_sec
    else:
        result["nivel_orden_sectorial"] = None
        result["delta_col_vs_sec"] = None
    return result


def calcular_percentil_empresa(df_empresa: pd.DataFrame, df_total: pd.DataFrame, dimension: str) -> float:
    """Calcula el percentil de una empresa vs todas las demás."""
    if dimension not in df_total.columns or dimension not in df_empresa.columns:
        return 50.0
    score_empresa = df_empresa[dimension].mean()
    all_scores = df_total.groupby("empresa")[dimension].mean()
    pct = (all_scores > score_empresa).sum() / len(all_scores) * 100
    return round(pct, 1)


# PROCESAMIENTO PRINCIPAL
def procesar_benchmarking(df_scores: pd.DataFrame) -> pd.DataFrame:
    """Aplica benchmarking Colombia + Sectorial. BINDING D2: Sectorial SOLO para intralaboral_total"""
    log.info("Aplicando benchmarking...")
    resultados = []
    
    for idx, row in df_scores.iterrows():
        cedula = row["cedula"]
        empresa = row["empresa"]
        sector = row.get("sector_rag", "Servicios")
        forma = row.get("forma_intra", "A")
        dimension = row.get("dimension", "")
        dominio = row.get("dominio", "")
        factor = row.get("factor", "")
        score = row.get("score_transformado", 0)
        
        # Clasificar Colombia (siempre)
        if dimension:
            nivel_col = clasificar_score_colombia(score, dimension, factor, forma)
        elif dominio:
            nivel_col = clasificar_score_colombia(score, dominio, factor, forma)
        elif factor:
            nivel_col = clasificar_score_colombia(score, f"{factor}_total", factor, forma)
        else:
            nivel_col = "N/A"
        
        # Clasificar Sectorial (SOLO para intralaboral_total)
        nivel_sec = "N/A"
        if factor == "intralaboral" and not dimension and not dominio:
            nivel_sec = clasificar_score_sectorial(score, sector, forma)
        
        delta = calcular_delta_benchmark(score, nivel_col, nivel_sec)
        
        resultado = {
            **row.to_dict(),
            "nivel_riesgo_colombia": nivel_col,
            "nivel_riesgo_sectorial": nivel_sec,
            "nivel_orden_colombia": delta["nivel_orden_colombia"],
            "delta_vs_medio": delta["delta_vs_medio"],
            "nivel_orden_sectorial": delta.get("nivel_orden_sectorial"),
            "delta_col_vs_sec": delta.get("delta_col_vs_sec"),
        }
        resultados.append(resultado)
    
    df_result = pd.DataFrame(resultados)
    log.info("  Procesados %d registros", len(df_result))
    return df_result


def validar_benchmark(df: pd.DataFrame) -> Tuple[bool, pd.DataFrame]:
    """Valida fact_benchmark."""
    log.info("Validando fact_benchmark...")
    errores = []
    
    niveles_validos = {"Sin riesgo", "Bajo", "Medio", "Alto", "Muy alto", "N/A"}
    invalidos_col = df[~df["nivel_riesgo_colombia"].isin(niveles_validos)]
    if len(invalidos_col) > 0:
        errores.append({"check": "nivel_colombia_invalido", "n": len(invalidos_col)})
    
    tiene_sectorial = df[(df["nivel_riesgo_sectorial"] != "N/A") & (df["nivel_riesgo_sectorial"].notna())]
    no_es_intra_total = tiene_sectorial[
        (tiene_sectorial["dimension"].notna() & (tiene_sectorial["dimension"] != "")) |
        (tiene_sectorial["dominio"].notna() & (tiene_sectorial["dominio"] != ""))
    ]
    if len(no_es_intra_total) > 0:
        errores.append({"check": "sectorial_fuera_intra_total", "n": len(no_es_intra_total)})
    
    df_errores = pd.DataFrame(errores)
    es_valido = len(errores) == 0
    if es_valido:
        log.info("Validación EXITOSA")
    else:
        log.error("Validación FALLIDA: %s", errores)
    return es_valido, df_errores


def main(config_path: str = "config/config.yaml") -> None:
    """Pipeline principal 06."""
    config = cargar_config(config_path)
    ruta_scores = Path(config["paths"]["processed"]) / "fact_scores_baremo.parquet"
    ruta_out = Path(config["paths"]["processed"]) / "fact_benchmark.parquet"
    
    log.info("=" * 60)
    log.info("06_benchmarking.py — Iniciando pipeline")
    log.info("Input:  %s", ruta_scores)
    log.info("Output: %s", ruta_out)
    log.info("=" * 60)
    
    if not ruta_scores.exists():
        log.error("Archivo no encontrado: %s", ruta_scores)
        sys.exit(1)
    
    df_scores = pd.read_parquet(ruta_scores)
    log.info("Cargados %d registros de scores", len(df_scores))
    
    df_benchmark = procesar_benchmarking(df_scores)
    
    ok, _ = validar_benchmark(df_benchmark)
    if not ok:
        log.warning("Pipeline 06 con advertencias de validación")
    
    ruta_out.parent.mkdir(parents=True, exist_ok=True)
    df_benchmark.to_parquet(ruta_out, index=False)
    log.info("Guardado: %s (%d filas)", ruta_out, len(df_benchmark))
    
    log.info("-" * 40)
    log.info("RESUMEN:")
    log.info("  Registros: %d", len(df_benchmark))
    log.info("  Empresas: %d", df_benchmark["empresa"].nunique())
    
    dist_col = df_benchmark["nivel_riesgo_colombia"].value_counts()
    log.info("  Distribución Colombia:")
    for nivel, n in dist_col.items():
        log.info("    %s: %d (%.1f%%)", nivel, n, 100*n/len(df_benchmark))
    
    df_sec = df_benchmark[df_benchmark["nivel_riesgo_sectorial"] != "N/A"]
    if len(df_sec) > 0:
        log.info("  Distribución Sectorial (solo intra total):")
        dist_sec = df_sec["nivel_riesgo_sectorial"].value_counts()
        for nivel, n in dist_sec.items():
            log.info("    %s: %d", nivel, n)
    
    log.info("-" * 40)
    log.info("06 completado exitosamente.")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="06_benchmarking.py — V1 Pasos 16-19")
    parser.add_argument("--config", default="config/config.yaml")
    args = parser.parse_args()
    main(config_path=args.config)   