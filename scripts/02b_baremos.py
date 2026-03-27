"""
02b_baremos.py — Implementa V1-Pasos 9 a 15
Pasos:
  V1-Paso9  : Baremos dimensiones intralaboral A/B (Res. 2764)
  V1-Paso10 : Baremos dimensiones extralaboral A/B (Res. 2764)
  V1-Paso11 : Baremos dimensiones individual AVANTUM (afrontamiento + cap.psicológico)
  V1-Paso12 : Baremos dominios intralaboral A/B (Res. 2764)
  V1-Paso13 : Baremos dominios individual AVANTUM
  V1-Paso14 : Baremos factor total (intra, extra, estrés) A/B (Res. 2764)
  V1-Paso15 : Baremos factor individual AVANTUM

Input:  data/processed/fact_scores_brutos.parquet
Output: data/processed/fact_scores_baremo.parquet

Reglas críticas:
  R2 : Baremos diferenciados A/B — NUNCA mezclar formas
  R5 : 5 niveles de riesgo normativos (Res. 2764/2022) — NUNCA reducir a 3
  R7 : Colores normativos: risk_1=#10B981, risk_2=#6EE7B7, risk_3=#F59E0B, risk_4=#F97316, risk_5=#EF4444

Fórmula general:
  puntaje_transformado = (puntaje_bruto / transformacion_max) * 100
  nivel_riesgo = clasificar según puntos de corte

Fuente documental: Visualizador 1, Pasos 9-15
Versión: 1.0 | Pipeline MentalPRO | Res. 2764/2022 MinTrabajo Colombia
"""

import logging
import sys
from pathlib import Path
from typing import Tuple, List, Dict
import numpy as np
import pandas as pd
import yaml

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("02b_baremos")


def cargar_config(config_path: str = "config/config.yaml") -> dict:
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


# ===========================================================================
# TABLAS DE BAREMOS — Puntos de corte Res. 2764/2022
# Fuente: Visualizador 1 Pasos 9-15
# Niveles: 1=Sin riesgo, 2=Bajo, 3=Medio, 4=Alto, 5=Muy alto
# ===========================================================================

# ---------------------------------------------------------------------------
# V1-Paso9: Baremos DIMENSIONES INTRALABORAL A/B (Res. 2764)
# Estructura: forma, dimension, transf_max, sin_riesgo_max, bajo_max, medio_max, alto_max
# ---------------------------------------------------------------------------
BAREMO_DIM_INTRA = [
    # --- Forma A ---
    ("A", "Demandas ambientales y de esfuerzo físico", 48, 14.6, 22.9, 31.3, 43.8),
    ("A", "Demandas emocionales", 36, 16.7, 25.0, 33.3, 47.2),
    ("A", "Demandas cuantitativas", 24, 25.0, 33.3, 41.7, 54.2),
    ("A", "Influencia del trabajo sobre el entorno extralaboral", 16, 18.8, 31.3, 43.8, 56.3),
    ("A", "Exigencias de responsabilidad del cargo", 24, 37.5, 54.2, 66.7, 83.3),
    ("A", "Demandas de carga mental", 20, 60.0, 70.0, 80.0, 95.0),
    ("A", "Consistencia del rol", 20, 15.0, 25.0, 35.0, 45.0),
    ("A", "Demandas de la jornada de trabajo", 12, 8.3, 25.0, 41.7, 58.3),
    ("A", "Claridad de rol", 28, 0.9, 10.7, 21.4, 35.7),
    ("A", "Capacitación", 12, 0.9, 16.7, 33.3, 50.0),
    ("A", "Participación y manejo del cambio", 16, 12.5, 25.0, 37.5, 50.0),
    ("A", "Oportunidades para el uso y desarrollo de habilidades y conocimientos", 16, 0.9, 6.3, 18.8, 37.5),
    ("A", "Control y autonomía sobre el trabajo", 12, 8.3, 25.0, 41.7, 58.3),
    ("A", "Características del liderazgo", 52, 3.8, 15.4, 26.9, 42.3),
    ("A", "Relaciones sociales en el trabajo", 56, 5.4, 16.1, 26.8, 41.1),
    ("A", "Retroalimentación del desempeño", 20, 10.0, 25.0, 40.0, 60.0),
    ("A", "Relación con los colaboradores", 36, 13.9, 25.0, 36.1, 50.0),
    ("A", "Recompensas derivadas de la pertenencia a la organización y del trabajo", 20, 0.9, 5.0, 15.0, 30.0),
    ("A", "Reconocimiento y compensación", 24, 4.2, 16.7, 29.2, 45.8),
    # --- Forma B ---
    ("B", "Demandas ambientales y de esfuerzo físico", 48, 22.9, 31.3, 39.6, 52.1),
    ("B", "Demandas emocionales", 36, 19.4, 27.8, 36.1, 50.0),
    ("B", "Demandas cuantitativas", 12, 16.7, 33.3, 50.0, 66.7),
    ("B", "Influencia del trabajo sobre el entorno extralaboral", 16, 12.5, 25.0, 37.5, 56.3),
    ("B", "Demandas de carga mental", 20, 50.0, 65.0, 75.0, 90.0),
    ("B", "Demandas de la jornada de trabajo", 24, 25.0, 37.5, 50.0, 66.7),
    ("B", "Claridad de rol", 20, 0.9, 5.0, 15.0, 30.0),
    ("B", "Capacitación", 12, 0.9, 16.7, 33.3, 50.0),
    ("B", "Participación y manejo del cambio", 12, 16.7, 33.3, 50.0, 66.7),
    ("B", "Oportunidades para el uso y desarrollo de habilidades y conocimientos", 16, 12.5, 25.0, 37.5, 56.3),
    ("B", "Control y autonomía sobre el trabajo", 12, 33.3, 50.0, 66.7, 83.3),
    ("B", "Características del liderazgo", 52, 3.8, 13.5, 25.0, 40.4),
    ("B", "Relaciones sociales en el trabajo", 48, 6.3, 14.6, 27.1, 41.7),
    ("B", "Retroalimentación del desempeño", 20, 5.0, 20.0, 35.0, 55.0),
    ("B", "Recompensas derivadas de la pertenencia a la organización y del trabajo", 16, 0.9, 6.3, 18.8, 37.5),
    ("B", "Reconocimiento y compensación", 24, 0.9, 12.5, 25.0, 41.7),
]

# ---------------------------------------------------------------------------
# V1-Paso10: Baremos DIMENSIONES EXTRALABORAL A/B (Res. 2764)
# ---------------------------------------------------------------------------
BAREMO_DIM_EXTRA = [
    # --- Forma A ---
    ("A", "Balance entre la vida laboral y familiar", 16, 6.3, 25.0, 43.8, 62.5),
    ("A", "Relaciones familiares", 12, 8.3, 25.0, 41.7, 58.3),
    ("A", "Comunicación y relaciones interpersonales", 20, 0.9, 10.0, 20.0, 35.0),
    ("A", "Situación económica del grupo familiar", 12, 8.3, 25.0, 41.7, 58.3),
    ("A", "Características de la vivienda y de su entorno", 36, 5.6, 11.1, 19.4, 33.3),
    ("A", "Influencia del entorno extralaboral sobre el trabajo", 12, 8.3, 16.7, 33.3, 50.0),
    ("A", "Desplazamiento vivienda trabajo vivienda", 16, 0.9, 12.5, 31.3, 56.3),
    # --- Forma B ---
    ("B", "Balance entre la vida laboral y familiar", 16, 6.3, 25.0, 43.8, 62.5),
    ("B", "Relaciones familiares", 12, 8.3, 25.0, 41.7, 58.3),
    ("B", "Comunicación y relaciones interpersonales", 20, 5.0, 15.0, 25.0, 40.0),
    ("B", "Situación económica del grupo familiar", 12, 16.7, 25.0, 41.7, 58.3),
    ("B", "Características de la vivienda y de su entorno", 36, 5.6, 11.1, 19.4, 33.3),
    ("B", "Influencia del entorno extralaboral sobre el trabajo", 12, 0.9, 16.7, 33.3, 50.0),
    ("B", "Desplazamiento vivienda trabajo vivienda", 16, 0.9, 12.5, 31.3, 56.3),
]

# ---------------------------------------------------------------------------
# V1-Paso11: Baremos DIMENSIONES INDIVIDUAL AVANTUM (afrontamiento + cap.psicológico)
# Escala de PROTECCIÓN (1=muy bajo protección/vulnerabilidad alta, 5=muy alta protección)
# ---------------------------------------------------------------------------
BAREMO_DIM_INDIVIDUAL = [
    # Afrontamiento — aplica A y B igual
    ("AyB", "afrontamiento_activo_planificacion", 4, 29, 51, 69, 89),
    ("AyB", "afrontamiento_pasivo_negacion", 4, 29, 51, 69, 89),
    ("AyB", "afrontamiento_activo_busquedasoporte", 4, 29, 51, 69, 89),
    # Capital psicológico — aplica A y B igual
    ("AyB", "Optimismo", 3, 29, 51, 69, 89),
    ("AyB", "Esperanza", 3, 29, 51, 69, 89),
    ("AyB", "Resiliencia", 3, 29, 51, 69, 89),
    ("AyB", "Autoeficacia", 3, 29, 51, 69, 89),
]

# ---------------------------------------------------------------------------
# V1-Paso12: Baremos DOMINIOS INTRALABORAL A/B (Res. 2764)
# ---------------------------------------------------------------------------
BAREMO_DOMINIO_INTRA = [
    ("A", "Demandas del trabajo", 200, 28.5, 35.0, 41.5, 50.5),
    ("A", "Control sobre el trabajo", 84, 10.7, 19.0, 29.8, 42.9),
    ("A", "Liderazgo y relaciones sociales en el trabajo", 164, 9.1, 17.7, 25.6, 36.6),
    ("A", "Recompensas", 44, 4.5, 11.4, 20.5, 34.1),
    ("B", "Demandas del trabajo", 156, 26.9, 33.3, 37.8, 46.8),
    ("B", "Control sobre el trabajo", 72, 19.4, 26.4, 34.7, 47.2),
    ("B", "Liderazgo y relaciones sociales en el trabajo", 120, 8.3, 17.5, 26.7, 40.0),
    ("B", "Recompensas", 40, 2.5, 10.0, 17.5, 30.0),
]

# ---------------------------------------------------------------------------
# V1-Paso13: Baremos DOMINIOS INDIVIDUAL AVANTUM
# ---------------------------------------------------------------------------
BAREMO_DOMINIO_INDIVIDUAL = [
    ("AyB", "Estrategias de Afrontamiento", 4, 29, 51, 69, 89),
    ("AyB", "Capital psicológico", 12, 29, 51, 69, 89),
]

# ---------------------------------------------------------------------------
# V1-Paso14: Baremos FACTOR TOTAL (intra, extra, estrés) — Res. 2764
# ---------------------------------------------------------------------------
BAREMO_FACTOR = [
    ("A", "Intralaboral", 492, 19.7, 25.8, 31.5, 38.8),
    ("B", "Intralaboral", 388, 20.6, 26.0, 31.2, 38.7),
    ("A", "Extralaboral", 124, 11.3, 16.9, 22.6, 29.0),
    ("B", "Extralaboral", 124, 12.9, 17.7, 24.2, 32.3),
    ("A", "Estres", 61.16, 7.8, 12.6, 14.7, 25.0),
    ("B", "Estres", 61.16, 6.5, 11.8, 17.0, 23.4),
]

# ---------------------------------------------------------------------------
# V1-Paso15: Baremos FACTOR INDIVIDUAL AVANTUM
# ---------------------------------------------------------------------------
BAREMO_FACTOR_INDIVIDUAL = [
    ("A", "Individual", 24, 29, 51, 69, 89),
    ("B", "Individual", 24, 29, 51, 69, 89),
]


# ===========================================================================
# FUNCIONES DE CLASIFICACIÓN
# ===========================================================================

NIVELES_RIESGO = {
    1: "Sin riesgo",
    2: "Bajo",
    3: "Medio",
    4: "Alto",
    5: "Muy alto",
}

NIVELES_PROTECCION = {
    1: "Muy bajo",   # Alta vulnerabilidad
    2: "Bajo",
    3: "Medio",
    4: "Alto",
    5: "Muy alto",   # Alta protección
}


def clasificar_nivel(puntaje_transf: float, cortes: tuple, tipo: str = "riesgo") -> int:
    """
    Clasifica un puntaje transformado en nivel 1-5.
    
    Args:
        puntaje_transf: Puntaje transformado (0-100)
        cortes: Tupla (sin_riesgo_max, bajo_max, medio_max, alto_max)
        tipo: "riesgo" (mayor score = peor) o "proteccion" (mayor score = mejor)
    Returns:
        int 1-5
    """
    if pd.isna(puntaje_transf):
        return np.nan
    
    c1, c2, c3, c4 = cortes
    
    if tipo == "riesgo":
        # Riesgo: mayor puntaje = peor nivel
        if puntaje_transf <= c1:
            return 1  # Sin riesgo
        elif puntaje_transf <= c2:
            return 2  # Bajo
        elif puntaje_transf <= c3:
            return 3  # Medio
        elif puntaje_transf <= c4:
            return 4  # Alto
        else:
            return 5  # Muy alto
    else:
        # Protección: mayor puntaje = mejor nivel
        if puntaje_transf <= c1:
            return 1  # Muy bajo (vulnerable)
        elif puntaje_transf <= c2:
            return 2  # Bajo
        elif puntaje_transf <= c3:
            return 3  # Medio
        elif puntaje_transf <= c4:
            return 4  # Alto
        else:
            return 5  # Muy alto (protegido)


def construir_lookup_baremos() -> pd.DataFrame:
    """
    Construye tabla lookup con todos los baremos.
    Columnas: forma, nivel_analisis, nombre_nivel, transf_max, c1, c2, c3, c4, tipo_baremo
    """
    registros = []
    
    # Dimensiones intralaboral (riesgo)
    for forma, dim, transf, c1, c2, c3, c4 in BAREMO_DIM_INTRA:
        registros.append({
            "forma": forma, "nivel_analisis": "dimension", "nombre_nivel": dim,
            "factor": "Intralaboral", "transf_max": transf,
            "c1": c1, "c2": c2, "c3": c3, "c4": c4, "tipo_baremo": "riesgo",
        })
    
    # Dimensiones extralaboral (riesgo)
    for forma, dim, transf, c1, c2, c3, c4 in BAREMO_DIM_EXTRA:
        registros.append({
            "forma": forma, "nivel_analisis": "dimension", "nombre_nivel": dim,
            "factor": "Extralaboral", "transf_max": transf,
            "c1": c1, "c2": c2, "c3": c3, "c4": c4, "tipo_baremo": "riesgo",
        })
    
    # Dimensiones individual (protección)
    for forma, dim, transf, c1, c2, c3, c4 in BAREMO_DIM_INDIVIDUAL:
        registros.append({
            "forma": forma, "nivel_analisis": "dimension", "nombre_nivel": dim,
            "factor": "Individual", "transf_max": transf,
            "c1": c1, "c2": c2, "c3": c3, "c4": c4, "tipo_baremo": "proteccion",
        })
    
    # Dominios intralaboral (riesgo)
    for forma, dom, transf, c1, c2, c3, c4 in BAREMO_DOMINIO_INTRA:
        registros.append({
            "forma": forma, "nivel_analisis": "dominio", "nombre_nivel": dom,
            "factor": "Intralaboral", "transf_max": transf,
            "c1": c1, "c2": c2, "c3": c3, "c4": c4, "tipo_baremo": "riesgo",
        })
    
    # Dominios individual (protección)
    for forma, dom, transf, c1, c2, c3, c4 in BAREMO_DOMINIO_INDIVIDUAL:
        registros.append({
            "forma": forma, "nivel_analisis": "dominio", "nombre_nivel": dom,
            "factor": "Individual", "transf_max": transf,
            "c1": c1, "c2": c2, "c3": c3, "c4": c4, "tipo_baremo": "proteccion",
        })
    
    # Factor total (riesgo)
    for forma, fac, transf, c1, c2, c3, c4 in BAREMO_FACTOR:
        registros.append({
            "forma": forma, "nivel_analisis": "factor", "nombre_nivel": fac,
            "factor": fac, "transf_max": transf,
            "c1": c1, "c2": c2, "c3": c3, "c4": c4, "tipo_baremo": "riesgo",
        })
    
    # Factor individual (protección)
    for forma, fac, transf, c1, c2, c3, c4 in BAREMO_FACTOR_INDIVIDUAL:
        registros.append({
            "forma": forma, "nivel_analisis": "factor", "nombre_nivel": fac,
            "factor": fac, "transf_max": transf,
            "c1": c1, "c2": c2, "c3": c3, "c4": c4, "tipo_baremo": "proteccion",
        })
    
    return pd.DataFrame(registros)


# ===========================================================================
# FUNCIONES DE CÁLCULO DE PUNTAJES
# ===========================================================================

def calcular_puntaje_bruto_dimension(df_scores: pd.DataFrame, cedula: str,
                                      forma: str, dimension: str) -> float:
    """
    Calcula puntaje bruto para una dimensión: suma de valor_invertido de sus ítems.
    """
    mask = (
        (df_scores["cedula"] == cedula) &
        (df_scores["forma_intra"].str.upper() == forma.upper()) &
        (df_scores["dimension"] == dimension)
    )
    return df_scores.loc[mask, "valor_invertido"].sum()


def calcular_puntaje_bruto_dominio(df_scores: pd.DataFrame, cedula: str,
                                    forma: str, dominio: str) -> float:
    """
    Calcula puntaje bruto para un dominio: suma de valor_invertido de sus ítems.
    """
    mask = (
        (df_scores["cedula"] == cedula) &
        (df_scores["forma_intra"].str.upper() == forma.upper()) &
        (df_scores["dominio"] == dominio)
    )
    return df_scores.loc[mask, "valor_invertido"].sum()


def calcular_puntaje_bruto_factor(df_scores: pd.DataFrame, cedula: str,
                                   forma: str, factor: str) -> float:
    """
    Calcula puntaje bruto para un factor: suma de valor_invertido de sus ítems.
    """
    mask = (
        (df_scores["cedula"] == cedula) &
        (df_scores["forma_intra"].str.upper() == forma.upper()) &
        (df_scores["factor"] == factor)
    )
    return df_scores.loc[mask, "valor_invertido"].sum()


def procesar_baremos_por_nivel(df_scores: pd.DataFrame, nivel_analisis: str) -> pd.DataFrame:
    """
    Procesa baremos para un nivel de análisis (dimension, dominio, factor).
    Genera una fila por trabajador × nivel.
    
    Returns:
        DataFrame con: cedula, empresa, forma_intra, sector_rag, nivel_analisis,
                       nombre_nivel, puntaje_bruto, puntaje_transformado, nivel_riesgo, tipo_baremo
    """
    log.info("Procesando baremos nivel: %s", nivel_analisis)
    
    lookup = construir_lookup_baremos()
    lookup_nivel = lookup[lookup["nivel_analisis"] == nivel_analisis]
    
    # Determinar columna de agrupación
    if nivel_analisis == "dimension":
        group_col = "dimension"
    elif nivel_analisis == "dominio":
        group_col = "dominio"
    else:  # factor
        group_col = "factor"
    
    # Agrupar por trabajador × forma × nivel
    df_grouped = df_scores.groupby(
        ["cedula", "empresa", "forma_intra", "sector_rag", group_col],
        as_index=False
    ).agg({"valor_invertido": "sum"})
    df_grouped.rename(columns={"valor_invertido": "puntaje_bruto", group_col: "nombre_nivel"}, inplace=True)
    df_grouped["nivel_analisis"] = nivel_analisis
    
    # Normalizar forma para lookup
    df_grouped["forma_key"] = df_grouped["forma_intra"].str.upper()
    
    # JOIN con baremos
    # Para Individual (AyB), hacer match con cualquier forma
    resultados = []
    for _, row in df_grouped.iterrows():
        forma = row["forma_key"]
        nombre = row["nombre_nivel"]
        
        # Buscar baremo: primero por forma específica, luego por AyB
        baremo = lookup_nivel[
            ((lookup_nivel["forma"] == forma) | (lookup_nivel["forma"] == "AyB")) &
            (lookup_nivel["nombre_nivel"] == nombre)
        ]
        
        if len(baremo) == 0:
            # Dimensión/dominio sin baremo definido (ej: ítems especiales)
            continue
        
        bar = baremo.iloc[0]
        transf_max = bar["transf_max"]
        puntaje_bruto = row["puntaje_bruto"]
        puntaje_transf = (puntaje_bruto / transf_max) * 100 if transf_max > 0 else 0
        cortes = (bar["c1"], bar["c2"], bar["c3"], bar["c4"])
        nivel = clasificar_nivel(puntaje_transf, cortes, bar["tipo_baremo"])
        
        resultados.append({
            "cedula": row["cedula"],
            "empresa": row["empresa"],
            "forma_intra": row["forma_intra"],
            "sector_rag": row["sector_rag"],
            "nivel_analisis": nivel_analisis,
            "nombre_nivel": nombre,
            "factor": bar["factor"],
            "puntaje_bruto": puntaje_bruto,
            "transf_max": transf_max,
            "puntaje_transformado": round(puntaje_transf, 2),
            "nivel_riesgo": nivel,
            "tipo_baremo": bar["tipo_baremo"],
        })
    
    df_result = pd.DataFrame(resultados)
    log.info("  %s: %d registros generados", nivel_analisis, len(df_result))
    return df_result


# ===========================================================================
# VALIDACIÓN DE INTEGRIDAD
# ===========================================================================

def validar_scores_baremo(df: pd.DataFrame) -> Tuple[bool, pd.DataFrame]:
    """
    Valida integridad de fact_scores_baremo.
    """
    log.info("Validando fact_scores_baremo...")
    errores = []
    
    # 1. PK sin duplicados
    pk = ["cedula", "forma_intra", "nivel_analisis", "nombre_nivel"]
    dupes = df[df.duplicated(subset=pk, keep=False)]
    if len(dupes) > 0:
        errores.append({
            "check": "PK_duplicada",
            "n": len(dupes),
            "detalle": f"PKs duplicadas: {dupes[pk].head(3).to_dict()}",
        })
    
    # 2. Niveles de riesgo válidos (1-5)
    niveles_invalidos = df[(df["nivel_riesgo"] < 1) | (df["nivel_riesgo"] > 5)]
    if len(niveles_invalidos) > 0:
        errores.append({
            "check": "nivel_riesgo_invalido",
            "n": len(niveles_invalidos),
            "detalle": f"Valores fuera de 1-5",
        })
    
    # 3. Puntajes transformados en rango razonable
    fuera_rango = df[(df["puntaje_transformado"] < 0) | (df["puntaje_transformado"] > 150)]
    if len(fuera_rango) > 0:
        errores.append({
            "check": "puntaje_transf_fuera_rango",
            "n": len(fuera_rango),
            "detalle": f"Puntajes < 0 o > 150",
        })
    
    # 4. Verificar formas válidas
    formas = df["forma_intra"].str.upper().unique().tolist()
    formas_invalidas = [f for f in formas if f not in ("A", "B")]
    if formas_invalidas:
        errores.append({
            "check": "forma_invalida",
            "n": len(formas_invalidas),
            "detalle": f"Formas no reconocidas: {formas_invalidas}",
        })
    
    # 5. Distribución de niveles — no puede ser todo nivel 1 o todo nivel 5
    dist = df["nivel_riesgo"].value_counts(normalize=True)
    if dist.get(1, 0) > 0.95 or dist.get(5, 0) > 0.95:
        errores.append({
            "check": "distribucion_sospechosa",
            "n": 1,
            "detalle": f"Distribución anómala: {dist.to_dict()}",
        })
    
    df_errores = pd.DataFrame(errores)
    es_valido = len(errores) == 0
    if es_valido:
        log.info("Validación EXITOSA — fact_scores_baremo integro")
    else:
        log.error("Validación FALLIDA — %d checks con errores", len(errores))
        log.error(df_errores.to_string())
    
    return es_valido, df_errores


# ===========================================================================
# PIPELINE PRINCIPAL
# ===========================================================================

def main(config_path: str = "config/config.yaml") -> None:
    """
    Pipeline principal 02b.
    1. Carga fact_scores_brutos
    2. Procesa baremos para dimensiones, dominios y factores
    3. Concatena todos los niveles
    4. Guarda fact_scores_baremo.parquet
    5. Valida y reporta
    """
    config   = cargar_config(config_path)
    ruta_in  = Path(config["paths"]["processed"]) / "fact_scores_brutos.parquet"
    ruta_out = Path(config["paths"]["processed"]) / "fact_scores_baremo.parquet"
    
    log.info("=" * 60)
    log.info("02b_baremos.py — Iniciando pipeline")
    log.info("Input:  %s", ruta_in)
    log.info("Output: %s", ruta_out)
    log.info("=" * 60)
    
    # --- Carga
    if not ruta_in.exists():
        log.error("Archivo no encontrado: %s", ruta_in)
        log.error("Ejecutar primero: 02a_scoring_bateria.py")
        sys.exit(1)
    
    df_scores = pd.read_parquet(ruta_in)
    log.info("Cargados %d registros × %d columnas", len(df_scores), df_scores.shape[1])
    log.info("Trabajadores únicos: %d", df_scores["cedula"].nunique())
    log.info("Formas: %s", df_scores["forma_intra"].str.upper().unique().tolist())
    
    # Validar columnas requeridas
    cols_req = ["cedula", "empresa", "forma_intra", "sector_rag", "dimension", "dominio", "factor", "valor_invertido"]
    faltantes = [c for c in cols_req if c not in df_scores.columns]
    if faltantes:
        log.error("Columnas faltantes en fact_scores_brutos: %s", faltantes)
        sys.exit(1)
    
    # --- Procesar baremos por nivel de análisis
    df_dimensiones = procesar_baremos_por_nivel(df_scores, "dimension")
    df_dominios    = procesar_baremos_por_nivel(df_scores, "dominio")
    df_factores    = procesar_baremos_por_nivel(df_scores, "factor")
    
    # --- Concatenar todos los niveles
    df_baremo = pd.concat([df_dimensiones, df_dominios, df_factores], ignore_index=True)
    log.info("Total registros fact_scores_baremo: %d", len(df_baremo))
    
    # --- Agregar etiquetas de nivel
    def etiquetar_nivel(row):
        nivel = row["nivel_riesgo"]
        if pd.isna(nivel):
            return np.nan
        nivel = int(nivel)
        if row["tipo_baremo"] == "proteccion":
            return NIVELES_PROTECCION.get(nivel, "Desconocido")
        return NIVELES_RIESGO.get(nivel, "Desconocido")
    
    df_baremo["nivel_etiqueta"] = df_baremo.apply(etiquetar_nivel, axis=1)
    
    # --- Guardar
    ruta_out.parent.mkdir(parents=True, exist_ok=True)
    df_baremo.to_parquet(ruta_out, index=False)
    log.info("Guardado: %s (%d filas × %d cols)", ruta_out, len(df_baremo), df_baremo.shape[1])
    
    # --- Validación final
    ok, df_err = validar_scores_baremo(df_baremo)
    if not ok:
        log.error("Pipeline 02b completado con ERRORES — revisar validación")
        sys.exit(1)
    
    # --- Resumen estadístico
    log.info("-" * 40)
    log.info("RESUMEN FACT_SCORES_BAREMO:")
    log.info("  Total registros      : %d", len(df_baremo))
    log.info("  Trabajadores únicos  : %d", df_baremo["cedula"].nunique())
    log.info("  Niveles de análisis  : %s", df_baremo["nivel_analisis"].unique().tolist())
    
    # Distribución por tipo de baremo
    log.info("  Por tipo_baremo:")
    for tipo in ["riesgo", "proteccion"]:
        df_tipo = df_baremo[df_baremo["tipo_baremo"] == tipo]
        if len(df_tipo) > 0:
            dist = df_tipo["nivel_riesgo"].value_counts().sort_index()
            log.info("    %s: %s", tipo, dist.to_dict())
    
    # Distribución por nivel de riesgo (general)
    log.info("  Distribución niveles:")
    dist_global = df_baremo["nivel_etiqueta"].value_counts()
    for etiq, n in dist_global.items():
        log.info("    %s: %d (%.1f%%)", etiq, n, 100 * n / len(df_baremo))
    
    log.info("-" * 40)
    log.info("02b completado exitosamente.")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="02b_baremos.py — V1 Pasos 9-15")
    parser.add_argument("--config", default="config/config.yaml",
                        help="Ruta al archivo config.yaml")
    args = parser.parse_args()
    main(config_path=args.config)