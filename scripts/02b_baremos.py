# -*- coding: utf-8 -*-
"""
02b_baremos.py
==============
Pasos 9-15 del pipeline V1 — Puntajes brutos + transformados + nivel de riesgo/protección.

PASO 9   Baremos Ministerio — Dimensiones intralaboral A y B
PASO 10  Baremos Ministerio — Dimensiones extralaboral A y B
PASO 11  Baremos Avantum   — Dimensiones individual (afrontamiento + capitalpsic) A y B
PASO 12  Baremos Ministerio — Dominios intralaboral A y B
PASO 13  Baremos Avantum   — Dominios individual A y B
PASO 14  Baremos Ministerio — Factores intra + extra + estrés A y B
PASO 14.1 Baremos Ministerio — Factor estrés (4 promedios ponderados)
PASO 15  Baremos Avantum   — Factor individual A y B (afront + cappsico combinado)

Etiquetas por tipo_baremo:
  riesgo             → Sin riesgo | Riesgo bajo | Riesgo medio | Riesgo alto | Riesgo muy alto
  afrontamiento_dim  → Muy inadecuado | Inadecuado | Algo adecuado | Adecuado | Muy adecuado
  capitalpsicologico_dim → Muy bajo capital psicológico | ... | Muy alto capital psicológico
  proteccion         → Muy bajo | Bajo | Medio | Alto | Muy alto  (dominio afront/cappsico)
  individual         → Muy bajo | Bajo | Medio | Alto | Muy alto  (factor individual)

Output: data/processed/fact_scores_baremo.parquet
Fuente: Documento marco.md — Ministerio de Trabajo Colombia / Avantum
"""

import logging
import sys
from pathlib import Path

import pandas as pd
import yaml

ROOT = Path(__file__).resolve().parents[1]
CONFIG_PATH = ROOT / "config" / "config.yaml"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════════════════════
# Etiquetas por tipo de baremo
# ══════════════════════════════════════════════════════════════════════════════
LABELS_POR_TIPO: dict[str, dict[int, str]] = {
    "riesgo": {
        1: "Sin riesgo",
        2: "Riesgo bajo",
        3: "Riesgo medio",
        4: "Riesgo alto",
        5: "Riesgo muy alto",
    },
    "afrontamiento_dim": {
        1: "Muy inadecuado",
        2: "Inadecuado",
        3: "Algo adecuado",
        4: "Adecuado",
        5: "Muy adecuado",
    },
    "capitalpsicologico_dim": {
        1: "Muy bajo capital psicológico",
        2: "Bajo capital psicológico",
        3: "Medio capital psicológico",
        4: "Alto capital psicológico",
        5: "Muy alto capital psicológico",
    },
    "proteccion": {
        1: "Muy bajo",
        2: "Bajo",
        3: "Medio",
        4: "Alto",
        5: "Muy alto",
    },
    "individual": {
        1: "Muy bajo",
        2: "Bajo",
        3: "Medio",
        4: "Alto",
        5: "Muy alto",
    },
}


# ══════════════════════════════════════════════════════════════════════════════
# BAREMOS DIMENSIONES — Pasos 9 (IntraA/B) y 10 (Extralaboral) y 11 (Individual)
# Fuente: Manual Técnico Batería Riesgo Psicosocial, Res. 2764/2022 + Avantum
# Formato: {dim_name: (transformacion_max, c_sr, c_b, c_m, c_a, tipo_baremo)}
# ══════════════════════════════════════════════════════════════════════════════

_DIM_INTRA_A: dict[str, tuple] = {
    "Demandas ambientales y de esfuerzo físico":     (48,  14.6, 22.9, 31.3, 39.6, "riesgo"),
    "Demandas emocionales":                          (36,  16.7, 25.0, 33.3, 47.2, "riesgo"),
    "Demandas cuantitativas":                        (24,  25.0, 33.3, 45.8, 54.2, "riesgo"),
    "Influencia del trabajo sobre el entorno extra": (16,  18.8, 31.3, 43.8, 50.0, "riesgo"),
    "Exigencias de responsabilidad del cargo":       (24,  37.5, 54.2, 66.7, 79.2, "riesgo"),
    "Demandas de carga mental":                      (20,  60.0, 70.0, 80.0, 90.0, "riesgo"),
    "Consistencia del rol":                          (20,  15.0, 25.0, 35.0, 45.0, "riesgo"),
    "Demandas de la jornada de trabajo":             (12,   8.3, 25.0, 33.3, 50.0, "riesgo"),
    "Claridad de rol":                               (28,   0.9, 10.7, 21.4, 39.3, "riesgo"),
    "Capacitación":                                  (12,   0.9, 16.7, 33.3, 50.0, "riesgo"),
    "Participación y manejo del cambio":             (16,  12.5, 25.0, 37.5, 50.0, "riesgo"),
    "Oportunidades de desarrollo y uso de habilidad":(16,   0.9,  6.3, 18.8, 31.3, "riesgo"),
    "Control y autonomía sobre el trabajo":          (12,   8.3, 25.0, 41.7, 58.3, "riesgo"),
    "Características del liderazgo":                 (52,   3.8, 15.4, 30.8, 46.2, "riesgo"),
    "Relaciones sociales en el trabajo":             (56,   5.4, 16.1, 25.0, 37.5, "riesgo"),
    "Retroalimentación del desempeño":               (20,  10.0, 25.0, 40.0, 55.0, "riesgo"),
    "Relación con los colaboradores (subordinados)": (36,  13.9, 25.0, 33.3, 47.2, "riesgo"),
    "Recompensas derivadas de la pertenencia a la":  (20,   0.9,  5.0, 10.0, 20.0, "riesgo"),
    "Reconocimiento y compensación":                 (24,   4.2, 16.7, 25.0, 37.5, "riesgo"),
}

_DIM_INTRA_B: dict[str, tuple] = {
    "Demandas ambientales y de esfuerzo físico":     (48,  22.9, 31.3, 39.6, 47.9, "riesgo"),
    "Demandas emocionales":                          (36,  19.4, 27.8, 38.9, 47.2, "riesgo"),
    "Demandas cuantitativas":                        (12,  16.7, 33.3, 41.7, 50.0, "riesgo"),
    "Influencia del trabajo sobre el entorno extra": (16,  12.5, 25.0, 31.3, 50.0, "riesgo"),
    "Demandas de carga mental":                      (20,  50.0, 65.0, 75.0, 85.0, "riesgo"),
    "Demandas de la jornada de trabajo":             (24,  25.0, 37.5, 45.8, 58.3, "riesgo"),
    "Claridad de rol":                               (20,   0.9,  5.0, 15.0, 30.0, "riesgo"),
    "Capacitación":                                  (12,   0.9, 16.7, 25.0, 50.0, "riesgo"),
    "Participación y manejo del cambio":             (12,  16.7, 33.3, 41.7, 58.3, "riesgo"),
    "Oportunidades de desarrollo y uso de habilidad":(16,  12.5, 25.0, 37.5, 56.3, "riesgo"),
    "Control y autonomía sobre el trabajo":          (12,  33.3, 50.0, 66.7, 75.0, "riesgo"),
    "Características del liderazgo":                 (52,   3.8, 13.5, 25.0, 38.5, "riesgo"),
    "Relaciones sociales en el trabajo":             (48,   6.3, 14.6, 27.1, 37.5, "riesgo"),
    "Retroalimentación del desempeño":               (20,   5.0, 20.0, 30.0, 50.0, "riesgo"),
    "Recompensas derivadas de la pertenencia a la":  (16,   0.9,  6.3, 12.5, 18.8, "riesgo"),
    "Reconocimiento y compensación":                 (24,   0.9, 12.5, 25.0, 37.5, "riesgo"),
}

_DIM_EXTRA_A: dict[str, tuple] = {
    "Balance entre la vida laboral y familiar":             (16,  6.3, 25.0, 37.5, 50.0, "riesgo"),
    "Relaciones familiares":                                (12,  8.3, 25.0, 33.3, 50.0, "riesgo"),
    "Comunicación y relaciones interpersonales":            (20,  0.9, 10.0, 20.0, 30.0, "riesgo"),
    "Situación económica del grupo familiar":               (12,  8.3, 25.0, 33.3, 50.0, "riesgo"),
    "Características de la vivienda y de su entorno":       (36,  5.6, 11.1, 13.9, 22.2, "riesgo"),
    "Influencia del entorno extralaboral sobre el trabajo": (12,  8.3, 16.7, 25.0, 41.7, "riesgo"),
    "Desplazamiento vivienda trabajo vivienda":             (16,  0.9, 12.5, 25.0, 43.8, "riesgo"),
}

_DIM_EXTRA_B: dict[str, tuple] = {
    "Balance entre la vida laboral y familiar":             (16,  6.3, 25.0, 37.5, 50.0, "riesgo"),
    "Relaciones familiares":                                (12,  8.3, 25.0, 33.3, 50.0, "riesgo"),
    "Comunicación y relaciones interpersonales":            (20,  5.0, 15.0, 25.0, 35.0, "riesgo"),
    "Situación económica del grupo familiar":               (12, 16.7, 25.0, 41.7, 50.0, "riesgo"),
    "Características de la vivienda y de su entorno":       (36,  5.6, 11.1, 16.7, 27.8, "riesgo"),
    "Influencia del entorno extralaboral sobre el trabajo": (12,  0.9, 16.7, 25.0, 41.7, "riesgo"),
    "Desplazamiento vivienda trabajo vivienda":             (16,  0.9, 12.5, 25.0, 43.8, "riesgo"),
}

_DIM_AFRONTAMIENTO: dict[str, tuple] = {
    "Afrontamiento activo_planificación":   (4, 29.0, 51.0, 69.0, 89.0, "afrontamiento_dim"),
    "Afrontamiento evitativo_negación":     (4, 29.0, 51.0, 69.0, 89.0, "afrontamiento_dim"),
    "Afrontamiento activo_busquedasoporte": (4, 29.0, 51.0, 69.0, 89.0, "afrontamiento_dim"),
}

_DIM_CAPPSICO: dict[str, tuple] = {
    "Optimismo":    (3, 29.0, 51.0, 69.0, 89.0, "capitalpsicologico_dim"),
    "Esperanza":    (3, 29.0, 51.0, 69.0, 89.0, "capitalpsicologico_dim"),
    "Resiliencia":  (3, 29.0, 51.0, 69.0, 89.0, "capitalpsicologico_dim"),
    "Autoeficacia": (3, 29.0, 51.0, 69.0, 89.0, "capitalpsicologico_dim"),
}

BAREMOS_DIMENSION: dict[tuple, dict] = {
    ("IntraA",       "A"): _DIM_INTRA_A,
    ("IntraB",       "B"): _DIM_INTRA_B,
    ("Extralaboral", "A"): _DIM_EXTRA_A,
    ("Extralaboral", "B"): _DIM_EXTRA_B,
    ("Afrontamiento","A"): _DIM_AFRONTAMIENTO,
    ("Afrontamiento","B"): _DIM_AFRONTAMIENTO,
    ("CapPsico",     "A"): _DIM_CAPPSICO,
    ("CapPsico",     "B"): _DIM_CAPPSICO,
}

# Lookup plano: (instrumento, forma_intra, nombre_nivel) → (c_sr, c_b, c_m, c_a, tipo_baremo)
# transformacion_max se deriva de datos reales (ver _construir_dim_baremo)
CORTES_DIMENSION: dict[tuple, tuple] = {
    (inst, forma, dim): (c_sr, c_b, c_m, c_a, tipo)
    for (inst, forma), dims in BAREMOS_DIMENSION.items()
    for dim, (_, c_sr, c_b, c_m, c_a, tipo) in dims.items()
}


# ══════════════════════════════════════════════════════════════════════════════
# BAREMOS DOMINIOS — Pasos 12 (IntraA/B) y 13 (CapPsico)
# Afrontamiento dominio se calcula con función ponderada especial (ver abajo)
# ══════════════════════════════════════════════════════════════════════════════
_DOM_INTRA_A: dict[str, tuple] = {
    "Demandas del trabajo":           (200, 28.5, 35.0, 41.5, 47.5, "riesgo"),
    "Control sobre el trabajo":       ( 84, 10.7, 19.0, 29.8, 40.5, "riesgo"),
    "Liderazgo y relaciones sociales":(164,  9.1, 17.7, 25.6, 34.8, "riesgo"),
    "Recompensas":                    ( 44,  4.5, 11.4, 20.5, 29.5, "riesgo"),
}

_DOM_INTRA_B: dict[str, tuple] = {
    "Demandas del trabajo":           (156, 26.9, 33.3, 37.8, 44.2, "riesgo"),
    "Control sobre el trabajo":       ( 72, 19.4, 26.4, 34.7, 43.1, "riesgo"),
    "Liderazgo y relaciones sociales":(120,  8.3, 17.5, 26.7, 38.3, "riesgo"),
    "Recompensas":                    ( 40,  2.5, 10.0, 17.5, 27.5, "riesgo"),
}

_DOM_CAPPSICO: dict[str, tuple] = {
    "Capital psicológico": (12, 29.0, 51.0, 69.0, 89.0, "proteccion"),
}

BAREMOS_DOMINIO: dict[tuple, dict] = {
    ("IntraA",   "A"): _DOM_INTRA_A,
    ("IntraB",   "B"): _DOM_INTRA_B,
    ("CapPsico", "A"): _DOM_CAPPSICO,
    ("CapPsico", "B"): _DOM_CAPPSICO,
}

# Lookup plano: (instrumento, forma_intra, nombre_nivel) → (c_sr, c_b, c_m, c_a, tipo_baremo)
CORTES_DOMINIO: dict[tuple, tuple] = {
    (inst, forma, dom): (c_sr, c_b, c_m, c_a, tipo)
    for (inst, forma), doms in BAREMOS_DOMINIO.items()
    for dom, (_, c_sr, c_b, c_m, c_a, tipo) in doms.items()
}


# ══════════════════════════════════════════════════════════════════════════════
# BAREMOS FACTOR — Pasos 14, 14.1, 15
# Formato: (transformacion_max, c_sr, c_b, c_m, c_a, tipo_baremo)
# Estrés e Individual tienen funciones propias (fórmulas especiales)
# ══════════════════════════════════════════════════════════════════════════════
CORTES_FACTOR_FORMA: dict[tuple, tuple] = {
    # Paso 14 — IntraA / IntraB
    ("IntraA",             "A"): (492.0, 19.7, 25.8, 31.5, 38.8, "riesgo"),
    ("IntraB",             "B"): (388.0, 20.6, 26.0, 31.2, 38.7, "riesgo"),
    # Paso 14 — Extralaboral (A y B tienen cortes distintos)
    ("Extralaboral",       "A"): (124.0, 11.3, 16.9, 22.6, 29.0, "riesgo"),
    ("Extralaboral",       "B"): (124.0, 12.9, 17.7, 24.2, 32.3, "riesgo"),
    # Paso 14 — IntraA+Extralaboral y IntraB+Extralaboral combinados
    ("IntraA+Extralaboral","A"): (616.0, 18.8, 24.4, 29.5, 35.4, "riesgo"),
    ("IntraB+Extralaboral","B"): (512.0, 19.9, 24.8, 29.5, 35.4, "riesgo"),
    # Paso 14.1 — Estrés (fórmula 4 promedios ponderados; max teórico 61.16)
    ("Estres",             "A"): (61.16,  7.8, 12.6, 14.7, 25.0, "riesgo"),
    ("Estres",             "B"): (61.16,  6.5, 11.8, 17.0, 23.4, "riesgo"),
    # Paso 15 — Factor Individual (afront + cappsico; max=24)
    ("Individual",         "A"): (24.0,  29.0, 51.0, 69.0, 89.0, "individual"),
    ("Individual",         "B"): (24.0,  29.0, 51.0, 69.0, 89.0, "individual"),
}

# Grupos ítems Estrés para cálculo ponderado (paso 14.1)
# Cada tupla: (lista_items, peso_multiplicador)
ESTRES_GRUPOS: list[tuple] = [
    (list(range(1,  9)), 4),   # PP1: ítems 1-8   × 4
    (list(range(9, 13)), 3),   # PP2: ítems 9-12  × 3
    (list(range(13,23)), 2),   # PP3: ítems 13-22 × 2
    (list(range(23,32)), 1),   # PP4: ítems 23-31 × 1
]


# ══════════════════════════════════════════════════════════════════════════════
# Helpers
# ══════════════════════════════════════════════════════════════════════════════

def cargar_config() -> dict:
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def clasificar_nivel(pt: float, c_sr: float, c_b: float, c_m: float, c_a: float) -> int:
    """Clasifica puntaje_transformado en nivel 1-5. ≤ corte → nivel."""
    if pd.isna(pt):
        return 0
    if pt <= c_sr:
        return 1
    if pt <= c_b:
        return 2
    if pt <= c_m:
        return 3
    if pt <= c_a:
        return 4
    return 5


def etiqueta_nivel(nivel: int, tipo_baremo: str) -> str:
    """Retorna etiqueta categórica según nivel (1-5) y tipo de baremo."""
    if nivel == 0:
        return "Sin datos"
    return LABELS_POR_TIPO.get(tipo_baremo, {}).get(nivel, f"Nivel {nivel}")


def _construir_dim_baremo(fact: pd.DataFrame) -> pd.DataFrame:
    """
    Construye tabla de baremos combinando:
    - transformacion_max: derivado de los datos reales (suma max_item_score por persona)
      para dimensiones y dominios, ya que categorias_analisis puede tener más ítems
      que los listados en el Marco.
    - cortes (sin_riesgo, bajo, medio, alto): hardcodeados desde el Marco.
    - Factores (IntraA/B, Extra, IntraExtra): max hardcodeado del Marco.
    """
    _DEFECTO_CORTES = (20.0, 40.0, 60.0, 80.0, "riesgo")
    rows = []

    # ── DIMENSIONES: max desde datos, cortes desde Marco ─────────────────────
    # Excluir Estrés: no tiene sub-dimensiones; se calcula como factor con función propia
    if "max_item_score" in fact.columns and "dimension" in fact.columns:
        dim_max = (
            fact[fact["dimension"].notna() & (fact["instrumento"] != "Estres")]
            .groupby(["cedula", "instrumento", "forma_intra", "dimension"])["max_item_score"]
            .sum()
            .reset_index(name="max_pp")
            .groupby(["instrumento", "forma_intra", "dimension"])["max_pp"]
            .max()
            .reset_index(name="transformacion_max")
        )
        sin_cortes = 0
        for _, r in dim_max.iterrows():
            key = (r["instrumento"], r["forma_intra"], r["dimension"])
            cortes = CORTES_DIMENSION.get(key)
            if cortes is None:
                sin_cortes += 1
                c_sr, c_b, c_m, c_a, tipo = _DEFECTO_CORTES
            else:
                c_sr, c_b, c_m, c_a, tipo = cortes
            rows.append({
                "instrumento": r["instrumento"], "forma_intra": r["forma_intra"],
                "nivel_analisis": "dimension", "nombre_nivel": r["dimension"],
                "transformacion_max": r["transformacion_max"],
                "corte_sin_riesgo": c_sr, "corte_bajo": c_b,
                "corte_medio": c_m, "corte_alto": c_a,
                "tipo_baremo": tipo,
            })
        if sin_cortes:
            log.warning("Dimensiones sin cortes en Marco (usan placeholder): %d", sin_cortes)

    # ── DOMINIOS: max desde datos, cortes desde Marco ─────────────────────────
    if "max_item_score" in fact.columns and "dominio" in fact.columns:
        dom_max = (
            fact[fact["dominio"].notna()]
            .groupby(["cedula", "instrumento", "forma_intra", "dominio"])["max_item_score"]
            .sum()
            .reset_index(name="max_pp")
            .groupby(["instrumento", "forma_intra", "dominio"])["max_pp"]
            .max()
            .reset_index(name="transformacion_max")
        )
        for _, r in dom_max.iterrows():
            key = (r["instrumento"], r["forma_intra"], r["dominio"])
            cortes = CORTES_DOMINIO.get(key)
            if cortes is None:
                continue  # dominios sin baremo Marco (Extralaboral, Estrés) se omiten
            c_sr, c_b, c_m, c_a, tipo = cortes
            rows.append({
                "instrumento": r["instrumento"], "forma_intra": r["forma_intra"],
                "nivel_analisis": "dominio", "nombre_nivel": r["dominio"],
                "transformacion_max": r["transformacion_max"],
                "corte_sin_riesgo": c_sr, "corte_bajo": c_b,
                "corte_medio": c_m, "corte_alto": c_a,
                "tipo_baremo": tipo,
            })

    # ── FACTORES: max hardcodeado del Marco (bien definidos) ──────────────────
    for (inst, forma), (tmax, c_sr, c_b, c_m, c_a, tipo) in CORTES_FACTOR_FORMA.items():
        if inst not in ("Estres", "Individual"):
            rows.append({
                "instrumento": inst, "forma_intra": forma,
                "nivel_analisis": "factor", "nombre_nivel": inst,
                "transformacion_max": tmax,
                "corte_sin_riesgo": c_sr, "corte_bajo": c_b,
                "corte_medio": c_m, "corte_alto": c_a,
                "tipo_baremo": tipo,
            })

    df = pd.DataFrame(rows)
    log.info("dim_baremo construida (max desde datos, cortes desde Marco): %d filas", len(df))
    return df


# ══════════════════════════════════════════════════════════════════════════════
# Agregación genérica
# ══════════════════════════════════════════════════════════════════════════════

def agregar_por_nivel(
    fact: pd.DataFrame,
    nivel_analisis: str,
    nombre_col: str,
) -> pd.DataFrame:
    """
    Agrupa fact_scores_brutos y calcula puntaje_bruto por nivel (suma simple).
    nivel_analisis : 'dimension' | 'dominio' | 'factor'
    nombre_col     : 'dimension' | 'dominio' | 'instrumento'
    """
    id_cols = ["cedula", "empresa", "forma_intra", "sector_rag", "instrumento"]
    group_cols = id_cols + ([nombre_col] if nombre_col != "instrumento" else [])

    agg = (
        fact[fact[nombre_col].notna()]
        .groupby(group_cols, dropna=False)["valor_invertido"]
        .sum()
        .reset_index(name="puntaje_bruto")
    )
    agg["nivel_analisis"] = nivel_analisis
    agg["nombre_nivel"] = agg[nombre_col] if nombre_col != "instrumento" else agg["instrumento"]
    return agg


# ══════════════════════════════════════════════════════════════════════════════
# Funciones de cálculo especial
# ══════════════════════════════════════════════════════════════════════════════

def calcular_dominio_afrontamiento_ponderado(fact: pd.DataFrame) -> pd.DataFrame:
    """
    PASO 13 — Dominio Estrategias de Afrontamiento: media ponderada.
    Fórmula Marco:
      ((planificacion × 0.25) + (busqueda_soporte × 0.25) + (negacion × 0.50)) / 4 × 100
    transformacion_max = 4, cortes proteccion: 29/51/69/89
    """
    PESOS = {
        "Afrontamiento activo_planificación":   0.25,
        "Afrontamiento activo_busquedasoporte": 0.25,
        "Afrontamiento evitativo_negación":     0.50,
    }
    TMAX, C_SR, C_B, C_M, C_A, TIPO = 4.0, 29.0, 51.0, 69.0, 89.0, "proteccion"

    id_cols = ["cedula", "empresa", "forma_intra", "sector_rag", "instrumento"]

    mask = (
        (fact["instrumento"] == "Afrontamiento") &
        fact["dimension"].isin(PESOS.keys())
    )
    afront = fact[mask].copy()
    if afront.empty:
        log.warning("Dominio Afrontamiento ponderado: sin datos.")
        return pd.DataFrame()

    sub = (
        afront.groupby(id_cols + ["dimension"], dropna=False)["valor_invertido"]
        .sum()
        .reset_index(name="suma_sub")
    )
    sub["peso"] = sub["dimension"].map(PESOS)
    sub["suma_ponderada"] = sub["suma_sub"] * sub["peso"]

    dom = (
        sub.groupby(id_cols, dropna=False)["suma_ponderada"]
        .sum()
        .reset_index(name="puntaje_bruto")
    )
    dom["nivel_analisis"] = "dominio"
    dom["nombre_nivel"] = "Estrategias de Afrontamiento"
    dom["tipo_baremo"] = TIPO
    dom["transformacion_max"] = TMAX
    dom["puntaje_transformado"] = (dom["puntaje_bruto"] / TMAX * 100).round(1)
    dom["nivel_riesgo"] = dom["puntaje_transformado"].apply(
        lambda pt: clasificar_nivel(pt, C_SR, C_B, C_M, C_A)
    )
    dom["etiqueta_nivel"] = dom.apply(
        lambda r: etiqueta_nivel(r["nivel_riesgo"], TIPO), axis=1
    )
    log.info("Dominio Afrontamiento ponderado: %d filas", len(dom))
    return dom


def calcular_estres_ponderado(fact: pd.DataFrame) -> pd.DataFrame:
    """
    PASO 14.1 — Factor Estrés: 4 promedios ponderados.
    PP1 = promedio(items 1-8)  × 4
    PP2 = promedio(items 9-12) × 3
    PP3 = promedio(items 13-22)× 2
    PP4 = promedio(items 23-31)× 1
    puntaje_bruto = PP1 + PP2 + PP3 + PP4  (max teórico: 61.16)
    Cortes: A → 7.8/12.6/14.7/25.0 | B → 6.5/11.8/17.0/23.4
    """
    id_cols = ["cedula", "empresa", "forma_intra", "sector_rag", "instrumento"]

    estres = fact[fact["instrumento"] == "Estres"].copy()
    if estres.empty:
        log.warning("calcular_estres_ponderado: sin datos Estres.")
        return pd.DataFrame()

    # Número de ítem desde id_pregunta (ej. "7_estres" → 7)
    estres["item_num"] = estres["id_pregunta"].str.split("_").str[0].astype(int)

    # Asignar grupo y peso
    def _grupo_peso(n: int) -> tuple:
        for items, peso in ESTRES_GRUPOS:
            if n in items:
                return peso
        return None

    estres["peso_pp"] = estres["item_num"].apply(_grupo_peso)

    # Calcular promedio por (worker, grupo/peso) × peso
    sub = (
        estres.groupby(id_cols + ["peso_pp"], dropna=False)
        .agg(suma_grupo=("valor_invertido", "sum"), n_items=("valor_invertido", "count"))
        .reset_index()
    )
    sub["pp"] = (sub["suma_grupo"] / sub["n_items"]) * sub["peso_pp"]

    # Sumar las 4 PP por trabajador
    puntaje = (
        sub.groupby(id_cols, dropna=False)["pp"]
        .sum()
        .reset_index(name="puntaje_bruto")
    )

    puntaje["nivel_analisis"] = "factor"
    puntaje["nombre_nivel"] = "Estres"

    def _aplicar_baremo_estres(row):
        clave = ("Estres", row["forma_intra"])
        if clave not in CORTES_FACTOR_FORMA:
            return pd.Series({
                "transformacion_max": None, "puntaje_transformado": None,
                "nivel_riesgo": 0, "tipo_baremo": "riesgo",
                "etiqueta_nivel": "Sin datos",
            })
        tmax, c_sr, c_b, c_m, c_a, tipo = CORTES_FACTOR_FORMA[clave]
        pt = round(row["puntaje_bruto"] / tmax * 100, 1)
        nivel = clasificar_nivel(pt, c_sr, c_b, c_m, c_a)
        return pd.Series({
            "transformacion_max": tmax,
            "puntaje_transformado": pt,
            "nivel_riesgo": nivel,
            "tipo_baremo": tipo,
            "etiqueta_nivel": etiqueta_nivel(nivel, tipo),
        })

    extras = puntaje.apply(_aplicar_baremo_estres, axis=1)
    resultado = pd.concat([puntaje, extras], axis=1)
    log.info("Factor Estrés ponderado: %d filas", len(resultado))
    return resultado


def calcular_factor_individual(fact: pd.DataFrame) -> pd.DataFrame:
    """
    PASO 15 — Factor Individual: suma Afrontamiento + CapPsico (max=24).
    Cortes protección: 29/51/69/89 (mismo A y B).
    """
    id_cols = ["cedula", "empresa", "forma_intra", "sector_rag"]

    ind = fact[fact["instrumento"].isin(["Afrontamiento", "CapPsico"])].copy()
    if ind.empty:
        log.warning("calcular_factor_individual: sin datos Individual.")
        return pd.DataFrame()

    puntaje = (
        ind.groupby(id_cols, dropna=False)["valor_invertido"]
        .sum()
        .reset_index(name="puntaje_bruto")
    )
    puntaje["instrumento"] = "Individual"
    puntaje["sector_rag"] = puntaje.get("sector_rag", None)

    puntaje["nivel_analisis"] = "factor"
    puntaje["nombre_nivel"] = "Individual"

    def _aplicar_baremo_ind(row):
        clave = ("Individual", row["forma_intra"])
        tmax, c_sr, c_b, c_m, c_a, tipo = CORTES_FACTOR_FORMA[clave]
        pt = round(row["puntaje_bruto"] / tmax * 100, 1)
        nivel = clasificar_nivel(pt, c_sr, c_b, c_m, c_a)
        return pd.Series({
            "transformacion_max": tmax,
            "puntaje_transformado": pt,
            "nivel_riesgo": nivel,
            "tipo_baremo": tipo,
            "etiqueta_nivel": etiqueta_nivel(nivel, tipo),
        })

    extras = puntaje.apply(_aplicar_baremo_ind, axis=1)
    resultado = pd.concat([puntaje, extras], axis=1)
    log.info("Factor Individual: %d filas", len(resultado))
    return resultado


def calcular_factor_intra_extra(fact: pd.DataFrame) -> pd.DataFrame:
    """
    PASO 14 — Factor IntraA+Extralaboral / IntraB+Extralaboral.
    Forma A: suma IntraA + Extralaboral → max 616
    Forma B: suma IntraB + Extralaboral → max 512
    """
    id_cols = ["cedula", "empresa", "forma_intra", "sector_rag"]
    rows_out = []

    for forma, inst_intra, nombre in [
        ("A", "IntraA", "IntraA+Extralaboral"),
        ("B", "IntraB", "IntraB+Extralaboral"),
    ]:
        sub = fact[
            (fact["forma_intra"] == forma) &
            (fact["instrumento"].isin([inst_intra, "Extralaboral"]))
        ]
        if sub.empty:
            continue
        agg = (
            sub.groupby(id_cols, dropna=False)["valor_invertido"]
            .sum()
            .reset_index(name="puntaje_bruto")
        )
        agg["instrumento"] = nombre
        agg["nivel_analisis"] = "factor"
        agg["nombre_nivel"] = nombre

        clave = (nombre, forma)
        tmax, c_sr, c_b, c_m, c_a, tipo = CORTES_FACTOR_FORMA[clave]
        agg["transformacion_max"] = tmax
        agg["puntaje_transformado"] = (agg["puntaje_bruto"] / tmax * 100).round(1)
        agg["nivel_riesgo"] = agg["puntaje_transformado"].apply(
            lambda pt: clasificar_nivel(pt, c_sr, c_b, c_m, c_a)
        )
        agg["tipo_baremo"] = tipo
        agg["etiqueta_nivel"] = agg.apply(
            lambda r: etiqueta_nivel(r["nivel_riesgo"], tipo), axis=1
        )
        rows_out.append(agg)
        log.info("Factor %s: %d filas", nombre, len(agg))

    return pd.concat(rows_out, ignore_index=True) if rows_out else pd.DataFrame()


# ══════════════════════════════════════════════════════════════════════════════
# Aplicar baremo a DataFrame agregado
# ══════════════════════════════════════════════════════════════════════════════

def aplicar_baremo_a_df(
    agg: pd.DataFrame,
    dim_baremo: pd.DataFrame,
    nivel_analisis: str,
) -> pd.DataFrame:
    """
    JOIN con dim_baremo y calcula puntaje_transformado + nivel_riesgo + etiqueta_nivel.
    """
    baremo_nivel = dim_baremo[dim_baremo["nivel_analisis"] == nivel_analisis].copy()

    if "nombre_nivel" not in baremo_nivel.columns and "dimension" in baremo_nivel.columns:
        baremo_nivel = baremo_nivel.rename(columns={"dimension": "nombre_nivel"})

    join_cols = (
        ["instrumento", "forma_intra", "nombre_nivel"]
        if nivel_analisis != "factor"
        else ["instrumento", "forma_intra"]
    )

    df = agg.merge(
        baremo_nivel[join_cols + [
            "transformacion_max", "corte_sin_riesgo", "corte_bajo",
            "corte_medio", "corte_alto", "tipo_baremo",
        ]].drop_duplicates(join_cols),
        on=join_cols,
        how="left",
    )

    sin_baremo = df["transformacion_max"].isna().sum()
    if sin_baremo > 0:
        faltantes = df[df["transformacion_max"].isna()][
            ["instrumento", "forma_intra", "nombre_nivel"]
        ].drop_duplicates()
        log.warning(
            "[%s] %d combinaciones sin baremo:\n%s",
            nivel_analisis, sin_baremo, faltantes.to_string(index=False),
        )

    df["puntaje_transformado"] = (
        df["puntaje_bruto"] / df["transformacion_max"] * 100
    ).round(1)

    df["nivel_riesgo"] = df.apply(
        lambda r: clasificar_nivel(
            r["puntaje_transformado"],
            r.get("corte_sin_riesgo", 0.0),
            r.get("corte_bajo", 25.0),
            r.get("corte_medio", 50.0),
            r.get("corte_alto", 75.0),
        ),
        axis=1,
    )

    df["etiqueta_nivel"] = df.apply(
        lambda r: etiqueta_nivel(r["nivel_riesgo"], r.get("tipo_baremo", "riesgo")),
        axis=1,
    )

    return df


# ══════════════════════════════════════════════════════════════════════════════
# Validación (R14)
# ══════════════════════════════════════════════════════════════════════════════

def validar_scores_baremo(df: pd.DataFrame) -> tuple[bool, pd.DataFrame]:
    """Valida integridad de fact_scores_baremo."""
    errores = []

    dup = df.duplicated(
        subset=["cedula", "instrumento", "forma_intra", "nivel_analisis", "nombre_nivel"]
    ).sum()
    if dup > 0:
        errores.append({"check": "pk_duplicada", "n": int(dup)})

    for col in ["cedula", "forma_intra", "nivel_analisis", "nombre_nivel",
                "puntaje_bruto", "nivel_riesgo"]:
        n = df[col].isna().sum()
        if n > 0:
            errores.append({"check": f"nulos_{col}", "n": int(n)})

    fuera = df[(df["nivel_riesgo"] < 0) | (df["nivel_riesgo"] > 5)].shape[0]
    if fuera > 0:
        errores.append({"check": "nivel_riesgo_fuera_de_rango", "n": fuera})

    n_rango = df[
        df["puntaje_transformado"].notna() &
        ((df["puntaje_transformado"] < 0) | (df["puntaje_transformado"] > 100))
    ].shape[0]
    if n_rango > 0:
        errores.append({"check": "pt_fuera_0_100", "n": n_rango})

    reporte = pd.DataFrame(errores) if errores else pd.DataFrame(columns=["check", "n"])
    return len(errores) == 0, reporte


# ══════════════════════════════════════════════════════════════════════════════
# Main
# ══════════════════════════════════════════════════════════════════════════════

def main():
    log.info("=" * 60)
    log.info("SCRIPT 02b — Baremos (Pasos 9-15)")
    log.info("=" * 60)

    cfg = cargar_config()
    proc = ROOT / cfg["paths"]["processed"]

    fact = pd.read_parquet(proc / "fact_scores_brutos.parquet")
    log.info("fact_scores_brutos: %d filas", len(fact))

    for col in ["empresa", "sector_rag"]:
        if col not in fact.columns:
            fact[col] = None

    # Tabla de baremos: max desde datos, cortes desde Marco
    dim_baremo = _construir_dim_baremo(fact)

    partes = []

    # ── Pasos 9-11: Dimensiones (intra A/B, extra A/B, individual A/B) ────────
    # Excluir Estrés (no tiene sub-dimensiones — mismo valor que factor)
    log.info("Pasos 9-11 — Dimensiones (sin Estrés)...")
    fact_dim = fact[fact["instrumento"] != "Estres"]
    dim_df = agregar_por_nivel(fact_dim, "dimension", "dimension")
    dim_df = aplicar_baremo_a_df(dim_df, dim_baremo, "dimension")
    partes.append(dim_df)
    log.info("  %d filas de dimensiones", len(dim_df))

    # ── Paso 12: Dominios IntraA/B + CapPsico (suma simple) ──────────────────
    # Excluir Afrontamiento (dominio ponderado especial) y Estrés (sin dominio)
    # Extralaboral no tiene sub-dominios en Marco (solo dimensiones directo)
    log.info("Paso 12 — Dominios IntraA/B + CapPsico (sin Afrontamiento, Estrés, Extralaboral)...")
    fact_dom = fact[~fact["instrumento"].isin(["Afrontamiento", "Estres", "Extralaboral"])]
    dom_df = agregar_por_nivel(fact_dom, "dominio", "dominio")
    dom_df = aplicar_baremo_a_df(dom_df, dim_baremo, "dominio")
    partes.append(dom_df)
    log.info("  %d filas de dominios", len(dom_df))

    # ── Paso 13: Dominio Afrontamiento (media ponderada) ─────────────────────
    log.info("Paso 13 — Dominio Afrontamiento ponderado (0.25/0.25/0.50)...")
    dom_afront = calcular_dominio_afrontamiento_ponderado(fact)
    if not dom_afront.empty:
        partes.append(dom_afront)

    # ── Paso 14: Factores IntraA, IntraB, Extralaboral (suma simple) ──────────
    log.info("Paso 14 — Factores IntraA/B + Extralaboral (suma simple)...")
    fact_fac = fact[fact["instrumento"].isin(["IntraA", "IntraB", "Extralaboral"])]
    fac_df = agregar_por_nivel(fact_fac, "factor", "instrumento")
    fac_df = aplicar_baremo_a_df(fac_df, dim_baremo, "factor")
    partes.append(fac_df)
    log.info("  %d filas de factores intra/extra", len(fac_df))

    # ── Paso 14: IntraA+Extralaboral / IntraB+Extralaboral combinados ─────────
    log.info("Paso 14 — Factores IntraA+Extra / IntraB+Extra...")
    fac_intra_extra = calcular_factor_intra_extra(fact)
    if not fac_intra_extra.empty:
        partes.append(fac_intra_extra)

    # ── Paso 14.1: Factor Estrés (4 promedios ponderados) ─────────────────────
    log.info("Paso 14.1 — Factor Estrés ponderado...")
    fac_estres = calcular_estres_ponderado(fact)
    if not fac_estres.empty:
        partes.append(fac_estres)

    # ── Paso 15: Factor Individual (Afrontamiento + CapPsico combinado) ───────
    log.info("Paso 15 — Factor Individual (afront + cappsico, max=24)...")
    fac_ind = calcular_factor_individual(fact)
    if not fac_ind.empty:
        partes.append(fac_ind)

    # ── Consolidar ────────────────────────────────────────────────────────────
    cols_output = [
        "cedula", "empresa", "forma_intra", "sector_rag",
        "instrumento", "nivel_analisis", "nombre_nivel",
        "puntaje_bruto", "transformacion_max", "puntaje_transformado",
        "nivel_riesgo", "etiqueta_nivel", "tipo_baremo",
    ]
    fact_baremo = pd.concat(partes, ignore_index=True)
    fact_baremo_out = fact_baremo[[c for c in cols_output if c in fact_baremo.columns]]

    # ── Validación ────────────────────────────────────────────────────────────
    es_valido, reporte = validar_scores_baremo(fact_baremo_out)
    if not es_valido:
        log.error("Validación FALLIDA:\n%s", reporte.to_string())
        sys.exit(1)
    log.info("Validación OK")

    # ── Resumen distribución (nivel factor) ───────────────────────────────────
    resumen = (
        fact_baremo_out[fact_baremo_out["nivel_analisis"] == "factor"]
        .groupby(["instrumento", "etiqueta_nivel"])["cedula"]
        .count()
        .reset_index(name="n")
    )
    log.info("Distribucion nivel factor:\n%s", resumen.to_string(index=False))

    # ── Guardar ───────────────────────────────────────────────────────────────
    out = proc / "fact_scores_baremo.parquet"
    fact_baremo_out.to_parquet(out, index=False)
    log.info("Guardado: %s (%d filas x %d columnas)", out, *fact_baremo_out.shape)
    log.info("=" * 60)


if __name__ == "__main__":
    main()
