"""
07_frecuencias_preguntas.py
===========================
Paso 20 del pipeline V1 — Distribución de frecuencias por pregunta + Top 20 comparables Colombia.

Proceso:
  A) Frecuencias generales: para cada id_pregunta × empresa × forma_intra
     calcular % de personas que eligieron cada opción de respuesta.

  B) Comparables Colombia (39 preguntas):
     - Discriminadas por forma: trabajadores A usan id_pregunta_A, trabajadores B usan id_pregunta_B
     - Fórmula alta presencia:
         intra/extra/estres → siempre + casi siempre
         afrontamiento      → frecuentemente hago eso + siempre hago eso
     - diferencia_pp = pct_empresa - pct_pais_encst
     - Top 20: las 20 primeras con diferencia_pp > 0, ordenadas desc por empresa

Output: data/processed/fact_frecuencias.parquet
        data/processed/fact_top20_comparables.parquet

Reglas:
  R8  — Grupos < 5 → pct marcado como None (confidencialidad)
  R13 — Output Parquet
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
# Tabla de 39 preguntas comparables con Colombia
# Fuente: Documento marco, Paso 20 — Fuente ENCST II y III (2013-2021)
#
# Estructura: (id_pregunta_A, id_pregunta_B, dimension, pct_pais, tipo_formula)
#   tipo_formula:
#     'likert'        → siempre + casi siempre
#     'afrontamiento' → frecuentemente hago eso + siempre hago eso
# ══════════════════════════════════════════════════════════════════════════════
PREGUNTAS_COMPARABLES: list[dict] = [
    # id_preg_A             id_preg_B            dimension                                   pct_pais  formula
    {"A": "11_afrontamiento", "B": "11_afrontamiento", "dim": "Autoeficacia",                              "pct": 95.2, "formula": "afrontamiento"},
    {"A": "61_intra",         "B": "47_intra",         "dim": "Capacitación",                             "pct": 87.9, "formula": "likert"},
    {"A": "65_intra",         "B": "50_intra",         "dim": "Características del liderazgo",            "pct": 54.4, "formula": "likert"},
    {"A": "75_intra",         "B": "61_intra",         "dim": "Características del liderazgo",            "pct": 94.5, "formula": "likert"},
    {"A": "58_intra",         "B": "58_intra",         "dim": "Características del liderazgo",            "pct": 14.6, "formula": "likert"},
    {"A": "53_intra",         "B": "41_intra",         "dim": "Claridad de rol",                          "pct": 95.8, "formula": "likert"},
    {"A": "59_intra",         "B": "45_intra",         "dim": "Claridad de rol",                          "pct": 21.8, "formula": "likert"},
    {"A": "55_intra",         "B": "43_intra",         "dim": "Claridad de rol",                          "pct": 19.1, "formula": "likert"},
    {"A": "46_intra",         "B": "36_intra",         "dim": "Control y autonomía sobre el trabajo",     "pct": 81.9, "formula": "likert"},
    {"A": "45_intra",         "B": "35_intra",         "dim": "Control y autonomía sobre el trabajo",     "pct": 82.3, "formula": "likert"},
    {"A": "13_intra",         "B": "13_intra",         "dim": "Demandas cuantitativas",                   "pct": 33.3, "formula": "likert"},
    {"A": "14_intra",         "B": "14_intra",         "dim": "Demandas cuantitativas",                   "pct": 16.2, "formula": "likert"},
    {"A": "15_intra",         "B": "15_intra",         "dim": "Demandas cuantitativas",                   "pct": 62.5, "formula": "likert"},
    {"A": "13_intra",         "B": "13_intra",         "dim": "Demandas cuantitativas",                   "pct": 51.5, "formula": "likert"},  # 2ª ref ENCST
    {"A": "17_intra",         "B": "17_intra",         "dim": "Demandas de carga mental",                 "pct": 72.5, "formula": "likert"},
    {"A": "20_intra",         "B": None,               "dim": "Demandas de carga mental",                 "pct": 53.4, "formula": "likert"},
    {"A": "21_intra",         "B": "20_intra",         "dim": "Demandas de carga mental",                 "pct": 37.3, "formula": "likert"},
    {"A": "113_intra",        "B": None,               "dim": "Demandas emocionales",                     "pct": 34.4, "formula": "likert"},
    {"A": "6_extra",          "B": "6_extra",          "dim": "Características de la vivienda y de su entorno", "pct": 17.8, "formula": "likert"},
    {"A": "17_extra",         "B": "17_extra",         "dim": "Balance entre la vida laboral y familiar", "pct": 90.1, "formula": "likert"},
    {"A": "3_extra",          "B": "3_extra",          "dim": "Desplazamiento vivienda trabajo vivienda",  "pct": 46.9, "formula": "likert"},
    {"A": "40_intra",         "B": "31_intra",         "dim": "Oportunidades para el uso y desarrollo de habilidades y conocimientos", "pct": 91.7, "formula": "likert"},
    {"A": "41_intra",         "B": "32_intra",         "dim": "Oportunidades para el uso y desarrollo de habilidades y conocimientos", "pct": 87.5, "formula": "likert"},
    {"A": None,               "B": "29_intra",         "dim": "Oportunidades para el uso y desarrollo de habilidades y conocimientos", "pct": 66.8, "formula": "likert"},
    {"A": "2_afrontamiento",  "B": "2_afrontamiento",  "dim": "Optimismo",                                "pct": 95.5, "formula": "afrontamiento"},
    {"A": "103_intra",        "B": "86_intra",         "dim": "Recompensas derivadas de la pertenencia a la organización y del trabajo que se realiza", "pct": 94.7, "formula": "likert"},
    {"A": "87_intra",         "B": "71_intra",         "dim": "Relaciones sociales en el trabajo",        "pct": 96.2, "formula": "likert"},
    {"A": "88_intra",         "B": "72_intra",         "dim": "Relaciones sociales en el trabajo",        "pct": 96.6, "formula": "likert"},
    {"A": "80_intra",         "B": "66_intra",         "dim": "Relaciones sociales en el trabajo",        "pct":  4.9, "formula": "likert"},
    {"A": "118_intra",        "B": None,               "dim": "Relación con los colaboradores (subordinados)", "pct": 5.1, "formula": "likert"},
    {"A": "8_afrontamiento",  "B": "8_afrontamiento",  "dim": "Resiliencia",                              "pct": 96.9, "formula": "afrontamiento"},
    {"A": "21_estres",        "B": "21_estres",        "dim": "Respuestas cognitivas de estrés",          "pct":  2.9, "formula": "likert"},
    {"A": "13_estres",        "B": "13_estres",        "dim": "Respuestas comportamentales de estrés",    "pct": 39.4, "formula": "likert"},
    {"A": "25_estres",        "B": "25_estres",        "dim": "Respuestas emocionales de estrés",         "pct": 40.2, "formula": "likert"},
    {"A": "12_estres",        "B": "12_estres",        "dim": "Respuestas emocionales de estrés",         "pct": 33.9, "formula": "likert"},
    {"A": "31_estres",        "B": "31_estres",        "dim": "Respuestas emocionales de estrés",         "pct":  8.6, "formula": "likert"},
    {"A": "27_estres",        "B": "27_estres",        "dim": "Respuestas emocionales de estrés",         "pct":  2.7, "formula": "likert"},
    {"A": "5_estres",         "B": "5_estres",         "dim": "Respuestas físicas de estrés",             "pct": 30.1, "formula": "likert"},
    {"A": "1_estres",         "B": "1_estres",         "dim": "Respuestas físicas de estrés",             "pct": 31.8, "formula": "likert"},
]

# Respuestas que cuentan como "alta presencia" por tipo de fórmula
ALTA_PRESENCIA_LIKERT = {"siempre", "casi siempre"}
ALTA_PRESENCIA_AFRONTAMIENTO = {"frecuentemente hago eso", "siempre hago eso"}


def cargar_config() -> dict:
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


# ══════════════════════════════════════════════════════════════════════════════
# Parte A — Frecuencias generales
# ══════════════════════════════════════════════════════════════════════════════

def calcular_frecuencias_generales(
    fact: pd.DataFrame, n_min: int
) -> pd.DataFrame:
    """
    Para cada empresa × id_pregunta × forma_intra × opcion_respuesta:
    calcula n_personas y pct_empresa.
    Aplica R8: pct=None si n_total_pregunta < n_min.
    """
    group_cols = ["empresa", "forma_intra", "id_pregunta", "id_respuesta"]

    freq = (
        fact.groupby(group_cols, dropna=False)["cedula"]
        .nunique()
        .reset_index(name="n_personas")
    )

    # Total por empresa × id_pregunta × forma
    total = (
        fact.groupby(["empresa", "forma_intra", "id_pregunta"], dropna=False)["cedula"]
        .nunique()
        .reset_index(name="n_total")
    )
    freq = freq.merge(total, on=["empresa", "forma_intra", "id_pregunta"], how="left")
    freq["pct_empresa"] = (freq["n_personas"] / freq["n_total"] * 100).round(2)

    # R8 confidencialidad
    freq.loc[freq["n_total"] < n_min, "pct_empresa"] = None

    freq = freq.rename(columns={"id_respuesta": "opcion_respuesta"})
    log.info("Frecuencias generales: %d filas", len(freq))
    return freq


# ══════════════════════════════════════════════════════════════════════════════
# Parte B — Comparables Colombia (39 preguntas, discriminadas por forma)
# ══════════════════════════════════════════════════════════════════════════════

def calcular_alta_presencia(
    fact: pd.DataFrame,
    id_pregunta: str,
    forma_intra: str,
    formula: str,
) -> pd.Series:
    """
    Para una pregunta y forma dadas, identifica las filas con respuesta de alta presencia.
    Retorna una Serie booleana indexada igual que fact.
    """
    mask_preg = (fact["id_pregunta"] == id_pregunta) & (fact["forma_intra"] == forma_intra)
    resp_lower = fact["id_respuesta"].str.lower().str.strip()
    if formula == "afrontamiento":
        mask_resp = resp_lower.isin(ALTA_PRESENCIA_AFRONTAMIENTO)
    else:
        mask_resp = resp_lower.isin(ALTA_PRESENCIA_LIKERT)
    return mask_preg & mask_resp


def calcular_comparables_colombia(
    fact: pd.DataFrame,
    n_min: int,
) -> pd.DataFrame:
    """
    Calcula % alta presencia empresa vs % ENCST para cada una de las 39 preguntas,
    discriminado por forma (A o B).
    El ítem 13 duplicado genera 2 filas independientes.
    """
    resultados = []
    empresas = fact["empresa"].unique()

    for ref_idx, ref in enumerate(PREGUNTAS_COMPARABLES):
        pct_pais = ref["pct"]
        formula = ref["formula"]
        dim = ref["dim"]

        for forma, id_preg_key in [("A", "A"), ("B", "B")]:
            id_preg = ref.get(id_preg_key)
            if not id_preg:
                continue  # n/a — esta forma no aplica para esta pregunta

            fact_forma = fact[fact["forma_intra"] == forma]
            if fact_forma.empty:
                continue

            for empresa in empresas:
                fact_emp = fact_forma[fact_forma["empresa"] == empresa]
                if fact_emp.empty:
                    continue

                # Total trabajadores con esa pregunta en esa empresa/forma
                total_mask = fact_emp["id_pregunta"] == id_preg
                n_total = fact_emp.loc[total_mask, "cedula"].nunique()
                if n_total == 0:
                    continue

                # Alta presencia
                alta_mask = calcular_alta_presencia(fact_emp, id_preg, forma, formula)
                n_alta = fact_emp.loc[alta_mask, "cedula"].nunique()

                pct_empresa = round(n_alta / n_total * 100, 2) if n_total >= n_min else None
                diferencia_pp = round(pct_empresa - pct_pais, 2) if pct_empresa is not None else None

                resultados.append({
                    "empresa":        empresa,
                    "forma_intra":    forma,
                    "id_pregunta":    id_preg,
                    "ref_idx":        ref_idx,          # índice para identificar filas duplicadas (13_intra)
                    "dimension":      dim,
                    "n_total":        n_total,
                    "n_alta_presencia": n_alta,
                    "pct_empresa":    pct_empresa,
                    "pct_pais_encst": pct_pais,
                    "diferencia_pp":  diferencia_pp,
                    "formula":        formula,
                })

    df = pd.DataFrame(resultados)
    log.info("Comparables Colombia — %d filas calculadas", len(df))
    return df


def calcular_top20(df_comparables: pd.DataFrame) -> pd.DataFrame:
    """
    Top 20 preguntas con mayor diferencia_pp positiva (empresa > Colombia) por empresa y forma.
    """
    top = (
        df_comparables[
            df_comparables["diferencia_pp"].notna() &
            (df_comparables["diferencia_pp"] > 0)
        ]
        .sort_values(["empresa", "forma_intra", "diferencia_pp"], ascending=[True, True, False])
        .groupby(["empresa", "forma_intra"], group_keys=False)
        .head(20)
        .copy()
    )
    top["top20_flag"] = True
    log.info("Top 20 — %d filas", len(top))
    return top


# ══════════════════════════════════════════════════════════════════════════════
# Main
# ══════════════════════════════════════════════════════════════════════════════

def main():
    log.info("=" * 60)
    log.info("SCRIPT 07 — Frecuencias + Comparables Colombia (Paso 20)")
    log.info("=" * 60)

    cfg = cargar_config()
    proc = ROOT / cfg["paths"]["processed"]
    n_min = cfg.get("confidencialidad_n_min", 5)

    fact = pd.read_parquet(proc / "fact_respuestas_clean.parquet")
    log.info("fact_respuestas_clean: %d filas", len(fact))

    # ── Parte A: frecuencias generales ──────────────────────────────────────
    log.info("Parte A — Frecuencias generales...")
    freq_general = calcular_frecuencias_generales(fact, n_min)
    out_freq = proc / "fact_frecuencias.parquet"
    freq_general.to_parquet(out_freq, index=False)
    log.info("Guardado: %s (%d filas)", out_freq, len(freq_general))

    # ── Parte B: comparables Colombia ────────────────────────────────────────
    log.info("Parte B — Comparables Colombia (39 preguntas)...")
    comparables = calcular_comparables_colombia(fact, n_min)

    top20 = calcular_top20(comparables)
    comparables["top20_flag"] = comparables.index.isin(top20.index)

    out_comp = proc / "fact_top20_comparables.parquet"
    comparables.to_parquet(out_comp, index=False)
    log.info("Guardado: %s (%d filas × %d columnas)", out_comp, *comparables.shape)

    # Resumen
    resumen = (
        top20.groupby(["empresa", "forma_intra"])["id_pregunta"]
        .count()
        .reset_index(name="n_top20")
    )
    log.info("Preguntas en Top 20 por empresa:\n%s", resumen.to_string(index=False))

    log.info("=" * 60)
    log.info("Paso 20 completado → fact_frecuencias + fact_top20_comparables")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
