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
PASO 15  Baremos Avantum   — Factor individual A y B

Proceso estándar para cada nivel:
  1. Suma de valor_invertido por (cedula, forma_intra, nivel, nombre) → puntaje_bruto
  2. Puntaje transformado = round(puntaje_bruto / transformacion_max * 100, 2)
  3. Clasificar en 5 niveles usando cortes de dim_baremo

Tipo baremo:
  'riesgo'     → Niveles 1-5: Sin riesgo | Bajo | Medio | Alto | Muy alto
  'proteccion' → Niveles 1-5: Muy bajo | Bajo | Medio | Alto | Muy alto

Output: data/processed/fact_scores_baremo.parquet

Reglas:
  R2  — Baremos diferenciados A (max 492) / B (max 388) para factor total intra
  R8  — Grupos < 5 trabajadores → tratados en dashboards (no aquí)
  R13 — Output Parquet
  R14 — Validación obligatoria

Nota Paso 13 — Dominio Afrontamiento: fórmula PONDERADA (no suma simple):
  score = ((activo_planificacion × 0.25) + (busqueda_soporte × 0.25) + (pasivo_negacion × 0.50)) / 4 × 100
  transformacion_max = 4, cortes protección: 29 | 51 | 69 | 89

Nota Paso 16 — Riesgo total empresa: se calcula en 06_benchmarking.py (nivel empresa).
  Este script genera solo puntajes INDIVIDUALES por trabajador.
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
# Mapeo instrumento → tipo_baremo por defecto (si dim_baremo no lo especifica)
# ══════════════════════════════════════════════════════════════════════════════
TIPO_BAREMO_DEFECTO: dict[str, str] = {
    "IntraA":       "riesgo",
    "IntraB":       "riesgo",
    "Extralaboral": "riesgo",
    "Estres":       "riesgo",
    "Afrontamiento": "proteccion",
    "CapPsico":     "proteccion",
}

# Etiquetas por nivel y tipo de baremo
LABELS_RIESGO = {1: "Sin riesgo", 2: "Bajo", 3: "Medio", 4: "Alto", 5: "Muy alto"}
LABELS_PROTECCION = {1: "Muy bajo", 2: "Bajo", 3: "Medio", 4: "Alto", 5: "Muy alto"}


# ══════════════════════════════════════════════════════════════════════════════
# Helpers
# ══════════════════════════════════════════════════════════════════════════════

def cargar_config() -> dict:
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def clasificar_nivel(pt: float, tipo: str, c_sr: float, c_b: float, c_m: float, c_a: float) -> int:
    """
    Clasifica puntaje_transformado en nivel 1-5.

    Para 'riesgo':     nivel sube con el puntaje (más score = más riesgo)
    Para 'proteccion': nivel sube con el puntaje (más score = más protección)
    En ambos casos el esquema de cortes es el mismo (≤ corte → nivel):
      ≤ corte_sin_riesgo → 1 | ≤ corte_bajo → 2 | ≤ corte_medio → 3 | ≤ corte_alto → 4 | else → 5
    """
    if pd.isna(pt):
        return 0  # indeterminado
    if pt <= c_sr:
        return 1
    if pt <= c_b:
        return 2
    if pt <= c_m:
        return 3
    if pt <= c_a:
        return 4
    return 5


# ══════════════════════════════════════════════════════════════════════════════
# Paso central: agregar por nivel y aplicar baremo
# ══════════════════════════════════════════════════════════════════════════════

def agregar_por_nivel(
    fact: pd.DataFrame,
    nivel_analisis: str,
    nombre_col: str,
) -> pd.DataFrame:
    """
    Agrupa fact_scores_brutos y calcula puntaje_bruto por nivel.

    Parámetros
    ----------
    nivel_analisis : 'dimension' | 'dominio' | 'factor'
    nombre_col     : columna en fact que identifica el nombre del grupo
                     ('dimension', 'dominio', o 'instrumento' para factor)

    Retorna DataFrame con columnas:
      cedula, empresa, forma_intra, sector_rag, instrumento,
      nivel_analisis, nombre_nivel, puntaje_bruto
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


def calcular_dominio_afrontamiento_ponderado(fact: pd.DataFrame) -> pd.DataFrame:
    """
    PASO 13 — Dominio Afrontamiento: media ponderada de las 3 subdimensiones.

    Fórmula (Documento marco, Paso 13):
      score = ((activo_planificacion × 0.25) + (busqueda_soporte × 0.25) + (pasivo_negacion × 0.50)) / 4 × 100

    Las 3 subdimensiones vienen de la columna 'dimension' en fact_scores_brutos:
      - 'Afrontamiento activo_planificación'  → peso 0.25
      - 'Afrontamiento activo_busquedasoporte' → peso 0.25
      - 'Afrontamiento evitativo_negación'    → peso 0.50  (ya invertido en Paso 2)

    transformacion_max = 4, cortes protección: 29 | 51 | 69 | 89
    """
    PESOS_AFRONTAMIENTO = {
        "Afrontamiento activo_planificación":   0.25,
        "Afrontamiento activo_busquedasoporte": 0.25,
        "Afrontamiento evitativo_negación":     0.50,
    }
    CORTES_AFRONTAMIENTO = (29.0, 51.0, 69.0, 89.0)  # (c_sr, c_b, c_m, c_a)

    id_cols = ["cedula", "empresa", "forma_intra", "sector_rag", "instrumento"]

    # Filtrar solo ítems de afrontamiento
    mask = (
        fact["instrumento"].str.lower().str.contains("afrontamiento", na=False) &
        fact["dimension"].isin(PESOS_AFRONTAMIENTO.keys())
    )
    afront = fact[mask].copy()

    if afront.empty:
        log.warning("Dominio Afrontamiento ponderado: sin datos de afrontamiento en fact_scores_brutos")
        return pd.DataFrame()

    # Suma por subdimensión (puntaje_bruto de cada subdimensión)
    sub = (
        afront.groupby(id_cols + ["dimension"], dropna=False)["valor_invertido"]
        .sum()
        .reset_index(name="suma_sub")
    )

    # Aplicar peso a cada subdimensión
    sub["peso"] = sub["dimension"].map(PESOS_AFRONTAMIENTO)
    sub["suma_ponderada"] = sub["suma_sub"] * sub["peso"]

    # Agregar al nivel dominio
    dom = (
        sub.groupby(id_cols, dropna=False)["suma_ponderada"]
        .sum()
        .reset_index(name="puntaje_bruto")
    )

    dom["nivel_analisis"] = "dominio"
    dom["nombre_nivel"] = "Estrategias de Afrontamiento"
    dom["tipo_baremo"] = "proteccion"
    dom["transformacion_max"] = 4.0
    dom["puntaje_transformado"] = (dom["puntaje_bruto"] / 4.0 * 100).round(2)

    c_sr, c_b, c_m, c_a = CORTES_AFRONTAMIENTO
    dom["nivel_riesgo"] = dom["puntaje_transformado"].apply(
        lambda pt: clasificar_nivel(pt, "proteccion", c_sr, c_b, c_m, c_a)
    )

    log.info("Dominio Afrontamiento ponderado — %d filas calculadas", len(dom))
    return dom


def aplicar_baremo_a_df(
    agg: pd.DataFrame,
    dim_baremo: pd.DataFrame,
    nivel_analisis: str,
) -> pd.DataFrame:
    """
    Hace JOIN con dim_baremo y calcula puntaje_transformado + nivel_riesgo.

    dim_baremo debe tener columnas:
      instrumento, nivel_analisis, nombre_nivel (o dimension),
      transformacion_max, corte_sin_riesgo, corte_bajo, corte_medio, corte_alto,
      tipo_baremo
    """
    # Filtrar dim_baremo al nivel correcto
    baremo_nivel = dim_baremo[dim_baremo["nivel_analisis"] == nivel_analisis].copy()

    # Normalizar nombre de columna en dim_baremo
    if "nombre_nivel" not in baremo_nivel.columns and "dimension" in baremo_nivel.columns:
        baremo_nivel = baremo_nivel.rename(columns={"dimension": "nombre_nivel"})

    join_cols = ["instrumento", "forma_intra", "nombre_nivel"] if nivel_analisis != "factor" else ["instrumento", "forma_intra"]

    df = agg.merge(
        baremo_nivel[join_cols + [
            "transformacion_max", "corte_sin_riesgo", "corte_bajo",
            "corte_medio", "corte_alto", "tipo_baremo",
        ]].drop_duplicates(join_cols),
        on=join_cols,
        how="left",
    )

    # Rellenar tipo_baremo si no vino del baremo
    df["tipo_baremo"] = df.apply(
        lambda r: r["tipo_baremo"]
        if pd.notna(r.get("tipo_baremo"))
        else TIPO_BAREMO_DEFECTO.get(r["instrumento"], "riesgo"),
        axis=1,
    )

    # Puntaje transformado: (bruto / max) * 100
    df["puntaje_transformado"] = (
        df["puntaje_bruto"] / df["transformacion_max"] * 100
    ).round(2)

    # Nivel 1-5
    df["nivel_riesgo"] = df.apply(
        lambda r: clasificar_nivel(
            r["puntaje_transformado"],
            r["tipo_baremo"],
            r.get("corte_sin_riesgo", 0),
            r.get("corte_bajo", 25),
            r.get("corte_medio", 50),
            r.get("corte_alto", 75),
        ),
        axis=1,
    )

    sin_baremo = df["transformacion_max"].isna().sum()
    if sin_baremo > 0:
        log.warning(
            "[%s] %d combinaciones sin baremo (instrumento/nivel/forma sin coincidencia en dim_baremo)",
            nivel_analisis, sin_baremo,
        )

    return df


# ══════════════════════════════════════════════════════════════════════════════
# Validación (R14)
# ══════════════════════════════════════════════════════════════════════════════

def validar_scores_baremo(df: pd.DataFrame) -> tuple[bool, pd.DataFrame]:
    """Valida integridad de fact_scores_baremo."""
    errores = []

    # PK sin duplicados — instrumento incluido porque misma dimensión puede
    # aparecer en IntraA e IntraB para el mismo trabajador (duplicado válido)
    dup = df.duplicated(
        subset=["cedula", "instrumento", "forma_intra", "nivel_analisis", "nombre_nivel"]
    ).sum()
    if dup > 0:
        errores.append({"check": "pk_duplicada", "n": int(dup)})

    # Nulos en columnas clave
    for col in ["cedula", "forma_intra", "nivel_analisis", "nombre_nivel",
                "puntaje_bruto", "nivel_riesgo"]:
        n = df[col].isna().sum()
        if n > 0:
            errores.append({"check": f"nulos_{col}", "n": int(n)})

    # nivel_riesgo debe ser 1-5 (0 = indeterminado, permitido si hay NaN en scores)
    fuera = df[(df["nivel_riesgo"] < 1) | (df["nivel_riesgo"] > 5)].shape[0]
    if fuera > 0:
        errores.append({"check": "nivel_riesgo_fuera_de_rango", "n": fuera})

    # puntaje_transformado debe estar en [0, 100]
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

# ══════════════════════════════════════════════════════════════════════════════
# Construcción interna de dim_baremo
# Los baremos del Ministerio (Res. 2764/2022) son valores fijos.
# transformacion_max se computa desde los datos reales (suma de max_item_score).
# ADVERTENCIA: Los cortes (corte_sin_riesgo..corte_alto) son PLACEHOLDERS 20/40/60/80.
# Deben ser reemplazados con los valores exactos del Manual Técnico del Ministerio.
# ══════════════════════════════════════════════════════════════════════════════

# Cortes estándar Ministerio por factor total (transformados 0-100)
# Fuente: Manual Técnico Batería de Riesgo Psicosocial (Ministerio de Trabajo)
CORTES_FACTOR: dict[str, tuple[float, float, float, float]] = {
    # (corte_sin_riesgo, corte_bajo, corte_medio, corte_alto)
    "IntraA":        (14.0, 27.0, 41.0, 59.0),
    "IntraB":        (14.4, 31.4, 49.2, 74.5),
    "Extralaboral":  (17.7, 30.6, 45.2, 63.7),
    "Estres":        (23.1, 37.1, 57.1, 75.3),
    "Afrontamiento": (29.0, 51.0, 69.0, 89.0),   # protección
    "CapPsico":      (29.0, 51.0, 69.0, 89.0),   # protección (placeholder)
}

# Para dominios y dimensiones, usar placeholders.
# TODO: reemplazar con cortes del Manual Técnico.
CORTES_DEFAULT = (20.0, 40.0, 60.0, 80.0)


def _construir_dim_baremo(fact: pd.DataFrame) -> pd.DataFrame:
    """
    Construye dim_baremo calculando transformacion_max desde los datos reales
    (suma de max_item_score por grupo).

    ADVERTENCIA: Los cortes de dominio y dimensión son PLACEHOLDERS (20/40/60/80).
    Deben actualizarse con los valores del Manual Técnico del Ministerio.
    """
    if "max_item_score" not in fact.columns:
        log.warning("Columna 'max_item_score' ausente — usando transformacion_max=100 (placeholder)")
        fact = fact.copy()
        fact["max_item_score"] = 4.0

    rows = []

    # ── FACTOR level ─────────────────────────────────────────────────────────
    fac_max = (
        fact.groupby(["instrumento", "forma_intra"])["max_item_score"]
        .sum()
        .reset_index(name="transformacion_max")
    )
    for _, r in fac_max.iterrows():
        inst = r["instrumento"]
        forma = r["forma_intra"]
        c_sr, c_b, c_m, c_a = CORTES_FACTOR.get(inst, CORTES_DEFAULT)
        tipo = TIPO_BAREMO_DEFECTO.get(inst, "riesgo")
        rows.append({
            "instrumento": inst, "forma_intra": forma,
            "nivel_analisis": "factor", "nombre_nivel": inst,
            "transformacion_max": r["transformacion_max"],
            "corte_sin_riesgo": c_sr, "corte_bajo": c_b,
            "corte_medio": c_m, "corte_alto": c_a,
            "tipo_baremo": tipo,
        })

    # ── DOMINIO level (cortes placeholder) ───────────────────────────────────
    if "dominio" in fact.columns:
        dom_max = (
            fact[fact["dominio"].notna()]
            .groupby(["instrumento", "forma_intra", "dominio"])["max_item_score"]
            .sum()
            .reset_index(name="transformacion_max")
        )
        for _, r in dom_max.iterrows():
            inst = r["instrumento"]
            tipo = TIPO_BAREMO_DEFECTO.get(inst, "riesgo")
            c_sr, c_b, c_m, c_a = CORTES_DEFAULT
            rows.append({
                "instrumento": inst, "forma_intra": r["forma_intra"],
                "nivel_analisis": "dominio", "nombre_nivel": r["dominio"],
                "transformacion_max": r["transformacion_max"],
                "corte_sin_riesgo": c_sr, "corte_bajo": c_b,
                "corte_medio": c_m, "corte_alto": c_a,
                "tipo_baremo": tipo,
            })

    # ── DIMENSION level (cortes placeholder) ─────────────────────────────────
    if "dimension" in fact.columns:
        dim_max = (
            fact[fact["dimension"].notna()]
            .groupby(["instrumento", "forma_intra", "dimension"])["max_item_score"]
            .sum()
            .reset_index(name="transformacion_max")
        )
        for _, r in dim_max.iterrows():
            inst = r["instrumento"]
            tipo = TIPO_BAREMO_DEFECTO.get(inst, "riesgo")
            c_sr, c_b, c_m, c_a = CORTES_DEFAULT
            rows.append({
                "instrumento": inst, "forma_intra": r["forma_intra"],
                "nivel_analisis": "dimension", "nombre_nivel": r["dimension"],
                "transformacion_max": r["transformacion_max"],
                "corte_sin_riesgo": c_sr, "corte_bajo": c_b,
                "corte_medio": c_m, "corte_alto": c_a,
                "tipo_baremo": tipo,
            })

    dim_baremo = pd.DataFrame(rows)
    log.warning(
        "dim_baremo construida con cortes PLACEHOLDER para dominios y dimensiones. "
        "Reemplazar con cortes del Manual Técnico del Ministerio."
    )
    log.info("dim_baremo: %d filas", len(dim_baremo))
    return dim_baremo


def main():
    log.info("=" * 60)
    log.info("SCRIPT 02b — Baremos (Pasos 9-15)")
    log.info("=" * 60)

    cfg = cargar_config()
    proc = ROOT / cfg["paths"]["processed"]

    # ── Cargar insumos ────────────────────────────────────────────────────────
    fact = pd.read_parquet(proc / "fact_scores_brutos.parquet")
    log.info("fact_scores_brutos: %d filas", len(fact))

    # Construir dim_baremo internamente (no existe como hoja Excel)
    dim_baremo = _construir_dim_baremo(fact)

    # Asegurar que empresa y sector_rag estén en fact
    for col in ["empresa", "sector_rag"]:
        if col not in fact.columns:
            log.warning("Columna '%s' ausente en fact_scores_brutos — se usará valor vacío", col)
            fact[col] = None

    # ── Calcular puntajes brutos por nivel ────────────────────────────────────
    partes = []

    # Pasos 9-11 — Dimensiones (intra A/B, extra, individual)
    log.info("Pasos 9-11 — Dimensiones...")
    dim_df = agregar_por_nivel(fact, "dimension", "dimension")
    dim_df = aplicar_baremo_a_df(dim_df, dim_baremo, "dimension")
    partes.append(dim_df)
    log.info("  → %d filas de dimensiones", len(dim_df))

    # Pasos 12-13 — Dominios (intra A/B + capital psicológico: suma simple)
    log.info("Pasos 12-13 — Dominios (suma simple, excluye dominio Afrontamiento)...")
    fact_sin_afrontamiento = fact[
        ~fact["instrumento"].str.lower().str.contains("afrontamiento", na=False)
    ]
    dom_df = agregar_por_nivel(fact_sin_afrontamiento, "dominio", "dominio")
    dom_df = aplicar_baremo_a_df(dom_df, dim_baremo, "dominio")
    partes.append(dom_df)
    log.info("  → %d filas de dominios (sin afrontamiento)", len(dom_df))

    # Paso 13 — Dominio Afrontamiento: fórmula ponderada especial
    log.info("Paso 13 — Dominio Afrontamiento (media ponderada 0.25/0.25/0.50)...")
    dom_afrontamiento = calcular_dominio_afrontamiento_ponderado(fact)
    if not dom_afrontamiento.empty:
        partes.append(dom_afrontamiento)
        log.info("  → %d filas de dominio Afrontamiento", len(dom_afrontamiento))

    # Pasos 14-15 — Factores (intra A/B, extra, estrés, individual)
    log.info("Pasos 14-15 — Factores...")
    fac_df = agregar_por_nivel(fact, "factor", "instrumento")
    fac_df = aplicar_baremo_a_df(fac_df, dim_baremo, "factor")
    partes.append(fac_df)
    log.info("  → %d filas de factores", len(fac_df))

    # ── Consolidar todos los niveles ─────────────────────────────────────────
    cols_output = [
        "cedula", "empresa", "forma_intra", "sector_rag",
        "instrumento", "nivel_analisis", "nombre_nivel",
        "puntaje_bruto", "puntaje_transformado",
        "nivel_riesgo", "tipo_baremo",
    ]
    fact_baremo = pd.concat(partes, ignore_index=True)
    fact_baremo_out = fact_baremo[[c for c in cols_output if c in fact_baremo.columns]]

    # ── Validación (R14) ────────────────────────────────────────────────────
    es_valido, reporte = validar_scores_baremo(fact_baremo_out)
    if not es_valido:
        log.error("Validación FALLIDA:\n%s", reporte.to_string())
        sys.exit(1)
    log.info("Validación OK")

    # ── Resumen por instrumento y nivel ────────────────────────────────────
    resumen = (
        fact_baremo_out[fact_baremo_out["nivel_analisis"] == "factor"]
        .groupby(["instrumento", "nivel_riesgo"])["cedula"]
        .count()
        .reset_index(name="n")
    )
    log.info("Distribución por instrumento (nivel factor):\n%s", resumen.to_string(index=False))

    # ── Guardar (R13) ───────────────────────────────────────────────────────
    out = proc / "fact_scores_baremo.parquet"
    fact_baremo_out.to_parquet(out, index=False)
    log.info("Guardado: %s (%d filas × %d columnas)", out, *fact_baremo_out.shape)

    log.info("=" * 60)
    log.info("Pasos 9-15 completados → fact_scores_baremo.parquet")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
