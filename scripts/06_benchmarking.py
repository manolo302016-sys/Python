"""
06_benchmarking.py
==================
Pasos 16-19 del pipeline V1 — Proporciones de riesgo y comparación con referencias nacionales.

PASO 16  Riesgo total empresa por Res. 2764/2022
         Promedio puntaje_bruto de todos los trabajadores de la empresa en factor intraA y B
         → clasificar con baremo factor → si nivel ≥ 4 → debe_reevaluar_1año = True

PASO 17  % personas Alto+MuyAlto en factor intralaboral A y B vs sector (ENCST III 2021)
         Comparación empresa vs sector económico + promedio general Colombia

PASO 18  % personas Alto+MuyAlto en dominios intra A/B, extralaboral A/B, estrés A/B,
         vulnerabilidad individual → vs referencias Colombia (ENCST II-III 2013-2021)

PASO 19  % personas Alto+MuyAlto en dimensiones intralaborales vs referencias Colombia

Output:  data/processed/fact_benchmark.parquet
         data/processed/fact_riesgo_empresa.parquet  (nivel empresa — Paso 16)

Reglas:
  R5  — Referencias ENCST como baseline externo
  R8  — Grupos < 5 trabajadores → semaforo='insuficiente' (no mostrar dato)
  R13 — Output Parquet
  R14 — Validación obligatoria
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
# Baremos factor — cortes para Paso 16 (igual que Paso 14 en dim_baremo)
# Hardcodeados como fallback si dim_baremo no los tiene en nivel='factor_empresa'
# ══════════════════════════════════════════════════════════════════════════════
BAREMO_FACTOR_EMPRESA = {
    "IntraA": {"max": 492.0, "c_sr": 19.7, "c_b": 25.8, "c_m": 31.5, "c_a": 38.8},
    "IntraB": {"max": 388.0, "c_sr": 20.6, "c_b": 26.0, "c_m": 31.2, "c_a": 38.7},
}


def cargar_config() -> dict:
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def semaforo_diff(diff_pp: float, n: int, n_min: int) -> str:
    """
    Semaforiza diferencia en puntos porcentuales empresa vs referencia.
    Si n < n_min → 'insuficiente' (R8).
    """
    if n < n_min:
        return "insuficiente"
    if diff_pp > 0:
        return "rojo"
    return "verde"


def clasificar_nivel_baremo(pt: float, c_sr: float, c_b: float, c_m: float, c_a: float) -> int:
    """Clasifica puntaje transformado en nivel 1-5 de riesgo."""
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


# ══════════════════════════════════════════════════════════════════════════════
# PASO 16 — Riesgo total empresa (nivel empresa, no trabajador)
# ══════════════════════════════════════════════════════════════════════════════

def calcular_riesgo_empresa(fact_baremo: pd.DataFrame) -> pd.DataFrame:
    """
    Paso 16 — Calcula el nivel de riesgo de la empresa para IntraA e IntraB.

    Proceso (Documento marco, Paso 16):
      1. Filtrar fact_scores_baremo en nivel='factor', instrumento IntraA o IntraB
      2. Promediar puntaje_bruto de todos los trabajadores de la empresa
      3. Aplicar baremo del Paso 14 sobre el promedio
      4. Si nivel ≥ 4 → debe_reevaluar_1año = True

    Retorna: 1 fila por empresa × instrumento (IntraA, IntraB)
    """
    factores = fact_baremo[
        (fact_baremo["nivel_analisis"] == "factor") &
        (fact_baremo["instrumento"].isin(["IntraA", "IntraB"]))
    ].copy()

    # Promedio puntaje_bruto por empresa + instrumento
    empresa_avg = (
        factores.groupby(["empresa", "sector_rag", "instrumento"], dropna=False)
        .agg(
            puntaje_bruto_promedio=("puntaje_bruto", "mean"),
            n_trabajadores=("cedula", "nunique"),
        )
        .reset_index()
    )

    # Aplicar baremo
    resultados = []
    for _, row in empresa_avg.iterrows():
        instr = row["instrumento"]
        bremo = BAREMO_FACTOR_EMPRESA.get(instr, {})
        if not bremo:
            continue
        pt = round(row["puntaje_bruto_promedio"] / bremo["max"] * 100, 1)
        nivel = clasificar_nivel_baremo(pt, bremo["c_sr"], bremo["c_b"], bremo["c_m"], bremo["c_a"])
        resultados.append({
            "empresa":               row["empresa"],
            "sector_rag":            row["sector_rag"],
            "instrumento":           instr,
            "n_trabajadores":        row["n_trabajadores"],
            "puntaje_bruto_empresa": round(row["puntaje_bruto_promedio"], 2),
            "puntaje_transformado":  pt,
            "nivel_riesgo_empresa":  nivel,
            "debe_reevaluar_1año":   nivel >= 4,
        })

    df = pd.DataFrame(resultados)
    n_reeval = df["debe_reevaluar_1año"].sum()
    log.info("Paso 16 — Empresas-formas que requieren reevaluación: %d de %d", n_reeval, len(df))
    return df


# ══════════════════════════════════════════════════════════════════════════════
# PASOS 17-19 — Proporciones % Alto+MuyAlto vs referencias
# ══════════════════════════════════════════════════════════════════════════════

def calcular_pct_alto_muy_alto(
    fact_baremo: pd.DataFrame,
    nivel_analisis: str,
    nombre_nivel: str,
    forma: str | None,
    grupo_by_cols: list[str],
) -> pd.DataFrame:
    """
    Calcula % personas con nivel_riesgo 4 o 5 para un nivel/nombre/forma dados.
    Retorna DataFrame con empresa, pct_empresa, n_total.
    """
    mask = fact_baremo["nivel_analisis"] == nivel_analisis
    mask &= fact_baremo["nombre_nivel"] == nombre_nivel
    if forma:
        mask &= fact_baremo["forma_intra"] == forma

    sub = fact_baremo[mask].copy()
    if sub.empty:
        return pd.DataFrame()

    total = (
        sub.groupby(grupo_by_cols, dropna=False)["cedula"]
        .nunique()
        .reset_index(name="n_total")
    )
    alto = (
        sub[sub["nivel_riesgo"] >= 4]
        .groupby(grupo_by_cols, dropna=False)["cedula"]
        .nunique()
        .reset_index(name="n_alto")
    )
    df = total.merge(alto, on=grupo_by_cols, how="left")
    df["n_alto"] = df["n_alto"].fillna(0).astype(int)
    df["pct_empresa"] = (df["n_alto"] / df["n_total"] * 100).round(2)
    df["nivel_analisis"] = nivel_analisis
    df["nombre_nivel"] = nombre_nivel
    df["forma_intra"] = forma if forma else "AB"
    return df


def construir_benchmark(cfg: dict, fact_baremo: pd.DataFrame, n_min: int) -> pd.DataFrame:
    """
    Pasos 17-19: itera sobre instrumentos, dominios y dimensiones,
    calcula pct_empresa y compara con referencia nacional.
    """
    bench_sector = cfg.get("benchmark_sector", {})
    bench_dominio = cfg.get("benchmark_dominio", {})
    bench_dimension = cfg.get("benchmark_dimension", {})
    prom_general = bench_sector.get("_promedio_general", 39.69)

    grupo_cols = ["empresa", "sector_rag"]
    resultados = []

    # ── Paso 17: factor intralaboral A y B vs sector ──────────────────────────
    for forma, instr in [("A", "IntraA"), ("B", "IntraB")]:
        sub = calcular_pct_alto_muy_alto(
            fact_baremo, "factor", instr, forma, grupo_cols
        )
        if sub.empty:
            continue
        for _, row in sub.iterrows():
            sector = row.get("sector_rag", "No clasificado")
            pct_sector = bench_sector.get(sector, prom_general)
            diff = round(row["pct_empresa"] - pct_sector, 2)
            resultados.append({
                **{c: row[c] for c in grupo_cols},
                "nivel_analisis":    "factor",
                "nombre_nivel":      instr,
                "forma_intra":       forma,
                "n_total":           row["n_total"],
                "n_alto":            row["n_alto"],
                "pct_empresa":       row["pct_empresa"],
                "pct_referencia":    pct_sector,
                "tipo_referencia":   "sector",
                "diferencia_pp":     diff,
                "semaforo":          semaforo_diff(diff, row["n_total"], n_min),
            })

    # ── Paso 18: dominios intra A/B vs Colombia ──────────────────────────────
    # Nombres exactos según nombre_nivel en fact_scores_baremo (nivel_analisis='dominio')
    dominios_intra = [
        ("Demandas del trabajo",            "IntraA", "A"),
        ("Demandas del trabajo",            "IntraB", "B"),
        ("Control sobre el trabajo",        "IntraA", "A"),
        ("Control sobre el trabajo",        "IntraB", "B"),
        ("Liderazgo y relaciones sociales", "IntraA", "A"),
        ("Liderazgo y relaciones sociales", "IntraB", "B"),
        ("Recompensas",                     "IntraA", "A"),
        ("Recompensas",                     "IntraB", "B"),
    ]

    for nombre_dom, instr_filter, forma in dominios_intra:
        sub_all = fact_baremo[
            (fact_baremo["nivel_analisis"] == "dominio") &
            (fact_baremo["nombre_nivel"] == nombre_dom) &
            (fact_baremo["instrumento"] == instr_filter)
        ].copy()
        if sub_all.empty:
            log.warning("Paso 18: dominio '%s' instrumento '%s' sin filas", nombre_dom, instr_filter)
            continue
        total = sub_all.groupby(grupo_cols, dropna=False)["cedula"].nunique().reset_index(name="n_total")
        alto = sub_all[sub_all["nivel_riesgo"] >= 4].groupby(grupo_cols, dropna=False)["cedula"].nunique().reset_index(name="n_alto")
        sub_calc = total.merge(alto, on=grupo_cols, how="left")
        sub_calc["n_alto"] = sub_calc["n_alto"].fillna(0).astype(int)
        sub_calc["pct_empresa"] = (sub_calc["n_alto"] / sub_calc["n_total"] * 100).round(2)
        pct_ref = bench_dominio.get(nombre_dom, prom_general)
        for _, row in sub_calc.iterrows():
            diff = round(row["pct_empresa"] - pct_ref, 2)
            resultados.append({
                **{c: row[c] for c in grupo_cols},
                "nivel_analisis":  "dominio",
                "nombre_nivel":    nombre_dom,
                "forma_intra":     forma,
                "n_total":         row["n_total"],
                "n_alto":          row["n_alto"],
                "pct_empresa":     row["pct_empresa"],
                "pct_referencia":  pct_ref,
                "tipo_referencia": "colombia",
                "diferencia_pp":   diff,
                "semaforo":        semaforo_diff(diff, row["n_total"], n_min),
            })

    # ── Paso 18: Extralaboral y Estrés (nivel='factor') vs Colombia ──────────
    for instr, forma_label in [("Extralaboral", "AB"), ("Estres", "AB")]:
        sub_all = fact_baremo[
            (fact_baremo["nivel_analisis"] == "factor") &
            (fact_baremo["instrumento"] == instr)
        ].copy()
        if sub_all.empty:
            continue
        total = sub_all.groupby(grupo_cols, dropna=False)["cedula"].nunique().reset_index(name="n_total")
        alto = sub_all[sub_all["nivel_riesgo"] >= 4].groupby(grupo_cols, dropna=False)["cedula"].nunique().reset_index(name="n_alto")
        sub_calc = total.merge(alto, on=grupo_cols, how="left")
        sub_calc["n_alto"] = sub_calc["n_alto"].fillna(0).astype(int)
        sub_calc["pct_empresa"] = (sub_calc["n_alto"] / sub_calc["n_total"] * 100).round(2)
        pct_ref = bench_dominio.get(instr, prom_general)
        for _, row in sub_calc.iterrows():
            diff = round(row["pct_empresa"] - pct_ref, 2)
            resultados.append({
                **{c: row[c] for c in grupo_cols},
                "nivel_analisis":  "dominio",
                "nombre_nivel":    instr,
                "forma_intra":     forma_label,
                "n_total":         row["n_total"],
                "n_alto":          row["n_alto"],
                "pct_empresa":     row["pct_empresa"],
                "pct_referencia":  pct_ref,
                "tipo_referencia": "colombia",
                "diferencia_pp":   diff,
                "semaforo":        semaforo_diff(diff, row["n_total"], n_min),
            })

    # ── Paso 18: Vulnerabilidad = % nivel muy_bajo+bajo en factor Individual ─
    sub_ind = fact_baremo[
        (fact_baremo["nivel_analisis"] == "factor") &
        (fact_baremo["instrumento"] == "Individual")
    ].copy()
    if not sub_ind.empty:
        total = sub_ind.groupby(grupo_cols, dropna=False)["cedula"].nunique().reset_index(name="n_total")
        vuln = sub_ind[sub_ind["nivel_riesgo"] <= 2].groupby(grupo_cols, dropna=False)["cedula"].nunique().reset_index(name="n_alto")
        sub_calc = total.merge(vuln, on=grupo_cols, how="left")
        sub_calc["n_alto"] = sub_calc["n_alto"].fillna(0).astype(int)
        sub_calc["pct_empresa"] = (sub_calc["n_alto"] / sub_calc["n_total"] * 100).round(2)
        pct_ref = bench_dominio.get("Individual", prom_general)
        for _, row in sub_calc.iterrows():
            diff = round(row["pct_empresa"] - pct_ref, 2)
            resultados.append({
                **{c: row[c] for c in grupo_cols},
                "nivel_analisis":  "dominio",
                "nombre_nivel":    "Vulnerabilidad",
                "forma_intra":     "AB",
                "n_total":         row["n_total"],
                "n_alto":          row["n_alto"],
                "pct_empresa":     row["pct_empresa"],
                "pct_referencia":  pct_ref,
                "tipo_referencia": "colombia",
                "diferencia_pp":   diff,
                "semaforo":        semaforo_diff(diff, row["n_total"], n_min),
            })

    # ── Paso 19: dimensiones comparables vs Colombia ──────────────────────────
    # Referencia diferenciada: "Claridad de rol" forma A=20.5%, forma B=5.8%
    claridad_ref_b = 5.8

    for nombre_dim, pct_ref_base in bench_dimension.items():
        sub_all = fact_baremo[
            (fact_baremo["nivel_analisis"] == "dimension") &
            (fact_baremo["nombre_nivel"] == nombre_dim)
        ]
        if sub_all.empty:
            continue

        for forma in sub_all["forma_intra"].unique():
            # Override para Claridad de rol forma B
            pct_ref = claridad_ref_b if (nombre_dim == "Claridad de rol" and forma == "B") else pct_ref_base

            sub_f = sub_all[sub_all["forma_intra"] == forma]
            total = sub_f.groupby(grupo_cols, dropna=False)["cedula"].nunique().reset_index(name="n_total")
            alto = sub_f[sub_f["nivel_riesgo"] >= 4].groupby(grupo_cols, dropna=False)["cedula"].nunique().reset_index(name="n_alto")
            sub_calc = total.merge(alto, on=grupo_cols, how="left")
            sub_calc["n_alto"] = sub_calc["n_alto"].fillna(0).astype(int)
            sub_calc["pct_empresa"] = (sub_calc["n_alto"] / sub_calc["n_total"] * 100).round(2)

            for _, row in sub_calc.iterrows():
                diff = round(row["pct_empresa"] - pct_ref, 2)
                resultados.append({
                    **{c: row[c] for c in grupo_cols},
                    "nivel_analisis":  "dimension",
                    "nombre_nivel":    nombre_dim,
                    "forma_intra":     forma,
                    "n_total":         row["n_total"],
                    "n_alto":          row["n_alto"],
                    "pct_empresa":     row["pct_empresa"],
                    "pct_referencia":  pct_ref,
                    "tipo_referencia": "colombia",
                    "diferencia_pp":   diff,
                    "semaforo":        semaforo_diff(diff, row["n_total"], n_min),
                })

    return pd.DataFrame(resultados)


# ══════════════════════════════════════════════════════════════════════════════
# Validación (R14)
# ══════════════════════════════════════════════════════════════════════════════

def validar_benchmark(df: pd.DataFrame) -> tuple[bool, pd.DataFrame]:
    errores = []
    for col in ["empresa", "nivel_analisis", "nombre_nivel", "pct_empresa", "semaforo"]:
        n = df[col].isna().sum()
        if n > 0:
            errores.append({"check": f"nulos_{col}", "n": int(n)})
    fuera = df[(df["pct_empresa"] < 0) | (df["pct_empresa"] > 100)].shape[0]
    if fuera > 0:
        errores.append({"check": "pct_empresa_fuera_0_100", "n": fuera})
    semaforos_validos = {"verde", "rojo", "insuficiente"}
    invalidos = df[~df["semaforo"].isin(semaforos_validos)].shape[0]
    if invalidos > 0:
        errores.append({"check": "semaforo_invalido", "n": invalidos})
    reporte = pd.DataFrame(errores) if errores else pd.DataFrame(columns=["check", "n"])
    return len(errores) == 0, reporte


# ══════════════════════════════════════════════════════════════════════════════
# Main
# ══════════════════════════════════════════════════════════════════════════════

def main():
    log.info("=" * 60)
    log.info("SCRIPT 06 — Benchmarking (Pasos 16-19)")
    log.info("=" * 60)

    cfg = cargar_config()
    proc = ROOT / cfg["paths"]["processed"]
    n_min = cfg.get("confidencialidad_n_min", 5)

    fact_baremo = pd.read_parquet(proc / "fact_scores_baremo.parquet")
    log.info("fact_scores_baremo: %d filas", len(fact_baremo))

    # ── Paso 16: riesgo total empresa ────────────────────────────────────────
    log.info("Paso 16 — Riesgo total empresa...")
    riesgo_empresa = calcular_riesgo_empresa(fact_baremo)
    out_re = proc / "fact_riesgo_empresa.parquet"
    riesgo_empresa.to_parquet(out_re, index=False)
    log.info("Guardado: %s (%d filas)", out_re, len(riesgo_empresa))

    # ── Pasos 17-19: proporciones y comparaciones ────────────────────────────
    log.info("Pasos 17-19 — Proporciones empresa vs referencias...")
    fact_benchmark = construir_benchmark(cfg, fact_baremo, n_min)

    es_valido, reporte = validar_benchmark(fact_benchmark)
    if not es_valido:
        log.error("Validación FALLIDA:\n%s", reporte.to_string())
        sys.exit(1)
    log.info("Validación OK")

    # Resumen semáforos
    resumen = fact_benchmark.groupby(["nivel_analisis", "semaforo"]).size().reset_index(name="n")
    log.info("Semáforos por nivel:\n%s", resumen.to_string(index=False))

    out_bm = proc / "fact_benchmark.parquet"
    fact_benchmark.to_parquet(out_bm, index=False)
    log.info("Guardado: %s (%d filas × %d columnas)", out_bm, *fact_benchmark.shape)

    log.info("=" * 60)
    log.info("Pasos 16-19 completados → fact_benchmark + fact_riesgo_empresa")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
