"""
08_consolidacion.py
===================
Paso 21 — Base consolidada: enriquece fact_scores_baremo con todas las
tablas dimensionales (trabajador, demografía, ausentismo).

Output: data/processed/fact_consolidado.parquet
  - Formato largo: múltiples filas por trabajador (una por nivel_analisis × nombre_nivel)
  - Columnas añadidas: área, cargo, datos demográficos, ausentismo
  - Usada por todos los dashboards para filtros y desgloses por área/demografía

Joins aplicados:
  fact_scores_baremo
    INNER JOIN dim_trabajador  ON cedula  → área, cargo, nombre
    LEFT  JOIN dim_demografia  ON cedula  → sexo, edad, escolaridad, etc.
    LEFT  JOIN dim_ausentismo  ON cedula  → ausentismo (solo ~17 registros)

Reglas:
  R6  — dim_ausentismo: LEFT JOIN (pocos registros — no descartar trabajadores sin ausencias)
  R8  — Confidencialidad aplicada en dashboards, no aquí
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


def cargar_config() -> dict:
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def validar_consolidado(df: pd.DataFrame) -> tuple[bool, pd.DataFrame]:
    errores = []
    for col in ["cedula", "empresa", "forma_intra", "nivel_analisis", "nombre_nivel", "nivel_riesgo"]:
        n = df[col].isna().sum()
        if n > 0:
            errores.append({"check": f"nulos_{col}", "n": int(n)})
    # area_departamento puede ser None legítimamente si dim_trabajador no la tiene para todos
    n_sin_area = df["area_departamento"].isna().sum()
    if n_sin_area > 0:
        log.warning("Trabajadores sin area_departamento: %d (se mostrará 'Sin área' en dashboards)", n_sin_area)
    reporte = pd.DataFrame(errores) if errores else pd.DataFrame(columns=["check", "n"])
    return len(errores) == 0, reporte


def main():
    log.info("=" * 60)
    log.info("SCRIPT 08 — Consolidación demográfica (Paso 21)")
    log.info("=" * 60)

    cfg = cargar_config()
    proc = ROOT / cfg["paths"]["processed"]

    # ── Cargar tablas ────────────────────────────────────────────────────────
    fact_baremo   = pd.read_parquet(proc / "fact_scores_baremo.parquet")
    dim_trabajador = pd.read_parquet(proc / "dim_trabajador.parquet")
    dim_demografia = pd.read_parquet(proc / "dim_demografia.parquet")
    dim_ausentismo = pd.read_parquet(proc / "dim_ausentismo.parquet")

    log.info("fact_scores_baremo : %d filas", len(fact_baremo))
    log.info("dim_trabajador     : %d filas", len(dim_trabajador))
    log.info("dim_demografia     : %d filas", len(dim_demografia))
    log.info("dim_ausentismo     : %d filas", len(dim_ausentismo))

    # Normalizar cedula como str en todas las tablas
    for df_ref in [fact_baremo, dim_trabajador, dim_demografia, dim_ausentismo]:
        df_ref["cedula"] = df_ref["cedula"].astype(str).str.strip()

    # ── Columnas de dim_trabajador a incorporar ──────────────────────────────
    # Nombres reales en planta_personal: nombre_trabajador, categoria_cargo, etc.
    cols_trabajador = [
        "cedula", "nombre_trabajador", "area_departamento", "tipo_cargo",
        "categoria_cargo", "modalidad_de_trabajo", "es_Jefe",
    ]
    cols_trabajador = [c for c in cols_trabajador if c in dim_trabajador.columns]

    # ── Columnas de dim_demografia a incorporar ──────────────────────────────
    # Nombres reales en ficha_demografia
    cols_demografia = [
        "cedula", "sexo", "edad_cumplida", "nivel_escolaridad",
        "estado_civil", "numero_dependientes_economicos", "estrato_economico",
        "tipo_salario", "antiguedad_empresa_años_cumplidos",
        "antiguedad_en_cargo_años_cumplidos", "horas_trabajo_diario",
    ]
    cols_demografia = [c for c in cols_demografia if c in dim_demografia.columns]

    # ── Columnas de dim_ausentismo a incorporar ──────────────────────────────
    # Nombres reales en ausentismo_12meses
    cols_ausentismo = [
        "cedula", "dias_ausencia", "tipo_ausentismo", "diagnostico_CIE",
    ]
    cols_ausentismo = [c for c in cols_ausentismo if c in dim_ausentismo.columns]

    # ── Joins ────────────────────────────────────────────────────────────────
    # INNER JOIN con dim_trabajador (todo trabajador debe tener registro)
    fact = fact_baremo.merge(
        dim_trabajador[cols_trabajador].drop_duplicates("cedula"),
        on="cedula",
        how="left",   # left para no perder si hay cedulas sin registro en dim_trabajador
    )

    # LEFT JOIN demografía
    if len(cols_demografia) > 1:
        fact = fact.merge(
            dim_demografia[cols_demografia].drop_duplicates("cedula"),
            on="cedula",
            how="left",
        )
        log.info("dim_demografia incorporada: %d columnas", len(cols_demografia) - 1)
    else:
        log.warning("dim_demografia sin columnas adicionales — solo cedula")

    # LEFT JOIN ausentismo (R6 — muy pocos registros)
    if len(cols_ausentismo) > 1:
        fact = fact.merge(
            dim_ausentismo[cols_ausentismo].drop_duplicates("cedula"),
            on="cedula",
            how="left",
        )
        log.info("dim_ausentismo incorporada: %d columnas", len(cols_ausentismo) - 1)
    else:
        log.warning("dim_ausentismo sin columnas adicionales")

    # Rellenar área faltante
    if "area_departamento" in fact.columns:
        fact["area_departamento"] = fact["area_departamento"].fillna("Sin área")
    else:
        fact["area_departamento"] = "Sin área"

    # ── Validación (R14) ────────────────────────────────────────────────────
    es_valido, reporte = validar_consolidado(fact)
    if not es_valido:
        log.error("Validación FALLIDA:\n%s", reporte.to_string())
        sys.exit(1)
    log.info("Validación OK")

    # ── Resumen ──────────────────────────────────────────────────────────────
    log.info("fact_consolidado: %d filas × %d columnas", *fact.shape)
    log.info(
        "Trabajadores únicos: %d | Empresas: %d | Áreas: %d",
        fact["cedula"].nunique(),
        fact["empresa"].nunique(),
        fact["area_departamento"].nunique(),
    )

    # ── Guardar (R13) ────────────────────────────────────────────────────────
    out = proc / "fact_consolidado.parquet"
    fact.to_parquet(out, index=False)
    log.info("Guardado: %s", out)

    log.info("=" * 60)
    log.info("Paso 21 completado → fact_consolidado.parquet")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
