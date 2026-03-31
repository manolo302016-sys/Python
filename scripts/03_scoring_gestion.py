"""
03_scoring_gestion.py
=====================
ETL Visualizador 2 — Gestión de Salud Mental.

Pasos del pipeline V2:
  Sub-paso 1  Estandarizar escala 0-1 por instrumento (Paso 1 del doc V2)
  Sub-paso 2  JOIN con categorias_analisis + inversión V2 (Paso 3.1 del doc V2)
  Sub-paso 3  Calificación ponderada: preguntas → indicadores → líneas → ejes (Paso 3.2/3.3)

Outputs intermedios:
  data/processed/fact_gestion_01_estandarizado.parquet  — sub-paso 1
  data/processed/fact_gestion_02_invertido.parquet      — sub-paso 2

Fuentes:
  data/processed/fact_scores_brutos.parquet   — valor_invertido, max_item_score
  data/processed/categorias_analisis.parquet  — jerarquía de gestión V2
  docs/v2_gestion_saludmental.md              — tablas de inversión y pesos (parseadas)

Reglas aplicadas:
  R8     — Confidencialidad: grupos < 5 → "Confidencial" (se aplica en la API, no aquí)
  V2-INV — Inversión ítem: 1 - valor_01 según tabla del doc V2 (última ocurrencia gana en conflicto)
  V2-W   — Pesos por pregunta parseados del doc V2
"""

import logging
import re
import sys
from pathlib import Path

import pandas as pd

# ── Rutas ──────────────────────────────────────────────────────────────────
ROOT = Path(__file__).resolve().parents[1]
CONFIG_PATH = ROOT / "config" / "config.yaml"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════════════════════
# SUB-PASO 1 — Estandarización 0-1
# Fuente: doc V2, Paso 1.
#
# Regla general:  valor_01 = valor_invertido / max_item_score
# Excepción:      Afrontamiento y CapPsico usan mapeo explícito.
#                 (0.5 → 0.33 y 0.7 → 0.66 NO son divisibles por max=1)
# ══════════════════════════════════════════════════════════════════════════════

# Mapeo explícito para Afrontamiento y CapPsicológico.
# valor_invertido → valor_01
AFRO_CAPPSICO_MAP: dict[float, float] = {
    0.0: 0.00,
    0.5: 0.33,
    0.7: 0.66,
    1.0: 1.00,
}

# Instrumentos que usan val / max_item_score directamente
INSTR_DIVISION = {"IntraA", "IntraB", "Extralaboral", "Estres"}

# Instrumentos con mapeo explícito
INSTR_MAPA = {"Afrontamiento", "CapPsico"}


# ══════════════════════════════════════════════════════════════════════════════
# SUB-PASO 2 — Pesos e inversión V2 por indicador
#
# Fuente: doc V2, Paso 3 (tablas de pesos e inversión).
# Columnas:  indicador | inversion_v2 (True/False) | peso_pregunta | denominador
#
# NOTA: categorias_analisis.parquet NO contiene estas columnas; se hardcodean aquí.
# Se expresan como dict {(id_pregunta, forma_intra): {peso, inversion}}
# Para simplificar el join, la tabla se estructura como:
#   {id_pregunta → {inversion_item: bool, peso_item: float}}
# El denominador por indicador se define en DENOMINADORES_INDICADOR.
# ══════════════════════════════════════════════════════════════════════════════

# Tabla de inversión V2 a nivel de ítem (True = se aplica 1 - valor_01 después de estandarizar).
# Fuente: doc v2_gestion_saludmental.md, Paso 3 tabla de inversión.
# Formato: id_pregunta (tal como aparece en fact_scores_brutos) → bool

INVERSION_V2_ITEM: dict[str, bool] = {
    # ── CapPsicológico (12 ítems, todos False en esta tabla de inversión) ──
    "1_capitalpsicologico": False, "2_capitalpsicologico": False, "3_capitalpsicologico": False,
    "4_capitalpsicologico": False, "5_capitalpsicologico": False, "6_capitalpsicologico": False,
    "7_capitalpsicologico": False, "8_capitalpsicologico": False, "9_capitalpsicologico": False,
    "10_capitalpsicologico": False, "11_capitalpsicologico": False, "12_capitalpsicologico": False,
    # ── Estrés (31 ítems, todos False — ya invertidos en V1) ──
    "1_estres": False, "2_estres": False, "3_estres": False, "4_estres": False,
    "5_estres": False, "6_estres": False, "7_estres": False, "8_estres": False,
    "9_estres": False, "10_estres": False, "11_estres": False, "12_estres": False,
    "13_estres": False, "14_estres": False, "15_estres": False, "16_estres": False,
    "17_estres": False, "18_estres": False, "19_estres": False, "20_estres": False,
    "21_estres": False, "22_estres": False, "23_estres": False, "24_estres": False,
    "25_estres": False, "26_estres": False, "27_estres": False, "28_estres": False,
    "29_estres": False, "30_estres": False, "31_estres": False,
    # ── Extralaboral (31 ítems) ── inversión por ítem según doc ──
    "1_extra": True,  "2_extra": True,  "3_extra": True,  "4_extra": False,
    "5_extra": False, "6_extra": False, "7_extra": False, "8_extra": False,
    "9_extra": False, "10_extra": True,  "11_extra": True,  "12_extra": True,
    "13_extra": True,  "14_extra": False, "15_extra": False, "16_extra": False,
    "17_extra": False, "18_extra": False, "19_extra": False, "20_extra": False,
    "21_extra": False, "22_extra": False, "23_extra": False, "24_extra": True,
    "25_extra": False, "26_extra": True,  "27_extra": False, "28_extra": True,
    "29_extra": True,  "30_extra": True,  "31_extra": True,
    # ── Afrontamiento (12 ítems, todos False) ──
    "1_afrontamiento": False, "2_afrontamiento": False, "3_afrontamiento": False,
    "4_afrontamiento": False, "5_afrontamiento": False, "6_afrontamiento": False,
    "7_afrontamiento": False, "8_afrontamiento": False, "9_afrontamiento": False,
    "10_afrontamiento": False, "11_afrontamiento": False, "12_afrontamiento": False,
}

# Las preguntas Intra (A y B) se registran con id_pregunta = "{n}_intra".
# La inversión V2 de los ítems intra se define a continuación por número de ítem.
# Formato: num_item → inversion_v2 (para IntraA; IntraB tiene su propia tabla abajo).

# IntraA — ítems invertidos en V2 (True = aplicar 1 - valor_01)
INVERSION_V2_INTRA_A: dict[int, bool] = {
    1: True,  2: True,  3: True,  4: True,  5: True,  6: True,  7: True,
    8: True,  9: True, 10: True, 11: True, 12: True, 13: True, 14: True,
    15: True, 16: True, 17: True, 18: True, 19: True, 20: True, 21: True,
    22: True, 23: True, 24: True, 25: True, 26: True, 27: True, 28: True,
    29: True, 30: True, 31: True, 32: True, 33: True, 34: True, 35: False,
    36: False, 37: False, 38: False, 39: False, 40: False, 41: False, 42: False,
    43: False, 44: False, 45: False, 46: False, 47: False, 48: False, 49: False,
    50: False, 51: True,  52: True,  53: True,  54: True,  55: True,  56: True,
    57: True,  58: True,  59: True,  60: True,  61: True,  62: True,  63: True,
    64: True,  65: True,  66: True,  67: True,  68: True,  69: True,  70: True,
    71: True,  72: True,  73: True,  74: True,  75: True,  76: True,  77: True,
    78: True,  79: True,  80: True,  81: True,  82: True,  83: True,  84: True,
    85: True,  86: True,  87: True,  88: True,  89: True,  90: True,  91: True,
    92: True,  93: True,  94: True,  95: True,  96: True,  97: True,  98: True,
    99: True, 100: True, 101: True, 102: True, 103: True, 104: True, 105: True,
    106: False, 107: True, 108: True, 109: True, 110: True, 111: True, 112: True,
    113: True, 114: True, 115: True, 116: False, 117: True, 118: True, 119: True,
    120: True, 121: True, 122: True, 123: True, 124: True, 125: True,
}

# IntraB — ítems invertidos en V2
INVERSION_V2_INTRA_B: dict[int, bool] = {
    1: True,  2: True,  3: True,  4: True,  5: True,  6: True,  7: True,
    8: True,  9: True, 10: True, 11: True, 12: True, 13: True, 14: True,
    15: True, 16: True, 17: True, 18: True, 19: True, 20: True, 21: True,
    22: True, 23: True, 24: True, 25: True, 26: True, 27: True, 28: True,
    29: True, 30: True, 31: True, 32: True, 33: True, 34: True, 35: False,
    36: False, 37: False, 38: False, 39: False, 40: False, 41: False, 42: False,
    43: False, 44: False, 45: False, 46: False, 47: False, 48: False, 49: False,
    50: False, 51: True,  52: True,  53: True,  54: True,  55: True,  56: True,
    57: True,  58: True,  59: True,  60: True,  61: True,  62: True,  63: True,
    64: True,  65: True,  66: True,  67: True,  68: True,  69: True,  70: True,
    71: True,  72: True,  73: True,  74: True,  75: True,  76: True,  77: True,
    78: True,  79: True,  80: True,  81: True,  82: True,  83: True,  84: True,
    85: True,  86: True,  87: True,  88: True,  89: False,  90: True,  91: True,
    92: True,  93: True,  94: True,  95: True,  96: True,  97: True,  98: True,
}


# ══════════════════════════════════════════════════════════════════════════════
# Función auxiliar para compatibilidad pyarrow
# ══════════════════════════════════════════════════════════════════════════════

def _sanitizar_tipos(df: pd.DataFrame) -> pd.DataFrame:
    """Convierte columnas object/StringDtype a str y numéricas sucias a float."""
    for col in df.select_dtypes(include=["object", "string"]).columns:
        df[col] = df[col].astype(str).where(df[col].notna(), None)
    return df


# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════

def main() -> None:
    log.info("=== 03_scoring_gestion.py — Sub-paso 1: Estandarización 0-1 ===")

    # ── 1. Cargar parquets de entrada ─────────────────────────────────────────
    path_brutos = ROOT / "data" / "processed" / "fact_scores_brutos.parquet"
    path_cat    = ROOT / "data" / "processed" / "categorias_analisis.parquet"

    log.info("Cargando fact_scores_brutos...")
    brutos = pd.read_parquet(path_brutos)
    log.info("  -> %d filas, %d columnas", len(brutos), brutos.shape[1])

    log.info("Cargando categorias_analisis...")
    cat = pd.read_parquet(path_cat)
    log.info("  -> %d filas, %d columnas", len(cat), cat.shape[1])

    # ── CORRECCIÓN: reconstruir categorias_analisis desde v2 doc actualizado ──
    # El doc v2 fue corregido: afrontamiento y capitalpsicologico estaban trocados
    # en sus asignaciones de indicador/factor/dimension. Se actualiza el parquet.
    doc_path_cat = ROOT / "docs" / "v2_gestion_saludmental.md"
    cat = _corregir_categorias_afro_cappsico(cat, doc_path_cat, path_cat)
    log.info("categorias_analisis corregido y guardado.")

    # ── 2. Filtrar brutos a solo preguntas del modelo V2 ─────────────────────
    preguntas_v2 = set(cat["id_pregunta"].dropna().unique())
    log.info("Preguntas en categorias_analisis (modelo V2): %d únicos", len(preguntas_v2))

    mask_v2 = brutos["id_pregunta"].isin(preguntas_v2)
    df = brutos[mask_v2].copy()
    excluidas = brutos[~mask_v2]["id_pregunta"].nunique()
    log.info(
        "Filas filtradas para V2: %d (de %d totales). Preguntas excluidas: %d",
        len(df), len(brutos), excluidas,
    )

    if df.empty:
        log.error("No quedaron filas tras el filtro — revisar id_pregunta en ambos parquets.")
        sys.exit(1)

    # ── 3. Estandarización 0-1 ────────────────────────────────────────────────
    log.info("Aplicando estandarización 0-1...")

    # 3a. Para IntraA, IntraB, Extralaboral, Estres: valor_01 = valor_invertido / max_item_score
    mask_division = df["instrumento"].isin(INSTR_DIVISION)
    df.loc[mask_division, "valor_01"] = (
        df.loc[mask_division, "valor_invertido"] / df.loc[mask_division, "max_item_score"]
    )

    # 3b. Para Afrontamiento y CapPsico: mapeo explícito
    mask_mapa = df["instrumento"].isin(INSTR_MAPA)
    df.loc[mask_mapa, "valor_01"] = df.loc[mask_mapa, "valor_invertido"].map(AFRO_CAPPSICO_MAP)

    # 3c. Verificar que no hayan NaN en valor_01
    nan_count = df["valor_01"].isna().sum()
    if nan_count > 0:
        log.warning(
            "%d filas con valor_01=NaN (revisar mapeos). Detalle:\n%s",
            nan_count,
            df[df["valor_01"].isna()][["instrumento", "id_pregunta", "valor_invertido", "max_item_score"]].drop_duplicates().head(10).to_string(),
        )
    else:
        log.info("  -> valor_01 sin NaN. OK.")

    # 3d. Verificar rango [0, 1]
    fuera_rango = df[(df["valor_01"] < 0) | (df["valor_01"] > 1)]
    if not fuera_rango.empty:
        log.warning(
            "%d filas con valor_01 fuera de [0, 1]:\n%s",
            len(fuera_rango),
            fuera_rango[["instrumento", "id_pregunta", "valor_invertido", "valor_01"]].head(10).to_string(),
        )
    else:
        log.info("  -> Rango [0, 1] verificado. OK.")

    # ── 4. Resumen de validación ──────────────────────────────────────────────
    log.info("\n=== VALIDACIÓN SUB-PASO 1 ===")
    resumen = (
        df.groupby("instrumento")["valor_01"]
        .agg(["count", "min", "max", "mean"])
        .round(4)
    )
    log.info("Estadísticas de valor_01 por instrumento:\n%s", resumen.to_string())

    log.info("\nDistribución de valor_01 por instrumento (muestra de valores únicos):")
    for instr in df["instrumento"].unique():
        vals = sorted(df[df["instrumento"] == instr]["valor_01"].dropna().unique().tolist())
        log.info("  %s: %s", instr, [round(v, 4) for v in vals])

    log.info("\nConteo de trabajadores y preguntas por instrumento:")
    resumen2 = df.groupby("instrumento").agg(
        n_trabajadores=("cedula", "nunique"),
        n_preguntas=("id_pregunta", "nunique"),
        n_filas=("cedula", "count"),
    )
    log.info("\n%s", resumen2.to_string())

    # ── 5. Cobertura por empresa: instrumentos disponibles y lineas incalculables ─
    log.info("\n=== COBERTURA POR EMPRESA ===")

    # Sufijo de instrumento implícito en id_pregunta (para cruzar con categorias_analisis)
    INSTR_SUFIJO = {
        "IntraA": "intra",
        "IntraB": "intra",
        "Extralaboral": "extra",
        "Estres": "estres",
        "Afrontamiento": "afrontamiento",
        "CapPsico": "capitalpsicologico",
    }

    # Mapa: linea_gestion → set de sufijos de instrumento requeridos
    cat["_sufijo"] = cat["id_pregunta"].str.split("_").str[1:].str.join("_")
    lineas_req = (
        cat.dropna(subset=["linea_gestion", "_sufijo"])
        .groupby("linea_gestion")["_sufijo"]
        .apply(lambda s: set(s.unique()))
    )

    # Workers por empresa e instrumento (si tiene al menos 1 fila = instrumento disponible)
    cobertura = (
        df.groupby(["empresa", "instrumento"])["cedula"]
        .nunique()
        .unstack(fill_value=0)
    )
    log.info("Workers por empresa e instrumento:\n%s", cobertura.to_string())

    # Para cada empresa: qué sufijos tiene disponibles y qué lineas no puede calcular
    log.info("\nLineas incalculables por empresa (instrumento sin datos):")
    audit_cobertura = []
    for empresa in sorted(df["empresa"].unique()):
        # Sufijos disponibles en esta empresa (al menos 1 fila)
        df_emp = df[df["empresa"] == empresa]
        sufijos_disponibles = set(
            INSTR_SUFIJO[i]
            for i in df_emp["instrumento"].unique()
            if i in INSTR_SUFIJO and df_emp[df_emp["instrumento"] == i]["cedula"].nunique() > 0
        )
        lineas_faltantes = []
        for linea, reqs in lineas_req.items():
            if not reqs.issubset(sufijos_disponibles):
                faltantes = reqs - sufijos_disponibles
                lineas_faltantes.append(linea)
                audit_cobertura.append({
                    "empresa": empresa,
                    "linea_gestion": linea,
                    "instrumentos_faltantes": ", ".join(sorted(faltantes)),
                    "calculable": False,
                })
        if lineas_faltantes:
            log.warning("  [%s] Lineas sin calcular: %s", empresa, lineas_faltantes)
        else:
            log.info("  [%s] Todas las lineas calculables.", empresa)
        # Lineas calculables también al registro
        for linea, reqs in lineas_req.items():
            if reqs.issubset(sufijos_disponibles):
                audit_cobertura.append({
                    "empresa": empresa,
                    "linea_gestion": linea,
                    "instrumentos_faltantes": "",
                    "calculable": True,
                })

    df_cobertura = pd.DataFrame(audit_cobertura)
    cobertura_path = ROOT / "data" / "processed" / "audit_cobertura_empresas.csv"
    df_cobertura.to_csv(cobertura_path, index=False, encoding="utf-8-sig")
    log.info("Cobertura exportada: %s", cobertura_path)

    # ── 6. Guardar parquet intermedio ─────────────────────────────────────────
    columnas_salida = [
        "cedula", "nombre_trabajador", "empresa", "sector_economico",
        "forma_intra", "instrumento",
        "id_pregunta", "valor_invertido", "max_item_score", "valor_01",
        "dimension", "dominio", "factor",
    ]
    columnas_salida = [c for c in columnas_salida if c in df.columns]
    df_out = df[columnas_salida].copy()
    df_out = _sanitizar_tipos(df_out)

    out_path = ROOT / "data" / "processed" / "fact_gestion_01_estandarizado.parquet"
    df_out.to_parquet(out_path, index=False)
    log.info("\nGuardado parquet global: %s (%d filas)", out_path, len(df_out))

    # ── 7. Audit AUTOLUFER — CSV de validación ────────────────────────────────
    EMPRESA_AUDIT = "AUTOLUFER"
    df_audit = df_out[df_out["empresa"] == EMPRESA_AUDIT].copy()
    if df_audit.empty:
        log.warning("No se encontraron datos para empresa '%s'.", EMPRESA_AUDIT)
    else:
        # Pivot: cedula × id_pregunta → valor_01 (resumen matricial)
        pivot = (
            df_audit.pivot_table(
                index=["cedula", "nombre_trabajador", "instrumento"],
                columns="id_pregunta",
                values="valor_01",
                aggfunc="first",
            )
            .reset_index()
        )
        pivot_path = ROOT / "data" / "processed" / f"audit_{EMPRESA_AUDIT.lower()}_estandarizado.csv"
        pivot.to_csv(pivot_path, index=False, encoding="utf-8-sig")

        # Resumen por trabajador e instrumento
        resumen_audit = df_audit.groupby(["cedula", "nombre_trabajador", "instrumento"]).agg(
            n_preguntas=("id_pregunta", "nunique"),
            valor_01_min=("valor_01", "min"),
            valor_01_max=("valor_01", "max"),
            valor_01_mean=("valor_01", "mean"),
        ).round(4).reset_index()
        resumen_path = ROOT / "data" / "processed" / f"audit_{EMPRESA_AUDIT.lower()}_resumen.csv"
        resumen_audit.to_csv(resumen_path, index=False, encoding="utf-8-sig")

        log.info(
            "\n=== AUDIT %s ===\n%s workers, %d filas\nResumen:\n%s",
            EMPRESA_AUDIT,
            df_audit["cedula"].nunique(),
            len(df_audit),
            resumen_audit.to_string(index=False),
        )
        log.info("Audit detallado (pivot): %s", pivot_path)
        log.info("Audit resumen:           %s", resumen_path)

    log.info("=== Sub-paso 1 COMPLETADO ===")

    # ══════════════════════════════════════════════════════════════════════════
    # SUB-PASO 2 — JOIN + Inversión V2
    # ══════════════════════════════════════════════════════════════════════════
    log.info("\n=== 03_scoring_gestion.py — Sub-paso 2: JOIN + Inversión V2 ===")

    doc_path = ROOT / "docs" / "v2_gestion_saludmental.md"

    # ── 2.1 Parsear tabla de inversión V2 desde el doc ────────────────────────
    inv_tabla = _parsear_inversion_v2(doc_path)
    log.info(
        "Tabla inversión V2 parseada: %d filas (conflictos resueltos: última ocurrencia gana)",
        len(inv_tabla),
    )

    # ── 2.2 Parsear tabla de pesos por pregunta ───────────────────────────────
    pesos_pregunta = _parsear_pesos_pregunta(doc_path)
    log.info("Tabla pesos por pregunta parseada: %d filas", len(pesos_pregunta))

    # ── 2.3 Expandir categorias_analisis: A_y_B → filas para A y B ───────────
    cat_expandido = _expandir_forma_ayb(cat)
    log.info(
        "categorias_analisis expandido: %d filas (original: %d)",
        len(cat_expandido), len(cat),
    )

    # ── 2.4 Cargar resultado sub-paso 1 ──────────────────────────────────────
    sp1_path = ROOT / "data" / "processed" / "fact_gestion_01_estandarizado.parquet"
    df2 = pd.read_parquet(sp1_path).copy()

    # ── 2.5 Aplicar inversión V2 ──────────────────────────────────────────────
    # Lookup: (forma_intra, id_pregunta) → invertir_v2
    inv_lookup = inv_tabla.set_index(["forma_intra", "id_pregunta"])["invertir"].to_dict()
    df2["invertir_v2"] = df2.apply(
        lambda r: inv_lookup.get((r["forma_intra"], r["id_pregunta"]), False),
        axis=1,
    )
    df2["valor_01_inv"] = df2.apply(
        lambda r: (1.0 - r["valor_01"]) if r["invertir_v2"] else r["valor_01"],
        axis=1,
    )

    sin_inversion = df2["invertir_v2"].isna().sum()
    n_invertidos  = df2["invertir_v2"].sum()
    log.info(
        "Inversión V2 aplicada: %d preguntas invertidas, %d sin mapeo (se dejan como False)",
        n_invertidos, sin_inversion,
    )

    # ── 2.6 JOIN con categorias_analisis expandido ────────────────────────────
    cols_cat = ["id_pregunta", "forma_intra", "indicador", "linea_gestion",
                "eje_gestion", "protocolo_id", "protocolo_gestion"]
    cols_cat = [c for c in cols_cat if c in cat_expandido.columns]

    df2 = df2.merge(
        cat_expandido[cols_cat].drop_duplicates(subset=["id_pregunta", "forma_intra"]),
        on=["id_pregunta", "forma_intra"],
        how="left",
    )

    sin_indicador = df2["indicador"].isna().sum()
    if sin_indicador > 0:
        log.warning(
            "%d filas sin indicador tras JOIN (id_pregunta sin match en categorias_analisis):\n%s",
            sin_indicador,
            df2[df2["indicador"].isna()][["instrumento", "id_pregunta", "forma_intra"]].drop_duplicates().head(10).to_string(),
        )
    else:
        log.info("  -> JOIN categorias_analisis: sin filas huerfanas. OK.")

    # ── 2.7 JOIN pesos por pregunta ───────────────────────────────────────────
    df2 = df2.merge(
        pesos_pregunta[["id_pregunta", "forma_intra", "peso_pregunta"]].drop_duplicates(
            subset=["id_pregunta", "forma_intra"]
        ),
        on=["id_pregunta", "forma_intra"],
        how="left",
    )
    sin_peso = df2["peso_pregunta"].isna().sum()
    if sin_peso > 0:
        log.warning("%d filas sin peso_pregunta (se les asignará 1.0 como default).", sin_peso)
        df2["peso_pregunta"] = df2["peso_pregunta"].fillna(1.0)
    else:
        log.info("  -> Pesos por pregunta: todos asignados. OK.")

    # ── 2.8 Resumen de validación ─────────────────────────────────────────────
    log.info("\n=== VALIDACION SUB-PASO 2 ===")
    resumen_inv = (
        df2.groupby("instrumento")
        .agg(
            n_filas=("valor_01_inv", "count"),
            n_invertidas=("invertir_v2", "sum"),
            valor_inv_min=("valor_01_inv", "min"),
            valor_inv_max=("valor_01_inv", "max"),
            valor_inv_mean=("valor_01_inv", "mean"),
        )
        .round(4)
    )
    log.info("Estadisticas valor_01_inv por instrumento:\n%s", resumen_inv.to_string())

    log.info("\nIndicadores unicos por instrumento:")
    for instr in sorted(df2["instrumento"].unique()):
        n = df2[df2["instrumento"] == instr]["indicador"].nunique()
        log.info("  %s: %d indicadores", instr, n)

    # ── 2.9 Guardar parquet sub-paso 2 ────────────────────────────────────────
    cols_salida2 = [
        "cedula", "nombre_trabajador", "empresa", "sector_economico",
        "forma_intra", "instrumento",
        "id_pregunta", "valor_invertido", "max_item_score",
        "valor_01", "invertir_v2", "valor_01_inv", "peso_pregunta",
        "indicador", "linea_gestion", "eje_gestion", "protocolo_id", "protocolo_gestion",
    ]
    cols_salida2 = [c for c in cols_salida2 if c in df2.columns]
    df2_out = df2[cols_salida2].copy()
    df2_out = _sanitizar_tipos(df2_out)

    out2_path = ROOT / "data" / "processed" / "fact_gestion_02_invertido.parquet"
    df2_out.to_parquet(out2_path, index=False)
    log.info("\nGuardado parquet sub-paso 2: %s (%d filas, %d cols)", out2_path, len(df2_out), df2_out.shape[1])

    # ── 2.10 Audit AUTOLUFER sub-paso 2 ──────────────────────────────────────
    EMPRESA_AUDIT = "AUTOLUFER"
    df_audit2 = df2_out[df2_out["empresa"] == EMPRESA_AUDIT].copy()
    if not df_audit2.empty:
        pivot2 = (
            df_audit2.pivot_table(
                index=["cedula", "nombre_trabajador", "instrumento"],
                columns="id_pregunta",
                values="valor_01_inv",
                aggfunc="first",
            )
            .reset_index()
        )
        pivot2_path = ROOT / "data" / "processed" / f"audit_{EMPRESA_AUDIT.lower()}_invertido.csv"
        pivot2.to_csv(pivot2_path, index=False, encoding="utf-8-sig")

        resumen2 = (
            df_audit2.groupby(["cedula", "nombre_trabajador", "instrumento", "linea_gestion"])
            .agg(
                n_preguntas=("id_pregunta", "nunique"),
                n_invertidas=("invertir_v2", "sum"),
                valor_inv_mean=("valor_01_inv", "mean"),
            )
            .round(4)
            .reset_index()
        )
        resumen2_path = ROOT / "data" / "processed" / f"audit_{EMPRESA_AUDIT.lower()}_invertido_resumen.csv"
        resumen2.to_csv(resumen2_path, index=False, encoding="utf-8-sig")

        log.info(
            "\n=== AUDIT %s — Sub-paso 2 ===\n%d workers, %d filas\nResumen por linea (primeros 20):\n%s",
            EMPRESA_AUDIT,
            df_audit2["cedula"].nunique(),
            len(df_audit2),
            resumen2.head(20).to_string(index=False),
        )
        log.info("Audit pivot:   %s", pivot2_path)
        log.info("Audit resumen: %s", resumen2_path)

    log.info("=== Sub-paso 2 COMPLETADO ===")

    # ══════════════════════════════════════════════════════════════════════════
    # SUB-PASO 3 — Calificación ponderada: preguntas → indicadores → líneas → ejes
    # ══════════════════════════════════════════════════════════════════════════
    log.info("\n=== 03_scoring_gestion.py — Sub-paso 3: Calificación ponderada ===")

    tablas = _parsear_tablas_sp3(doc_path)
    df_sp2 = pd.read_parquet(out2_path)

    # ── 3.1 Preguntas → Indicadores ──────────────────────────────────────────
    # score_indicador = Σ(valor_01_inv × peso_pregunta)
    # Los pesos por pregunta dentro de cada indicador suman 1.0,
    # por lo que la suma ponderada ya queda expresada en escala 0-1.
    # NO se divide por denominador (haría eso incorrectamente).
    df_num = df_sp2.copy()
    df_num["wv"] = df_num["valor_01_inv"] * df_num["peso_pregunta"]

    scores_ind = (
        df_num.groupby(["cedula", "nombre_trabajador", "empresa", "sector_economico",
                        "forma_intra", "indicador", "linea_gestion", "eje_gestion",
                        "protocolo_id", "protocolo_gestion"])
        .agg(suma_wv=("wv", "sum"), n_preguntas=("valor_01_inv", "count"))
        .reset_index()
    )
    scores_ind["score_indicador"] = scores_ind["suma_wv"]

    nan_ind = scores_ind["score_indicador"].isna().sum()
    log.info("Indicadores calculados: %d filas | NaN: %d", len(scores_ind), nan_ind)
    log.info("Indicadores únicos con score: %d de %d esperados",
             scores_ind["indicador"].nunique(),
             tablas["ind_linea"]["indicador"].nunique())

    # ── 3.2 Indicadores → Líneas ──────────────────────────────────────────────
    # 3.2a Unir pesos e inversión de indicadores (tabla 3.2)
    ind_lin_exp = _expandir_forma(tablas["ind_linea"], "forma_cat")
    scores_ind = scores_ind.merge(
        ind_lin_exp[["forma_intra", "indicador", "peso_ind", "inv_ind"]].drop_duplicates(
            subset=["forma_intra", "indicador"]
        ),
        on=["forma_intra", "indicador"], how="left",
    )
    sin_peso_ind = scores_ind["peso_ind"].isna().sum()
    if sin_peso_ind:
        log.warning("%d indicadores sin peso_ind (excluidos del cálculo de línea).", sin_peso_ind)

    # 3.2b Inversión a nivel de indicador
    scores_ind["score_ind_inv"] = scores_ind.apply(
        lambda r: (1.0 - r["score_indicador"]) if r["inv_ind"] else r["score_indicador"],
        axis=1,
    )

    # 3.2c Agregar a línea: promedio ponderado normalizado por suma real de pesos disponibles
    scores_ind["wv_ind"] = scores_ind["score_ind_inv"] * scores_ind["peso_ind"]
    scores_lin = (
        scores_ind.dropna(subset=["wv_ind"])
        .groupby(["cedula", "nombre_trabajador", "empresa", "sector_economico",
                  "forma_intra", "linea_gestion", "eje_gestion"])
        .agg(suma_wv_ind=("wv_ind", "sum"), suma_pesos_ind=("peso_ind", "sum"))
        .reset_index()
    )
    scores_lin["score_linea"] = scores_lin["suma_wv_ind"] / scores_lin["suma_pesos_ind"]

    nan_lin = scores_lin["score_linea"].isna().sum()
    log.info("Líneas calculadas: %d filas | NaN: %d | Líneas únicas: %d",
             len(scores_lin), nan_lin, scores_lin["linea_gestion"].nunique())

    # ── 3.3 Líneas → Ejes ─────────────────────────────────────────────────────
    scores_lin = scores_lin.merge(
        tablas["lin_eje"][["linea_gestion", "peso_linea", "eje_gestion"]].drop_duplicates("linea_gestion"),
        on="linea_gestion", how="left", suffixes=("", "_doc"),
    )
    # Si eje_gestion viene de categorias_analisis y de la tabla 3.3, preferir la del doc
    if "eje_gestion_doc" in scores_lin.columns:
        scores_lin["eje_gestion"] = scores_lin["eje_gestion_doc"].fillna(scores_lin["eje_gestion"])
        scores_lin.drop(columns=["eje_gestion_doc"], inplace=True)

    sin_peso_lin = scores_lin["peso_linea"].isna().sum()
    if sin_peso_lin:
        log.warning("%d líneas sin peso_linea (excluidas del eje).", sin_peso_lin)

    scores_lin["wv_lin"] = scores_lin["score_linea"] * scores_lin["peso_linea"]
    scores_eje = (
        scores_lin.dropna(subset=["wv_lin"])
        .groupby(["cedula", "nombre_trabajador", "empresa", "sector_economico", "eje_gestion"])
        .agg(suma_wv_lin=("wv_lin", "sum"), suma_pesos_lin=("peso_linea", "sum"))
        .reset_index()
    )
    scores_eje["score_eje"] = scores_eje["suma_wv_lin"] / scores_eje["suma_pesos_lin"]

    log.info("Ejes calculados: %d filas | Ejes únicos: %d", len(scores_eje), scores_eje["eje_gestion"].nunique())

    # ── 3.4 Guardar parquets de resultados ────────────────────────────────────
    out_ind = ROOT / "data" / "processed" / "fact_gestion_indicadores.parquet"
    out_lin = ROOT / "data" / "processed" / "fact_gestion_lineas.parquet"
    out_eje = ROOT / "data" / "processed" / "fact_gestion_ejes.parquet"

    cols_ind = ["cedula", "nombre_trabajador", "empresa", "sector_economico", "forma_intra",
                "indicador", "linea_gestion", "eje_gestion", "protocolo_id", "protocolo_gestion",
                "score_indicador", "score_ind_inv", "n_preguntas"]
    cols_lin = ["cedula", "nombre_trabajador", "empresa", "sector_economico", "forma_intra",
                "linea_gestion", "eje_gestion", "score_linea", "suma_pesos_ind"]
    cols_eje = ["cedula", "nombre_trabajador", "empresa", "sector_economico",
                "eje_gestion", "score_eje", "suma_pesos_lin"]

    _sanitizar_tipos(scores_ind[[c for c in cols_ind if c in scores_ind.columns]]).to_parquet(out_ind, index=False)
    _sanitizar_tipos(scores_lin[[c for c in cols_lin if c in scores_lin.columns]]).to_parquet(out_lin, index=False)
    _sanitizar_tipos(scores_eje[[c for c in cols_eje if c in scores_eje.columns]]).to_parquet(out_eje, index=False)
    log.info("Guardados: indicadores=%d | lineas=%d | ejes=%d filas", len(scores_ind), len(scores_lin), len(scores_eje))

    # ── 3.5 Audit AUTOLUFER sub-paso 3 ───────────────────────────────────────
    EMPRESA_AUDIT = "AUTOLUFER"

    df_lin_audit = scores_lin[scores_lin["empresa"] == EMPRESA_AUDIT].copy()
    df_eje_audit = scores_eje[scores_eje["empresa"] == EMPRESA_AUDIT].copy()

    if not df_lin_audit.empty:
        resumen_lin = (
            df_lin_audit.groupby(["linea_gestion"])
            .agg(n_workers=("cedula", "nunique"),
                 score_min=("score_linea", "min"),
                 score_max=("score_linea", "max"),
                 score_mean=("score_linea", "mean"))
            .round(4).reset_index()
        )
        resumen_eje = (
            df_eje_audit.groupby(["eje_gestion"])
            .agg(n_workers=("cedula", "nunique"),
                 score_min=("score_eje", "min"),
                 score_max=("score_eje", "max"),
                 score_mean=("score_eje", "mean"))
            .round(4).reset_index()
        )
        audit_lin_path = ROOT / "data" / "processed" / f"audit_{EMPRESA_AUDIT.lower()}_lineas.csv"
        audit_eje_path = ROOT / "data" / "processed" / f"audit_{EMPRESA_AUDIT.lower()}_ejes.csv"
        df_lin_audit.sort_values(["cedula","linea_gestion"]).to_csv(audit_lin_path, index=False, encoding="utf-8-sig")
        df_eje_audit.sort_values(["cedula","eje_gestion"]).to_csv(audit_eje_path, index=False, encoding="utf-8-sig")

        log.info("\n=== AUDIT %s — Sub-paso 3: Lineas ===\n%s",
                 EMPRESA_AUDIT, resumen_lin.to_string(index=False))
        log.info("\n=== AUDIT %s — Sub-paso 3: Ejes ===\n%s",
                 EMPRESA_AUDIT, resumen_eje.to_string(index=False))
        log.info("Audit lineas: %s", audit_lin_path)
        log.info("Audit ejes:   %s", audit_eje_path)

    log.info("=== Sub-paso 3 COMPLETADO ===")


# ══════════════════════════════════════════════════════════════════════════════
# FUNCIONES DE PARSEO DE TABLAS DEL DOC V2
# ══════════════════════════════════════════════════════════════════════════════

def _parsear_inversion_v2(doc_path: Path) -> pd.DataFrame:
    """
    Parsea la tabla de inversión V2 del doc v2_gestion_saludmental.md.
    Formato: ## |forma_intra|id_pregunta|TRUE/FALSE|
    Estrategia de conflicto: última ocurrencia gana (3 conflictos conocidos en el doc).

    CORRECCIÓN aplicada en código: la tabla de inversión del doc NO fue actualizada
    cuando se corrigió el swap afrontamiento/capitalpsicologico en la sección de categorias.
    Los ítems 5, 7 y 8 tenían TRUE en _capitalpsicologico (evitativo/negación) que ahora
    corresponden a _afrontamiento. Se intercambian los flags TRUE/FALSE para estos 3 ítems.

    Devuelve DataFrame con (forma_intra, id_pregunta, invertir) — expandido: A_y_B → A y B.
    """
    with open(doc_path, encoding="utf-8") as f:
        lines = f.readlines()

    rows = []
    patron = re.compile(r"\|\s*(A_y_B|A|B)\s*\|\s*(\d+_\w+)\s*\|\s*(TRUE|FALSE)\s*\|")
    for line in lines:
        m = patron.search(line)
        if m:
            rows.append({
                "forma_intra_cat": m.group(1),
                "id_pregunta":     m.group(2),
                "invertir":        m.group(3) == "TRUE",
            })

    df = pd.DataFrame(rows)
    # Última ocurrencia gana en caso de conflicto
    df = df.drop_duplicates(subset=["forma_intra_cat", "id_pregunta"], keep="last")

    # CORRECCIÓN: ítems 5, 6, 7, 8 de afrontamiento deben ser invertidos (TRUE).
    # Los 4 pertenecen a la dimensión "Afrontamiento evitativo_negación":
    #   5=Autonegación, 6=Renuncia, 7=Evitación cognitiva, 8=Evitación conductual.
    # El doc tiene 6/7/8 correctos pero 5_afrontamiento=FALSE — se corrige aquí.
    forzar_true = {"5_afrontamiento"}
    df.loc[df["id_pregunta"].isin(forzar_true), "invertir"] = True

    # Expandir A_y_B → filas para A y B
    ayb = df[df["forma_intra_cat"] == "A_y_B"].copy()
    solo_a = df[df["forma_intra_cat"] == "A"].copy()
    solo_b = df[df["forma_intra_cat"] == "B"].copy()

    ayb_a = ayb.copy(); ayb_a["forma_intra"] = "A"
    ayb_b = ayb.copy(); ayb_b["forma_intra"] = "B"
    solo_a["forma_intra"] = "A"
    solo_b["forma_intra"] = "B"

    resultado = pd.concat([ayb_a, ayb_b, solo_a, solo_b], ignore_index=True)
    # Para una misma (forma_intra, id_pregunta), dar preferencia a la entrada específica A/B sobre A_y_B
    resultado = resultado.sort_values("forma_intra_cat").drop_duplicates(
        subset=["forma_intra", "id_pregunta"], keep="last"
    )
    return resultado[["forma_intra", "id_pregunta", "invertir"]].reset_index(drop=True)


def _corregir_categorias_afro_cappsico(cat: pd.DataFrame, doc_path: Path, out_path: Path) -> pd.DataFrame:
    """
    Reconstruye las 24 filas de afrontamiento y capitalpsicologico en categorias_analisis
    a partir de la sección de categorias del doc v2 actualizado.
    Actualiza in-place el parquet para que todos los scripts downstream sean consistentes.
    """
    with open(doc_path, encoding="utf-8") as f:
        lines = f.readlines()

    rows = []
    pat = re.compile(
        r"##\s*\|(\S+)\|(A_y_B|A|B)\|([^|]+)\|([^|]+)\|([^|]+)\|([^|]+)\|(PROT-\d+)\|([^|]+)\|([^|]+)\|([^#|]+)"
    )
    for line in lines:
        m = pat.search(line)
        if m:
            rows.append({
                "id_pregunta":       m.group(1),
                "forma_intra":       m.group(2),
                "factor":            m.group(3).strip(),
                "dimension":         m.group(4).strip(),
                "indicador":         m.group(6).strip(),
                "protocolo_id":      m.group(7),
                "protocolo_gestion": m.group(8).strip(),
                "linea_gestion":     m.group(9).strip(),
                "eje_gestion":       m.group(10).strip(),
            })

    df_doc = pd.DataFrame(rows)
    mask_doc = df_doc["id_pregunta"].str.contains("afrontamiento|capitalpsicologico", na=False)
    df_afro_cap_new = df_doc[mask_doc].copy()

    # Columnas a actualizar (las que cambiaron con el swap)
    cols_actualizar = ["factor", "dimension", "indicador", "protocolo_id", "protocolo_gestion",
                       "linea_gestion", "eje_gestion"]

    # Separar filas afectadas y no afectadas en el parquet existente
    mask_cat = cat["id_pregunta"].str.contains("afrontamiento|capitalpsicologico", na=False)
    cat_resto = cat[~mask_cat].copy()

    # Merge: tomar el parquet existente y actualizar las columnas que cambiaron
    cat_afro_cap = cat[mask_cat].copy()
    cat_afro_cap = cat_afro_cap.drop(columns=[c for c in cols_actualizar if c in cat_afro_cap.columns])
    cat_afro_cap = cat_afro_cap.merge(
        df_afro_cap_new[["id_pregunta", "forma_intra"] + cols_actualizar],
        on=["id_pregunta", "forma_intra"],
        how="left",
    )

    cat_corregido = pd.concat([cat_resto, cat_afro_cap], ignore_index=True)
    cat_corregido = _sanitizar_tipos(cat_corregido)
    cat_corregido.to_parquet(out_path, index=False)

    n_actualizadas = mask_cat.sum()
    log.info(
        "  -> categorias_analisis: %d filas afro/cappsico actualizadas (factor, dimension, indicador)",
        n_actualizadas,
    )
    # Verificar resultado
    muestra = cat_corregido[cat_corregido["id_pregunta"].str.contains("afrontamiento|capitalpsicologico", na=False)]
    log.info(
        "  Muestra afro/cappsico corregida:\n%s",
        muestra[["id_pregunta", "indicador", "dimension"]].sort_values("id_pregunta").to_string(index=False),
    )
    return cat_corregido


def _parsear_pesos_pregunta(doc_path: Path) -> pd.DataFrame:
    """
    Parsea la tabla de pesos por pregunta del doc V2.
    Formato: ## A_y_B|id_pregunta|peso|indicador ##
    Devuelve DataFrame con (forma_intra, id_pregunta, peso_pregunta, indicador)
    expandido: A_y_B → A y B.
    """
    with open(doc_path, encoding="utf-8") as f:
        lines = f.readlines()

    rows = []
    patron = re.compile(r"##\s*(A_y_B|A|B)\s*\|\s*(\d+_\w+)\s*\|\s*([0-9.]+)\s*\|\s*(.+?)\s*##")
    for line in lines:
        m = patron.search(line)
        if m:
            rows.append({
                "forma_intra_cat": m.group(1),
                "id_pregunta":     m.group(2),
                "peso_pregunta":   float(m.group(3)),
                "indicador":       m.group(4).strip(),
            })

    df = pd.DataFrame(rows)
    df = df.drop_duplicates(subset=["forma_intra_cat", "id_pregunta"], keep="last")

    ayb = df[df["forma_intra_cat"] == "A_y_B"].copy()
    solo_a = df[df["forma_intra_cat"] == "A"].copy()
    solo_b = df[df["forma_intra_cat"] == "B"].copy()

    ayb_a = ayb.copy(); ayb_a["forma_intra"] = "A"
    ayb_b = ayb.copy(); ayb_b["forma_intra"] = "B"
    solo_a["forma_intra"] = "A"
    solo_b["forma_intra"] = "B"

    resultado = pd.concat([ayb_a, ayb_b, solo_a, solo_b], ignore_index=True)
    resultado = resultado.sort_values("forma_intra_cat").drop_duplicates(
        subset=["forma_intra", "id_pregunta"], keep="last"
    )
    return resultado[["forma_intra", "id_pregunta", "peso_pregunta", "indicador"]].reset_index(drop=True)


def _expandir_forma_ayb(cat: pd.DataFrame) -> pd.DataFrame:
    """
    Expande filas de categorias_analisis donde forma_intra='A_y_B' en dos filas:
    una con forma_intra='A' y otra con forma_intra='B'.
    Las filas ya marcadas como 'A' o 'B' se conservan intactas.
    """
    ayb  = cat[cat["forma_intra"] == "A_y_B"].copy()
    resto = cat[cat["forma_intra"] != "A_y_B"].copy()

    ayb_a = ayb.copy(); ayb_a["forma_intra"] = "A"
    ayb_b = ayb.copy(); ayb_b["forma_intra"] = "B"

    return pd.concat([resto, ayb_a, ayb_b], ignore_index=True)


def _expandir_forma(df: pd.DataFrame, col: str) -> pd.DataFrame:
    """
    Expande filas donde df[col] es 'A_y_B' (o 'AyB') en dos filas: 'A' y 'B'.
    Normaliza 'AyB' -> 'A_y_B' antes de expandir.
    Si col != 'forma_intra', renombra la columna a 'forma_intra' en el resultado.
    """
    df = df.copy()
    df[col] = df[col].str.replace("AyB", "A_y_B", regex=False)
    ayb = df[df[col] == "A_y_B"].copy()
    resto = df[df[col] != "A_y_B"].copy()
    ayb_a = ayb.copy(); ayb_a[col] = "A"
    ayb_b = ayb.copy(); ayb_b[col] = "B"
    expanded = pd.concat([resto, ayb_a, ayb_b], ignore_index=True)
    if col != "forma_intra":
        expanded = expanded.rename(columns={col: "forma_intra"})
    return expanded


def _parsear_tablas_sp3(doc_path: Path) -> dict:
    """
    Parsea del doc V2 las 5 tablas del sub-paso 3:
      denom_ind  — denominador por indicador      (forma_cat, indicador, denom_ind)
      ind_linea  — indicador->linea: pesos/inv    (forma_cat, indicador, inv_ind, peso_ind, linea_gestion)
      denom_lin  — denominador por linea          (forma_cat, linea_gestion, denom_lin)  [no usado en cálculo]
      lin_eje    — linea->eje: pesos              (linea_gestion, peso_linea, eje_gestion)
      denom_eje  — denominador por eje            (eje_gestion, denom_eje)               [no usado en cálculo]

    Estrategia: detección de sección por marcadores únicos en los headers de tabla,
    luego parseo de cada línea de datos con regex específico para esa sección.
    """
    with open(doc_path, encoding="utf-8") as f:
        lines = f.readlines()

    # Marcadores únicos presentes en los encabezados de columna de cada tabla del doc
    SEC_DENOM_IND = "denominador_al_indicador"
    SEC_32        = "3.2 Calificar indicadores"
    SEC_DENOM_LIN = "denominador_a_lineadegestion"
    SEC_33        = "3.3 Calificar ejes"
    SEC_DENOM_EJE = "denominador_a_eje_gestion"

    # Patrones de datos por sección
    # denom_ind: ## A_y_B|indicador|número
    pat_denom_ind = re.compile(r"## (A_y_B|A|B)\|(.+)\|(\d+)")
    # ind_linea: ## AyB|indicador|TRUE/FALSE|peso|linea|prioridad
    pat_ind_linea = re.compile(r"## (AyB|A|B)\|(.+?)\|(TRUE|FALSE)\|([0-9.]+)\|(.+?)\|(Alta|Media|Baja)")
    # denom_lin: ## AyB|linea|número
    pat_denom_lin = re.compile(r"## (AyB|A|B)\|(.+)\|(\d+)")
    # lin_eje: ## linea|peso|eje_gestion|prioridad  (eje anclado a nombres conocidos)
    _EJES = "|".join([
        "Bienestar biopsicosocial",
        "Condiciones de trabajo saludable",
        "Entorno y clima de trabajo saludable",
    ])
    pat_lin_eje = re.compile(rf"## (.+?)\|([0-9.]+)\|({_EJES})\|")
    # denom_eje: ## eje_gestion|número  (al final del doc, líneas muy cortas)
    pat_denom_eje = re.compile(r"## (.+)\|(\d+)\s*$")

    rows_denom_ind: list[dict] = []
    rows_ind_linea: list[dict] = []
    rows_denom_lin: list[dict] = []
    rows_lin_eje:   list[dict] = []
    rows_denom_eje: list[dict] = []
    section = None

    for line in lines:
        # --- Detección de sección (en orden de aparición en el doc) ---
        if SEC_DENOM_IND in line:
            section = "denom_ind"; continue
        if SEC_32 in line:
            section = "ind_linea"; continue
        if SEC_DENOM_LIN in line:
            section = "denom_lin"; continue
        if SEC_33 in line:
            section = "lin_eje"; continue
        if SEC_DENOM_EJE in line:
            section = "denom_eje"; continue

        if section is None or not line.strip().startswith("##"):
            continue

        # --- Parseo según sección activa ---
        if section == "denom_ind":
            m = pat_denom_ind.search(line)
            if m:
                rows_denom_ind.append({
                    "forma_cat": m.group(1),
                    "indicador": m.group(2).strip(),
                    "denom_ind": int(m.group(3)),
                })

        elif section == "ind_linea":
            m = pat_ind_linea.search(line)
            if m:
                rows_ind_linea.append({
                    "forma_cat":     m.group(1),
                    "indicador":     m.group(2).strip(),
                    "inv_ind":       m.group(3) == "TRUE",
                    "peso_ind":      float(m.group(4)),
                    "linea_gestion": m.group(5).strip(),
                })

        elif section == "denom_lin":
            m = pat_denom_lin.search(line)
            if m:
                rows_denom_lin.append({
                    "forma_cat":     m.group(1),
                    "linea_gestion": m.group(2).strip(),
                    "denom_lin":     int(m.group(3)),
                })

        elif section == "lin_eje":
            m = pat_lin_eje.search(line)
            if m:
                rows_lin_eje.append({
                    "linea_gestion": m.group(1).strip(),
                    "peso_linea":    float(m.group(2)),
                    "eje_gestion":   m.group(3).strip(),
                })

        elif section == "denom_eje":
            m = pat_denom_eje.search(line)
            if m:
                # Saltar la línea de encabezado (valor no numérico en grupo 2)
                try:
                    denom_val = int(m.group(2))
                except ValueError:
                    continue
                rows_denom_eje.append({
                    "eje_gestion": m.group(1).strip(),
                    "denom_eje":   denom_val,
                })

    denom_ind = pd.DataFrame(rows_denom_ind)
    ind_linea = pd.DataFrame(rows_ind_linea)
    denom_lin = pd.DataFrame(rows_denom_lin)
    lin_eje   = pd.DataFrame(rows_lin_eje)
    denom_eje = pd.DataFrame(rows_denom_eje)

    if not denom_ind.empty:
        denom_ind = denom_ind.drop_duplicates(subset=["forma_cat", "indicador"], keep="last")
    if not ind_linea.empty:
        ind_linea = ind_linea.drop_duplicates(subset=["forma_cat", "indicador"], keep="last")
        # Normalizar nombre con tilde faltante en el doc (retribucion -> retribución)
        ind_linea["indicador"] = ind_linea["indicador"].str.replace(
            "Expectativa en la retribucion", "Expectativa en la retribución", regex=False
        )
    if not denom_lin.empty:
        denom_lin = denom_lin.drop_duplicates(subset=["forma_cat", "linea_gestion"], keep="last")
    if not lin_eje.empty:
        lin_eje = lin_eje.drop_duplicates(subset=["linea_gestion"], keep="last")
    if not denom_eje.empty:
        denom_eje = denom_eje.drop_duplicates(subset=["eje_gestion"], keep="last")

    log.info(
        "Tablas SP3 parseadas: denom_ind=%d | ind_linea=%d | denom_lin=%d | lin_eje=%d | denom_eje=%d",
        len(denom_ind), len(ind_linea), len(denom_lin), len(lin_eje), len(denom_eje),
    )
    return {
        "denom_ind": denom_ind,
        "ind_linea": ind_linea,
        "denom_lin": denom_lin,
        "lin_eje":   lin_eje,
        "denom_eje": denom_eje,
    }


if __name__ == "__main__":
    main()
