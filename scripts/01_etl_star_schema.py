"""
01_etl_star_schema.py
=====================
ETL base — Carga y valida las tablas fuente, aplica homologación de sector
y exporta fact_respuestas_clean.parquet + todas las dim_*.parquet.

Prerequisito de todos los scripts de scoring (02a, 02b, 06, 07).

Reglas aplicadas:
  R1  — PK triple: cedula + forma_intra + id_pregunta
  R6  — LEFT JOIN en ausentismo (dim tiene ~17 filas, no INNER)
  R7  — empresa='ASIGNAR' es real, no filtrar
  R13 — Outputs solo en Parquet
  R15 — fact_respuestas original NO se modifica; se crea _clean
  R16 — 2 archivos de datos separados (fact vs dims)
  R17 — sector_economico → sector_rag (homologación obligatoria)
  R18 — Escalabilidad: funciona para N empresas
"""

import logging
import sys
from pathlib import Path

import pandas as pd
import yaml

# ── Rutas ──────────────────────────────────────────────────────────────────
ROOT = Path(__file__).resolve().parents[1]
CONFIG_PATH = ROOT / "config" / "config.yaml"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)


# ── Homologación sector (R17) ───────────────────────────────────────────────
# Mapeo sector_economico (tal como llega en raw) → sector_rag estándar ENCST.
# Los valores destino deben coincidir exactamente con las claves de benchmark_sector en config.yaml.
# Estándar para cualquier dataset futuro — agregar aliases según se detecten variantes.
SECTOR_MAP: dict[str, str] = {
    # Variantes comunes → valor canónico
    "Comercio":                         "Comercio/financiero",
    "Comercio y financiero":            "Comercio/financiero",
    "Financiero":                       "Comercio/financiero",
    "Comercio/Financiero":              "Comercio/financiero",
    "Minas":                            "Minas y canteras",
    "Minería":                          "Minas y canteras",
    "Explotación minas y canteras":     "Minas y canteras",
    "Explotación de minas y canteras":  "Minas y canteras",
    "Administración Pública":           "Administración pública",
    "Administracion publica":           "Administración pública",
    "Administración publica":           "Administración pública",
    "Administracion Publica":           "Administración pública",
    "Salud y bienestar":                "Salud",
    "Salud humana":                     "Salud",
    "Actividades de salud":             "Salud",
    "Enseñanza":                        "Educación",
    "Educacion":                        "Educación",
    "Agricultura, ganadería":           "Agricultura",
    "Agricultura, ganaderia, caza":     "Agricultura",
    "Agropecuario":                     "Agricultura",
    "Construccion":                     "Construcción",
    # Valores canónicos (identidad — por si llegan bien escritos)
    "Agricultura":                      "Agricultura",
    "Manufactura":                      "Manufactura",
    "Servicios":                        "Servicios",
    "Construcción":                     "Construcción",
    "Transporte":                       "Transporte",
    "Educación":                        "Educación",
    "Salud":                            "Salud",
    "Minas y canteras":                 "Minas y canteras",
    "Administración pública":           "Administración pública",
}

# Valores canónicos reconocidos (sector_rag final válido)
SECTORES_VALIDOS: set[str] = {
    "Agricultura", "Manufactura", "Servicios", "Construcción",
    "Comercio/financiero", "Transporte", "Minas y canteras",
    "Administración pública", "Educación", "Salud",
}


def homologar_sector(df: pd.DataFrame, col: str = "sector_economico") -> pd.DataFrame:
    """Mapea sector_economico → sector_rag estándar (R17).

    Cualquier valor no reconocido → 'No clasificado' con advertencia.
    """
    df = df.copy()
    df["sector_rag"] = df[col].map(
        lambda s: SECTOR_MAP.get(str(s).strip(), SECTOR_MAP.get(str(s).strip().title(), str(s).strip()))
    )
    sectores_nuevos = set(df["sector_rag"].unique()) - SECTORES_VALIDOS - {"No clasificado"}
    if sectores_nuevos:
        log.warning("Sectores no reconocidos → sector_rag='No clasificado': %s", sectores_nuevos)
        df.loc[df["sector_rag"].isin(sectores_nuevos), "sector_rag"] = "No clasificado"
    log.info("sector_rag distribucion:\n%s",
             df["sector_rag"].value_counts().to_string())
    return df


# ── Carga de tablas ─────────────────────────────────────────────────────────
def cargar_config() -> dict:
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def cargar_fact_respuestas(cfg: dict) -> pd.DataFrame:
    path = ROOT / cfg["paths"]["raw_fact"]
    sheet = cfg["paths"]["sheet_fact"]
    log.info("Cargando fact_respuestas desde %s [%s]", path, sheet)
    df = pd.read_excel(path, sheet_name=sheet, dtype={"cedula": str, "id_respuesta": str})
    log.info("  → %d filas × %d columnas cargadas", *df.shape)
    return df


def cargar_dims(cfg: dict) -> dict[str, pd.DataFrame]:
    path = ROOT / cfg["paths"]["raw_dims"]
    sheets = {
        "dim_trabajador":      cfg["paths"]["sheet_trabajador"],
        "dim_pregunta":        cfg["paths"]["sheet_pregunta"],    # puede ser null
        "dim_respuesta":       cfg["paths"]["sheet_respuesta"],   # puede ser null
        "dim_baremo":          cfg["paths"]["sheet_baremo"],      # puede ser null
        "dim_demografia":      cfg["paths"]["sheet_demografia"],
        "dim_ausentismo":      cfg["paths"]["sheet_ausentismo"],
        "categorias_analisis": cfg["paths"]["sheet_categorias"],
    }
    dims = {}
    for nombre, hoja in sheets.items():
        if not hoja:
            log.warning("Hoja '%s' no configurada (null) — se omite", nombre)
            continue
        log.info("Cargando %s [hoja: %s]", nombre, hoja)
        dims[nombre] = pd.read_excel(path, sheet_name=hoja, dtype={"cedula": str})
        log.info("  → %d filas", len(dims[nombre]))
    return dims


# ── Validación fact_respuestas ──────────────────────────────────────────────
def validar_fact_respuestas(df: pd.DataFrame) -> tuple[bool, pd.DataFrame]:
    """R14 — Validación obligatoria. Retorna (es_valido, reporte_errores)."""
    errores = []

    # Columnas obligatorias (id_respuesta: nulos son ítems sin respuesta → warning, no error)
    cols_req_criticas = ["cedula", "forma_intra", "id_pregunta", "empresa", "sector_economico"]
    for col in cols_req_criticas:
        nulos = df[col].isna().sum()
        if nulos > 0:
            errores.append({"check": f"nulos_{col}", "n": nulos})
    # id_respuesta: nulos legítimos (ítem no respondido) — se descartarán en limpieza
    n_sin_respuesta = df["id_respuesta"].isna().sum()
    if n_sin_respuesta > 0:
        log.warning("id_respuesta nulo en %d filas (%.2f%%) — se descartan antes de scoring",
                    n_sin_respuesta, 100 * n_sin_respuesta / len(df))

    # PK sin duplicados (R1)
    dup = df.duplicated(subset=["cedula", "forma_intra", "id_pregunta"]).sum()
    if dup > 0:
        errores.append({"check": "pk_duplicada", "n": dup})

    # forma_intra solo 'A' o 'B'
    formas_invalidas = df[~df["forma_intra"].isin(["A", "B"])].shape[0]
    if formas_invalidas > 0:
        errores.append({"check": "forma_intra_invalida", "n": formas_invalidas})

    # Volumen mínimo razonable
    if len(df) < 1000:
        errores.append({"check": "volumen_bajo", "n": len(df)})

    reporte = pd.DataFrame(errores) if errores else pd.DataFrame(columns=["check", "n"])
    es_valido = len(errores) == 0
    return es_valido, reporte


# ── Limpieza básica ─────────────────────────────────────────────────────────
def limpiar_fact(df: pd.DataFrame) -> pd.DataFrame:
    """
    Limpieza estándar:
    - Strip de strings en columnas clave
    - Normalizar forma_intra a mayúsculas
    - Homologar sector (R17)
    """
    df = df.copy()

    # Descartar filas sin respuesta (ítems no respondidos — no tienen valor para scoring)
    n_antes = len(df)
    df = df.dropna(subset=["id_respuesta"])
    n_descartados = n_antes - len(df)
    if n_descartados > 0:
        log.warning("Descartadas %d filas con id_respuesta nulo (%.2f%%)",
                    n_descartados, 100 * n_descartados / n_antes)

    str_cols = ["cedula", "forma_intra", "empresa", "sector_economico", "id_respuesta"]
    for col in str_cols:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()

    # Normalizar forma_intra → 'A' | 'B'
    df["forma_intra"] = df["forma_intra"].str.upper()

    # Normalizar dias_ausencia: reemplazar 'Sin dato' y no-numéricos por 0
    if "dias_ausencia" in df.columns:
        df["dias_ausencia"] = pd.to_numeric(df["dias_ausencia"], errors="coerce").fillna(0).astype(int)

    # Homologar sector (R17)
    df = homologar_sector(df, col="sector_economico")

    return df


# ── Exportar parquets ───────────────────────────────────────────────────────
def _sanitizar_tipos(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convierte columnas object con tipos mixtos a tipos compatibles con parquet.
    Si >=50% de los valores no-nulos son numéricos, convierte toda la columna
    (los no-convertibles → NaN). Maneja 'Sin dato', 'Revisar', etc. sin lista.
    """
    df = df.copy()
    for col in df.columns:
        if df[col].dtype == object:
            convertido = pd.to_numeric(df[col], errors="coerce")
            n_total = df[col].notna().sum()
            n_convertido = convertido.notna().sum()
            if n_total > 0 and (n_convertido / n_total) >= 0.5:
                n_perdidos = n_total - n_convertido
                if n_perdidos > 0:
                    log.warning("Columna '%s': %d valor(es) no numérico(s) → NaN", col, n_perdidos)
                df[col] = convertido
            else:
                # Columna de texto — convertir a StringDtype nullable para compatibilidad pyarrow
                df[col] = df[col].astype(pd.StringDtype())
    return df

def guardar_parquet(df: pd.DataFrame, nombre: str, cfg: dict) -> Path:
    """Guarda DataFrame como parquet en data/processed/ (R13)."""
    out_dir = ROOT / cfg["paths"]["processed"]
    out_dir.mkdir(parents=True, exist_ok=True)
    ruta = out_dir / f"{nombre}.parquet"
    df_limpio = _sanitizar_tipos(df)
    df_limpio.to_parquet(ruta, index=False)
    log.info("Guardado: %s (%d filas)", ruta, len(df))
    return ruta


# ── Main ────────────────────────────────────────────────────────────────────
def main():
    log.info("=" * 60)
    log.info("SCRIPT 01 — ETL Star Schema")
    log.info("=" * 60)

    cfg = cargar_config()

    # 1. Cargar datos fuente
    fact_raw = cargar_fact_respuestas(cfg)
    dims = cargar_dims(cfg)

    # 2. Limpiar y homologar fact
    fact_clean = limpiar_fact(fact_raw)

    # 3. Validar (R14)
    es_valido, reporte = validar_fact_respuestas(fact_clean)
    if not es_valido:
        log.error("Validación FALLIDA. Errores encontrados:")
        log.error("\n%s", reporte.to_string())
        sys.exit(1)
    log.info("Validación OK — fact_respuestas_clean sin errores")

    # 4. Resumen por empresa y forma
    resumen = (
        fact_clean.groupby(["empresa", "forma_intra"])["cedula"]
        .nunique()
        .reset_index(name="n_trabajadores")
    )
    log.info("Resumen por empresa y forma:\n%s", resumen.to_string(index=False))

    # 5. Guardar fact_respuestas_clean
    guardar_parquet(fact_clean, "fact_respuestas_clean", cfg)

    # 6. Guardar todas las dimensiones
    for nombre, df_dim in dims.items():
        guardar_parquet(df_dim, nombre, cfg)

    log.info("=" * 60)
    log.info("ETL completado. Parquets disponibles en data/processed/")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
