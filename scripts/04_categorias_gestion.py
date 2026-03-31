"""
04_categorias_gestion.py
========================
ETL Visualizador 2 — Gestión de Salud Mental.

Paso 4: Calificar según puntos de corte los ejes y líneas de gestión.

  4.1  Asignar nivel_gestion a cada trabajador × eje y trabajador × línea,
       usando los puntos de corte del doc V2.
  4.2  Etiqueta e interpretación de cada nivel.
  4.3  Relación línea ↔ protocolos de gestión (tabla de referencia).

Escala (mayor = mejor gestión):
  > 0.79  → Gestión prorrogable
  0.65 – 0.79 → Gestión preventiva
  0.45 – 0.65 → Gestión de mejora selectiva
  0.29 – 0.45 → Intervención correctiva
  ≤ 0.29      → Intervención Urgente

Fuentes:
  data/processed/fact_gestion_indicadores.parquet
  data/processed/fact_gestion_lineas.parquet
  data/processed/fact_gestion_ejes.parquet

Outputs:
  data/processed/fact_gestion_04_niveles_indicadores.parquet
  data/processed/fact_gestion_04_niveles_lineas.parquet
  data/processed/fact_gestion_04_niveles_ejes.parquet
  data/processed/fact_gestion_04_resumen_empresa_ejes.parquet
  data/processed/fact_gestion_04_resumen_empresa_lineas.parquet
  data/processed/dim_protocolos_lineas.parquet
"""

import logging
import re
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════════════════════
# Puntos de corte y etiquetas (doc V2, Paso 4.1 y 4.2)
# Escala invertida: mayor score = mejor gestión
# ══════════════════════════════════════════════════════════════════════════════

# (limite_inferior_exclusivo, limite_superior_inclusivo, nivel, etiqueta, enfoque)
NIVELES = [
    (0.79, 1.00, "Gestion prorrogable",       "Promocion",
     "Reforzar y mantener factores protectores y controles actuales."),
    (0.65, 0.79, "Gestion preventiva",         "Educacion y prevencion",
     "Actuar desde actividades de formacion y capacitacion para desarrollar pautas de autocuidado."),
    (0.45, 0.65, "Gestion de mejora selectiva","Ajuste y mejora",
     "Intervencion en indicadores especificos que puntuaron alto para mejorar focos de riesgo."),
    (0.29, 0.45, "Intervencion correctiva",    "Control correctivo",
     "Implementacion de intervenciones mediante protocolos estructurados para mitigar factores de riesgo."),
    (0.00, 0.29, "Intervencion Urgente",       "Intervencion urgente",
     "Intervencion dentro de los 6 meses siguientes sobre lineas de alta prioridad."),
]

# Mapa ordenado: orden numérico para sorting (1=mejor, 5=peor)
NIVEL_ORDEN = {
    "Gestion prorrogable":        1,
    "Gestion preventiva":         2,
    "Gestion de mejora selectiva":3,
    "Intervencion correctiva":    4,
    "Intervencion Urgente":       5,
}


def _asignar_nivel(score: float) -> tuple[str, str, str]:
    """Retorna (nivel_gestion, etiqueta_gestion, enfoque_gestion) para un score."""
    for lo, hi, nivel, etiqueta, enfoque in NIVELES:
        if lo < score <= hi:
            return nivel, etiqueta, enfoque
    # score exactamente 0 o NaN
    if pd.isna(score):
        return None, None, None
    return "Intervencion Urgente", "Intervencion urgente", NIVELES[-1][4]


def _aplicar_niveles(df: pd.DataFrame, col_score: str) -> pd.DataFrame:
    """Añade columnas nivel_gestion, etiqueta_gestion, enfoque_gestion, orden_nivel."""
    resultados = df[col_score].apply(_asignar_nivel)
    df["nivel_gestion"]   = resultados.apply(lambda x: x[0])
    df["etiqueta_gestion"]= resultados.apply(lambda x: x[1])
    df["enfoque_gestion"] = resultados.apply(lambda x: x[2])
    df["orden_nivel"]     = df["nivel_gestion"].map(NIVEL_ORDEN)
    return df


def _parsear_protocolos(doc_path: Path) -> pd.DataFrame:
    """
    Parsea la tabla 4.3 del doc: eje_gestion | linea_gestion | prot_id | protocolo_nombre | objetivo | resultado_esperado
    """
    with open(doc_path, encoding="utf-8") as f:
        lines = f.readlines()

    # Detectar sección 4.3
    in_section = False
    rows = []
    pat = re.compile(
        r"##\s*([^|]+)\|\s*([^|]+)\|\s*(PROT-\d+)\|\s*([^|]+)\|\s*([^|]+)\|\s*(.+)"
    )
    for line in lines:
        if "4.3" in line and "relacion" in line.lower():
            in_section = True
            continue
        if in_section and "Paso 5" in line:
            break
        if in_section:
            m = pat.search(line)
            if m:
                rows.append({
                    "eje_gestion":       m.group(1).strip(),
                    "linea_gestion":     m.group(2).strip(),
                    "protocolo_id":      m.group(3).strip(),
                    "protocolo_nombre":  m.group(4).strip(),
                    "objetivo":          m.group(5).strip(),
                    "resultado_esperado":m.group(6).strip(),
                })
    return pd.DataFrame(rows)


def _resumen_empresa(df: pd.DataFrame, col_grupo: str, col_score: str) -> pd.DataFrame:
    """
    Genera resumen por empresa × grupo (eje o línea):
    - score_mean, score_min, score_max
    - n_trabajadores
    - conteo y % por nivel de gestión
    """
    agg = (
        df.groupby(["empresa", col_grupo])
        .agg(
            n_trabajadores=(col_score, "count"),
            score_mean=(col_score, "mean"),
            score_min=(col_score, "min"),
            score_max=(col_score, "max"),
        )
        .round(4)
        .reset_index()
    )

    # Conteo por nivel
    niveles_count = (
        df.groupby(["empresa", col_grupo, "nivel_gestion"])
        .size()
        .reset_index(name="n_nivel")
    )
    total_por_grupo = df.groupby(["empresa", col_grupo]).size().reset_index(name="n_total")
    niveles_count = niveles_count.merge(total_por_grupo, on=["empresa", col_grupo])
    niveles_count["pct_nivel"] = (niveles_count["n_nivel"] / niveles_count["n_total"] * 100).round(1)

    # Pivot de niveles como columnas
    pivot_n = niveles_count.pivot_table(
        index=["empresa", col_grupo],
        columns="nivel_gestion",
        values="pct_nivel",
        aggfunc="sum",
        fill_value=0.0,
    ).reset_index()
    # Renombrar columnas nivel a pct_<nivel>
    pivot_n.columns.name = None
    pivot_n = pivot_n.rename(columns={
        n: f"pct_{n.lower().replace(' ', '_')}"
        for n in NIVEL_ORDEN.keys()
        if n in pivot_n.columns
    })

    resultado = agg.merge(pivot_n, on=["empresa", col_grupo], how="left")

    # Agregar nivel_predominante (el que tiene mayor % en la empresa × grupo)
    nivel_pred = (
        df.groupby(["empresa", col_grupo])["nivel_gestion"]
        .agg(lambda x: x.value_counts().index[0])
        .reset_index(name="nivel_predominante")
    )
    resultado = resultado.merge(nivel_pred, on=["empresa", col_grupo], how="left")
    resultado["orden_nivel_predominante"] = resultado["nivel_predominante"].map(NIVEL_ORDEN)

    return resultado.sort_values(["empresa", "orden_nivel_predominante", col_grupo])


def _sanitizar_tipos(df: pd.DataFrame) -> pd.DataFrame:
    for col in df.select_dtypes(include=["object", "string"]).columns:
        df[col] = df[col].astype(str).where(df[col].notna(), None)
    return df


def main() -> None:
    log.info("=== 04_categorias_gestion.py — Paso 4: Niveles de gestión ===")

    doc_path  = ROOT / "docs" / "v2_gestion_saludmental.md"
    path_ind  = ROOT / "data" / "processed" / "fact_gestion_indicadores.parquet"
    path_lin  = ROOT / "data" / "processed" / "fact_gestion_lineas.parquet"
    path_eje  = ROOT / "data" / "processed" / "fact_gestion_ejes.parquet"

    df_ind = pd.read_parquet(path_ind)
    df_lin = pd.read_parquet(path_lin)
    df_eje = pd.read_parquet(path_eje)

    log.info("Cargados: indicadores=%d | lineas=%d | ejes=%d filas",
             len(df_ind), len(df_lin), len(df_eje))

    # ── 4.1 Asignar niveles ────────────────────────────────────────────────────
    log.info("\n--- 4.1 Asignando niveles de gestion ---")

    df_ind = _aplicar_niveles(df_ind.copy(), "score_ind_inv")
    df_lin = _aplicar_niveles(df_lin.copy(), "score_linea")
    df_eje = _aplicar_niveles(df_eje.copy(), "score_eje")

    # Validación
    for nombre, df, col in [
        ("Indicadores", df_ind, "nivel_gestion"),
        ("Lineas",      df_lin, "nivel_gestion"),
        ("Ejes",        df_eje, "nivel_gestion"),
    ]:
        nan_n = df[col].isna().sum()
        dist = df[col].value_counts().to_dict()
        log.info("  %s: NaN=%d | Distribución: %s", nombre, nan_n, dist)

    # ── 4.2 Resúmenes por empresa ──────────────────────────────────────────────
    log.info("\n--- 4.2 Resumenes por empresa ---")

    resumen_eje = _resumen_empresa(df_eje, "eje_gestion", "score_eje")
    resumen_lin = _resumen_empresa(df_lin, "linea_gestion", "score_linea")

    log.info("Resumen ejes por empresa:\n%s",
             resumen_eje[["empresa","eje_gestion","n_trabajadores","score_mean","nivel_predominante"]]
             .to_string(index=False))

    # ── 4.3 Parsear tabla de protocolos ───────────────────────────────────────
    log.info("\n--- 4.3 Tabla de protocolos por linea ---")
    df_prot = _parsear_protocolos(doc_path)
    log.info("Protocolos parseados: %d registros | %d protocolos unicos",
             len(df_prot), df_prot["protocolo_id"].nunique())

    # Unir nivel predominante de cada empresa por línea al mapa de protocolos
    prot_con_nivel = df_prot.merge(
        resumen_lin[["empresa","linea_gestion","score_mean","nivel_predominante","orden_nivel_predominante"]],
        on="linea_gestion", how="left",
    )

    # ── Guardar outputs ────────────────────────────────────────────────────────
    out_ind     = ROOT / "data" / "processed" / "fact_gestion_04_niveles_indicadores.parquet"
    out_lin     = ROOT / "data" / "processed" / "fact_gestion_04_niveles_lineas.parquet"
    out_eje     = ROOT / "data" / "processed" / "fact_gestion_04_niveles_ejes.parquet"
    out_res_eje = ROOT / "data" / "processed" / "fact_gestion_04_resumen_empresa_ejes.parquet"
    out_res_lin = ROOT / "data" / "processed" / "fact_gestion_04_resumen_empresa_lineas.parquet"
    out_prot    = ROOT / "data" / "processed" / "dim_protocolos_lineas.parquet"

    _sanitizar_tipos(df_ind).to_parquet(out_ind, index=False)
    _sanitizar_tipos(df_lin).to_parquet(out_lin, index=False)
    _sanitizar_tipos(df_eje).to_parquet(out_eje, index=False)
    _sanitizar_tipos(resumen_eje).to_parquet(out_res_eje, index=False)
    _sanitizar_tipos(resumen_lin).to_parquet(out_res_lin, index=False)
    _sanitizar_tipos(df_prot).to_parquet(out_prot, index=False)

    log.info("\nOutputs guardados:")
    for p in [out_ind, out_lin, out_eje, out_res_eje, out_res_lin, out_prot]:
        log.info("  %s", p.name)

    # ── Validación de rango de scores post-nivel ───────────────────────────────
    log.info("\n=== VALIDACION FINAL ===")
    for nombre, df, col_score in [
        ("Indicadores", df_ind, "score_ind_inv"),
        ("Lineas",      df_lin, "score_linea"),
        ("Ejes",        df_eje, "score_eje"),
    ]:
        fuera = df[(df[col_score] < 0) | (df[col_score] > 1.0001)]
        log.info("  %s: rango [%.4f, %.4f] | fuera de [0,1]: %d",
                 nombre, df[col_score].min(), df[col_score].max(), len(fuera))

    log.info("=== Paso 4 COMPLETADO ===")


if __name__ == "__main__":
    main()
