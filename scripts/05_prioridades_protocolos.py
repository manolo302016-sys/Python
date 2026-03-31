"""
05_prioridades_protocolos.py
============================
ETL Visualizador 2 — Gestión de Salud Mental.

Paso 5: Priorización de protocolos de gestión.

  5.1  Para cada empresa: calcular % trabajadores en "Intervencion correctiva"
       o "Intervencion Urgente" por protocolo (vía línea_gestion).
  5.2  Consultar protocolos prioritarios del sector económico de la empresa.
  5.3  Generar ranking final de protocolos: combina urgencia clínica (resultado)
       con exigencia legal (sector). Score final = pct_critico × 0.7 + rango_sector × 0.3.
  5.4  Guardar tabla de prioridades por empresa.

Outputs:
  data/processed/fact_gestion_05_prioridades_protocolos.parquet
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
# Mapa sector -> protocolos prioritarios por orden legal/técnico
# ══════════════════════════════════════════════════════════════════════════════
PRIORIDAD_SECTOR: dict[str, list[str]] = {
    "Salud":              ["PROT-04", "PROT-05", "PROT-13", "PROT-10"],
    "Educacion":          ["PROT-18", "PROT-13", "PROT-02", "PROT-11"],
    "Admon. Publica":     ["PROT-17", "PROT-11", "PROT-19", "PROT-20"],
    "Comercio/Financiero":["PROT-09", "PROT-15", "PROT-20", "PROT-07"],
    "Comercio":           ["PROT-09", "PROT-15", "PROT-20", "PROT-07"],
    "Construccion":       ["PROT-08", "PROT-14", "PROT-16", "PROT-10"],
    "Servicios":          ["PROT-09", "PROT-18", "PROT-13", "PROT-03"],
    "Manufactura":        ["PROT-14", "PROT-08", "PROT-15", "PROT-16"],
    "Transporte":         ["PROT-16", "PROT-08", "PROT-14", "PROT-09"],
    "Minas/Energia":      ["PROT-08", "PROT-14", "PROT-10", "PROT-05"],
    "Agricultura":        ["PROT-08", "PROT-06", "PROT-07", "PROT-16"],
}

NIVELES_CRITICOS = {"Intervencion correctiva", "Intervencion Urgente"}


def _normalizar_sector(sector: str) -> str:
    """Normaliza el sector de la empresa para matchear PRIORIDAD_SECTOR."""
    s = sector.strip()
    # Coincidencia exacta primero
    if s in PRIORIDAD_SECTOR:
        return s
    # Coincidencia por inicio (ej. "Comercio y retail" -> "Comercio")
    s_lower = s.lower()
    for key in PRIORIDAD_SECTOR:
        if key.lower() in s_lower or s_lower in key.lower():
            return key
    return None


def _sanitizar_tipos(df: pd.DataFrame) -> pd.DataFrame:
    for col in df.select_dtypes(include=["object", "string"]).columns:
        df[col] = df[col].astype(str).where(df[col].notna(), None)
    return df


def main() -> None:
    log.info("=== 05_prioridades_protocolos.py — Paso 5: Priorizacion de protocolos ===")

    path_lin  = ROOT / "data" / "processed" / "fact_gestion_04_niveles_lineas.parquet"
    path_prot = ROOT / "data" / "processed" / "dim_protocolos_lineas.parquet"

    df_lin  = pd.read_parquet(path_lin)
    df_prot = pd.read_parquet(path_prot)

    log.info("Lineas cargadas: %d | Protocolos dim: %d", len(df_lin), len(df_prot))

    # ── 5.1 % trabajadores en nivel critico por empresa × linea_gestion ───────
    df_lin["es_critico"] = df_lin["nivel_gestion"].isin(NIVELES_CRITICOS)

    critico_lin = (
        df_lin.groupby(["empresa", "sector_economico", "linea_gestion"])
        .agg(
            n_total=("cedula", "count"),
            n_critico=("es_critico", "sum"),
            score_linea_mean=("score_linea", "mean"),
            nivel_predominante=("nivel_gestion", lambda x: x.value_counts().index[0]),
        )
        .reset_index()
    )
    critico_lin["pct_critico"] = (critico_lin["n_critico"] / critico_lin["n_total"] * 100).round(1)

    # ── 5.2 Unir protocolos por línea ─────────────────────────────────────────
    # dim_protocolos_lineas tiene protocolo_id por linea_gestion
    prot_por_linea = df_prot[["linea_gestion", "protocolo_id", "protocolo_nombre",
                               "objetivo", "resultado_esperado"]].drop_duplicates()

    ranking = critico_lin.merge(prot_por_linea, on="linea_gestion", how="left")

    # Agregar % crítico por protocolo (una línea puede tener varios protocolos)
    # Usar el máximo pct_critico de las líneas que comparten protocolo
    ranking_prot = (
        ranking.groupby(["empresa", "sector_economico", "protocolo_id",
                         "protocolo_nombre", "objetivo", "resultado_esperado"])
        .agg(
            pct_critico_max=("pct_critico", "max"),
            pct_critico_mean=("pct_critico", "mean"),
            score_linea_mean=("score_linea_mean", "mean"),
            n_lineas=("linea_gestion", "nunique"),
        )
        .round(3)
        .reset_index()
    )

    # ── 5.3 Rango de prioridad sectorial ─────────────────────────────────────
    def _rango_sector(row):
        sector_norm = _normalizar_sector(row["sector_economico"])
        if sector_norm is None:
            return 0.0
        prots_sector = PRIORIDAD_SECTOR.get(sector_norm, [])
        if row["protocolo_id"] in prots_sector:
            # 1=mas prioritario -> normalizar a [0,1] invertido (pos 1 = 1.0)
            pos = prots_sector.index(row["protocolo_id"]) + 1
            return round(1 - (pos - 1) / len(prots_sector), 3)
        return 0.0

    ranking_prot["rango_sector"] = ranking_prot.apply(_rango_sector, axis=1)
    ranking_prot["es_prioritario_sector"] = ranking_prot["rango_sector"] > 0

    # Score combinado: 70% urgencia clínica + 30% exigencia sectorial
    ranking_prot["score_prioridad"] = (
        ranking_prot["pct_critico_max"] / 100 * 0.7 +
        ranking_prot["rango_sector"] * 0.3
    ).round(4)

    ranking_prot = ranking_prot.sort_values(
        ["empresa", "score_prioridad"], ascending=[True, False]
    ).reset_index(drop=True)

    # Rango ordinal dentro de cada empresa
    ranking_prot["rango_empresa"] = (
        ranking_prot.groupby("empresa")["score_prioridad"]
        .rank(method="min", ascending=False)
        .astype(int)
    )

    # ── 5.4 Guardar ───────────────────────────────────────────────────────────
    out_path = ROOT / "data" / "processed" / "fact_gestion_05_prioridades_protocolos.parquet"
    _sanitizar_tipos(ranking_prot).to_parquet(out_path, index=False)
    log.info("Guardado: %s (%d filas)", out_path.name, len(ranking_prot))

    # ── Log resumen por empresa ────────────────────────────────────────────────
    log.info("\n=== RESUMEN PRIORIDADES POR EMPRESA (top 5) ===")
    for empresa in sorted(ranking_prot["empresa"].unique()):
        top5 = ranking_prot[ranking_prot["empresa"] == empresa].head(5)
        sector = top5["sector_economico"].iloc[0]
        log.info("\n  [%s] Sector: %s", empresa, sector)
        for _, r in top5.iterrows():
            sector_tag = " (*sector*)" if r["es_prioritario_sector"] else ""
            log.info("    #%d %s — %s | pct_critico=%.1f%% | score=%.3f%s",
                     r["rango_empresa"], r["protocolo_id"], r["protocolo_nombre"],
                     r["pct_critico_max"], r["score_prioridad"], sector_tag)

    log.info("\n=== Paso 5 COMPLETADO ===")


if __name__ == "__main__":
    main()
