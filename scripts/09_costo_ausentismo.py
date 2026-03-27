"""
09_costo_ausentismo.py — Implementa V4-Pasos 1-6: Costo del Ausentismo

Calcula el impacto económico del riesgo psicosocial en ausentismo.

REGLA CRÍTICA (BINDING D5):
  Costo total = Costo directo × 1.40 (factor presentismo 40%)

Pasos:
  V4-Paso1: Cargar datos de ausentismo (días perdidos por trabajador)
  V4-Paso2: Calcular costo diario por trabajador (salario / 30)
  V4-Paso3: Calcular costo directo (días × costo_diario)
  V4-Paso4: Aplicar factor presentismo (× 1.40)
  V4-Paso5: Agregar por empresa, área, causa
  V4-Paso6: Proyectar impacto anual

Input:
  - data/processed/fact_benchmark.parquet
  - data/raw/dim_ausentismo.parquet (días perdidos)
  - data/raw/dim_trabajador.parquet (salarios)

Output:
  - data/final/costo_ausentismo.parquet
  - data/final/costo_por_causa.parquet

Fuente documental: Visualizador 4, Pasos 1-6
Versión: 1.0 | Pipeline MentalPRO | Modelo AVANTUM
"""

import logging
import sys
from pathlib import Path
from typing import Tuple, Dict, List
import numpy as np
import pandas as pd
import yaml

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("08_costo_ausentismo")


def cargar_config(config_path: str = "config/config.yaml") -> dict:
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


# ===========================================================================
# CONSTANTES — BINDING D5
# ===========================================================================

# Factor de presentismo: por cada $1 de ausentismo directo,
# hay $0.40 adicionales de productividad perdida (presentismo)
FACTOR_PRESENTISMO = 1.40  # BINDING D5: NUNCA MODIFICAR

# Días laborales por mes (para cálculo de costo diario)
DIAS_LABORALES_MES = 30

# Días laborales por año (para proyección anual)
DIAS_LABORALES_ANIO = 240

# Causas de ausentismo relacionadas con riesgo psicosocial
CAUSAS_PSICOSOCIALES = [
    "Estrés laboral",
    "Ansiedad",
    "Depresión",
    "Burnout",
    "Trastorno adaptativo",
    "Conflicto laboral",
    "Acoso laboral",
    "Fatiga crónica",
    "Trastorno del sueño",
    "Cefalea tensional",
]

# Mapeo causa -> categoría para reportes
CATEGORIA_CAUSA = {
    "Estrés laboral": "Salud mental",
    "Ansiedad": "Salud mental",
    "Depresión": "Salud mental",
    "Burnout": "Salud mental",
    "Trastorno adaptativo": "Salud mental",
    "Conflicto laboral": "Clima organizacional",
    "Acoso laboral": "Clima organizacional",
    "Fatiga crónica": "Condiciones físicas",
    "Trastorno del sueño": "Condiciones físicas",
    "Cefalea tensional": "Condiciones físicas",
    "Enfermedad general": "Otras causas",
    "Accidente laboral": "Otras causas",
    "Licencia maternidad": "Licencias",
    "Licencia paternidad": "Licencias",
    "Cita médica": "Otras causas",
}


# ===========================================================================
# V4-PASO 2: CÁLCULO DE COSTO DIARIO
# ===========================================================================

def calcular_costo_diario(salario_mensual: float) -> float:
    """
    Calcula el costo diario de un trabajador.
    
    Fórmula: salario_mensual / DIAS_LABORALES_MES
    
    Args:
        salario_mensual: Salario mensual bruto del trabajador
    
    Returns:
        Costo diario del trabajador
    """
    if pd.isna(salario_mensual) or salario_mensual <= 0:
        return 0.0
    return salario_mensual / DIAS_LABORALES_MES


# ===========================================================================
# V4-PASO 3-4: CÁLCULO DE COSTO CON PRESENTISMO
# ===========================================================================

def calcular_costo_ausentismo(
    dias_perdidos: float,
    costo_diario: float,
    aplicar_presentismo: bool = True
) -> Dict[str, float]:
    """
    Calcula el costo total del ausentismo.
    
    BINDING D5: Costo total = Costo directo × 1.40
    
    Args:
        dias_perdidos: Número de días de ausencia
        costo_diario: Costo diario del trabajador
        aplicar_presentismo: Si True, aplica factor 1.40
    
    Returns:
        dict con costo_directo, costo_presentismo, costo_total
    """
    costo_directo = dias_perdidos * costo_diario
    
    if aplicar_presentismo:
        costo_total = costo_directo * FACTOR_PRESENTISMO
        costo_presentismo = costo_total - costo_directo
    else:
        costo_total = costo_directo
        costo_presentismo = 0.0
    
    return {
        "costo_directo": round(costo_directo, 2),
        "costo_presentismo": round(costo_presentismo, 2),
        "costo_total": round(costo_total, 2),
    }


# ===========================================================================
# V4-PASO 5: AGREGACIÓN POR DIMENSIONES
# ===========================================================================

def agregar_por_empresa(df_costos: pd.DataFrame) -> pd.DataFrame:
    """Agrega costos a nivel empresa."""
    aggs = {
        "dias_perdidos": "sum",
        "costo_directo": "sum",
        "costo_presentismo": "sum",
        "costo_total": "sum",
        "cedula": "nunique",
    }
    
    df_agg = df_costos.groupby(["empresa", "sector_rag"]).agg(aggs).reset_index()
    df_agg = df_agg.rename(columns={"cedula": "n_trabajadores_ausentes"})
    
    # Calcular métricas derivadas
    df_agg["dias_promedio_por_trabajador"] = (
        df_agg["dias_perdidos"] / df_agg["n_trabajadores_ausentes"]
    ).round(1)
    df_agg["costo_promedio_por_trabajador"] = (
        df_agg["costo_total"] / df_agg["n_trabajadores_ausentes"]
    ).round(2)
    
    return df_agg


def agregar_por_causa(df_costos: pd.DataFrame) -> pd.DataFrame:
    """Agrega costos por causa de ausentismo."""
    if "causa" not in df_costos.columns:
        return pd.DataFrame()
    
    aggs = {
        "dias_perdidos": "sum",
        "costo_total": "sum",
        "cedula": "nunique",
    }
    
    df_agg = df_costos.groupby(["empresa", "causa"]).agg(aggs).reset_index()
    df_agg = df_agg.rename(columns={"cedula": "n_casos"})
    
    # Agregar categoría
    df_agg["categoria_causa"] = df_agg["causa"].map(CATEGORIA_CAUSA).fillna("Otras causas")
    
    # Marcar si es causa psicosocial
    df_agg["es_psicosocial"] = df_agg["causa"].isin(CAUSAS_PSICOSOCIALES)
    
    return df_agg


def agregar_por_nivel_riesgo(df_costos: pd.DataFrame) -> pd.DataFrame:
    """Agrega costos por nivel de riesgo psicosocial."""
    if "nivel_riesgo" not in df_costos.columns:
        return pd.DataFrame()
    
    aggs = {
        "dias_perdidos": "sum",
        "costo_total": "sum",
        "cedula": "nunique",
    }
    
    df_agg = df_costos.groupby(["empresa", "nivel_riesgo"]).agg(aggs).reset_index()
    df_agg = df_agg.rename(columns={"cedula": "n_trabajadores"})
    
    return df_agg


# ===========================================================================
# V4-PASO 6: PROYECCIÓN ANUAL
# ===========================================================================

def proyectar_costo_anual(
    df_empresa: pd.DataFrame,
    meses_datos: int = 12
) -> pd.DataFrame:
    """
    Proyecta el costo anual basado en datos históricos.
    
    Args:
        df_empresa: DataFrame con costos agregados por empresa
        meses_datos: Número de meses de datos disponibles
    
    Returns:
        DataFrame con proyección anual
    """
    df_proy = df_empresa.copy()
    
    # Factor de proyección
    factor_anual = 12 / meses_datos if meses_datos > 0 else 1
    
    df_proy["costo_anual_proyectado"] = (df_proy["costo_total"] * factor_anual).round(2)
    df_proy["dias_anuales_proyectados"] = (df_proy["dias_perdidos"] * factor_anual).round(1)
    
    return df_proy


def calcular_roi_intervencion(
    costo_actual: float,
    reduccion_esperada_pct: float = 20,
    costo_intervencion: float = 0
) -> Dict[str, float]:
    """
    Calcula el ROI potencial de una intervención.
    
    Args:
        costo_actual: Costo total actual del ausentismo
        reduccion_esperada_pct: % de reducción esperada (default 20%)
        costo_intervencion: Costo de implementar la intervención
    
    Returns:
        dict con ahorro_esperado, roi, payback_meses
    """
    ahorro_esperado = costo_actual * (reduccion_esperada_pct / 100)
    
    if costo_intervencion > 0:
        roi = ((ahorro_esperado - costo_intervencion) / costo_intervencion) * 100
        payback_meses = (costo_intervencion / (ahorro_esperado / 12)) if ahorro_esperado > 0 else float('inf')
    else:
        roi = float('inf') if ahorro_esperado > 0 else 0
        payback_meses = 0
    
    return {
        "ahorro_esperado": round(ahorro_esperado, 2),
        "roi_pct": round(roi, 1) if roi != float('inf') else None,
        "payback_meses": round(payback_meses, 1) if payback_meses != float('inf') else None,
    }


# ===========================================================================
# PROCESAMIENTO PRINCIPAL
# ===========================================================================

def procesar_costos_ausentismo(
    df_benchmark: pd.DataFrame,
    df_ausentismo: pd.DataFrame,
    df_trabajador: pd.DataFrame
) -> pd.DataFrame:
    """
    Procesa y calcula costos de ausentismo por trabajador.
    
    1. Merge datos de ausentismo con trabajadores (salario)
    2. Calcula costo diario
    3. Calcula costo total con presentismo
    4. Enriquece con nivel de riesgo psicosocial
    """
    log.info("Procesando costos de ausentismo...")
    
    # Merge ausentismo con trabajador para obtener salario
    df = df_ausentismo.merge(
        df_trabajador[["cedula", "empresa", "salario_mensual", "cargo", "area"]],
        on=["cedula", "empresa"],
        how="left"
    )
    
    # Calcular costo diario
    df["costo_diario"] = df["salario_mensual"].apply(calcular_costo_diario)
    
    # Calcular costos
    costos = df.apply(
        lambda row: calcular_costo_ausentismo(
            row.get("dias_perdidos", 0),
            row["costo_diario"]
        ),
        axis=1
    )
    df_costos = pd.DataFrame(costos.tolist())
    df = pd.concat([df, df_costos], axis=1)
    
    # Enriquecer con nivel de riesgo del trabajador
    df_riesgo = df_benchmark[
        (df_benchmark["factor"] == "intralaboral") &
        (df_benchmark["dimension"].isna() | (df_benchmark["dimension"] == ""))
    ][["cedula", "empresa", "nivel_riesgo_colombia", "sector_rag"]].drop_duplicates()
    
    df = df.merge(
        df_riesgo.rename(columns={"nivel_riesgo_colombia": "nivel_riesgo"}),
        on=["cedula", "empresa"],
        how="left"
    )
    
    log.info("  Procesados %d registros de ausentismo", len(df))
    return df


# ===========================================================================
# VALIDACIÓN
# ===========================================================================

def validar_costos(df: pd.DataFrame) -> Tuple[bool, List[str]]:
    """Valida cálculos de costos."""
    log.info("Validando costos...")
    errores = []
    
    # 1. No hay costos negativos
    if (df["costo_total"] < 0).any():
        errores.append("Costos negativos encontrados")
    
    # 2. Factor presentismo aplicado correctamente
    tolerancia = 0.01
    df_check = df[df["costo_directo"] > 0]
    if len(df_check) > 0:
        ratio = df_check["costo_total"] / df_check["costo_directo"]
        if not ((ratio >= FACTOR_PRESENTISMO - tolerancia) & (ratio <= FACTOR_PRESENTISMO + tolerancia)).all():
            errores.append(f"Factor presentismo incorrecto (esperado {FACTOR_PRESENTISMO})")
    
    # 3. Días perdidos consistentes
    if (df["dias_perdidos"] < 0).any():
        errores.append("Días perdidos negativos")
    
    es_valido = len(errores) == 0
    if es_valido:
        log.info("Validación EXITOSA")
    else:
        log.error("Validación FALLIDA: %s", errores)
    return es_valido, errores


# ===========================================================================
# PIPELINE PRINCIPAL
# ===========================================================================

def main(config_path: str = "config/config.yaml") -> None:
    """
    Pipeline principal 08.
    1. Carga fact_benchmark, dim_ausentismo, dim_trabajador
    2. Calcula costos por trabajador (con presentismo ×1.40)
    3. Agrega por empresa, causa, nivel de riesgo
    4. Proyecta costo anual
    5. Guarda outputs
    """
    config = cargar_config(config_path)
    raw_path = Path(config["paths"]["raw"])
    proc_path = Path(config["paths"]["processed"])
    final_path = Path(config["paths"]["final"])
    final_path.mkdir(parents=True, exist_ok=True)
    
    log.info("=" * 60)
    log.info("08_costo_ausentismo.py — Iniciando pipeline")
    log.info("FACTOR PRESENTISMO: %.2f (BINDING D5)", FACTOR_PRESENTISMO)
    log.info("=" * 60)
    
    # --- Carga
    ruta_benchmark = proc_path / "fact_benchmark.parquet"
    ruta_ausentismo = raw_path / "dim_ausentismo.parquet"
    ruta_trabajador = raw_path / "dim_trabajador.parquet"
    
    if not ruta_benchmark.exists():
        log.error("Archivo no encontrado: %s", ruta_benchmark)
        sys.exit(1)
    
    df_benchmark = pd.read_parquet(ruta_benchmark)
    
    # Verificar si existen datos de ausentismo
    if ruta_ausentismo.exists() and ruta_trabajador.exists():
        df_ausentismo = pd.read_parquet(ruta_ausentismo)
        df_trabajador = pd.read_parquet(ruta_trabajador)
        log.info("Cargados: benchmark=%d, ausentismo=%d, trabajador=%d",
                 len(df_benchmark), len(df_ausentismo), len(df_trabajador))
    else:
        log.warning("Datos de ausentismo no encontrados, generando estructura vacía")
        df_ausentismo = pd.DataFrame(columns=["cedula", "empresa", "dias_perdidos", "causa", "fecha"])
        df_trabajador = pd.DataFrame(columns=["cedula", "empresa", "salario_mensual", "cargo", "area"])
    
    # --- Procesar
    if len(df_ausentismo) > 0:
        df_costos = procesar_costos_ausentismo(df_benchmark, df_ausentismo, df_trabajador)
        
        # --- Validación
        ok, _ = validar_costos(df_costos)
        if not ok:
            log.warning("Pipeline 08 con advertencias")
        
        # --- Agregar
        df_empresa = agregar_por_empresa(df_costos)
        df_causa = agregar_por_causa(df_costos)
        df_nivel = agregar_por_nivel_riesgo(df_costos)
        
        # --- Proyección anual
        df_empresa = proyectar_costo_anual(df_empresa, meses_datos=12)
        
        # --- Guardar
        df_costos.to_parquet(final_path / "costo_ausentismo_detalle.parquet", index=False)
        df_empresa.to_parquet(final_path / "costo_ausentismo.parquet", index=False)
        if len(df_causa) > 0:
            df_causa.to_parquet(final_path / "costo_por_causa.parquet", index=False)
        if len(df_nivel) > 0:
            df_nivel.to_parquet(final_path / "costo_por_nivel_riesgo.parquet", index=False)
        
        log.info("Guardados en %s", final_path)
        
        # --- Resumen
        log.info("-" * 40)
        log.info("RESUMEN:")
        log.info("  Empresas: %d", df_empresa["empresa"].nunique())
        log.info("  Total días perdidos: %.0f", df_empresa["dias_perdidos"].sum())
        log.info("  Costo directo total: $%.2f", df_empresa["costo_directo"].sum())
        log.info("  Costo presentismo: $%.2f", df_empresa["costo_presentismo"].sum())
        log.info("  COSTO TOTAL (×%.2f): $%.2f", FACTOR_PRESENTISMO, df_empresa["costo_total"].sum())
        
        if len(df_causa) > 0:
            costo_psico = df_causa[df_causa["es_psicosocial"]]["costo_total"].sum()
            log.info("  Costo causas psicosociales: $%.2f", costo_psico)
    else:
        log.warning("Sin datos de ausentismo para procesar")
        # Crear estructura vacía para consistencia
        pd.DataFrame().to_parquet(final_path / "costo_ausentismo.parquet", index=False)
    
    log.info("-" * 40)
    log.info("08 completado exitosamente.")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="08_costo_ausentismo.py — V4 Pasos 1-6")
    parser.add_argument("--config", default="config/config.yaml")
    args = parser.parse_args()
    main(config_path=args.config)