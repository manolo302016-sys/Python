"""
api/routers/v1_riesgo.py
========================
Endpoints FastAPI para el Visualizador 1: Resultados de la Evaluación
de Riesgo Psicosocial (Res. 2764/2022).

Secciones del Visualizador:
  S0  Encabezado: empresa, sector, N evaluados, cobertura, fecha
  S1  Filtros de segmentación demográfica (dropdowns)
  S2  KPIs: Intralaboral A/B, Estrés, Vulnerabilidad, Extralaboral, Dimensiones críticas
  S3  Gráficas: Barras dominios, Histograma scores, Dona protección, Barras extralaboral
  S4  Heatmap áreas × dominios + Treemap cargos × riesgo
  S5  Tablas: Dimensiones vs País, Referencia nacional, Res. 2764 fórmula global
  S6  Tablas: Frecuencias alta presencia empresa vs país

Arquitectura:
  - Lee archivos .parquet generados por el pipeline ETL validado (scripts 01-08)
  - Aplica Regla R8 de confidencialidad (N < 5) en backend antes de enviar JSON
  - El Frontend (Next.js) consume estos endpoints y renderiza las visualizaciones
"""

import logging
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
import yaml
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from ..utils.confidencialidad import aplicar_regla_r8
from ..utils.storytelling import (
    generar_insights_kpis,
    generar_insights_dominios,
    generar_insights_proteccion,
    generar_insights_heatmap,
    generar_insights_treemap,
    generar_insights_tabla_dimensiones,
    generar_insights_res2764,
    generar_insights_frecuencias,
    generar_insights_alta_presencia,
)

log = logging.getLogger(__name__)

router = APIRouter(
    prefix="/api/v1/riesgo",
    tags=["Visualizador 1 — Riesgo Psicosocial"],
    responses={404: {"description": "Recurso no encontrado"}},
)

# ══════════════════════════════════════════════════════════════════════════════
# Configuración y carga de datos
# ══════════════════════════════════════════════════════════════════════════════

ROOT = Path(__file__).resolve().parents[2]
CONFIG_PATH = ROOT / "config" / "config.yaml"

def _load_config() -> dict:
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def _proc_path() -> Path:
    cfg = _load_config()
    return ROOT / cfg["paths"]["processed"]

N_MIN = 5  # R8 — umbral de confidencialidad


class FiltrosRequest(BaseModel):
    """Modelo Pydantic para recibir filtros opcionales del frontend."""
    empresa: str
    fecha_evaluacion: Optional[str] = None
    area_departamento: Optional[str] = None
    categoria_cargo: Optional[str] = None
    modalidad_trabajo: Optional[str] = None
    nombre_jefe: Optional[str] = None
    rango_edad: Optional[str] = None
    antiguedad_empresa: Optional[str] = None


def _aplicar_filtros_demograficos(df: pd.DataFrame, filtros: FiltrosRequest) -> pd.DataFrame:
    """Aplica filtros opcionales de segmentación demográfica al DataFrame.
    Solo filtra por las columnas que el usuario seleccionó en el UI."""
    out = df.copy()
    if filtros.area_departamento:
        out = out[out["area_departamento"] == filtros.area_departamento]
    if filtros.categoria_cargo:
        out = out[out["categoria_cargo"] == filtros.categoria_cargo]
    if filtros.modalidad_trabajo and "modalidad_de_trabajo" in out.columns:
        out = out[out["modalidad_de_trabajo"] == filtros.modalidad_trabajo]
    if filtros.nombre_jefe and "nombre_jefe" in out.columns:
        out = out[out["nombre_jefe"] == filtros.nombre_jefe]
    if filtros.rango_edad and "rango_edad" in out.columns:
        out = out[out["rango_edad"] == filtros.rango_edad]
    if filtros.antiguedad_empresa and "antiguedad_rango" in out.columns:
        out = out[out["antiguedad_rango"] == filtros.antiguedad_empresa]
    return out


def _cargar_baremo(empresa: str) -> pd.DataFrame:
    proc = _proc_path()
    df = pd.read_parquet(proc / "fact_scores_baremo.parquet")
    return df[df["empresa"] == empresa].copy()


def _cargar_consolidado(empresa: str) -> pd.DataFrame:
    proc = _proc_path()
    df = pd.read_parquet(proc / "fact_consolidado.parquet")
    return df[df["empresa"] == empresa].copy()


def _cargar_benchmark(empresa: str) -> pd.DataFrame:
    proc = _proc_path()
    df = pd.read_parquet(proc / "fact_benchmark.parquet")
    return df[df["empresa"] == empresa].copy()


def _semaforo_pct(valor: float) -> str:
    """Asigna color semáforo según umbrales de riesgo. >35% rojo, 15-34% amarillo, <15% verde."""
    if valor > 35:
        return "rojo"
    elif valor >= 15:
        return "amarillo"
    else:
        return "verde"


# ══════════════════════════════════════════════════════════════════════════════
# S0 + S1: ENCABEZADO Y FILTROS
# ══════════════════════════════════════════════════════════════════════════════

@router.get("/encabezado")
def get_encabezado(empresa: str):
    """
    Sección 0: Datos generales del encabezado del visualizador.
    Retorna: nombre empresa, sector, N evaluados, % cobertura, fecha evaluación.
    """
    proc = _proc_path()

    # Consolidado para contar evaluados
    consolidado = pd.read_parquet(proc / "fact_consolidado.parquet")
    df_emp = consolidado[consolidado["empresa"] == empresa]
    if df_emp.empty:
        raise HTTPException(status_code=404, detail=f"Empresa '{empresa}' no encontrada")

    n_evaluados = df_emp["cedula"].nunique()

    # Sector económico
    sector = df_emp["sector_economico"].iloc[0] if "sector_economico" in df_emp.columns else "No clasificado"

    # Planta personal para cobertura
    dim_trab = pd.read_parquet(proc / "dim_trabajador.parquet")
    planta_emp = dim_trab[dim_trab["empresa"] == empresa]
    n_planta = len(planta_emp)
    cobertura_pct = round((n_evaluados / n_planta * 100), 1) if n_planta > 0 else 0.0

    # Fecha evaluación
    fecha = "---"
    if "fecha_aplicacion" in df_emp.columns:
        fecha_raw = df_emp["fecha_aplicacion"].dropna()
        if not fecha_raw.empty:
            fecha = pd.to_datetime(fecha_raw.iloc[0]).strftime("%d-%m-%Y")

    return {
        "titulo": "Resultados evaluación de factores de riesgo psicosocial",
        "empresa": empresa,
        "sector_economico": sector,
        "n_evaluados": int(n_evaluados),
        "n_planta": int(n_planta),
        "cobertura_pct": cobertura_pct,
        "fecha_evaluacion": fecha,
    }


@router.get("/filtros")
def get_filtros(empresa: str):
    """
    Sección 1: Retorna las opciones únicas para los 6 filtros de segmentación.
    Aplica R8 en filtros: excluye opciones con N < 5 personas.
    """
    proc = _proc_path()
    consolidado = pd.read_parquet(proc / "fact_consolidado.parquet")
    df_emp = consolidado[consolidado["empresa"] == empresa]

    if df_emp.empty:
        raise HTTPException(status_code=404, detail=f"Empresa '{empresa}' no encontrada")

    def _valores_seguros(col: str) -> list[str]:
        """Retorna valores únicos de una columna SOLO si esa categoría tiene N >= 5 personas."""
        if col not in df_emp.columns:
            return []
        conteos = df_emp.groupby(col)["cedula"].nunique().reset_index()
        conteos.columns = [col, "n"]
        seguros = conteos[conteos["n"] >= N_MIN][col].dropna().tolist()
        return sorted(seguros)

    return {
        "area_departamento": _valores_seguros("area_departamento"),
        "categoria_cargo": _valores_seguros("categoria_cargo"),
        "modalidad_trabajo": _valores_seguros("modalidad_de_trabajo"),
        "nombre_jefe": _valores_seguros("nombre_jefe"),
        "rangos_edad": _valores_seguros("rango_edad"),
        "antiguedad_empresa": _valores_seguros("antiguedad_rango"),
    }


# ══════════════════════════════════════════════════════════════════════════════
# S2: KPIs — TARJETAS PRINCIPALES
# ══════════════════════════════════════════════════════════════════════════════

@router.post("/kpis")
def get_kpis(filtros: FiltrosRequest):
    """
    Sección 2: 5 tarjetas KPI maestras.
    2.1 % Riesgo A+MA Intralaboral A y B (+ referente sector/país)
    2.2 Trabajadores A+B con estrés A+MA (N, %, referente país)
    2.3 Vulnerabilidad psicológica (Individual bajo + muy bajo)
    2.4 % Riesgo A+MA Extralaboral A y B (+ referente país)
    2.5 Dimensiones críticas por encima del referente país
    """
    cfg = _load_config()
    baremo = _cargar_baremo(filtros.empresa)
    if baremo.empty:
        raise HTTPException(status_code=404, detail="Sin datos para esta empresa")

    baremo = _aplicar_filtros_demograficos(baremo, filtros)
    bench_dominio = cfg.get("benchmark_dominio", {})
    bench_sector = cfg.get("benchmark_sector", {})
    bench_dimension = cfg.get("benchmark_dimension", {})

    # Detectar sector económico de la empresa
    sector = "No clasificado"
    if "sector_economico" in baremo.columns:
        sector = baremo["sector_economico"].iloc[0] if not baremo.empty else "No clasificado"
    ref_sector = bench_sector.get(sector, bench_sector.get("_promedio_general", 39.69))

    # ── Helper: % Alto + Muy Alto para un instrumento/factor dado ──
    def _pct_alto_ma(nivel_analisis: str, nombre_nivel: str, forma: str = None) -> dict:
        sub = baremo[
            (baremo["nivel_analisis"] == nivel_analisis) &
            (baremo["nombre_nivel"] == nombre_nivel)
        ]
        if forma:
            sub = sub[sub["forma_intra"] == forma]
        total = sub["cedula"].nunique()
        if total < N_MIN:
            return {"pct": "Confidencial", "n_total": total, "n_riesgo": 0}
        n_riesgo = sub[sub["nivel_riesgo"] >= 4]["cedula"].nunique()
        pct = round(n_riesgo / total * 100, 1)
        return {"pct": pct, "n_total": int(total), "n_riesgo": int(n_riesgo)}

    # ── Helper: % Vulnerabilidad (nivel_riesgo <= 2 en Individual) ──
    def _pct_vulnerabilidad() -> dict:
        sub = baremo[
            (baremo["nivel_analisis"] == "factor") &
            (baremo["nombre_nivel"] == "Individual")
        ]
        total = sub["cedula"].nunique()
        if total < N_MIN:
            return {"pct": "Confidencial", "n_total": total, "n_vulnerables": 0}
        n_vuln = sub[sub["nivel_riesgo"] <= 2]["cedula"].nunique()
        pct = round(n_vuln / total * 100, 1)
        return {"pct": pct, "n_total": int(total), "n_vulnerables": int(n_vuln)}

    # ── KPI 2.1: % Riesgo A+MA Intralaboral A y B ──
    intra_a = _pct_alto_ma("factor", "IntraA", "A")
    intra_b = _pct_alto_ma("factor", "IntraB", "B")
    kpi_2_1 = {
        "nombre": "% riesgo A+MA intralaboral A y B",
        "forma_a": {**intra_a, "semaforo": _semaforo_pct(intra_a["pct"]) if isinstance(intra_a["pct"], (int, float)) else "gris"},
        "forma_b": {**intra_b, "semaforo": _semaforo_pct(intra_b["pct"]) if isinstance(intra_b["pct"], (int, float)) else "gris"},
        "referente": {"pct": ref_sector, "tipo": f"sector ({sector})" if sector != "No clasificado" else "país"},
    }

    # ── KPI 2.2: Trabajadores A+B con estrés A+MA ──
    estres_total = _pct_alto_ma("factor", "Estres")
    ref_estres_pais = bench_dominio.get("Estres", 32.9)
    diff_estres = round(estres_total["pct"] - ref_estres_pais, 1) if isinstance(estres_total["pct"], (int, float)) else None
    kpi_2_2 = {
        "nombre": "Trabajadores A+B con estrés A+MA",
        "n_estres": estres_total.get("n_riesgo", 0),
        "pct_empresa": estres_total["pct"],
        "pct_pais": ref_estres_pais,
        "diferencia_pp": diff_estres,
        "semaforo_diff": _semaforo_pct(abs(diff_estres)) if diff_estres is not None else "gris",
    }

    # ── KPI 2.3: Vulnerabilidad psicológica ──
    vuln = _pct_vulnerabilidad()
    ref_vuln_pais = bench_dominio.get("Individual", 4.2)
    diff_vuln = round(vuln["pct"] - ref_vuln_pais, 1) if isinstance(vuln["pct"], (int, float)) else None
    kpi_2_3 = {
        "nombre": "Vulnerabilidad psicológica",
        "n_vulnerables": vuln.get("n_vulnerables", 0),
        "pct_empresa": vuln["pct"],
        "pct_pais": ref_vuln_pais,
        "diferencia_pp": diff_vuln,
        "semaforo_diff": _semaforo_pct(abs(diff_vuln)) if diff_vuln is not None else "gris",
    }

    # ── KPI 2.4: % Riesgo A+MA Extralaboral A y B ──
    extra_a = _pct_alto_ma("factor", "Extralaboral", "A")
    extra_b = _pct_alto_ma("factor", "Extralaboral", "B")
    ref_extra_pais = bench_dominio.get("Extralaboral", 26.3)
    kpi_2_4 = {
        "nombre": "% riesgo A+MA extralaboral A y B",
        "forma_a": {**extra_a, "semaforo": _semaforo_pct(extra_a["pct"]) if isinstance(extra_a["pct"], (int, float)) else "gris"},
        "forma_b": {**extra_b, "semaforo": _semaforo_pct(extra_b["pct"]) if isinstance(extra_b["pct"], (int, float)) else "gris"},
        "referente": {"pct": ref_extra_pais, "tipo": "país"},
    }

    # ── KPI 2.5: Dimensiones críticas por encima del referente país ──
    benchmark_df = _cargar_benchmark(filtros.empresa)
    dims_criticas = []
    if not benchmark_df.empty:
        dims_bench = benchmark_df[
            (benchmark_df["nivel_analisis"] == "dimension") &
            (benchmark_df["tipo_referencia"] == "colombia")
        ]
        for _, row in dims_bench.iterrows():
            diff = row.get("diferencia_pp", 0)
            if diff > 0:
                dims_criticas.append({
                    "dimension": row["nombre_nivel"],
                    "pct_empresa": round(row["pct_empresa"], 1),
                    "pct_pais": round(row["pct_referencia"], 1),
                    "diferencia_pp": round(diff, 1),
                })

    kpi_2_5 = {
        "nombre": "Dimensiones críticas",
        "conteo": len(dims_criticas),
        "dimensiones": dims_criticas,
    }

    respuesta = {
        "kpi_2_1_intralaboral": kpi_2_1,
        "kpi_2_2_estres": kpi_2_2,
        "kpi_2_3_vulnerabilidad": kpi_2_3,
        "kpi_2_4_extralaboral": kpi_2_4,
        "kpi_2_5_dimensiones_criticas": kpi_2_5,
    }
    respuesta["insights"] = generar_insights_kpis(respuesta)
    return respuesta


# ══════════════════════════════════════════════════════════════════════════════
# S3: GRÁFICAS DE RESULTADOS
# ══════════════════════════════════════════════════════════════════════════════

@router.post("/graficas/dominios-intralaboral")
def get_dominios_intralaboral(filtros: FiltrosRequest):
    """
    Sección 3.1: Barras agrupadas — % Riesgo A+MA en dominios intralaborales
    separados por Forma A y Forma B.
    """
    baremo = _cargar_baremo(filtros.empresa)
    if baremo.empty:
        raise HTTPException(status_code=404, detail="Sin datos")

    baremo = _aplicar_filtros_demograficos(baremo, filtros)
    dominios_nombres = [
        "Demandas del trabajo",
        "Control sobre el trabajo",
        "Liderazgo y relaciones sociales",
        "Recompensas",
    ]

    resultado = []
    for dom in dominios_nombres:
        row = {"dominio": dom}
        for forma in ["A", "B"]:
            sub = baremo[
                (baremo["nivel_analisis"] == "dominio") &
                (baremo["nombre_nivel"] == dom) &
                (baremo["forma_intra"] == forma)
            ]
            total = sub["cedula"].nunique()
            if total < N_MIN:
                row[f"pct_{forma}"] = "Confidencial"
                row[f"n_{forma}"] = int(total)
            else:
                n_riesgo = sub[sub["nivel_riesgo"] >= 4]["cedula"].nunique()
                pct = round(n_riesgo / total * 100, 1)
                row[f"pct_{forma}"] = pct
                row[f"n_{forma}"] = int(total)
                row[f"semaforo_{forma}"] = _semaforo_pct(pct)
        resultado.append(row)

    return {"dominios": resultado, "insights": generar_insights_dominios(resultado)}


@router.post("/graficas/histograma-scores")
def get_histograma_scores(filtros: FiltrosRequest):
    """
    Sección 3.2: Histograma de distribución de puntajes transformados (0-100)
    para Estrés e Intralaboral. Bins de 15 en 15.
    Retorna ambas distribuciones para que el Frontend alterne con tabs.
    """
    proc = _proc_path()
    scores = pd.read_parquet(proc / "fact_scores_brutos.parquet")
    df_emp = scores[scores["empresa"] == filtros.empresa]

    if df_emp.empty:
        raise HTTPException(status_code=404, detail="Sin datos de scores")

    df_emp = _aplicar_filtros_demograficos(df_emp, filtros)

    # Bins de 0 a 100 en intervalos de 15
    bins = list(range(0, 106, 15))  # [0, 15, 30, 45, 60, 75, 90, 105]
    labels = [f"{bins[i]}-{bins[i+1]-1}" for i in range(len(bins) - 1)]

    def _histograma(instrumento_filtro: list[str]) -> list[dict]:
        sub = df_emp[df_emp["nombre_nivel"].isin(instrumento_filtro)]
        if "puntaje_transformado" not in sub.columns:
            return []
        valores = sub["puntaje_transformado"].dropna()
        conteos, _ = np.histogram(valores, bins=bins)
        return [
            {"rango": labels[i], "frecuencia": int(conteos[i]), "bin_inicio": bins[i]}
            for i in range(len(conteos))
        ]

    return {
        "estres": _histograma(["Estres"]),
        "intralaboral": _histograma(["IntraA", "IntraB"]),
        "bins_size": 15,
    }


@router.post("/graficas/dona-proteccion")
def get_dona_proteccion(filtros: FiltrosRequest):
    """
    Sección 3.3: Distribución de la población según nivel de protección psicológica
    (Factor Individual). Retorna frecuencias absolutas y relativas por nivel.
    Colores invertidos: Muy bajo → rojo, Muy alto → verde.
    """
    baremo = _cargar_baremo(filtros.empresa)
    if baremo.empty:
        raise HTTPException(status_code=404, detail="Sin datos")

    baremo = _aplicar_filtros_demograficos(baremo, filtros)

    # Factor Individual
    sub = baremo[
        (baremo["nivel_analisis"] == "factor") &
        (baremo["nombre_nivel"] == "Individual")
    ]

    total = sub["cedula"].nunique()
    niveles_map = {1: "Muy bajo", 2: "Bajo", 3: "Medio", 4: "Alto", 5: "Muy alto"}
    colores_map = {
        1: "#EF4444",  # Rojo — máxima vulnerabilidad
        2: "#F97316",  # Naranja
        3: "#F59E0B",  # Amarillo
        4: "#6EE7B7",  # Verde claro
        5: "#10B981",  # Verde — máxima protección
    }

    resultado = []
    for lvl in range(1, 6):
        n = sub[sub["nivel_riesgo"] == lvl]["cedula"].nunique()
        pct = round(n / total * 100, 1) if total > 0 else 0.0
        resultado.append({
            "nivel": niveles_map[lvl],
            "nivel_num": lvl,
            "n_personas": int(n),
            "pct": pct,
            "color_sugerido": colores_map[lvl],
        })

    return {
        "total_evaluados": int(total),
        "distribucion": resultado,
        "insights": generar_insights_proteccion(resultado),
    }


@router.post("/graficas/dimensiones-extralaboral")
def get_dimensiones_extralaboral(filtros: FiltrosRequest):
    """
    Sección 3.4: Barras horizontales — % Riesgo A+MA en dimensiones extralaboral
    consolidando Forma A y B en una sola barra.
    """
    baremo = _cargar_baremo(filtros.empresa)
    if baremo.empty:
        raise HTTPException(status_code=404, detail="Sin datos")

    baremo = _aplicar_filtros_demograficos(baremo, filtros)

    # Dimensiones extralaborales
    sub = baremo[
        (baremo["nivel_analisis"] == "dimension") &
        (baremo["instrumento"] == "Extralaboral") if "instrumento" in baremo.columns
        else (baremo["nombre_nivel"].str.contains("extra", case=False, na=False))
    ]

    dims = sub["nombre_nivel"].unique()
    resultado = []
    for dim in dims:
        dsub = sub[sub["nombre_nivel"] == dim]
        total = dsub["cedula"].nunique()
        if total < N_MIN:
            resultado.append({
                "dimension": dim, "pct": "Confidencial",
                "n_total": int(total), "n_riesgo": 0,
            })
        else:
            n_riesgo = dsub[dsub["nivel_riesgo"] >= 4]["cedula"].nunique()
            pct = round(n_riesgo / total * 100, 1)
            resultado.append({
                "dimension": dim, "pct": pct,
                "n_total": int(total), "n_riesgo": int(n_riesgo),
                "semaforo": _semaforo_pct(pct),
            })

    # Ordenar de mayor a menor riesgo
    resultado.sort(key=lambda x: x["pct"] if isinstance(x["pct"], (int, float)) else -1, reverse=True)
    return resultado


# ══════════════════════════════════════════════════════════════════════════════
# S4: HEATMAP Y TREEMAP
# ══════════════════════════════════════════════════════════════════════════════

@router.post("/heatmap-areas-dominios")
def get_heatmap_areas(filtros: FiltrosRequest):
    """
    Sección 4.1: Heatmap — % Riesgo A+MA de dominios intralaborales por áreas.
    Combinando Forma A y B. Regla R8 aplicada por celda.
    """
    consolidado = _cargar_consolidado(filtros.empresa)
    if consolidado.empty:
        raise HTTPException(status_code=404, detail="Sin datos")

    consolidado = _aplicar_filtros_demograficos(consolidado, filtros)

    # Solo dominios de instrumentos intralaborales
    sub = consolidado[
        (consolidado["nivel_analisis"] == "dominio") &
        (consolidado["instrumento"].isin(["IntraA", "IntraB"]) if "instrumento" in consolidado.columns else True)
    ]

    dominios = ["Demandas del trabajo", "Control sobre el trabajo",
                "Liderazgo y relaciones sociales", "Recompensas"]
    areas = sorted([a for a in sub["area_departamento"].unique() if pd.notna(a) and a != "Sin área"])

    matriz = []
    for area in areas:
        fila = {"area": area}
        area_sub = sub[sub["area_departamento"] == area]
        n_area = area_sub["cedula"].nunique()
        fila["n_personas"] = int(n_area)

        for dom in dominios:
            dom_sub = area_sub[area_sub["nombre_nivel"] == dom]
            n_dom = dom_sub["cedula"].nunique()
            dom_key = dom.lower().replace(" ", "_")[:15]

            if n_dom < N_MIN:
                fila[f"pct_{dom_key}"] = "Confidencial"
                fila[f"n_{dom_key}"] = int(n_dom)
            else:
                n_riesgo = dom_sub[dom_sub["nivel_riesgo"] >= 4]["cedula"].nunique()
                pct = round(n_riesgo / n_dom * 100, 1)
                fila[f"pct_{dom_key}"] = pct
                fila[f"n_{dom_key}"] = int(n_dom)
                fila[f"semaforo_{dom_key}"] = _semaforo_pct(pct)

        matriz.append(fila)

    return {
        "dominios": dominios,
        "areas": matriz,
        "insights": generar_insights_heatmap(matriz, dominios),
    }


@router.post("/treemap-cargos")
def get_treemap_cargos(filtros: FiltrosRequest):
    """
    Sección 4.2: Treemap — % Riesgo intralaboral A+MA por cargos.
    Combina Forma A y B. Regla R8 aplicada.
    """
    consolidado = _cargar_consolidado(filtros.empresa)
    if consolidado.empty:
        raise HTTPException(status_code=404, detail="Sin datos")

    consolidado = _aplicar_filtros_demograficos(consolidado, filtros)

    # Factor Intralaboral totales (A y B combinados)
    sub = consolidado[
        (consolidado["nivel_analisis"] == "factor") &
        (consolidado["nombre_nivel"].isin(["IntraA", "IntraB"]))
    ]

    cargos = sub["categoria_cargo"].unique()
    resultado = []
    for cargo in cargos:
        if pd.isna(cargo):
            continue
        csub = sub[sub["categoria_cargo"] == cargo]
        total = csub["cedula"].nunique()
        if total < N_MIN:
            resultado.append({
                "cargo": cargo, "pct": "Confidencial",
                "n_total": int(total), "n_riesgo": 0,
            })
        else:
            n_riesgo = csub[csub["nivel_riesgo"] >= 4]["cedula"].nunique()
            pct = round(n_riesgo / total * 100, 1)
            resultado.append({
                "cargo": cargo, "pct": pct,
                "n_total": int(total), "n_riesgo": int(n_riesgo),
                "semaforo": _semaforo_pct(pct),
            })

    resultado.sort(key=lambda x: x["pct"] if isinstance(x["pct"], (int, float)) else -1, reverse=True)
    return {"cargos": resultado, "insights": generar_insights_treemap(resultado)}


# ══════════════════════════════════════════════════════════════════════════════
# S5: TABLAS DE DIMENSIONES
# ══════════════════════════════════════════════════════════════════════════════

@router.post("/tablas/dimensiones-intralaboral")
def get_tabla_dimensiones(filtros: FiltrosRequest, forma: str = Query("A", pattern="^(A|B)$")):
    """
    Sección 5.1: Tabla de dimensiones intralaboral con % RA, % RMA, comparación país.
    El frontend alterna entre Forma A y Forma B con un selector.
    """
    cfg = _load_config()
    baremo = _cargar_baremo(filtros.empresa)
    if baremo.empty:
        raise HTTPException(status_code=404, detail="Sin datos")

    baremo = _aplicar_filtros_demograficos(baremo, filtros)
    bench_dim = cfg.get("benchmark_dimension", {})

    # Filtrar dimensiones intralaborales por forma
    sub = baremo[
        (baremo["nivel_analisis"] == "dimension") &
        (baremo["forma_intra"] == forma)
    ]

    dims = sub["nombre_nivel"].unique()
    resultado = []

    for dim in sorted(dims):
        dsub = sub[sub["nombre_nivel"] == dim]
        total = dsub["cedula"].nunique()

        if total < N_MIN:
            resultado.append({
                "dimension": dim, "pct_alto": "Confidencial",
                "pct_muy_alto": "Confidencial", "pct_pais": bench_dim.get(dim, "---"),
                "diferencia_pp": "---", "is_critical": False,
            })
        else:
            n_alto = dsub[dsub["nivel_riesgo"] == 4]["cedula"].nunique()
            n_muy_alto = dsub[dsub["nivel_riesgo"] == 5]["cedula"].nunique()
            pct_alto = round(n_alto / total * 100, 1)
            pct_muy_alto = round(n_muy_alto / total * 100, 1)
            pct_empresa_ama = round(pct_alto + pct_muy_alto, 1)

            ref = bench_dim.get(dim, None)
            # Claridad de rol: Forma B usa 5.8 en lugar de 20.5
            if dim == "Claridad de rol" and forma == "B":
                ref = 5.8

            if ref is not None:
                diff = round(pct_empresa_ama - ref, 1)
                is_critical = diff > 0
            else:
                diff = "---"
                is_critical = False

            resultado.append({
                "dimension": dim,
                "pct_alto": pct_alto,
                "pct_muy_alto": pct_muy_alto,
                "pct_pais": round(ref, 1) if ref is not None else "---",
                "diferencia_pp": diff,
                "is_critical": is_critical,
                "n_total": int(total),
            })

    return {
        "forma": forma,
        "dimensiones": resultado,
        "insights": generar_insights_tabla_dimensiones(resultado, forma),
    }


@router.get("/tablas/referencia-pais")
def get_tabla_referencia_pais():
    """
    Sección 5.2: Tabla de referencia — % Riesgo A+MA por dimensión según ENCST.
    Es una tabla constante de referencia pública.
    """
    cfg = _load_config()
    bench = cfg.get("benchmark_dimension", {})

    tabla = [
        {"dimension": dim, "pct_riesgo_ama": val}
        for dim, val in bench.items()
        if not dim.startswith("_")
    ]

    return {
        "fuente": "II y III Encuesta Nacional de Condiciones de Salud y Trabajo 2013 - 2021",
        "referencia": tabla,
    }


@router.post("/tablas/res2764-global")
def get_tabla_res2764(filtros: FiltrosRequest):
    """
    Sección 5.3: Tabla Res. 2764/2022 — Resultado global promedio empresa
    riesgo intralaboral Forma A y B.
    Calcula: total evaluados, promedio puntaje bruto, puntaje transformado, nivel de riesgo.
    """
    proc = _proc_path()
    riesgo_emp = pd.read_parquet(proc / "fact_riesgo_empresa.parquet")
    df_emp = riesgo_emp[riesgo_emp["empresa"] == filtros.empresa]

    if df_emp.empty:
        raise HTTPException(status_code=404, detail="Sin datos de riesgo empresa")

    resultado = []
    for forma in ["A", "B"]:
        row_forma = df_emp[
            (df_emp["instrumento"].isin(["IntraA", "IntraB"])) &
            (df_emp["forma_intra"] == forma) if "forma_intra" in df_emp.columns
            else (df_emp["instrumento"] == f"Intra{forma}")
        ]

        if row_forma.empty:
            resultado.append({
                "forma": forma, "total_evaluados": 0,
                "puntaje_bruto_promedio": "---",
                "puntaje_transformado": "---",
                "nivel_riesgo": "---",
                "flag_alert": False,
            })
        else:
            r = row_forma.iloc[0]
            nivel = r.get("nivel_riesgo_empresa", "---")
            flag = nivel in ["Alto", "Muy alto"]
            resultado.append({
                "forma": forma,
                "total_evaluados": int(r.get("n_evaluados", 0)),
                "puntaje_bruto_promedio": round(float(r.get("puntaje_bruto_promedio", 0)), 1),
                "puntaje_transformado": round(float(r.get("puntaje_transformado", 0)), 1),
                "nivel_riesgo": nivel,
                "flag_alert": flag,
            })

    return {
        "titulo": "Res. 2764/2022. Resultado global promedio empresa",
        "filas": resultado,
        "insights": generar_insights_res2764(resultado),
    }


# ══════════════════════════════════════════════════════════════════════════════
# S6: TABLAS DE FRECUENCIAS — ALTA PRESENCIA
# ══════════════════════════════════════════════════════════════════════════════

@router.post("/tablas/frecuencias-vs-pais")
def get_frecuencias_vs_pais(filtros: FiltrosRequest):
    """
    Sección 6.1: Diferencia % entre empresa y país respecto a la alta presencia
    de indicadores (preguntas). Solo preguntas con referencia nacional.
    """
    proc = _proc_path()
    top20 = pd.read_parquet(proc / "fact_top20_comparables.parquet")
    dim_preg = pd.read_parquet(proc / "dim_pregunta.parquet")

    sub = top20[top20["empresa"] == filtros.empresa].copy()
    if sub.empty:
        raise HTTPException(status_code=404, detail="Sin datos comparables")

    # Enriquecer con texto de pregunta
    if "texto_pregunta" in dim_preg.columns:
        sub = sub.merge(
            dim_preg[["id_pregunta", "texto_pregunta"]].drop_duplicates("id_pregunta"),
            on="id_pregunta", how="left",
        )

    resultado = []
    for _, row in sub.iterrows():
        pct_emp = round(float(row.get("pct_empresa", 0)), 1)
        pct_pais = round(float(row.get("pct_pais_encst", 0)), 1)
        diff = round(pct_emp - pct_pais, 1)
        resultado.append({
            "pregunta": row.get("texto_pregunta", row.get("id_pregunta", "---")),
            "pct_alta_presencia_empresa": pct_emp,
            "pct_alta_presencia_pais": pct_pais,
            "diferencia_pp": diff,
            "is_critical": diff > 0,  # Rojo si la empresa está por encima del país
        })

    # Ordenar por diferencia descendente (más preocupantes primero)
    resultado.sort(key=lambda x: x["diferencia_pp"], reverse=True)
    return {"preguntas": resultado, "insights": generar_insights_frecuencias(resultado)}


@router.post("/tablas/frecuencias-todas")
def get_frecuencias_todas(filtros: FiltrosRequest, forma: str = Query("A", pattern="^(A|B)$")):
    """
    Sección 6.2: Distribución (%) de todas las respuestas en alta presencia.
    Se puede elegir Forma A o B. Marca preguntas > 40% con flag is_critical.
    """
    proc = _proc_path()
    frecuencias = pd.read_parquet(proc / "fact_frecuencias.parquet")
    dim_preg = pd.read_parquet(proc / "dim_pregunta.parquet")

    sub = frecuencias[frecuencias["empresa"] == filtros.empresa].copy()
    if sub.empty:
        raise HTTPException(status_code=404, detail="Sin datos de frecuencias")

    # Filtrar por forma
    if "forma_intra" in sub.columns:
        sub = sub[sub["forma_intra"] == forma]

    # Enriquecer con texto de pregunta
    if "texto_pregunta" in dim_preg.columns:
        sub = sub.merge(
            dim_preg[["id_pregunta", "texto_pregunta"]].drop_duplicates("id_pregunta"),
            on="id_pregunta", how="left",
        )

    resultado = []
    for _, row in sub.iterrows():
        pct = round(float(row.get("pct_alta_presencia", 0)), 1)
        resultado.append({
            "pregunta": row.get("texto_pregunta", row.get("id_pregunta", "---")),
            "instrumento": row.get("instrumento", row.get("nombre_nivel", "---")),
            "pct_alta_presencia": pct,
            "is_critical": pct > 40,  # Marca preguntas con > 40% de alta frecuencia
        })

    # Ordenar por % descendente
    resultado.sort(key=lambda x: x["pct_alta_presencia"], reverse=True)
    return {
        "forma": forma,
        "preguntas": resultado,
        "insights": generar_insights_alta_presencia(resultado, forma),
    }
