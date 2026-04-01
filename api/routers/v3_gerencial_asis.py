"""
v3_gerencial_asis.py — MentalPRO · Router FastAPI Visualizador 3
=================================================================
Expone los endpoints del Visualizador 3 (Gerencial + ASIS Consolidado):

    GET  /v3/encabezado          → S0: Identificadores de empresa
    POST /v3/kpis_globales       → S1: Tarjetas KPI globales
    POST /v3/demografia          → S2A: Variables demográficas
    POST /v3/costos_ausentismo   → S2B: Cálculo R10 ausentismo
    POST /v3/benchmarking        → S3: Benchmarking ejecutivo
    POST /v3/ranking_areas       → S4: Top 5 áreas críticas
    POST /v3/alertas_protocolos  → S5: Fichas técnicas 3 líneas gestión

Reglas:
    R8  — Confidencialidad grupos < 5 personas
    R10 — Fórmula costo económico
    R11 — Colores inamovibles
    R18 — Escalable por empresa + fecha_evaluacion

Resolución 2764/2022 — Ministerio de Trabajo Colombia
"""

from pathlib import Path
from typing import Optional

import pandas as pd
import yaml
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

# ---------------------------------------------------------------------------
# Inicialización
# ---------------------------------------------------------------------------

router = APIRouter(prefix="/v3", tags=["Visualizador 3 — Gerencial + ASIS"])

BASE_DIR = Path(__file__).resolve().parent.parent.parent
CONFIG_PATH = BASE_DIR / "config" / "config.yaml"

_CONFIG_CACHE: Optional[dict] = None
_PARQUET_CACHE: dict = {}


def _load_config() -> dict:
    global _CONFIG_CACHE
    if _CONFIG_CACHE is None:
        with open(CONFIG_PATH, "r", encoding="utf-8") as f:
            _CONFIG_CACHE = yaml.safe_load(f)
    return _CONFIG_CACHE


def _proc_path(filename: str) -> Path:
    cfg = _load_config()
    return BASE_DIR / cfg["paths"]["processed"] / filename


def _read_parquet(filename: str) -> pd.DataFrame:
    """Lee parquet con cache en memoria."""
    if filename not in _PARQUET_CACHE:
        path = _proc_path(filename)
        if not path.exists():
            raise HTTPException(
                status_code=503,
                detail=f"Parquet no disponible: {filename}. Ejecute scripts/09_asis_gerencial.py primero.",
            )
        _PARQUET_CACHE[filename] = pd.read_parquet(path)
    return _PARQUET_CACHE[filename].copy()


def _semaforo_pct(pct: float) -> str:
    """Semáforo estándar: >35 rojo, 15-34 amarillo, <15 verde (R11)."""
    if pct > 35:
        return "#EF4444"
    if pct >= 15:
        return "#F59E0B"
    return "#10B981"


def _check_confidencial(n: int) -> bool:
    """Regla R8: grupos < 5 personas son confidenciales."""
    return n < 5


# ---------------------------------------------------------------------------
# Modelos Pydantic
# ---------------------------------------------------------------------------

class FiltrosRequest(BaseModel):
    empresa: str
    fecha_evaluacion: Optional[str] = None
    area_departamento: Optional[str] = None
    categoria_cargo: Optional[str] = None
    forma_intra: Optional[str] = None  # "A" | "B" | None = ambas


class FiltrosExtendidosRequest(FiltrosRequest):
    modalidad_trabajo: Optional[str] = None
    nombre_jefe: Optional[str] = None


# ---------------------------------------------------------------------------
# S0 — Encabezado
# ---------------------------------------------------------------------------

@router.get("/encabezado")
def get_encabezado(empresa: str, fecha_evaluacion: Optional[str] = None):
    """
    S0: Devuelve identificadores de empresa, población evaluada, cobertura y fecha.
    """
    try:
        consolidado = _read_parquet("fact_consolidado.parquet")
        dim_trab = _read_parquet("dim_trabajador.parquet")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    # Filtrar empresa
    df_emp = consolidado[consolidado["empresa"] == empresa]
    if fecha_evaluacion:
        df_emp = df_emp[df_emp["fecha_evaluacion"] == fecha_evaluacion]

    if df_emp.empty:
        raise HTTPException(status_code=404, detail=f"Empresa '{empresa}' no encontrada.")

    n_evaluados = int(df_emp["cedula"].nunique())

    # Planta personal
    sub_trab = dim_trab[dim_trab["empresa"] == empresa]
    n_planta = int(sub_trab["total_planta"].iloc[0]) \
        if not sub_trab.empty and "total_planta" in sub_trab.columns \
        else n_evaluados

    pct_cobertura = round(n_evaluados / n_planta * 100, 1) if n_planta > 0 else 100.0

    # Fecha
    fecha = str(df_emp["fecha_evaluacion"].max()) if "fecha_evaluacion" in df_emp.columns else "N/A"

    # Sector
    sector = str(df_emp["sector_homologado"].iloc[0]) \
        if "sector_homologado" in df_emp.columns else "No clasificado"

    # Formas disponibles
    formas = sorted(df_emp["forma_intra"].dropna().unique().tolist()) \
        if "forma_intra" in df_emp.columns else []

    return {
        "empresa": empresa,
        "sector": sector,
        "n_evaluados": n_evaluados,
        "n_planta": n_planta,
        "pct_cobertura": pct_cobertura,
        "fecha_evaluacion": fecha,
        "formas_disponibles": formas,
        "instrumento": "Batería MinTrabajo Col. Res. 2764/2022",
    }


# ---------------------------------------------------------------------------
# S1 — KPIs Globales
# ---------------------------------------------------------------------------

@router.post("/kpis_globales")
def post_kpis_globales(filtros: FiltrosRequest):
    """
    S1: Tarjetas KPI con distribución 5 niveles por instrumento,
    vulnerabilidad psicológica, protocolos urgentes y vigilancia epidemiológica.
    """
    try:
        kpis = _read_parquet("fact_v3_kpis_globales.parquet")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    df = kpis[kpis["empresa"] == filtros.empresa]

    if filtros.fecha_evaluacion:
        df = df[df["fecha_evaluacion"] == filtros.fecha_evaluacion]

    if filtros.forma_intra:
        df = df[df["forma"].isin([filtros.forma_intra, "A+B"])]

    if df.empty:
        raise HTTPException(status_code=404, detail=f"Sin datos KPI para '{filtros.empresa}'.")

    # Estructurar por kpi_grupo
    grupos = {}
    for grupo, sub in df.groupby("kpi_grupo"):
        niveles = sub[["nivel_label", "n_nivel", "pct_nivel"]].to_dict("records")
        pct_ama = float(sub["pct_alto_muy_alto"].iloc[0]) if "pct_alto_muy_alto" in sub.columns else 0.0
        pct_ref = sub["pct_referente"].iloc[0]
        tipo_ref = str(sub["tipo_referente"].iloc[0]) if "tipo_referente" in sub.columns else "pais"
        diff_pp = sub["diferencia_pp"].iloc[0]
        semaforo = str(sub["semaforo"].iloc[0])

        grupos[grupo] = {
            "kpi_grupo": grupo,
            "niveles": niveles,
            "pct_alto_muy_alto": pct_ama,
            "pct_referente": None if pd.isna(pct_ref) else float(pct_ref),
            "tipo_referente": tipo_ref,
            "diferencia_pp": None if pd.isna(diff_pp) else float(diff_pp),
            "semaforo": semaforo,
        }

    # Generar insight narrativo por KPI
    insights = _generar_insights_kpis_v3(grupos)

    return {
        "empresa": filtros.empresa,
        "kpis": list(grupos.values()),
        "insights": insights,
    }


def _generar_insights_kpis_v3(grupos: dict) -> list:
    """Genera insights narrativos para los KPIs del V3."""
    insights = []
    umbrales_alerta = {
        "Intralaboral A": 35,
        "Intralaboral B": 35,
        "Extralaboral A": 35,
        "Extralaboral B": 35,
        "Estrés A": 25,
        "Estrés B": 25,
    }

    for grupo, datos in grupos.items():
        pct = datos.get("pct_alto_muy_alto", 0)
        diff = datos.get("diferencia_pp")

        umbral = umbrales_alerta.get(grupo, 35)
        if pct > umbral:
            severity = "crítica" if pct > 45 else "alta"
            insights.append({
                "kpi": grupo,
                "tipo": "alerta",
                "severity": severity,
                "mensaje": (
                    f"{pct:.1f}% de trabajadores en riesgo Alto o Muy Alto en {grupo}. "
                    f"{'Significativamente por encima' if diff and diff > 15 else 'Por encima'} "
                    f"del referente nacional ({diff:+.1f} pp)."
                    if diff else f"Supera el umbral crítico del {umbral}%."
                ),
            })
        elif pct >= 15:
            insights.append({
                "kpi": grupo,
                "tipo": "advertencia",
                "severity": "media",
                "mensaje": f"{pct:.1f}% en riesgo Alto+Muy Alto en {grupo}. Requiere monitoreo preventivo.",
            })

    return insights


# ---------------------------------------------------------------------------
# S2A — Demografía
# ---------------------------------------------------------------------------

@router.post("/demografia")
def post_demografia(filtros: FiltrosRequest):
    """
    S2A: Variables demográficas: pirámide poblacional, antigüedades,
    estado civil, dependientes, áreas, cargos, forma intralaboral.
    """
    try:
        demo = _read_parquet("fact_v3_demografia.parquet")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    df = demo[demo["empresa"] == filtros.empresa].copy()
    if df.empty:
        raise HTTPException(status_code=404, detail=f"Sin datos demográficos para '{filtros.empresa}'.")

    # Aplicar R8: enmascarar confidenciales
    if "confidencial" in df.columns:
        df.loc[df["confidencial"], ["n", "pct"]] = None

    resultado = {}
    for var, sub in df.groupby("variable"):
        registros = sub[["categoria", "n", "pct"]].copy()
        if "sexo" in sub.columns and sub["sexo"].notna().any():
            registros = sub[["categoria", "sexo", "n", "pct"]].copy()
        resultado[var] = registros.to_dict("records")

    return {
        "empresa": filtros.empresa,
        "graficas": resultado,
        "nota_r8": "Grupos con menos de 5 personas se muestran como confidencial.",
    }


# ---------------------------------------------------------------------------
# S2B — Costos Ausentismo
# ---------------------------------------------------------------------------

@router.post("/costos_ausentismo")
def post_costos_ausentismo(filtros: FiltrosRequest):
    """
    S2B: Fórmula R10 — 6 pasos de cálculo de pérdida económica por ausentismo
    + estimación de costo atribuible a riesgo psicosocial (30%) y ROI esperado.
    """
    try:
        costos = _read_parquet("fact_v3_costos.parquet")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    df = costos[costos["empresa"] == filtros.empresa].copy()
    if df.empty:
        raise HTTPException(status_code=404, detail=f"Sin datos de costos para '{filtros.empresa}'.")

    pasos = df[["paso", "nombre_paso", "valor", "unidad", "nota"]].to_dict("records")

    # Meta-información de ausentismo
    row_p2 = df[df["paso"] == 2].iloc[0] if (df["paso"] == 2).any() else None
    meta = {}
    if row_p2 is not None:
        meta = {
            "pct_ausentismo": float(row_p2.get("pct_ausentismo", 0)),
            "diferencia_pp_vs_pais": float(row_p2.get("diferencia_pp_vs_pais", 0)),
            "referente_pais_pct": 3.0,
            "semaforo_ausentismo": str(row_p2.get("semaforo_ausentismo", "#F59E0B")),
            "n_planta": int(row_p2.get("n_planta", 0)),
            "total_dias_ausencia": int(row_p2.get("total_dias_ausencia", 0)),
            "dias_cap_instalada": int(row_p2.get("dias_cap_instalada", 0)),
        }

    # Paso 8 = costo psicosocial
    costo_psico = None
    row_p8 = df[df["paso"] == 8]
    if not row_p8.empty:
        costo_psico = float(row_p8["valor"].iloc[0])

    # ROI estimado: si inversión típica SST = 2% masa salarial
    cfg = _load_config()
    n_planta = meta.get("n_planta", 1)
    smlv = cfg.get("smlv_mensual", 2_800_000)
    inversion_sst_estimada = n_planta * smlv * 12 * 0.02  # 2% masa salarial

    roi_pct = None
    if costo_psico and inversion_sst_estimada > 0:
        roi_pct = round((costo_psico - inversion_sst_estimada) / inversion_sst_estimada * 100, 1)

    return {
        "empresa": filtros.empresa,
        "pasos_calculo": pasos,
        "meta_ausentismo": meta,
        "costo_atribuible_psicosocial": costo_psico,
        "inversion_sst_estimada": round(inversion_sst_estimada, 0),
        "roi_estimado_pct": roi_pct,
        "nota_roi": (
            "ROI estimado sobre inversión del 2% de masa salarial en SST. "
            "Ajustar según presupuesto real de la empresa."
        ),
        "fuente": "Fórmula R10 — config/config.yaml · parámetros económicos",
    }


# ---------------------------------------------------------------------------
# S3 — Benchmarking Ejecutivo
# ---------------------------------------------------------------------------

@router.post("/benchmarking")
def post_benchmarking(filtros: FiltrosRequest):
    """
    S3: Posición de la empresa vs sector/país:
    - % riesgo A+MA IntraA/B con diferencia pp
    - Protocolos urgentes vs prioritarios del sector
    - Top 3 dimensiones vs Colombia
    """
    try:
        bench = _read_parquet("fact_v3_benchmarking.parquet")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    df = bench[bench["empresa"] == filtros.empresa].copy()
    if df.empty:
        raise HTTPException(status_code=404, detail=f"Sin datos benchmarking para '{filtros.empresa}'.")

    riesgo_intra = df[df["tipo"] == "riesgo_intralaboral"].to_dict("records")
    protocolos = df[df["tipo"] == "protocolos_urgentes"].to_dict("records")
    dimensiones = df[df["tipo"] == "dimension_critica"].to_dict("records")

    # Sector
    sector = str(df["sector"].iloc[0]) if "sector" in df.columns else "N/A"

    # Insight narrativo benchmarking
    insights = []
    for row in riesgo_intra:
        diff = row.get("diferencia_pp", 0)
        if diff and diff > 15:
            insights.append({
                "tipo": "alerta",
                "mensaje": (
                    f"{row['instrumento']} supera el referente {row.get('tipo_referente', 'nacional')} "
                    f"en {diff:+.1f} puntos porcentuales (significativo)."
                ),
            })
        elif diff and diff > 5:
            insights.append({
                "tipo": "advertencia",
                "mensaje": f"{row['instrumento']} está {diff:+.1f} pp por encima del referente. Requiere atención.",
            })

    return {
        "empresa": filtros.empresa,
        "sector": sector,
        "riesgo_intralaboral_vs_referente": riesgo_intra,
        "protocolos_urgentes_vs_sector": protocolos,
        "top3_dimensiones_vs_colombia": dimensiones,
        "insights": insights,
        "fuente_referente": "III ENCST 2021 — Ministerio de Trabajo Colombia",
    }


# ---------------------------------------------------------------------------
# S4 — Ranking Áreas Críticas
# ---------------------------------------------------------------------------

@router.post("/ranking_areas")
def post_ranking_areas(filtros: FiltrosRequest):
    """
    S4: Top 5 áreas con mayor % riesgo Alto+Muy Alto Intralaboral.
    R8: áreas con < 5 personas aparecen como confidencial.
    """
    try:
        ranking = _read_parquet("fact_v3_ranking_areas.parquet")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    df = ranking[ranking["empresa"] == filtros.empresa].copy()
    if df.empty:
        raise HTTPException(status_code=404, detail=f"Sin datos de ranking para '{filtros.empresa}'.")

    top5 = df[~df["confidencial"].fillna(False)].nsmallest(5, "ranking") \
        if "ranking" in df.columns and not df.empty else df.head(5)

    areas_conf = df[df["confidencial"].fillna(False)]["area_departamento"].tolist()

    resultado = top5[[
        "ranking", "area_departamento", "n_evaluados",
        "pct_ama", "nivel_predominante",
        "dimension_critica", "semaforo"
    ]].to_dict("records")

    return {
        "empresa": filtros.empresa,
        "top5_areas_criticas": resultado,
        "areas_confidenciales": areas_conf,
        "nota": "Ordenado de mayor a menor % riesgo Alto+Muy Alto Intralaboral (A+B promedio).",
        "nota_r8": f"{len(areas_conf)} área(s) excluida(s) por confidencialidad (n < 5).",
    }


# ---------------------------------------------------------------------------
# S5 — Alertas y Prioridades: Fichas Técnicas
# ---------------------------------------------------------------------------

@router.post("/alertas_protocolos")
def post_alertas_protocolos(filtros: FiltrosRequest):
    """
    S5: Las 3 líneas de gestión principales con ficha técnica de protocolo:
    nombre, objetivo, KPI, resultado esperado, N trabajadores a intervenir,
    sector, dimensiones impactadas e indicadores.
    """
    try:
        prot_emp = _read_parquet("fact_gestion_05_prioridades_protocolos.parquet")
        dim_prot = _read_parquet("dim_protocolos_lineas.parquet")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    # Población por protocolo
    pob_prot = None
    try:
        pob_prot = _read_parquet("fact_gestion_07_protocolos_poblacion.parquet")
    except Exception:
        pass

    df_emp = prot_emp[prot_emp["empresa"] == filtros.empresa].copy() \
        if "empresa" in prot_emp.columns else prot_emp.copy()

    cfg = _load_config()
    sector_empresa = df_emp["sector_economico"].iloc[0] \
        if not df_emp.empty and "sector_economico" in df_emp.columns else "No clasificado"

    # Prioridades por sector desde config
    prios = cfg.get("prioridades_sector", {}).get(sector_empresa, [])

    # Selección top 3 líneas (mayor lesividad × % afectados)
    if not df_emp.empty and "lesividad" in df_emp.columns:
        orden_lesividad = {"Alta": 3, "Media": 2, "Baja": 1}
        df_emp["lesividad_num"] = df_emp["lesividad"].map(orden_lesividad).fillna(0)
        df_emp_sorted = df_emp.sort_values(
            ["lesividad_num", "pct_trabajadores_afectados"],
            ascending=False
        ) if "pct_trabajadores_afectados" in df_emp.columns else df_emp.sort_values(
            "lesividad_num", ascending=False
        )
        top3_prot = df_emp_sorted.head(3)
    else:
        top3_prot = df_emp.head(3)

    fichas = []
    for _, row in top3_prot.iterrows():
        prot_id = str(row.get("protocolo_id", ""))
        linea = str(row.get("protocolo_nombre", prot_id))

        # Buscar ficha en dim_protocolos_lineas
        ficha_dim = dim_prot[dim_prot["protocolo_id"] == prot_id] \
            if not dim_prot.empty and "protocolo_id" in dim_prot.columns \
            else pd.DataFrame()

        objetivo = str(ficha_dim["objetivo"].iloc[0]) if not ficha_dim.empty and "objetivo" in ficha_dim.columns \
            else "Reducir la exposición a factores de riesgo psicosocial identificados."

        kpi_seguimiento = str(ficha_dim["kpi"].iloc[0]) if not ficha_dim.empty and "kpi" in ficha_dim.columns \
            else "% trabajadores en riesgo Alto+Muy Alto"

        resultado_esperado = str(ficha_dim["resultado_esperado"].iloc[0]) \
            if not ficha_dim.empty and "resultado_esperado" in ficha_dim.columns \
            else "Reducción ≥ 10 pp en riesgo Alto+Muy Alto en próxima evaluación"

        dimensiones_impacto = []
        if not ficha_dim.empty and "dimensiones_intralaboral" in ficha_dim.columns:
            val = ficha_dim["dimensiones_intralaboral"].iloc[0]
            dimensiones_impacto = val.split("|") if isinstance(val, str) else []

        indicadores_impacto = []
        if not ficha_dim.empty and "indicadores" in ficha_dim.columns:
            val = ficha_dim["indicadores"].iloc[0]
            indicadores_impacto = val.split("|") if isinstance(val, str) else []

        # Población a intervenir
        n_intervenir = int(row.get("n_trabajadores", 0))
        if pob_prot is not None and not pob_prot.empty and "protocolo_id" in pob_prot.columns:
            sub_pob = pob_prot[
                (pob_prot["empresa"] == filtros.empresa) &
                (pob_prot["protocolo_id"] == prot_id)
            ] if "empresa" in pob_prot.columns else pd.DataFrame()
            if not sub_pob.empty and "n_trabajadores" in sub_pob.columns:
                n_intervenir = int(sub_pob["n_trabajadores"].sum())

        nivel_urgencia = str(row.get("nivel_gestion", "Preventivo"))
        badge_color = (
            "#EF4444" if nivel_urgencia == "Urgente"
            else "#F97316" if nivel_urgencia == "Correctiva"
            else "#F59E0B" if nivel_urgencia == "Mejora selectiva"
            else "#10B981"
        )

        fichas.append({
            "protocolo_id": prot_id,
            "nombre_protocolo": linea,
            "nivel_urgencia": nivel_urgencia,
            "badge_color": badge_color,
            "objetivo": objetivo,
            "kpi_seguimiento": kpi_seguimiento,
            "resultado_esperado": resultado_esperado,
            "n_trabajadores_intervenir": n_intervenir,
            "sector_economico": sector_empresa,
            "dimensiones_intralaboral_impactadas": dimensiones_impacto,
            "indicadores_que_impacta": indicadores_impacto,
            "es_prioritario_sector": prot_id in prios,
        })

    return {
        "empresa": filtros.empresa,
        "sector": sector_empresa,
        "lineas_gestion_top3": fichas,
        "protocolos_prioritarios_sector": prios,
        "nota": (
            "Las 3 líneas se seleccionan por mayor lesividad × % trabajadores afectados, "
            "priorizadas según normatividad vigente para el sector económico."
        ),
        "marco_legal": "Resolución 2764/2022 — Ministerio de Trabajo Colombia",
    }
