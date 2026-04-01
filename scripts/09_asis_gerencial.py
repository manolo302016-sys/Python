"""
09_asis_gerencial.py  — MentalPRO · ETL Visualizador 3
=======================================================
Genera los parquets de apoyo para el Visualizador 3 (Gerencial + ASIS):

    fact_v3_kpis_globales.parquet   → S1 Tarjetas KPI globales
    fact_v3_demografia.parquet      → S2A Variables demográficas
    fact_v3_costos.parquet          → S2B Cálculo R10 ausentismo 6 pasos
    fact_v3_benchmarking.parquet    → S3 Benchmarking ejecutivo
    fact_v3_ranking_areas.parquet   → S4 Top 5 áreas críticas

Reglas aplicadas:
    R8  — Confidencialidad: grupos < 5 personas → "Confidencial"
    R10 — Fórmula costo económico (6 pasos + presentismo + psicosocial)
    R11 — Paleta de colores inamovible
    R13 — Outputs sólo en parquet
    R14 — Validación de columnas obligatorias
    R17 — Homologación sector
    R18 — Escalable: filtra por empresa

Resolución 2764/2022 — Ministerio de Trabajo Colombia
"""

import sys
import logging
from pathlib import Path

import pandas as pd
import numpy as np
import yaml

# ---------------------------------------------------------------------------
# Configuración de rutas y logging
# ---------------------------------------------------------------------------

BASE_DIR = Path(__file__).resolve().parent.parent
CONFIG_PATH = BASE_DIR / "config" / "config.yaml"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)


def _load_config() -> dict:
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def _proc(cfg: dict, filename: str) -> Path:
    """Ruta a un parquet en data/processed/."""
    return BASE_DIR / cfg["paths"]["processed"] / filename


def _save_parquet(df: pd.DataFrame, cfg: dict, filename: str) -> None:
    """Guarda DataFrame como parquet (R13)."""
    path = _proc(cfg, filename)
    df.to_parquet(path, index=False)
    log.info(f"Guardado: {path}  ({len(df):,} filas)")


# ---------------------------------------------------------------------------
# Rangos demográficos estándar
# ---------------------------------------------------------------------------

RANGOS_EDAD = [
    (0, 24, "<25"),
    (25, 34, "25-34"),
    (35, 44, "35-44"),
    (45, 54, "45-54"),
    (55, 999, "≥55"),
]

RANGOS_ANTIG_EMP = [
    (0, 0.99, "<1 año"),
    (1, 2.99, "1-3 años"),
    (3, 4.99, "3-5 años"),
    (5, 9.99, "5-10 años"),
    (10, 999, ">10 años"),
]

RANGOS_ANTIG_CARGO = [
    (0, 0.99, "<1 año"),
    (1, 2.99, "1-3 años"),
    (3, 4.99, "3-5 años"),
    (5, 999, ">5 años"),
]

RANGOS_DEPENDIENTES = [
    (0, 0, "0"),
    (1, 1, "1"),
    (2, 2, "2"),
    (3, 3, "3"),
    (4, 999, "4+"),
]


def _asignar_rango(valor: float, rangos: list) -> str:
    for lo, hi, etiqueta in rangos:
        if lo <= valor <= hi:
            return etiqueta
    return "Sin dato"


def _sanitizar_tipos(df: pd.DataFrame) -> pd.DataFrame:
    """Convierte StringDtype → object para compatibilidad pyarrow (feedback_parquet_tipos)."""
    for col in df.columns:
        if hasattr(df[col], "dtype") and str(df[col].dtype) == "string":
            df[col] = df[col].astype(object)
    return df


# ---------------------------------------------------------------------------
# PASO 1 — KPIs Globales (S1)
# ---------------------------------------------------------------------------

NIVELES_RIESGO = ["Sin riesgo", "Bajo", "Medio", "Alto", "Muy alto"]
NIVEL_RIESGO_NUM = {n: i + 1 for i, n in enumerate(NIVELES_RIESGO)}
PROTECCION_NIVELES = ["Muy alto", "Alto", "Medio", "Bajo", "Muy bajo"]  # orden inverso para individual


def calcular_kpis_globales(cfg: dict) -> pd.DataFrame:
    """
    Genera fact_v3_kpis_globales.parquet

    Columnas resultado:
        empresa, fecha_evaluacion,
        instrumento, forma, kpi_grupo,
        nivel, n_trabajadores, pct_trabajadores,
        pct_referente_sector, pct_referente_pais,
        diferencia_pp_sector, diferencia_pp_pais,
        semaforo_color
    """
    log.info("=== PASO 1: KPIs Globales ===")

    baremo = pd.read_parquet(_proc(cfg, "fact_scores_baremo.parquet"))
    benchmark = pd.read_parquet(_proc(cfg, "fact_benchmark.parquet"))
    trabajadores = pd.read_parquet(_proc(cfg, "dim_trabajador.parquet"))

    # R14: validar columnas obligatorias
    cols_req_baremo = {"cedula", "empresa", "forma_intra", "nivel_analisis", "nivel_riesgo", "tipo_baremo"}
    faltantes = cols_req_baremo - set(baremo.columns)
    if faltantes:
        raise ValueError(f"fact_scores_baremo falta columnas: {faltantes}")

    # Benchmark sectorial por instrumento (IntraA, IntraB, Extra, Estres)
    bench_ref = benchmark[benchmark["tipo_comparativa"] == "sector_o_pais"].copy() \
        if "tipo_comparativa" in benchmark.columns else benchmark.copy()

    resultados = []

    for empresa in baremo["empresa"].unique():
        df_emp = baremo[baremo["empresa"] == empresa].copy()

        # Fecha evaluación
        fecha = df_emp["fecha_evaluacion"].max() if "fecha_evaluacion" in df_emp.columns else "N/A"

        for tipo_baremo, forma, kpi_grupo in [
            ("intralaboral", "A", "Intralaboral A"),
            ("intralaboral", "B", "Intralaboral B"),
            ("extralaboral", "A", "Extralaboral A"),
            ("extralaboral", "B", "Extralaboral B"),
            ("estres", "A", "Estrés A"),
            ("estres", "B", "Estrés B"),
        ]:
            mask = (
                (df_emp["tipo_baremo"] == tipo_baremo) &
                (df_emp["forma_intra"] == forma) &
                (df_emp["nivel_analisis"] == "factor")
            )
            sub = df_emp[mask]
            if sub.empty:
                continue

            n_total = sub["cedula"].nunique()
            if n_total < 5:  # R8
                log.info(f"  R8 Confidencial: {empresa} / {kpi_grupo} n={n_total}")
                continue

            # % por nivel
            conteo = sub.groupby("nivel_riesgo")["cedula"].nunique().reindex(
                NIVELES_RIESGO, fill_value=0
            )

            # Referente: buscar en benchmark
            pct_sector = None
            pct_pais = None
            if not bench_ref.empty:
                m_sector = bench_ref[
                    (bench_ref["empresa"] == empresa) &
                    (bench_ref["tipo_baremo"] == tipo_baremo) &
                    (bench_ref["forma_intra"] == forma)
                ]
                if not m_sector.empty and "pct_alto_muy_alto_sector" in m_sector.columns:
                    pct_sector = m_sector["pct_alto_muy_alto_sector"].iloc[0]
                if not m_sector.empty and "pct_alto_muy_alto_pais" in m_sector.columns:
                    pct_pais = m_sector["pct_alto_muy_alto_pais"].iloc[0]

            for nivel in NIVELES_RIESGO:
                n_nivel = int(conteo.get(nivel, 0))
                pct = round(n_nivel / n_total * 100, 1) if n_total > 0 else 0.0

                # Diferencia pp sólo para Alto + Muy Alto
                pct_ref = pct_sector if pct_sector is not None else pct_pais
                diff_sector = round(pct - pct_sector, 1) if pct_sector is not None and nivel in ("Alto", "Muy alto") else None
                diff_pais = round(pct - pct_pais, 1) if pct_pais is not None and nivel in ("Alto", "Muy alto") else None

                # Semáforo sobre % Alto+MuyAlto agregado
                pct_a_ma = round(
                    (conteo.get("Alto", 0) + conteo.get("Muy alto", 0)) / n_total * 100, 1
                ) if n_total > 0 else 0.0
                if pct_a_ma > 35:
                    semaforo = "#EF4444"  # rojo
                elif pct_a_ma >= 15:
                    semaforo = "#F59E0B"  # amarillo
                else:
                    semaforo = "#10B981"  # verde

                resultados.append({
                    "empresa": empresa,
                    "fecha_evaluacion": fecha,
                    "instrumento": tipo_baremo,
                    "forma": forma,
                    "kpi_grupo": kpi_grupo,
                    "nivel": nivel,
                    "n_total": n_total,
                    "n_nivel": n_nivel,
                    "pct_nivel": pct,
                    "pct_alto_muy_alto": pct_a_ma,
                    "pct_referente_sector": pct_sector,
                    "pct_referente_pais": pct_pais,
                    "diferencia_pp_sector": diff_sector,
                    "diferencia_pp_pais": diff_pais,
                    "semaforo_color": semaforo,
                })

    # KPI Factor Individual (Vulnerabilidad)
    for empresa in baremo["empresa"].unique():
        df_emp = baremo[baremo["empresa"] == empresa].copy()
        fecha = df_emp["fecha_evaluacion"].max() if "fecha_evaluacion" in df_emp.columns else "N/A"

        mask_ind = (
            (df_emp["tipo_baremo"] == "individual") &
            (df_emp["nivel_analisis"] == "factor")
        )
        sub_ind = df_emp[mask_ind]
        if sub_ind.empty:
            continue

        n_total_ind = sub_ind["cedula"].nunique()
        if n_total_ind < 5:
            continue

        # Vulnerabilidad = Bajo + Muy bajo (orden inverso protección)
        conteo_ind = sub_ind.groupby("nivel_riesgo")["cedula"].nunique()
        n_vuln = int(conteo_ind.get("Bajo", 0) + conteo_ind.get("Muy bajo", 0))
        pct_vuln = round(n_vuln / n_total_ind * 100, 1)

        # Referente Colombia individual (4.2% según config)
        pct_pais_ind = cfg.get("benchmarking_dominios", {}).get("Individual (vulnerabilidad)", 4.2)

        resultados.append({
            "empresa": empresa,
            "fecha_evaluacion": fecha,
            "instrumento": "individual",
            "forma": "A+B",
            "kpi_grupo": "Vulnerabilidad Psicológica",
            "nivel": "Bajo+Muy bajo",
            "n_total": n_total_ind,
            "n_nivel": n_vuln,
            "pct_nivel": pct_vuln,
            "pct_alto_muy_alto": pct_vuln,
            "pct_referente_sector": None,
            "pct_referente_pais": pct_pais_ind,
            "diferencia_pp_sector": None,
            "diferencia_pp_pais": round(pct_vuln - pct_pais_ind, 1),
            "semaforo_color": "#EF4444" if pct_vuln > 35 else "#F59E0B" if pct_vuln >= 15 else "#10B981",
        })

    # KPIs Protocolos Urgentes + Vigilancia
    _kpis_intervencion(cfg, resultados, baremo)

    df_out = pd.DataFrame(resultados)
    df_out = _sanitizar_tipos(df_out)
    _save_parquet(df_out, cfg, "fact_v3_kpis_globales.parquet")
    return df_out


def _kpis_intervencion(cfg: dict, resultados: list, baremo: pd.DataFrame) -> None:
    """Agrega KPIs de protocolos urgentes y vigilancia epidemiológica."""
    try:
        prot = pd.read_parquet(_proc(cfg, "fact_gestion_04_niveles_ejes.parquet"))
        vig = pd.read_parquet(_proc(cfg, "fact_gestion_06_vigilancia_resumen.parquet"))
    except FileNotFoundError as e:
        log.warning(f"Parquet no encontrado para KPI intervención: {e}")
        return

    for empresa in baremo["empresa"].unique():
        n_total_emp = baremo[baremo["empresa"] == empresa]["cedula"].nunique()
        fecha = baremo[baremo["empresa"] == empresa]["fecha_evaluacion"].max() \
            if "fecha_evaluacion" in baremo.columns else "N/A"

        # Protocolos Urgentes + Inmediatos
        if not prot.empty and "empresa" in prot.columns and "nivel_gestion" in prot.columns:
            mask_urg = (
                (prot["empresa"] == empresa) &
                (prot["nivel_gestion"].isin(["Urgente", "Correctiva"]))
            )
            sub_prot = prot[mask_urg]
            n_urg = sub_prot["cedula"].nunique() if "cedula" in sub_prot.columns else len(sub_prot)
            pct_urg = round(n_urg / n_total_emp * 100, 1) if n_total_emp > 0 else 0.0

            resultados.append({
                "empresa": empresa,
                "fecha_evaluacion": fecha,
                "instrumento": "gestion",
                "forma": "A+B",
                "kpi_grupo": "Protocolos Urgentes e Inmediatos",
                "nivel": "Urgente+Inmediato",
                "n_total": n_total_emp,
                "n_nivel": n_urg,
                "pct_nivel": pct_urg,
                "pct_alto_muy_alto": pct_urg,
                "pct_referente_sector": None,
                "pct_referente_pais": None,
                "diferencia_pp_sector": None,
                "diferencia_pp_pais": None,
                "semaforo_color": "#EF4444" if pct_urg > 35 else "#F59E0B" if pct_urg >= 15 else "#10B981",
            })

        # Vigilancia Epidemiológica
        if not vig.empty and "empresa" in vig.columns:
            sub_vig = vig[vig["empresa"] == empresa]
            n_vig = sub_vig["n_casos"].sum() if "n_casos" in sub_vig.columns else len(sub_vig)
            pct_vig = round(n_vig / n_total_emp * 100, 1) if n_total_emp > 0 else 0.0

            resultados.append({
                "empresa": empresa,
                "fecha_evaluacion": fecha,
                "instrumento": "vigilancia",
                "forma": "A+B",
                "kpi_grupo": "Vigilancia Epidemiológica",
                "nivel": "Casos sospechosos",
                "n_total": n_total_emp,
                "n_nivel": int(n_vig),
                "pct_nivel": pct_vig,
                "pct_alto_muy_alto": pct_vig,
                "pct_referente_sector": None,
                "pct_referente_pais": None,
                "diferencia_pp_sector": None,
                "diferencia_pp_pais": None,
                "semaforo_color": "#EF4444" if pct_vig > 20 else "#F59E0B" if pct_vig >= 10 else "#10B981",
            })


# ---------------------------------------------------------------------------
# PASO 2 — Demografía (S2A)
# ---------------------------------------------------------------------------

def calcular_demografia(cfg: dict) -> pd.DataFrame:
    """
    Genera fact_v3_demografia.parquet

    Columnas resultado:
        empresa, variable_demografica, categoria, n, pct,
        sexo (solo para pirámide), confidencial
    """
    log.info("=== PASO 2: Variables Demográficas ===")

    demo = pd.read_parquet(_proc(cfg, "dim_demografia.parquet"))
    trab = pd.read_parquet(_proc(cfg, "dim_trabajador.parquet"))

    # R14: columnas mínimas
    cols_req_demo = {"cedula"}
    if not cols_req_demo.issubset(demo.columns):
        raise ValueError(f"dim_demografia falta columnas: {cols_req_demo - set(demo.columns)}")

    df_merge = demo.merge(trab[["cedula", "empresa", "area_departamento",
                                 "categoria_cargo", "forma_intralaboral"]],
                          on="cedula", how="left")

    resultados = []

    for empresa in df_merge["empresa"].dropna().unique():
        sub = df_merge[df_merge["empresa"] == empresa].copy()
        n_emp = sub["cedula"].nunique()

        # G1: Pirámide poblacional (edad × sexo)
        if "edad_cumplida" in sub.columns and "sexo" in sub.columns:
            sub["rango_edad"] = sub["edad_cumplida"].apply(
                lambda x: _asignar_rango(float(x), RANGOS_EDAD) if pd.notna(x) else "Sin dato"
            )
            pir = sub.groupby(["rango_edad", "sexo"])["cedula"].nunique().reset_index()
            pir.columns = ["categoria", "sexo", "n"]
            pir["pct"] = (pir["n"] / n_emp * 100).round(1)
            pir["variable_demografica"] = "piramide_poblacional"
            pir["empresa"] = empresa
            pir["confidencial"] = pir["n"] < 5  # R8
            resultados.append(pir)

        # G2: Antigüedad empresa
        if "antiguedad_empresa" in sub.columns:
            sub["rango_antig_emp"] = sub["antiguedad_empresa"].apply(
                lambda x: _asignar_rango(float(x), RANGOS_ANTIG_EMP) if pd.notna(x) else "Sin dato"
            )
            _agg_variable(sub, "rango_antig_emp", "antiguedad_empresa", empresa, n_emp, resultados)

        # G3: Antigüedad cargo
        if "antiguedad_cargo" in sub.columns:
            sub["rango_antig_cargo"] = sub["antiguedad_cargo"].apply(
                lambda x: _asignar_rango(float(x), RANGOS_ANTIG_CARGO) if pd.notna(x) else "Sin dato"
            )
            _agg_variable(sub, "rango_antig_cargo", "antiguedad_cargo", empresa, n_emp, resultados)

        # G4: Estado civil
        if "estado_civil" in sub.columns:
            _agg_variable(sub, "estado_civil", "estado_civil", empresa, n_emp, resultados)

        # G5: Dependientes económicos
        if "dependientes_economicos" in sub.columns:
            sub["rango_dep"] = sub["dependientes_economicos"].apply(
                lambda x: _asignar_rango(float(x), RANGOS_DEPENDIENTES) if pd.notna(x) else "Sin dato"
            )
            _agg_variable(sub, "rango_dep", "dependientes_economicos", empresa, n_emp, resultados)

        # G6: Distribución por área
        if "area_departamento" in sub.columns:
            _agg_variable(sub, "area_departamento", "area_departamento", empresa, n_emp, resultados, top=10)

        # G7: Distribución por cargo
        if "categoria_cargo" in sub.columns:
            _agg_variable(sub, "categoria_cargo", "categoria_cargo", empresa, n_emp, resultados, top=10)

        # G8: Forma intralaboral
        if "forma_intralaboral" in sub.columns:
            _agg_variable(sub, "forma_intralaboral", "forma_intralaboral", empresa, n_emp, resultados)

    if not resultados:
        log.warning("No se generaron datos demográficos.")
        return pd.DataFrame()

    df_out = pd.concat(resultados, ignore_index=True)

    # Garantizar columnas mínimas
    for col in ["sexo", "confidencial"]:
        if col not in df_out.columns:
            df_out[col] = None

    df_out = _sanitizar_tipos(df_out)
    _save_parquet(df_out, cfg, "fact_v3_demografia.parquet")
    return df_out


def _agg_variable(
    sub: pd.DataFrame,
    col_cat: str,
    nombre_var: str,
    empresa: str,
    n_total: int,
    resultados: list,
    top: int = None,
) -> None:
    """Helper: agrega una variable demográfica y la agrega a resultados."""
    agg = sub.groupby(col_cat)["cedula"].nunique().reset_index()
    agg.columns = ["categoria", "n"]
    agg["pct"] = (agg["n"] / n_total * 100).round(1)
    agg["variable_demografica"] = nombre_var
    agg["empresa"] = empresa
    agg["sexo"] = None
    agg["confidencial"] = agg["n"] < 5  # R8

    if top:
        agg = agg.nlargest(top, "n")

    resultados.append(agg)


# ---------------------------------------------------------------------------
# PASO 3 — Costos de Ausentismo R10 (S2B)
# ---------------------------------------------------------------------------

def calcular_costos_ausentismo(cfg: dict) -> pd.DataFrame:
    """
    Genera fact_v3_costos.parquet

    Aplica fórmula R10 (6 pasos + presentismo + psicosocial) por empresa.

    Columnas resultado:
        empresa, paso, nombre_paso, valor, unidad, nota
    """
    log.info("=== PASO 3: Costos Ausentismo R10 ===")

    dim_ausen = pd.read_parquet(_proc(cfg, "dim_ausentismo.parquet"))
    dim_trab = pd.read_parquet(_proc(cfg, "dim_trabajador.parquet"))

    # Parámetros económicos desde config
    eco = cfg.get("parametros_economicos", {})
    SMLV_MENSUAL = eco.get("SMLV_mensual", 2_800_000)
    FACTOR_PRESENTISMO = eco.get("presentismo_factor", 1.40)
    COSTO_EMPLEADOR = eco.get("costo_empleador", 0.60)
    FACTOR_PSICOSOCIAL = eco.get("factor_psicosocial", 0.30)
    AUSENTISMO_REFERENTE_PAIS = 0.03  # 3% promedio Colombia
    DIAS_LABORALES_ANIO = 240

    resultados = []

    for empresa in dim_trab["empresa"].dropna().unique():
        sub_trab = dim_trab[dim_trab["empresa"] == empresa]
        n_planta = int(sub_trab["total_planta"].iloc[0]) if "total_planta" in sub_trab.columns \
            else sub_trab["cedula"].nunique()

        # Datos ausentismo
        sub_ausen = dim_ausen[dim_ausen["empresa"] == empresa] if "empresa" in dim_ausen.columns \
            else dim_ausen
        total_dias_ausencia = int(sub_ausen["total_dias_ausencia"].sum()) \
            if "total_dias_ausencia" in sub_ausen.columns else 0

        dias_cap_instalada = n_planta * DIAS_LABORALES_ANIO

        # ---- PASO 1: N trabajadores ----
        p1 = n_planta

        # ---- PASO 2: % ausentismo actual ----
        pct_ausentismo = round(total_dias_ausencia / dias_cap_instalada * 100, 2) \
            if dias_cap_instalada > 0 else 0.0
        diferencia_vs_pais = round(pct_ausentismo - AUSENTISMO_REFERENTE_PAIS * 100, 2)

        # ---- PASO 3: Costo salario anual FTE ----
        p3 = SMLV_MENSUAL * 12

        # ---- PASO 4: Capacidad productiva anual ----
        p4 = p1 * p3

        # ---- PASO 5: Pérdida económica anual ----
        p5 = (pct_ausentismo / 100) * p4

        # ---- PASO 6: Pérdida atribuida al empleador ----
        p6 = p5 * COSTO_EMPLEADOR

        # ---- Resultado: Pérdida productividad + presentismo ----
        perdida_productividad = p6 * FACTOR_PRESENTISMO

        # ---- Estimación psicosocial ----
        costo_psicosocial = perdida_productividad * FACTOR_PSICOSOCIAL

        # Semáforo ausentismo
        if pct_ausentismo > 5:
            semaforo_ausen = "#EF4444"
        elif pct_ausentismo >= 3:
            semaforo_ausen = "#F59E0B"
        else:
            semaforo_ausen = "#10B981"

        pasos = [
            (1, "N° trabajadores en planta de personal", p1, "trabajadores",
             "Universo total de la empresa"),
            (2, "% ausentismo actual", pct_ausentismo, "%",
             f"Referente Colombia: 3% | Diferencia: {diferencia_vs_pais:+.2f} pp"),
            (3, "Costo salario anual × FTE", p3, "COP",
             f"SMLV mensual ({SMLV_MENSUAL:,.0f}) × 12 meses"),
            (4, "Capacidad productiva anual", p4, "COP",
             "N trabajadores × Costo FTE anual"),
            (5, "Pérdida económica anual por ausentismo", p5, "COP",
             "% ausentismo × Capacidad productiva. Empleador + SGSS"),
            (6, "Pérdida económica del empleador", p6, "COP",
             f"{int(COSTO_EMPLEADOR*100)}% del costo total (empleador). {int((1-COSTO_EMPLEADOR)*100)}% = SGSS"),
            (7, "Pérdida de productividad (ausentismo + presentismo)", perdida_productividad, "COP",
             f"Paso 6 × {FACTOR_PRESENTISMO} (+{int((FACTOR_PRESENTISMO-1)*100)}% por presentismo atribuible)"),
            (8, "Costo estimado atribuible a riesgo psicosocial (30%)", costo_psicosocial, "COP",
             f"Paso 7 × {int(FACTOR_PSICOSOCIAL*100)}% según estudios vigentes"),
        ]

        for paso_num, nombre, valor, unidad, nota in pasos:
            resultados.append({
                "empresa": empresa,
                "paso": paso_num,
                "nombre_paso": nombre,
                "valor": round(float(valor), 2),
                "unidad": unidad,
                "nota": nota,
                "n_planta": p1,
                "total_dias_ausencia": total_dias_ausencia,
                "dias_cap_instalada": dias_cap_instalada,
                "pct_ausentismo": pct_ausentismo,
                "diferencia_pp_vs_pais": diferencia_vs_pais,
                "semaforo_ausentismo": semaforo_ausen,
            })

    df_out = pd.DataFrame(resultados)
    df_out = _sanitizar_tipos(df_out)
    _save_parquet(df_out, cfg, "fact_v3_costos.parquet")
    return df_out


# ---------------------------------------------------------------------------
# PASO 4 — Benchmarking Ejecutivo (S3)
# ---------------------------------------------------------------------------

def calcular_benchmarking_ejecutivo(cfg: dict) -> pd.DataFrame:
    """
    Genera fact_v3_benchmarking.parquet

    Consolida:
        - % riesgo A+MA IntraA/B vs sector/país
        - Protocolos urgentes vs sector
        - Top 3 dimensiones vs Colombia
    """
    log.info("=== PASO 4: Benchmarking Ejecutivo ===")

    benchmark = pd.read_parquet(_proc(cfg, "fact_benchmark.parquet"))
    baremo = pd.read_parquet(_proc(cfg, "fact_scores_baremo.parquet"))

    prot = None
    try:
        prot = pd.read_parquet(_proc(cfg, "fact_gestion_05_prioridades_protocolos.parquet"))
    except FileNotFoundError:
        log.warning("fact_gestion_05_prioridades_protocolos.parquet no encontrado. KPI protocolos omitido.")

    bench_sector = cfg.get("benchmarking_sectorial", {})
    prios_sector = cfg.get("prioridades_sector", {})

    resultados = []

    for empresa in baremo["empresa"].unique():
        # Sector de la empresa
        sub_trab_query = baremo[baremo["empresa"] == empresa]
        sector = sub_trab_query["sector_homologado"].iloc[0] \
            if "sector_homologado" in sub_trab_query.columns else "No clasificado"

        ref_sector_pct = bench_sector.get(sector, bench_sector.get("No clasificado", 39.69))
        prios_sect = prios_sector.get(sector, [])

        bench_emp = benchmark[benchmark["empresa"] == empresa] if "empresa" in benchmark.columns \
            else pd.DataFrame()

        # Riesgo IntraA/B vs sector
        for tipo, forma, label in [
            ("intralaboral", "A", "IntraA"),
            ("intralaboral", "B", "IntraB"),
        ]:
            mask = (
                (baremo["empresa"] == empresa) &
                (baremo["tipo_baremo"] == tipo) &
                (baremo["forma_intra"] == forma) &
                (baremo["nivel_analisis"] == "factor")
            )
            sub = baremo[mask]
            if sub.empty or sub["cedula"].nunique() < 5:
                continue

            n_total = sub["cedula"].nunique()
            n_ama = sub[sub["nivel_riesgo"].isin(["Alto", "Muy alto"])]["cedula"].nunique()
            pct_emp = round(n_ama / n_total * 100, 1)
            ref_pais = ref_sector_pct  # Referente por sector o país

            # Buscar referente específico en benchmark
            if not bench_emp.empty:
                b_row = bench_emp[
                    (bench_emp.get("tipo_baremo", pd.Series()) == tipo) &
                    (bench_emp.get("forma_intra", pd.Series()) == forma)
                ] if "tipo_baremo" in bench_emp.columns else pd.DataFrame()
                if not b_row.empty and "pct_referente" in b_row.columns:
                    ref_pais = float(b_row["pct_referente"].iloc[0])

            diff = round(pct_emp - ref_pais, 1)

            if diff > 10:
                semaforo = "#EF4444"
            elif diff > 0:
                semaforo = "#F59E0B"
            else:
                semaforo = "#10B981"

            resultados.append({
                "empresa": empresa,
                "sector": sector,
                "tipo": "riesgo_intralaboral",
                "instrumento": label,
                "pct_empresa_a_ma": pct_emp,
                "pct_referente": ref_pais,
                "diferencia_pp": diff,
                "semaforo_color": semaforo,
                "descripcion": f"% trabajadores en riesgo Alto+Muy Alto {label}",
                "fuente_referente": sector if sector in bench_sector else "País (ENCST)",
            })

        # Protocolos urgentes vs sector
        if prot is not None and not prot.empty:
            mask_urg = (prot["empresa"] == empresa) & \
                       (prot.get("nivel_gestion", pd.Series()) == "Urgente") \
                if "empresa" in prot.columns and "nivel_gestion" in prot.columns \
                else pd.Series(False, index=prot.index)
            n_prot_urg = int(mask_urg.sum())
            prot_ids_empresa = prot[mask_urg]["protocolo_id"].unique().tolist() \
                if "protocolo_id" in prot.columns else []

            coincidencias = [p for p in prot_ids_empresa if p in prios_sect]

            resultados.append({
                "empresa": empresa,
                "sector": sector,
                "tipo": "protocolos_urgentes",
                "instrumento": "Gestión",
                "pct_empresa_a_ma": n_prot_urg,
                "pct_referente": len(prios_sect),
                "diferencia_pp": len(coincidencias),
                "semaforo_color": "#EF4444" if n_prot_urg > 5 else "#F59E0B" if n_prot_urg > 2 else "#10B981",
                "descripcion": f"N protocolos urgentes empresa | {len(coincidencias)} coinciden con sector",
                "fuente_referente": f"Prioridades sector {sector}",
            })

        # Top 3 dimensiones vs Colombia
        if not bench_emp.empty and "nivel_analisis" in bench_emp.columns:
            dim_bench = bench_emp[bench_emp["nivel_analisis"] == "dimension"].copy()
            if not dim_bench.empty and "pct_empresa" in dim_bench.columns:
                dim_bench["diff"] = dim_bench["pct_empresa"] - dim_bench["pct_referente"]
                top3 = dim_bench.nlargest(3, "diff")

                for _, row in top3.iterrows():
                    diff_dim = round(float(row["diff"]), 1)
                    resultados.append({
                        "empresa": empresa,
                        "sector": sector,
                        "tipo": "dimension_critica",
                        "instrumento": str(row.get("dimension", "N/A")),
                        "pct_empresa_a_ma": round(float(row["pct_empresa"]), 1),
                        "pct_referente": round(float(row["pct_referente"]), 1),
                        "diferencia_pp": diff_dim,
                        "semaforo_color": "#EF4444" if diff_dim > 15 else "#F59E0B" if diff_dim > 5 else "#10B981",
                        "descripcion": f"Dimensión intralaboral vs Colombia",
                        "fuente_referente": "ENCST País",
                    })

    df_out = pd.DataFrame(resultados)
    df_out = _sanitizar_tipos(df_out)
    _save_parquet(df_out, cfg, "fact_v3_benchmarking.parquet")
    return df_out


# ---------------------------------------------------------------------------
# PASO 5 — Ranking Áreas Críticas Top 5 (S4)
# ---------------------------------------------------------------------------

def calcular_ranking_areas(cfg: dict) -> pd.DataFrame:
    """
    Genera fact_v3_ranking_areas.parquet

    Top 5 áreas con mayor % riesgo Alto+MuyAlto Intralaboral (promedio A+B).
    Regla R8: excluir áreas con < 5 personas.
    """
    log.info("=== PASO 5: Ranking Áreas Críticas ===")

    consolidado = pd.read_parquet(_proc(cfg, "fact_consolidado.parquet"))

    # R14: validar columnas
    cols_req = {"cedula", "empresa", "area_departamento", "tipo_baremo", "nivel_riesgo"}
    faltantes = cols_req - set(consolidado.columns)
    if faltantes:
        raise ValueError(f"fact_consolidado falta columnas: {faltantes}")

    resultados = []

    for empresa in consolidado["empresa"].unique():
        df_emp = consolidado[consolidado["empresa"] == empresa].copy()

        # Filtrar intralaboral nivel factor
        mask_intra = (
            (df_emp["tipo_baremo"] == "intralaboral") &
            (df_emp["nivel_analisis"] == "factor")
        ) if "nivel_analisis" in df_emp.columns else (df_emp["tipo_baremo"] == "intralaboral")

        sub_intra = df_emp[mask_intra].copy()

        if sub_intra.empty:
            continue

        # Agrupar por área
        for area in sub_intra["area_departamento"].dropna().unique():
            sub_area = sub_intra[sub_intra["area_departamento"] == area]
            n_area = sub_area["cedula"].nunique()

            if n_area < 5:  # R8
                resultados.append({
                    "empresa": empresa,
                    "area_departamento": area,
                    "n_evaluados": n_area,
                    "confidencial": True,
                    "pct_alto_muy_alto_intra": None,
                    "nivel_predominante": None,
                    "dimension_critica": None,
                    "semaforo_color": None,
                    "ranking": None,
                })
                continue

            n_ama = sub_area[sub_area["nivel_riesgo"].isin(["Alto", "Muy alto"])]["cedula"].nunique()
            pct_ama = round(n_ama / n_area * 100, 1)

            # Nivel predominante
            nivel_counts = sub_area.groupby("nivel_riesgo")["cedula"].nunique()
            nivel_pred = nivel_counts.idxmax() if not nivel_counts.empty else "Sin dato"

            # Dimensión crítica (mayor % AMA si hay datos dimensión)
            dim_critica = None
            if "dimension" in sub_area.columns:
                dim_agg = sub_area.groupby("dimension").apply(
                    lambda x: (x[x["nivel_riesgo"].isin(["Alto", "Muy alto"])]["cedula"].nunique() /
                               max(x["cedula"].nunique(), 1) * 100)
                )
                if not dim_agg.empty:
                    dim_critica = dim_agg.idxmax()

            semaforo = "#EF4444" if pct_ama > 35 else "#F59E0B" if pct_ama >= 15 else "#10B981"

            resultados.append({
                "empresa": empresa,
                "area_departamento": area,
                "n_evaluados": n_area,
                "confidencial": False,
                "pct_alto_muy_alto_intra": pct_ama,
                "nivel_predominante": nivel_pred,
                "dimension_critica": dim_critica,
                "semaforo_color": semaforo,
                "ranking": None,  # se asigna tras ordenar
            })

    df_out = pd.DataFrame(resultados)

    # Asignar ranking por empresa (solo no confidenciales)
    dfs_ranking = []
    for empresa in df_out["empresa"].unique():
        sub_e = df_out[df_out["empresa"] == empresa].copy()
        sub_no_conf = sub_e[~sub_e["confidencial"]].sort_values(
            "pct_alto_muy_alto_intra", ascending=False
        ).reset_index(drop=True)
        sub_no_conf["ranking"] = sub_no_conf.index + 1

        # Mantener sólo top 5
        sub_top5 = sub_no_conf[sub_no_conf["ranking"] <= 5]
        dfs_ranking.append(sub_top5)

        # Añadir confidenciales
        sub_conf = sub_e[sub_e["confidencial"]]
        if not sub_conf.empty:
            dfs_ranking.append(sub_conf)

    if dfs_ranking:
        df_out = pd.concat(dfs_ranking, ignore_index=True)
    else:
        df_out = pd.DataFrame()

    df_out = _sanitizar_tipos(df_out)
    _save_parquet(df_out, cfg, "fact_v3_ranking_areas.parquet")
    return df_out


# ---------------------------------------------------------------------------
# Orquestador principal
# ---------------------------------------------------------------------------

def main():
    log.info("╔══════════════════════════════════════════════════════════╗")
    log.info("║  MentalPRO · ETL 09 · Visualizador 3 Gerencial + ASIS   ║")
    log.info("╚══════════════════════════════════════════════════════════╝")

    cfg = _load_config()

    # Ejecutar pasos en orden (cada uno depende del parquet de entrada)
    kpis = calcular_kpis_globales(cfg)
    demo = calcular_demografia(cfg)
    costos = calcular_costos_ausentismo(cfg)
    bench = calcular_benchmarking_ejecutivo(cfg)
    ranking = calcular_ranking_areas(cfg)

    log.info("=== RESUMEN FINAL ===")
    log.info(f"KPIs globales:         {len(kpis):,} filas")
    log.info(f"Demografía:            {len(demo):,} filas")
    log.info(f"Costos ausentismo:     {len(costos):,} filas")
    log.info(f"Benchmarking:          {len(bench):,} filas")
    log.info(f"Ranking áreas:         {len(ranking):,} filas")
    log.info("✅ ETL 09 completado exitosamente.")


if __name__ == "__main__":
    main()
