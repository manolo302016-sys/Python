"""
09_asis_gerencial.py  — MentalPRO · ETL Visualizador 3
=======================================================
Genera los parquets de apoyo para el Visualizador 3 (Gerencial + ASIS):

    fact_v3_kpis_globales.parquet   → S1 Tarjetas KPI globales
    fact_v3_demografia.parquet      → S2A Variables demográficas
    fact_v3_costos.parquet          → S2B Cálculo R10 ausentismo
    fact_v3_benchmarking.parquet    → S3 Benchmarking ejecutivo
    fact_v3_ranking_areas.parquet   → S4 Top 5 áreas críticas

Columnas reales del modelo de datos:
    fact_scores_baremo  → cedula, empresa, forma_intra, sector_rag,
                          instrumento, nivel_analisis, nombre_nivel,
                          nivel_riesgo (1-5), etiqueta_nivel, tipo_baremo
    dim_trabajador      → cedula, empresa, sector_economico, area_departamento,
                          categoria_cargo, forma_intra, es_Jefe
    dim_demografia      → cedula, sexo, edad_cumplida, estado_civil,
                          numero_dependientes_economicos,
                          'antiguedad_empresa_años_cumplidos',
                          'antiguedad_en_cargo_años_cumplidos', fecha_aplicacion
    dim_ausentismo      → cedula, dias_ausencia (sin empresa directa)
    fact_consolidado    → baremo + demografía merged, dias_ausencia incluido
    fact_benchmark      → empresa, sector_rag, nivel_analisis, nombre_nivel,
                          forma_intra, n_total, n_alto, pct_empresa,
                          pct_referencia, tipo_referencia, diferencia_pp, semaforo

config.yaml claves:
    smlv_mensual, costo_empleador_pct, presentismo_factor, psicosocial_pct,
    confidencialidad_n_min, benchmark_sector, benchmark_dominio
"""

import sys
import logging
from pathlib import Path

import pandas as pd
import yaml

BASE_DIR = Path(__file__).resolve().parent.parent
CONFIG_PATH = BASE_DIR / "config" / "config.yaml"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)

# ── Mapeos de nivel_riesgo (numérico → etiqueta) ───────────────────────────
NIVELES_RIESGO_LABEL = {
    1: "Sin riesgo",
    2: "Riesgo bajo",
    3: "Riesgo medio",
    4: "Riesgo alto",
    5: "Riesgo muy alto",
}

# Instrumentos de riesgo (tipo_baremo='riesgo')
INSTRUMENTOS_RIESGO = {
    "IntraA":       "Intralaboral A",
    "IntraB":       "Intralaboral B",
    "Extralaboral": "Extralaboral",
    "Estres":       "Estrés",
}

# Rangos demográficos
RANGOS_EDAD = [
    (0, 24, "<25"), (25, 34, "25-34"), (35, 44, "35-44"),
    (45, 54, "45-54"), (55, 999, "≥55"),
]
RANGOS_ANTIG_EMP = [
    (0, 0.99, "<1 año"), (1, 2.99, "1-3 años"), (3, 4.99, "3-5 años"),
    (5, 9.99, "5-10 años"), (10, 999, ">10 años"),
]
RANGOS_ANTIG_CARGO = [
    (0, 0.99, "<1 año"), (1, 2.99, "1-3 años"),
    (3, 4.99, "3-5 años"), (5, 999, ">5 años"),
]
RANGOS_DEPENDIENTES = [
    (0, 0, "0"), (1, 1, "1"), (2, 2, "2"), (3, 3, "3"), (4, 999, "4+"),
]


def _asignar_rango(valor, rangos: list) -> str:
    try:
        v = float(valor)
    except (TypeError, ValueError):
        return "Sin dato"
    for lo, hi, label in rangos:
        if lo <= v <= hi:
            return label
    return "Sin dato"


def _load_config() -> dict:
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def _proc(cfg: dict, filename: str) -> Path:
    return BASE_DIR / cfg["paths"]["processed"] / filename


def _save(df: pd.DataFrame, cfg: dict, filename: str) -> None:
    """Guarda parquet; convierte StringDtype → object (compatibilidad pyarrow)."""
    for col in df.columns:
        if str(df[col].dtype) == "string":
            df[col] = df[col].astype(object)
    path = _proc(cfg, filename)
    df.to_parquet(path, index=False)
    log.info(f"  → {filename}  ({len(df):,} filas)")


def _col_antig_empresa(df: pd.DataFrame) -> str:
    """Devuelve el nombre real de la columna de antigüedad empresa (tiene ñ)."""
    candidates = [c for c in df.columns if "antiguedad_empresa" in c.lower()]
    return candidates[0] if candidates else None


def _col_antig_cargo(df: pd.DataFrame) -> str:
    candidates = [c for c in df.columns if "antiguedad_en_cargo" in c.lower()
                  or "antiguedad_cargo" in c.lower()]
    return candidates[0] if candidates else None


def _semaforo(pct: float) -> str:
    cfg_sem = 35.0
    if pct > cfg_sem:
        return "#EF4444"
    if pct >= 15:
        return "#F59E0B"
    return "#10B981"


# ── PASO 1: KPIs Globales (S1) ─────────────────────────────────────────────

def calcular_kpis_globales(cfg: dict) -> pd.DataFrame:
    log.info("=== PASO 1: KPIs Globales ===")
    baremo = pd.read_parquet(_proc(cfg, "fact_scores_baremo.parquet"))
    benchmark = pd.read_parquet(_proc(cfg, "fact_benchmark.parquet"))
    n_min = cfg.get("confidencialidad_n_min", 5)
    bench_sector = cfg.get("benchmark_sector", {})
    bench_dominio = cfg.get("benchmark_dominio", {})

    resultados = []

    for empresa in baremo["empresa"].dropna().unique():
        df_e = baremo[baremo["empresa"] == empresa]
        bm_e = benchmark[benchmark["empresa"] == empresa] if "empresa" in benchmark.columns else pd.DataFrame()

        # -- Grupo 1: Riesgo por instrumento (IntraA, IntraB, Extralaboral, Estrés) --
        for instr_key, instr_label in INSTRUMENTOS_RIESGO.items():
            sub = df_e[
                (df_e["tipo_baremo"] == "riesgo") &
                (df_e["instrumento"] == instr_key) &
                (df_e["nivel_analisis"] == "factor")
            ]
            if sub.empty:
                continue
            n_total = sub["cedula"].nunique()
            if n_total < n_min:
                continue

            forma = sub["forma_intra"].iloc[0] if "forma_intra" in sub.columns else "?"
            n_ama = sub[sub["nivel_riesgo"].isin([4, 5])]["cedula"].nunique()
            pct_ama = round(n_ama / n_total * 100, 1)

            # Referente desde benchmark parquet
            pct_ref = None
            tipo_ref = None
            if not bm_e.empty:
                bm_row = bm_e[
                    (bm_e["nombre_nivel"] == instr_key) &
                    (bm_e["nivel_analisis"] == "factor")
                ]
                if not bm_row.empty:
                    pct_ref = float(bm_row["pct_referencia"].iloc[0])
                    tipo_ref = str(bm_row["tipo_referencia"].iloc[0])

            # Fallback a config
            if pct_ref is None:
                sector = str(df_e["sector_rag"].iloc[0]) if "sector_rag" in df_e.columns else "No clasificado"
                pct_ref = bench_sector.get(sector, bench_sector.get("_promedio_general", 39.69))
                tipo_ref = "país"

            diff = round(pct_ama - pct_ref, 1)

            # Distribución 5 niveles
            for nivel_num in range(1, 6):
                n_nivel = sub[sub["nivel_riesgo"] == nivel_num]["cedula"].nunique()
                pct_nivel = round(n_nivel / n_total * 100, 1)
                resultados.append({
                    "empresa": empresa,
                    "kpi_grupo": instr_label,
                    "instrumento": instr_key,
                    "forma": forma,
                    "nivel_num": nivel_num,
                    "nivel_label": NIVELES_RIESGO_LABEL.get(nivel_num, str(nivel_num)),
                    "n_total": n_total,
                    "n_nivel": n_nivel,
                    "pct_nivel": pct_nivel,
                    "pct_alto_muy_alto": pct_ama,
                    "pct_referente": pct_ref,
                    "tipo_referente": tipo_ref,
                    "diferencia_pp": diff,
                    "semaforo": _semaforo(pct_ama),
                })

        # -- Grupo 2: Vulnerabilidad psicológica (tipo_baremo='individual', niveles 1-2=Muy bajo+Bajo) --
        # BUG FIX: el tipo_baremo real en baremo es 'individual', no 'proteccion'
        sub_ind = df_e[
            (df_e["tipo_baremo"] == "individual") &
            (df_e["instrumento"] == "Individual") &
            (df_e["nivel_analisis"] == "factor")
        ]
        if not sub_ind.empty:
            n_tot_ind = sub_ind["cedula"].nunique()
            if n_tot_ind >= n_min:
                n_vuln = sub_ind[sub_ind["nivel_riesgo"].isin([1, 2])]["cedula"].nunique()
                pct_vuln = round(n_vuln / n_tot_ind * 100, 1)
                pct_ref_ind = bench_dominio.get("Individual", 4.2)
                diff_ind = round(pct_vuln - pct_ref_ind, 1)
                resultados.append({
                    "empresa": empresa,
                    "kpi_grupo": "Vulnerabilidad Psicológica",
                    "instrumento": "Individual",
                    "forma": "A+B",
                    "nivel_num": 99,
                    "nivel_label": "Bajo+Muy bajo",
                    "n_total": n_tot_ind,
                    "n_nivel": n_vuln,
                    "pct_nivel": pct_vuln,
                    "pct_alto_muy_alto": pct_vuln,
                    "pct_referente": pct_ref_ind,
                    "tipo_referente": "país",
                    "diferencia_pp": diff_ind,
                    "semaforo": _semaforo(pct_vuln),
                })

        # -- Grupo 3: Protocolos urgentes --
        # BUG FIX: fuente correcta es fact_gestion_07_protocolos_poblacion.
        # fact_gestion_04_niveles_ejes agrega por eje (3 ejes/persona), solo cuenta urgente
        # si los 3 ejes de una misma persona son urgentes → subestima masivamente.
        # fact_gestion_07 tiene una fila por cedula×protocolo con nivel_gestion asignado
        # individualmente → count(distinct cedula where nivel_gestion='Intervencion Urgente')
        # es el criterio correcto.
        try:
            g7 = pd.read_parquet(_proc(cfg, "fact_gestion_07_protocolos_poblacion.parquet"))
            sub_g7 = g7[g7["empresa"] == empresa] if "empresa" in g7.columns else pd.DataFrame()
            # Total evaluados en gestión = cédulas únicas en cualquier protocolo
            n_tot_g7 = sub_g7["cedula"].nunique() if not sub_g7.empty else 0
            if n_tot_g7 >= n_min:
                n_urg = sub_g7[
                    sub_g7["nivel_gestion"] == "Intervencion Urgente"
                ]["cedula"].nunique() if "nivel_gestion" in sub_g7.columns else 0
                pct_urg = round(n_urg / n_tot_g7 * 100, 1) if n_tot_g7 > 0 else 0.0
                resultados.append({
                    "empresa": empresa,
                    "kpi_grupo": "Protocolos Urgentes",
                    "instrumento": "Gestión",
                    "forma": "A+B",
                    "nivel_num": 98,
                    "nivel_label": "Intervencion urgente",
                    "n_total": n_tot_g7,
                    "n_nivel": n_urg,
                    "pct_nivel": pct_urg,
                    "pct_alto_muy_alto": pct_urg,
                    "pct_referente": None,
                    "tipo_referente": None,
                    "diferencia_pp": None,
                    "semaforo": _semaforo(pct_urg),
                })
        except FileNotFoundError:
            pass

        # -- Grupo 4: Vigilancia Epidemiológica --
        try:
            vig = pd.read_parquet(_proc(cfg, "fact_gestion_06_vigilancia_resumen.parquet"))
            sub_vig = vig[vig["empresa"] == empresa] if "empresa" in vig.columns else pd.DataFrame()
            if not sub_vig.empty:
                n_tot_vig = int(sub_vig["n_total"].iloc[0]) if "n_total" in sub_vig.columns else 0
                n_casos = int(sub_vig["n_casos"].sum()) if "n_casos" in sub_vig.columns else 0
                pct_vig = round(n_casos / n_tot_vig * 100, 1) if n_tot_vig > 0 else 0.0
                resultados.append({
                    "empresa": empresa,
                    "kpi_grupo": "Vigilancia Epidemiológica",
                    "instrumento": "VEP",
                    "forma": "A+B",
                    "nivel_num": 97,
                    "nivel_label": "Casos sospechosos",
                    "n_total": n_tot_vig,
                    "n_nivel": n_casos,
                    "pct_nivel": pct_vig,
                    "pct_alto_muy_alto": pct_vig,
                    "pct_referente": None,
                    "tipo_referente": None,
                    "diferencia_pp": None,
                    "semaforo": "#EF4444" if pct_vig > 20 else "#F59E0B" if pct_vig >= 10 else "#10B981",
                })
        except FileNotFoundError:
            pass

    df_out = pd.DataFrame(resultados)
    _save(df_out, cfg, "fact_v3_kpis_globales.parquet")
    return df_out


# ── PASO 2: Demografía (S2A) ───────────────────────────────────────────────

def calcular_demografia(cfg: dict) -> pd.DataFrame:
    log.info("=== PASO 2: Demografía ===")
    demo = pd.read_parquet(_proc(cfg, "dim_demografia.parquet"))
    trab = pd.read_parquet(_proc(cfg, "dim_trabajador.parquet"))
    n_min = cfg.get("confidencialidad_n_min", 5)

    # Columnas con ñ — detectar por patrón
    col_antig_emp = _col_antig_empresa(demo)
    col_antig_cargo = _col_antig_cargo(demo)

    df = demo.merge(
        trab[["cedula", "empresa", "area_departamento", "categoria_cargo", "forma_intra"]],
        on="cedula", how="left"
    )

    resultados = []

    for empresa in df["empresa"].dropna().unique():
        sub = df[df["empresa"] == empresa].copy()
        n_emp = sub["cedula"].nunique()

        def _agg(sub_df, col_cat, var_nombre, top=None):
            agg = sub_df.groupby(col_cat, dropna=False)["cedula"].nunique().reset_index()
            agg.columns = ["categoria", "n"]
            agg["pct"] = (agg["n"] / n_emp * 100).round(1)
            agg["variable"] = var_nombre
            agg["empresa"] = empresa
            agg["sexo"] = None
            agg["confidencial"] = agg["n"] < n_min
            if top:
                agg = agg.nlargest(top, "n")
            return agg

        # G1: Pirámide (edad × sexo)
        if "edad_cumplida" in sub.columns and "sexo" in sub.columns:
            sub["rango_edad"] = sub["edad_cumplida"].apply(lambda x: _asignar_rango(x, RANGOS_EDAD))
            pir = sub.groupby(["rango_edad", "sexo"], dropna=False)["cedula"].nunique().reset_index()
            pir.columns = ["categoria", "sexo", "n"]
            pir["pct"] = (pir["n"] / n_emp * 100).round(1)
            pir["variable"] = "piramide_poblacional"
            pir["empresa"] = empresa
            pir["confidencial"] = pir["n"] < n_min
            resultados.append(pir)

        # G2: Antigüedad empresa
        # FIX: la columna ya tiene valores categóricos de texto ("Menos de 1 año", "1 a 2 años"…)
        # NO aplicar rangos numéricos — usar directamente el valor de texto como categoría
        if col_antig_emp and col_antig_emp in sub.columns:
            resultados.append(_agg(sub, col_antig_emp, "antiguedad_empresa"))

        # G3: Antigüedad cargo
        if col_antig_cargo and col_antig_cargo in sub.columns:
            resultados.append(_agg(sub, col_antig_cargo, "antiguedad_cargo"))

        # G4: Estado civil
        if "estado_civil" in sub.columns:
            resultados.append(_agg(sub, "estado_civil", "estado_civil"))

        # G5: Dependientes económicos
        if "numero_dependientes_economicos" in sub.columns:
            sub["r_dep"] = sub["numero_dependientes_economicos"].apply(
                lambda x: _asignar_rango(x, RANGOS_DEPENDIENTES))
            resultados.append(_agg(sub, "r_dep", "dependientes_economicos"))

        # G6: Áreas (top 10)
        resultados.append(_agg(sub, "area_departamento", "area_departamento", top=10))

        # G7: Cargos (top 10)
        resultados.append(_agg(sub, "categoria_cargo", "categoria_cargo", top=10))

        # G8: Forma intralaboral
        resultados.append(_agg(sub, "forma_intra", "forma_intralaboral"))

    if not resultados:
        return pd.DataFrame()

    df_out = pd.concat(resultados, ignore_index=True)
    # Asegurar columna sexo
    if "sexo" not in df_out.columns:
        df_out["sexo"] = None
    _save(df_out, cfg, "fact_v3_demografia.parquet")
    return df_out


# ── PASO 3: Costos Ausentismo R10 (S2B) ────────────────────────────────────

def calcular_costos(cfg: dict) -> pd.DataFrame:
    log.info("=== PASO 3: Costos Ausentismo R10 ===")
    trab = pd.read_parquet(_proc(cfg, "dim_trabajador.parquet"))
    ausen = pd.read_parquet(_proc(cfg, "dim_ausentismo.parquet"))

    SMLV = float(cfg.get("smlv_mensual", 2_800_000))
    PRES = float(cfg.get("presentismo_factor", 1.40))
    EMP_PCT = float(cfg.get("costo_empleador_pct", 0.60))
    PSICO_PCT = float(cfg.get("psicosocial_pct", 0.30))
    REF_PAIS = 3.0
    DIAS_LABORALES = 240

    # Unir ausentismo con empresa vía cedula
    trab_cedula_emp = trab[["cedula", "empresa"]].drop_duplicates()
    ausen_emp = ausen.merge(trab_cedula_emp, on="cedula", how="left")

    resultados = []

    for empresa in trab["empresa"].dropna().unique():
        sub_t = trab[trab["empresa"] == empresa]
        n_planta = sub_t["cedula"].nunique()  # evaluados como proxy de planta

        sub_a = ausen_emp[ausen_emp["empresa"] == empresa] if "empresa" in ausen_emp.columns else pd.DataFrame()
        total_dias = int(sub_a["dias_ausencia"].sum()) if not sub_a.empty and "dias_ausencia" in sub_a.columns else 0

        dias_cap = n_planta * DIAS_LABORALES
        pct_ausen = round(total_dias / dias_cap * 100, 2) if dias_cap > 0 else 0.0
        diff_pais = round(pct_ausen - REF_PAIS, 2)

        p3 = SMLV * 12
        p4 = n_planta * p3
        p5 = (pct_ausen / 100) * p4
        p6 = p5 * EMP_PCT
        p7 = p6 * PRES
        p8 = p7 * PSICO_PCT

        sem = "#EF4444" if pct_ausen > 5 else "#F59E0B" if pct_ausen >= 3 else "#10B981"

        pasos = [
            (1, "N° trabajadores (planta evaluada)", n_planta, "trabajadores",
             "Universo total de cédulas únicas en dim_trabajador"),
            (2, "% ausentismo actual (últimos 12 meses)", pct_ausen, "%",
             f"Referente Colombia: {REF_PAIS}% | Diferencia: {diff_pais:+.2f} pp"),
            (3, "Costo salario anual × FTE", p3, "COP",
             f"SMLV ${SMLV:,.0f} × 12 meses"),
            (4, "Capacidad productiva anual", p4, "COP",
             "N° planta × Costo FTE anual"),
            (5, "Pérdida económica anual por ausentismo", p5, "COP",
             "% ausentismo × Capacidad productiva (empleador + SGSS)"),
            (6, "Pérdida económica del empleador (60%)", p6, "COP",
             f"{int(EMP_PCT*100)}% al empleador | {int((1-EMP_PCT)*100)}% al SGSS"),
            (7, "Pérdida productividad (ausentismo + presentismo)", p7, "COP",
             f"Paso 6 × {PRES} (+{int((PRES-1)*100)}% por presentismo atribuible)"),
            (8, "Costo estimado riesgo psicosocial (30%)", p8, "COP",
             f"Paso 7 × {int(PSICO_PCT*100)}% según literatura internacional"),
        ]

        for paso_num, nombre, valor, unidad, nota in pasos:
            resultados.append({
                "empresa": empresa,
                "paso": paso_num,
                "nombre_paso": nombre,
                "valor": round(float(valor), 2),
                "unidad": unidad,
                "nota": nota,
                "n_planta": n_planta,
                "total_dias_ausencia": total_dias,
                "dias_cap_instalada": dias_cap,
                "pct_ausentismo": pct_ausen,
                "diferencia_pp_vs_pais": diff_pais,
                "semaforo_ausentismo": sem,
            })

    df_out = pd.DataFrame(resultados)
    _save(df_out, cfg, "fact_v3_costos.parquet")
    return df_out


# ── PASO 4: Benchmarking Ejecutivo (S3) ────────────────────────────────────

def calcular_benchmarking(cfg: dict) -> pd.DataFrame:
    log.info("=== PASO 4: Benchmarking ===")
    baremo = pd.read_parquet(_proc(cfg, "fact_scores_baremo.parquet"))
    benchmark = pd.read_parquet(_proc(cfg, "fact_benchmark.parquet"))
    bench_sector = cfg.get("benchmark_sector", {})
    n_min = cfg.get("confidencialidad_n_min", 5)

    try:
        prot5 = pd.read_parquet(_proc(cfg, "fact_gestion_05_prioridades_protocolos.parquet"))
    except FileNotFoundError:
        prot5 = pd.DataFrame()

    resultados = []

    for empresa in baremo["empresa"].dropna().unique():
        df_e = baremo[baremo["empresa"] == empresa]
        bm_e = benchmark[benchmark["empresa"] == empresa] if "empresa" in benchmark.columns else pd.DataFrame()
        sector = str(df_e["sector_rag"].iloc[0]) if "sector_rag" in df_e.columns else "No clasificado"

        # 3A: Riesgo IntraA y IntraB vs sector/país
        for instr_key in ("IntraA", "IntraB"):
            sub = df_e[
                (df_e["tipo_baremo"] == "riesgo") &
                (df_e["instrumento"] == instr_key) &
                (df_e["nivel_analisis"] == "factor")
            ]
            if sub.empty or sub["cedula"].nunique() < n_min:
                continue

            n_total = sub["cedula"].nunique()
            n_ama = sub[sub["nivel_riesgo"].isin([4, 5])]["cedula"].nunique()
            pct_emp = round(n_ama / n_total * 100, 1)

            # Buscar en benchmark parquet
            pct_ref, tipo_ref = None, None
            if not bm_e.empty:
                row_bm = bm_e[
                    (bm_e["nombre_nivel"] == instr_key) &
                    (bm_e["nivel_analisis"] == "factor")
                ]
                if not row_bm.empty:
                    pct_ref = float(row_bm["pct_referencia"].iloc[0])
                    tipo_ref = str(row_bm["tipo_referencia"].iloc[0])

            if pct_ref is None:
                pct_ref = bench_sector.get(sector, bench_sector.get("_promedio_general", 39.69))
                tipo_ref = "país"

            diff = round(pct_emp - pct_ref, 1)
            sem = "#EF4444" if diff > 10 else "#F59E0B" if diff > 0 else "#10B981"

            resultados.append({
                "empresa": empresa,
                "sector": sector,
                "tipo": "riesgo_intralaboral",
                "instrumento": instr_key,
                "pct_empresa_ama": pct_emp,
                "pct_referente": pct_ref,
                "tipo_referente": tipo_ref,
                "diferencia_pp": diff,
                "semaforo": sem,
                "descripcion": f"% riesgo Alto+Muy Alto {instr_key}",
            })

        # 3B: Protocolos urgentes empresa vs sector
        if not prot5.empty and "empresa" in prot5.columns:
            sub_p = prot5[prot5["empresa"] == empresa]
            n_urgentes = len(sub_p[
                sub_p["protocolo_nombre"].notna()
            ]) if "rango_empresa" not in sub_p.columns else len(sub_p[sub_p["rango_empresa"] <= 3])
            n_prioritarios_sector = int(sub_p["es_prioritario_sector"].sum()) \
                if "es_prioritario_sector" in sub_p.columns else 0

            resultados.append({
                "empresa": empresa,
                "sector": sector,
                "tipo": "protocolos_urgentes",
                "instrumento": "Gestión",
                "pct_empresa_ama": float(n_urgentes),
                "pct_referente": float(n_prioritarios_sector),
                "tipo_referente": "sector",
                "diferencia_pp": float(n_urgentes - n_prioritarios_sector),
                "semaforo": "#EF4444" if n_urgentes > 5 else "#F59E0B" if n_urgentes > 2 else "#10B981",
                "descripcion": f"N protocolos top empresa | {n_prioritarios_sector} prioritarios del sector",
            })

        # 3C: Top 3 dimensiones vs Colombia
        if not bm_e.empty and "nivel_analisis" in bm_e.columns:
            dim_b = bm_e[bm_e["nivel_analisis"] == "dimension"].copy()
            if not dim_b.empty and "diferencia_pp" in dim_b.columns:
                top3 = dim_b.nlargest(3, "diferencia_pp")
                for _, row in top3.iterrows():
                    diff_d = float(row["diferencia_pp"]) if not pd.isna(row["diferencia_pp"]) else 0.0
                    sem_d = "#EF4444" if diff_d > 15 else "#F59E0B" if diff_d > 5 else "#10B981"
                    resultados.append({
                        "empresa": empresa,
                        "sector": sector,
                        "tipo": "dimension_critica",
                        "instrumento": str(row.get("nombre_nivel", "N/A")),
                        "pct_empresa_ama": round(float(row.get("pct_empresa", 0)), 1),
                        "pct_referente": round(float(row.get("pct_referencia", 0)), 1),
                        "tipo_referente": str(row.get("tipo_referencia", "país")),
                        "diferencia_pp": round(diff_d, 1),
                        "semaforo": sem_d,
                        "descripcion": "Dimensión intralaboral vs Colombia",
                    })

    df_out = pd.DataFrame(resultados)
    _save(df_out, cfg, "fact_v3_benchmarking.parquet")
    return df_out


# ── PASO 5: Ranking Áreas (S4) ─────────────────────────────────────────────

def calcular_ranking_areas(cfg: dict) -> pd.DataFrame:
    log.info("=== PASO 5: Ranking Áreas ===")
    consolidado = pd.read_parquet(_proc(cfg, "fact_consolidado.parquet"))
    n_min = cfg.get("confidencialidad_n_min", 5)

    resultados = []

    for empresa in consolidado["empresa"].dropna().unique():
        df_e = consolidado[consolidado["empresa"] == empresa]

        sub_intra = df_e[
            (df_e["tipo_baremo"] == "riesgo") &
            (df_e["instrumento"].isin(["IntraA", "IntraB"])) &
            (df_e["nivel_analisis"] == "factor")
        ]
        if sub_intra.empty:
            continue

        for area in sub_intra["area_departamento"].dropna().unique():
            sub_a = sub_intra[sub_intra["area_departamento"] == area]
            n_area = sub_a["cedula"].nunique()

            if n_area < n_min:
                resultados.append({
                    "empresa": empresa,
                    "area_departamento": area,
                    "n_evaluados": n_area,
                    "confidencial": True,
                    "pct_ama": None,
                    "nivel_predominante": None,
                    "dimension_critica": None,
                    "semaforo": None,
                    "ranking": None,
                })
                continue

            n_ama = sub_a[sub_a["nivel_riesgo"].isin([4, 5])]["cedula"].nunique()
            pct = round(n_ama / n_area * 100, 1)
            conteo_nivel = sub_a.groupby("nivel_riesgo")["cedula"].nunique()
            nivel_pred = int(conteo_nivel.idxmax()) if not conteo_nivel.empty else 0
            nivel_label = NIVELES_RIESGO_LABEL.get(nivel_pred, "N/A")

            # Dimensión crítica: buscar en dimensiones (nivel_analisis='dimension')
            # BUG FIX: columna es 'nombre_nivel', no 'dimension'
            dim_critica = None
            sub_dim = df_e[
                (df_e["tipo_baremo"] == "riesgo") &
                (df_e["instrumento"].isin(["IntraA", "IntraB"])) &
                (df_e["nivel_analisis"] == "dimension") &
                (df_e["area_departamento"] == area)
            ]
            if not sub_dim.empty and "nombre_nivel" in sub_dim.columns:
                dim_agg = sub_dim.groupby("nombre_nivel").apply(
                    lambda x: x[x["nivel_riesgo"].isin([4, 5])]["cedula"].nunique() /
                              max(x["cedula"].nunique(), 1) * 100
                )
                if not dim_agg.empty:
                    dim_critica = dim_agg.idxmax()

            resultados.append({
                "empresa": empresa,
                "area_departamento": area,
                "n_evaluados": n_area,
                "confidencial": False,
                "pct_ama": pct,
                "nivel_predominante": nivel_label,
                "dimension_critica": dim_critica,
                "semaforo": _semaforo(pct),
                "ranking": None,
            })

    df_out = pd.DataFrame(resultados)

    # Asignar ranking top 5 por empresa
    dfs = []
    for empresa in df_out["empresa"].dropna().unique():
        sub_e = df_out[df_out["empresa"] == empresa].copy()
        no_conf = sub_e[~sub_e["confidencial"]].sort_values("pct_ama", ascending=False).reset_index(drop=True)
        no_conf["ranking"] = no_conf.index + 1
        top5 = no_conf[no_conf["ranking"] <= 5]
        dfs.append(top5)
        conf = sub_e[sub_e["confidencial"]]
        if not conf.empty:
            dfs.append(conf)

    df_out = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
    _save(df_out, cfg, "fact_v3_ranking_areas.parquet")
    return df_out


# ── Orquestador ────────────────────────────────────────────────────────────

def main():
    log.info("╔══════════════════════════════════════════════════════╗")
    log.info("║  MentalPRO · ETL 09 · Visualizador 3 Gerencial+ASIS ║")
    log.info("╚══════════════════════════════════════════════════════╝")
    cfg = _load_config()

    kpis    = calcular_kpis_globales(cfg)
    demo    = calcular_demografia(cfg)
    costos  = calcular_costos(cfg)
    bench   = calcular_benchmarking(cfg)
    ranking = calcular_ranking_areas(cfg)

    log.info("=== RESUMEN ===")
    for nombre, df in [("KPIs", kpis), ("Demografía", demo), ("Costos", costos),
                       ("Benchmarking", bench), ("Ranking", ranking)]:
        log.info(f"  {nombre:15s}: {len(df):,} filas")
    log.info("✅ ETL 09 completado.")


if __name__ == "__main__":
    main()
