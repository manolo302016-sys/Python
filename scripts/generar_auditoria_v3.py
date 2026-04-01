"""
generar_auditoria_v3.py — MentalPRO · Libro de Auditoría Visualizador 3
========================================================================
Genera: output/auditoria_v3_consultel_indecol.xlsx

Hojas incluidas:
    00_Portada              — Resumen ejecutivo del libro
    01_Encabezado           — S0: Identificadores por empresa
    02_KPIs_Globales        — S1: Distribución 5 niveles + semáforo
    03_Vulnerabilidad       — S1: Factor Individual vulnerabilidad
    04_Protocolos_Urgentes  — S1: KPI protocolos urgentes + vigilancia
    05_Piramide_Poblacional — S2A: Pirámide edad × sexo
    06_Antiguedades         — S2A: Antigüedad empresa y cargo
    07_Perfil_Ocupacional   — S2A: Áreas, cargos, forma intralaboral
    08_Costos_Ausentismo    — S2B: Fórmula R10 paso a paso
    09_ROI_SaludMental      — S2B: Estimación ROI
    10_Benchmarking_Intra   — S3: Riesgo vs sector/país
    11_Protocolos_Sector    — S3: Protocolos empresa vs sector
    12_Dimensiones_Colombia — S3: Top 3 dimensiones vs ENCST
    13_Ranking_Areas        — S4: Top 5 áreas críticas
    14_Fichas_Protocolos    — S5: Fichas técnicas líneas gestión
    15_Calculos_Verificacion— Trazabilidad fórmulas R10 + umbral semáforo

Empresas auditadas: consultel, indecol
"""

import sys
import logging
from pathlib import Path

import pandas as pd
import numpy as np
import yaml

BASE_DIR = Path(__file__).resolve().parent.parent
CONFIG_PATH = BASE_DIR / "config" / "config.yaml"
OUTPUT_PATH = BASE_DIR / "output" / "auditoria_v3_consultel_indecol.xlsx"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)

# ── Paleta R11 ──────────────────────────────────────────────────────────────
COLOR_ROJO       = "EF4444"
COLOR_NARANJA    = "F97316"
COLOR_AMARILLO   = "F59E0B"
COLOR_VERDE_CLR  = "6EE7B7"
COLOR_VERDE      = "10B981"
COLOR_NAVY       = "0A1628"
COLOR_GOLD       = "C9952A"
COLOR_CYAN       = "00C2CB"
COLOR_GRIS       = "F3F4F6"
COLOR_BLANCO     = "FFFFFF"


def _load_config() -> dict:
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def _proc(cfg: dict, filename: str) -> Path:
    return BASE_DIR / cfg["paths"]["processed"] / filename


def _read_safe(cfg: dict, filename: str) -> pd.DataFrame:
    path = _proc(cfg, filename)
    if not path.exists():
        log.warning(f"Parquet no encontrado: {filename}")
        return pd.DataFrame()
    return pd.read_parquet(path)


def _semaforo_hex(pct: float) -> str:
    if pct > 35:
        return COLOR_ROJO
    if pct >= 15:
        return COLOR_AMARILLO
    return COLOR_VERDE


def _semaforo_diff(diff: float) -> str:
    if diff > 15:
        return COLOR_ROJO
    if diff > 5:
        return COLOR_AMARILLO
    if diff > 0:
        return COLOR_VERDE_CLR
    return COLOR_VERDE


# ── Helpers de formato xlsxwriter ──────────────────────────────────────────

def _fmt(wb, bold=False, bg=None, font_color="000000", border=1,
         align="left", wrap=False, num_format=None):
    props = {
        "font_name": "Inter",
        "font_size": 9,
        "border": border,
        "align": align,
        "valign": "vcenter",
        "text_wrap": wrap,
        "bold": bold,
    }
    if bg:
        props["bg_color"] = bg
        props["fg_color"] = bg
    if font_color:
        props["font_color"] = font_color
    if num_format:
        props["num_format"] = num_format
    return wb.add_format(props)


def _header_row(ws, wb, headers: list, row: int = 0, bg: str = COLOR_NAVY,
                font_color: str = COLOR_BLANCO, widths: list = None):
    fmt = _fmt(wb, bold=True, bg=bg, font_color=font_color, align="center")
    for col, h in enumerate(headers):
        ws.write(row, col, h, fmt)
        if widths and col < len(widths):
            ws.set_column(col, col, widths[col])


def _write_df(ws, wb, df: pd.DataFrame, start_row: int = 1,
              semaforo_col: str = None):
    """Escribe DataFrame fila a fila. Si semaforo_col indica columna de color hex, la aplica."""
    fmt_def = _fmt(wb)

    for r_idx, row in df.iterrows():
        bg = None
        if semaforo_col and semaforo_col in row.index:
            raw_color = str(row[semaforo_col]).lstrip("#")
            if len(raw_color) == 6:
                bg = raw_color

        for c_idx, (col_name, val) in enumerate(row.items()):
            if col_name in (semaforo_col,):
                continue
            cell_fmt = _fmt(wb, bg=bg) if bg else fmt_def
            if isinstance(val, float) and not np.isnan(val):
                ws.write_number(start_row + r_idx, c_idx, val, cell_fmt)
            elif val is None or (isinstance(val, float) and np.isnan(val)):
                ws.write_blank(start_row + r_idx, c_idx, None, cell_fmt)
            else:
                ws.write(start_row + r_idx, c_idx, str(val), cell_fmt)


# ── Hojas ───────────────────────────────────────────────────────────────────

def _hoja_portada(wb, cfg: dict, empresas: list):
    ws = wb.add_worksheet("00_Portada")
    ws.set_column(0, 0, 40)
    ws.set_column(1, 1, 60)

    fmt_title = _fmt(wb, bold=True, bg=COLOR_NAVY, font_color=COLOR_GOLD,
                     align="center", border=0)
    fmt_sub = _fmt(wb, bold=True, bg=COLOR_NAVY, font_color=COLOR_BLANCO,
                   align="center", border=0)
    fmt_body = _fmt(wb, wrap=True)
    fmt_label = _fmt(wb, bold=True, bg=COLOR_GRIS)

    ws.merge_range("A1:B1", "MentalPRO — Libro de Auditoría Visualizador 3", fmt_title)
    ws.merge_range("A2:B2", "Gerencial + ASIS Consolidado · Resolución 2764/2022", fmt_sub)
    ws.merge_range("A3:B3", "Ministerio de Trabajo Colombia", fmt_sub)

    ws.write(4, 0, "Empresas auditadas", fmt_label)
    ws.write(4, 1, ", ".join(empresas), fmt_body)
    ws.write(5, 0, "Fecha generación", fmt_label)
    ws.write(5, 1, "2026-04-01", fmt_body)
    ws.write(6, 0, "Versión ETL", fmt_label)
    ws.write(6, 1, "MentalPRO v3.0.0 · script 09_asis_gerencial.py", fmt_body)
    ws.write(7, 0, "SMLV mensual (R10)", fmt_label)
    smlv = cfg.get("parametros_economicos", {}).get("SMLV_mensual", 2_800_000)
    ws.write(7, 1, f"${smlv:,.0f} COP", fmt_body)
    ws.write(8, 0, "Referente ausentismo país", fmt_label)
    ws.write(8, 1, "3.0% (promedio empresas Colombia)", fmt_body)
    ws.write(9, 0, "Factor presentismo (R10)", fmt_label)
    ws.write(9, 1, f"{cfg.get('parametros_economicos', {}).get('presentismo_factor', 1.40):.0%}", fmt_body)
    ws.write(10, 0, "Fuente referentes nacionales", fmt_label)
    ws.write(10, 1, "II y III Encuesta Nacional Condiciones Salud y Trabajo (ENCST 2013-2021)", fmt_body)

    ws.merge_range("A12:B12", "Índice de hojas", fmt_label)
    hojas = [
        ("01_Encabezado", "S0: Identificadores empresa, cobertura, fecha"),
        ("02_KPIs_Globales", "S1: % 5 niveles Intra A/B, Extra A/B, Estrés A/B con semáforo"),
        ("03_Vulnerabilidad", "S1: Factor Individual — vulnerabilidad psicológica"),
        ("04_Protocolos_Urgentes", "S1: N trabajadores en protocolos urgentes + vigilancia"),
        ("05_Piramide_Poblacional", "S2A: Edad × sexo (pirámide)"),
        ("06_Antiguedades", "S2A: Antigüedad empresa y cargo"),
        ("07_Perfil_Ocupacional", "S2A: Áreas, cargos, forma intralaboral"),
        ("08_Costos_Ausentismo", "S2B: Fórmula R10 — 6 pasos + presentismo"),
        ("09_ROI_SaludMental", "S2B: Estimación ROI inversión SST"),
        ("10_Benchmarking_Intra", "S3: Riesgo A+MA empresa vs sector/país con diff pp"),
        ("11_Protocolos_Sector", "S3: Protocolos urgentes empresa vs prioritarios sector"),
        ("12_Dimensiones_Colombia", "S3: Top 3 dimensiones intralaboral vs ENCST"),
        ("13_Ranking_Areas", "S4: Top 5 áreas con mayor riesgo A+MA"),
        ("14_Fichas_Protocolos", "S5: Fichas técnicas 3 líneas gestión principales"),
        ("15_Calculos_Verificacion", "Trazabilidad: fórmulas R10, umbrales semáforo, fuentes"),
    ]
    fmt_idx_h = _fmt(wb, bold=True, bg=COLOR_GRIS)
    for i, (nombre, desc) in enumerate(hojas):
        ws.write(13 + i, 0, nombre, fmt_idx_h)
        ws.write(13 + i, 1, desc, fmt_body)


def _hoja_kpis_globales(wb, cfg: dict, empresas: list):
    """02_KPIs_Globales: distribución 5 niveles por instrumento."""
    ws = wb.add_worksheet("02_KPIs_Globales")
    headers = [
        "Empresa", "Instrumento", "Forma", "Nivel",
        "N Total", "N Nivel", "% Nivel",
        "% Alto+Muy Alto", "% Referente Sector", "% Referente País",
        "Diferencia pp Sector", "Diferencia pp País", "Semáforo"
    ]
    widths = [15, 16, 8, 12, 8, 8, 9, 14, 16, 14, 16, 14, 12]
    _header_row(ws, wb, headers, widths=widths)

    df = _read_safe(cfg, "fact_v3_kpis_globales.parquet")
    if df.empty:
        ws.write(1, 0, "Sin datos — ejecutar 09_asis_gerencial.py", _fmt(wb))
        return

    df_filt = df[df["empresa"].isin(empresas)].copy() if "empresa" in df.columns else df
    df_filt = df_filt.reset_index(drop=True)

    for r, row in df_filt.iterrows():
        # BUG FIX: columna en parquet es 'pct_alto_muy_alto', no 'pct_alto_muy_alto_intra'
        pct_ama = float(row.get("pct_alto_muy_alto", 0) or 0)
        bg = _semaforo_hex(pct_ama)
        fmt_row = _fmt(wb, bg=bg)
        fmt_num = _fmt(wb, bg=bg, num_format="0.0")

        ws.write(r + 1, 0, str(row.get("empresa", "")), fmt_row)
        ws.write(r + 1, 1, str(row.get("kpi_grupo", row.get("instrumento", "")), ), fmt_row)
        ws.write(r + 1, 2, str(row.get("forma", "")), fmt_row)
        # BUG FIX: columna es 'nivel_label', no 'nivel'
        ws.write(r + 1, 3, str(row.get("nivel_label", "")), fmt_row)
        ws.write_number(r + 1, 4, int(row.get("n_total", 0) or 0), fmt_row)
        ws.write_number(r + 1, 5, int(row.get("n_nivel", 0) or 0), fmt_row)
        # BUG FIX: col 6 = % de ESE nivel (distribución), col 7 = % acumulado A+MA
        ws.write_number(r + 1, 6, float(row.get("pct_nivel", 0) or 0), fmt_num)
        ws.write_number(r + 1, 7, pct_ama, fmt_num)

        # BUG FIX: columnas reales son 'pct_referente', 'tipo_referente', 'diferencia_pp'
        # (el parquet tiene un solo referente, no separado sector/país)
        pct_ref = row.get("pct_referente")
        tipo_ref = str(row.get("tipo_referente") or "")
        diff = row.get("diferencia_pp")

        if pct_ref is not None and not (isinstance(pct_ref, float) and np.isnan(pct_ref)):
            ws.write_number(r + 1, 8, float(pct_ref), fmt_num)
        else:
            ws.write_blank(r + 1, 8, None, fmt_row)
        ws.write(r + 1, 9, tipo_ref, fmt_row)
        if diff is not None and not (isinstance(diff, float) and np.isnan(diff)):
            ws.write_number(r + 1, 10, float(diff), fmt_num)
        else:
            ws.write_blank(r + 1, 10, None, fmt_row)
        ws.write_blank(r + 1, 11, None, fmt_row)  # diferencia_pp_pais no aplica

        # BUG FIX: columna semáforo es 'semaforo', no 'semaforo_color'
        ws.write(r + 1, 12, str(row.get("semaforo", "")), fmt_row)


def _hoja_costos(wb, cfg: dict, empresas: list):
    """08_Costos_Ausentismo: fórmula R10 paso a paso."""
    ws = wb.add_worksheet("08_Costos_Ausentismo")
    headers = ["Empresa", "Paso", "Nombre del Paso", "Valor", "Unidad", "Nota / Observación"]
    widths = [15, 6, 45, 18, 12, 60]
    _header_row(ws, wb, headers, widths=widths)

    df = _read_safe(cfg, "fact_v3_costos.parquet")
    if df.empty:
        ws.write(1, 0, "Sin datos — ejecutar 09_asis_gerencial.py", _fmt(wb))
        return

    df_filt = df[df["empresa"].isin(empresas)].copy() if "empresa" in df.columns else df
    df_filt = df_filt.reset_index(drop=True)

    fmt_def = _fmt(wb)
    fmt_paso7 = _fmt(wb, bold=True, bg=COLOR_GOLD, font_color=COLOR_NAVY)
    fmt_paso8 = _fmt(wb, bold=True, bg=COLOR_CYAN, font_color=COLOR_NAVY)

    for r, row in df_filt.iterrows():
        paso = int(row.get("paso", 0))
        fmt_use = fmt_paso7 if paso == 7 else fmt_paso8 if paso == 8 else fmt_def
        fmt_val = _fmt(wb, num_format='$ #,##0',
                       bold=(paso in (7, 8)),
                       bg=COLOR_GOLD if paso == 7 else COLOR_CYAN if paso == 8 else None)

        ws.write(r + 1, 0, str(row.get("empresa", "")), fmt_use)
        ws.write_number(r + 1, 1, paso, fmt_use)
        ws.write(r + 1, 2, str(row.get("nombre_paso", "")), fmt_use)
        val = row.get("valor", 0) or 0
        if row.get("unidad") == "%" :
            ws.write_number(r + 1, 3, float(val), _fmt(wb, num_format='0.00"%"',
                            bold=(paso in (7, 8))))
        else:
            ws.write_number(r + 1, 3, float(val), fmt_val)
        ws.write(r + 1, 4, str(row.get("unidad", "")), fmt_use)
        ws.write(r + 1, 5, str(row.get("nota", "")), fmt_use)


def _hoja_roi(wb, cfg: dict, empresas: list):
    """09_ROI_SaludMental: estimación ROI por empresa."""
    ws = wb.add_worksheet("09_ROI_SaludMental")
    headers = [
        "Empresa", "N Planta", "% Ausentismo",
        "Diff vs País (pp)", "Costo Psicosocial (30%)",
        "Inversión SST Estimada (2%)", "ROI Estimado (%)", "Semáforo"
    ]
    widths = [15, 10, 14, 16, 24, 26, 18, 12]
    _header_row(ws, wb, headers, widths=widths)

    df = _read_safe(cfg, "fact_v3_costos.parquet")
    if df.empty:
        ws.write(1, 0, "Sin datos — ejecutar 09_asis_gerencial.py", _fmt(wb))
        return

    df_filt = df[df["empresa"].isin(empresas)].copy() if "empresa" in df.columns else df
    # BUG FIX: config.yaml usa 'smlv_mensual' en raíz, no en sub-dict 'parametros_economicos'
    smlv = cfg.get("smlv_mensual", cfg.get("parametros_economicos", {}).get("SMLV_mensual", 2_800_000))

    r_out = 1
    for empresa in df_filt["empresa"].unique():
        sub = df_filt[df_filt["empresa"] == empresa]

        p1 = sub[sub["paso"] == 1]["valor"].iloc[0] if (sub["paso"] == 1).any() else 0
        p2_row = sub[sub["paso"] == 2]
        p2 = float(p2_row["valor"].iloc[0]) if not p2_row.empty else 0
        diff_pais = float(p2_row["diferencia_pp_vs_pais"].iloc[0]) if not p2_row.empty \
            and "diferencia_pp_vs_pais" in p2_row.columns else 0
        p8 = float(sub[sub["paso"] == 8]["valor"].iloc[0]) if (sub["paso"] == 8).any() else 0

        n_planta = int(p1)
        inv_sst = n_planta * smlv * 12 * 0.02
        roi = round((p8 - inv_sst) / inv_sst * 100, 1) if inv_sst > 0 else 0

        sem_hex = _semaforo_hex(p2)
        fmt_r = _fmt(wb, bg=sem_hex)
        fmt_cop = _fmt(wb, num_format='$ #,##0', bg=sem_hex)
        fmt_pct = _fmt(wb, num_format='0.00"%"', bg=sem_hex)

        ws.write(r_out, 0, empresa, fmt_r)
        ws.write_number(r_out, 1, n_planta, fmt_r)
        ws.write_number(r_out, 2, p2, fmt_pct)
        ws.write_number(r_out, 3, diff_pais, fmt_pct)
        ws.write_number(r_out, 4, p8, fmt_cop)
        ws.write_number(r_out, 5, inv_sst, fmt_cop)
        ws.write_number(r_out, 6, roi, _fmt(wb, num_format='0.0"%"', bg=sem_hex,
                                            bold=True))
        ws.write(r_out, 7, f"#{sem_hex}", fmt_r)
        r_out += 1


def _hoja_benchmarking(wb, cfg: dict, empresas: list):
    """10_Benchmarking_Intra: riesgo empresa vs sector/país."""
    ws = wb.add_worksheet("10_Benchmarking_Intra")
    headers = [
        "Empresa", "Sector", "Instrumento",
        "% Empresa A+MA", "% Referente", "Diferencia pp",
        "Semáforo", "Fuente Referente"
    ]
    widths = [15, 20, 14, 16, 14, 14, 12, 28]
    _header_row(ws, wb, headers, widths=widths)

    df = _read_safe(cfg, "fact_v3_benchmarking.parquet")
    if df.empty:
        ws.write(1, 0, "Sin datos — ejecutar 09_asis_gerencial.py", _fmt(wb))
        return

    df_filt = df[
        (df["empresa"].isin(empresas)) &
        (df["tipo"] == "riesgo_intralaboral")
    ].reset_index(drop=True) if "empresa" in df.columns else df

    for r, row in df_filt.iterrows():
        diff = float(row.get("diferencia_pp", 0) or 0)
        bg = _semaforo_diff(diff)
        fmt_r = _fmt(wb, bg=bg)
        fmt_n = _fmt(wb, num_format="0.0", bg=bg)

        ws.write(r + 1, 0, str(row.get("empresa", "")), fmt_r)
        ws.write(r + 1, 1, str(row.get("sector", "")), fmt_r)
        ws.write(r + 1, 2, str(row.get("instrumento", "")), fmt_r)
        # BUG FIX: columna es 'pct_empresa_ama', no 'pct_empresa_a_ma'
        ws.write_number(r + 1, 3, float(row.get("pct_empresa_ama", 0) or 0), fmt_n)
        ws.write_number(r + 1, 4, float(row.get("pct_referente", 0) or 0), fmt_n)
        ws.write_number(r + 1, 5, diff, fmt_n)
        # BUG FIX: columna es 'semaforo', no 'semaforo_color'
        sem_val = str(row.get("semaforo", "") or "")
        ws.write(r + 1, 6, sem_val, fmt_r)
        ws.write(r + 1, 7, str(row.get("tipo_referente", "") or ""), fmt_r)


def _hoja_ranking_areas(wb, cfg: dict, empresas: list):
    """13_Ranking_Areas: top 5 áreas críticas."""
    ws = wb.add_worksheet("13_Ranking_Areas")
    headers = [
        "Empresa", "Ranking", "Área / Departamento",
        "N Evaluados", "% A+MA Intralaboral",
        "Nivel Predominante", "Dimensión Crítica",
        "Semáforo", "Confidencial"
    ]
    widths = [15, 8, 30, 12, 18, 18, 30, 12, 12]
    _header_row(ws, wb, headers, widths=widths)

    df = _read_safe(cfg, "fact_v3_ranking_areas.parquet")
    if df.empty:
        ws.write(1, 0, "Sin datos — ejecutar 09_asis_gerencial.py", _fmt(wb))
        return

    df_filt = df[df["empresa"].isin(empresas)].reset_index(drop=True) \
        if "empresa" in df.columns else df

    for r, row in df_filt.iterrows():
        es_conf = bool(row.get("confidencial", False))
        # BUG FIX: columna es 'pct_ama', no 'pct_alto_muy_alto_intra'
        pct_ama = float(row.get("pct_ama", 0) or 0) if not es_conf else 0.0
        bg = _semaforo_hex(pct_ama) if not es_conf else COLOR_GRIS
        fmt_r = _fmt(wb, bg=bg)
        fmt_n = _fmt(wb, num_format="0.0", bg=bg)

        ws.write(r + 1, 0, str(row.get("empresa", "")), fmt_r)
        ranking = row.get("ranking")
        if ranking is not None and not (isinstance(ranking, float) and np.isnan(ranking)):
            ws.write_number(r + 1, 1, int(ranking), fmt_r)
        else:
            ws.write_blank(r + 1, 1, None, fmt_r)
        ws.write(r + 1, 2, str(row.get("area_departamento", "")), fmt_r)
        ws.write_number(r + 1, 3, int(row.get("n_evaluados", 0) or 0), fmt_r)
        if es_conf:
            ws.write(r + 1, 4, "Confidencial (R8)", fmt_r)
        else:
            ws.write_number(r + 1, 4, pct_ama, fmt_n)
        ws.write(r + 1, 5, str(row.get("nivel_predominante", "") or ""), fmt_r)
        ws.write(r + 1, 6, str(row.get("dimension_critica", "") or ""), fmt_r)
        # BUG FIX: columna es 'semaforo', no 'semaforo_color'
        ws.write(r + 1, 7, str(row.get("semaforo", "") or ""), fmt_r)
        ws.write(r + 1, 8, "Si" if es_conf else "No", fmt_r)


def _hoja_fichas_protocolos(wb, cfg: dict, empresas: list):
    """14_Fichas_Protocolos: protocolo × empresa con N trabajadores en intervención urgente.

    BUG FIX: la versión anterior mostraba sólo el catálogo genérico (dim_protocolos_lineas)
    sin datos por empresa. La fuente correcta es fact_gestion_07_protocolos_poblacion que
    tiene una fila por cedula×protocolo con nivel_gestion asignado.
    """
    ws = wb.add_worksheet("14_Fichas_Protocolos")
    headers = [
        "Empresa", "Protocolo ID", "Nombre Protocolo",
        "Eje Gestión", "Línea Gestión",
        "N Trabajadores Urgente", "% del Total Evaluado",
        "Objetivo", "Resultado Esperado", "Es Prioritario Sector"
    ]
    widths = [15, 10, 35, 28, 28, 18, 18, 50, 40, 20]
    _header_row(ws, wb, headers, widths=widths)

    g7 = _read_safe(cfg, "fact_gestion_07_protocolos_poblacion.parquet")
    dim_p = _read_safe(cfg, "dim_protocolos_lineas.parquet")
    prot5 = _read_safe(cfg, "fact_gestion_05_prioridades_protocolos.parquet")

    if g7.empty:
        ws.write(1, 0, "Sin datos — ejecutar scripts V2 primero.", _fmt(wb))
        return

    r_out = 1
    for empresa in empresas:
        sub_g7 = g7[(g7["empresa"] == empresa) & (g7["nivel_gestion"] == "Intervencion Urgente")] \
            if "empresa" in g7.columns else pd.DataFrame()

        n_total_emp = g7[g7["empresa"] == empresa]["cedula"].nunique() \
            if "empresa" in g7.columns else 0

        if sub_g7.empty:
            ws.write(r_out, 0, empresa, _fmt(wb))
            ws.write(r_out, 1, "Sin trabajadores en nivel urgente", _fmt(wb))
            r_out += 1
            continue

        # Agrupar por protocolo
        prot_agg = sub_g7.groupby("protocolo_id").agg(
            n_urgente=("cedula", "nunique"),
            protocolo_nombre=("protocolo_nombre", "first"),
            objetivo=("objetivo", "first"),
            resultado_esperado=("resultado_esperado", "first"),
            linea_gestion=("linea_gestion", "first"),
            eje_gestion=("eje_gestion", "first"),
        ).reset_index().sort_values("n_urgente", ascending=False)

        # Es prioritario para el sector
        prio_ids = set()
        if not prot5.empty and "empresa" in prot5.columns and "es_prioritario_sector" in prot5.columns:
            sub_p5 = prot5[(prot5["empresa"] == empresa) & (prot5["es_prioritario_sector"] == True)]
            prio_ids = set(sub_p5["protocolo_id"].unique()) if "protocolo_id" in sub_p5.columns else set()

        for _, prow in prot_agg.iterrows():
            n_urg = int(prow.get("n_urgente", 0))
            pct_urg = round(n_urg / n_total_emp * 100, 1) if n_total_emp > 0 else 0.0
            es_prio = prow["protocolo_id"] in prio_ids
            sem_bg = "#EF4444" if pct_urg > 35 else "#F59E0B" if pct_urg >= 15 else "#10B981"
            fmt_r = _fmt(wb, bg=sem_bg)
            fmt_n = _fmt(wb, num_format="0.0", bg=sem_bg)

            ws.write(r_out, 0, empresa, fmt_r)
            ws.write(r_out, 1, str(prow.get("protocolo_id", "")), fmt_r)
            ws.write(r_out, 2, str(prow.get("protocolo_nombre", "")), fmt_r)
            ws.write(r_out, 3, str(prow.get("eje_gestion", "")), fmt_r)
            ws.write(r_out, 4, str(prow.get("linea_gestion", "")), fmt_r)
            ws.write_number(r_out, 5, n_urg, fmt_r)
            ws.write_number(r_out, 6, pct_urg, fmt_n)
            ws.write(r_out, 7, str(prow.get("objetivo", "")), _fmt(wb, wrap=True))
            ws.write(r_out, 8, str(prow.get("resultado_esperado", "")), _fmt(wb, wrap=True))
            ws.write(r_out, 9, "Si" if es_prio else "No", fmt_r)
            r_out += 1


def _hoja_calculos_verificacion(wb, cfg: dict):
    """15_Calculos_Verificacion: trazabilidad completa de fórmulas."""
    ws = wb.add_worksheet("15_Calculos_Verificacion")
    ws.set_column(0, 0, 35)
    ws.set_column(1, 1, 65)

    fmt_title = _fmt(wb, bold=True, bg=COLOR_NAVY, font_color=COLOR_GOLD, align="center")
    fmt_b = _fmt(wb, wrap=True)

    ws.merge_range("A1:B1", "Trazabilidad de Cálculos — Visualizador 3", fmt_title)

    eco = cfg.get("parametros_economicos", {})
    smlv = eco.get("SMLV_mensual", 2_800_000)
    pres = eco.get("presentismo_factor", 1.40)
    emp = eco.get("costo_empleador", 0.60)
    psico = eco.get("factor_psicosocial", 0.30)

    datos = [
        ("── FÓRMULA R10 AUSENTISMO ──", ""),
        ("Paso 1: N trabajadores planta", "dim_trabajador.total_planta"),
        ("Paso 2: % ausentismo", "(total_dias_ausencia / (N_planta × 240)) × 100"),
        ("Referente país ausentismo", "3.0% — promedio empresas Colombia"),
        ("Paso 3: Costo FTE anual", f"SMLV ${smlv:,.0f} × 12 = ${smlv*12:,.0f} COP"),
        ("Paso 4: Capacidad productiva", "N_planta × Costo_FTE_anual"),
        ("Paso 5: Pérdida anual", "% ausentismo × Capacidad_productiva"),
        ("Paso 6: Pérdida empleador", f"Pérdida × {emp:.0%} (empleador). {1-emp:.0%} = SGSS"),
        ("Paso 7: Pérdida + presentismo", f"Paso 6 × {pres} (+{(pres-1):.0%} presentismo)"),
        ("Paso 8: Costo psicosocial", f"Paso 7 × {psico:.0%} (riesgo psicosocial)"),
        ("ROI estimado", "(Costo_psicosocial − Inversión_SST) / Inversión_SST × 100"),
        ("Inversión SST estimada", "N_planta × Costo_FTE_anual × 2% masa salarial"),
        ("", ""),
        ("── SEMÁFORO % RIESGO ALTO+MUY ALTO ──", ""),
        ("Rojo (crítico)", "> 35%  →  #EF4444"),
        ("Amarillo (moderado)", "15% – 34%  →  #F59E0B"),
        ("Verde (controlado)", "< 15%  →  #10B981"),
        ("", ""),
        ("── SEMÁFORO DIFERENCIA pp VS REFERENTE ──", ""),
        ("Rojo", "> +15 pp por encima del referente"),
        ("Amarillo", "+5 a +15 pp por encima"),
        ("Verde claro", "0 a +5 pp"),
        ("Verde", "por debajo del referente"),
        ("", ""),
        ("── SEMÁFORO AUSENTISMO ──", ""),
        ("Rojo", "> 5%"),
        ("Amarillo", "3% – 5%"),
        ("Verde", "< 3%"),
        ("", ""),
        ("── REGLA R8 CONFIDENCIALIDAD ──", ""),
        ("Umbral", "Grupos < 5 personas → 'Confidencial'"),
        ("Aplicación", "Pirámide, áreas, cargos, estado civil, antigüedades"),
        ("", ""),
        ("── FUENTES REFERENTES NACIONALES ──", ""),
        ("Benchmarking sector", "III ENCST 2021 — Ministerio de Trabajo Colombia"),
        ("Benchmarking dominios", "II + III ENCST 2013–2021"),
        ("Ausentismo país", "Estudios SST Colombia — 3% promedio"),
        ("Presentismo", "40% adicional sobre pérdida empleador (estudios vigentes)"),
        ("Psicosocial", "30% del costo total ausentismo+presentismo (literatura internacional)"),
        ("", ""),
        ("── PARQUETS FUENTE ──", ""),
        ("fact_v3_kpis_globales", "ETL 09 — fact_scores_baremo + fact_benchmark + vigilancia"),
        ("fact_v3_demografia", "ETL 09 — dim_demografia + dim_trabajador"),
        ("fact_v3_costos", "ETL 09 — dim_ausentismo + dim_trabajador + config"),
        ("fact_v3_benchmarking", "ETL 09 — fact_benchmark + fact_gestion_05 + config"),
        ("fact_v3_ranking_areas", "ETL 09 — fact_consolidado + fact_scores_baremo"),
    ]

    for i, (campo, valor) in enumerate(datos):
        is_title = campo.startswith("──")
        fmt_use = _fmt(wb, bold=True, bg=COLOR_GRIS) if is_title else fmt_b
        ws.write(2 + i, 0, campo, fmt_use)
        ws.write(2 + i, 1, valor, fmt_use)


def _hoja_simple_parquet(wb, cfg: dict, nombre_hoja: str, parquet: str,
                          empresas: list, filtro_tipo: str = None):
    """Hoja genérica: vuelca un parquet filtrado por empresa."""
    ws = wb.add_worksheet(nombre_hoja)
    df = _read_safe(cfg, parquet)
    if df.empty:
        ws.write(0, 0, f"Sin datos — ejecutar 09_asis_gerencial.py ({parquet})", _fmt(wb))
        return
    if "empresa" in df.columns:
        df = df[df["empresa"].isin(empresas)].copy()
    if filtro_tipo and "tipo" in df.columns:
        df = df[df["tipo"] == filtro_tipo].copy()
    if df.empty:
        ws.write(0, 0, "Sin datos para las empresas seleccionadas.", _fmt(wb))
        return
    df = df.reset_index(drop=True)
    _header_row(ws, wb, df.columns.tolist())
    for r, row in df.iterrows():
        for c, val in enumerate(row):
            fmt = _fmt(wb)
            if isinstance(val, (int, np.integer)):
                ws.write_number(r + 1, c, int(val), fmt)
            elif isinstance(val, (float, np.floating)):
                if np.isnan(val):
                    ws.write_blank(r + 1, c, None, fmt)
                else:
                    ws.write_number(r + 1, c, float(val), fmt)
            elif val is None:
                ws.write_blank(r + 1, c, None, fmt)
            else:
                ws.write(r + 1, c, str(val), fmt)


# ── Orquestador ─────────────────────────────────────────────────────────────

def main():
    try:
        import xlsxwriter
    except ImportError:
        log.error("xlsxwriter no instalado. Ejecute: pip install xlsxwriter")
        sys.exit(1)

    log.info("╔══════════════════════════════════════════════════════════════╗")
    log.info("║  MentalPRO · Auditoría V3 · consultel + indecol             ║")
    log.info("╚══════════════════════════════════════════════════════════════╝")

    cfg = _load_config()
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    EMPRESAS_AUDITADAS = ["CONSULTEL", "INDECOL"]

    wb = xlsxwriter.Workbook(str(OUTPUT_PATH), {"constant_memory": False})
    wb.set_properties({"title": "Auditoría V3 MentalPRO", "author": "MentalPRO ETL"})

    log.info("Generando hojas...")

    _hoja_portada(wb, cfg, EMPRESAS_AUDITADAS)

    # 01 Encabezado — desde fact_consolidado + dim_trabajador
    _hoja_simple_parquet(wb, cfg, "01_Encabezado", "dim_trabajador.parquet",
                         EMPRESAS_AUDITADAS)

    # 02 KPIs Globales
    _hoja_kpis_globales(wb, cfg, EMPRESAS_AUDITADAS)

    # 03 Vulnerabilidad — KPI individual
    ws_vuln = wb.add_worksheet("03_Vulnerabilidad")
    df_kpis = _read_safe(cfg, "fact_v3_kpis_globales.parquet")
    if not df_kpis.empty and "kpi_grupo" in df_kpis.columns:
        df_v = df_kpis[
            (df_kpis["empresa"].isin(EMPRESAS_AUDITADAS)) &
            (df_kpis["kpi_grupo"] == "Vulnerabilidad Psicológica")
        ].reset_index(drop=True)
        _header_row(ws_vuln, wb, df_v.columns.tolist())
        for r, row in df_v.iterrows():
            for c, val in enumerate(row):
                ws_vuln.write(r + 1, c, str(val) if val is not None else "", _fmt(wb))
    else:
        ws_vuln.write(0, 0, "Sin datos.", _fmt(wb))

    # 04 Protocolos Urgentes
    ws_urg = wb.add_worksheet("04_Protocolos_Urgentes")
    if not df_kpis.empty and "kpi_grupo" in df_kpis.columns:
        df_u = df_kpis[
            (df_kpis["empresa"].isin(EMPRESAS_AUDITADAS)) &
            # BUG FIX: nombre real en parquet es "Protocolos Urgentes" (no "e Inmediatos")
            (df_kpis["kpi_grupo"].isin(["Protocolos Urgentes", "Vigilancia Epidemiológica"]))
        ].reset_index(drop=True)
        _header_row(ws_urg, wb, df_u.columns.tolist())
        for r, row in df_u.iterrows():
            for c, val in enumerate(row):
                ws_urg.write(r + 1, c, str(val) if val is not None else "", _fmt(wb))
    else:
        ws_urg.write(0, 0, "Sin datos.", _fmt(wb))

    # 05 Pirámide Poblacional
    ws_pir = wb.add_worksheet("05_Piramide_Poblacional")
    df_demo = _read_safe(cfg, "fact_v3_demografia.parquet")
    COL_VAR = "variable" if "variable" in df_demo.columns else "variable_demografica"
    if not df_demo.empty and COL_VAR in df_demo.columns:
        df_p = df_demo[
            (df_demo["empresa"].isin(EMPRESAS_AUDITADAS)) &
            (df_demo[COL_VAR] == "piramide_poblacional")
        ].reset_index(drop=True)
        _header_row(ws_pir, wb, df_p.columns.tolist())
        for r, row in df_p.iterrows():
            for c, val in enumerate(row):
                ws_pir.write(r + 1, c, str(val) if val is not None else "", _fmt(wb))
    else:
        ws_pir.write(0, 0, "Sin datos.", _fmt(wb))

    # 06 Antigüedades
    ws_ant = wb.add_worksheet("06_Antiguedades")
    if not df_demo.empty:
        df_a = df_demo[
            (df_demo["empresa"].isin(EMPRESAS_AUDITADAS)) &
            (df_demo[COL_VAR].isin(["antiguedad_empresa", "antiguedad_cargo"]))
        ].reset_index(drop=True)
        _header_row(ws_ant, wb, df_a.columns.tolist())
        for r, row in df_a.iterrows():
            for c, val in enumerate(row):
                ws_ant.write(r + 1, c, str(val) if val is not None else "", _fmt(wb))
    else:
        ws_ant.write(0, 0, "Sin datos.", _fmt(wb))

    # 07 Perfil Ocupacional
    ws_ocu = wb.add_worksheet("07_Perfil_Ocupacional")
    if not df_demo.empty:
        df_o = df_demo[
            (df_demo["empresa"].isin(EMPRESAS_AUDITADAS)) &
            (df_demo[COL_VAR].isin(
                ["area_departamento", "categoria_cargo", "forma_intralaboral",
                 "estado_civil", "dependientes_economicos"]
            ))
        ].reset_index(drop=True)
        _header_row(ws_ocu, wb, df_o.columns.tolist())
        for r, row in df_o.iterrows():
            for c, val in enumerate(row):
                ws_ocu.write(r + 1, c, str(val) if val is not None else "", _fmt(wb))
    else:
        ws_ocu.write(0, 0, "Sin datos.", _fmt(wb))

    # 08 Costos Ausentismo
    _hoja_costos(wb, cfg, EMPRESAS_AUDITADAS)

    # 09 ROI Salud Mental
    _hoja_roi(wb, cfg, EMPRESAS_AUDITADAS)

    # 10 Benchmarking Intralaboral
    _hoja_benchmarking(wb, cfg, EMPRESAS_AUDITADAS)

    # 11 Protocolos Sector
    _hoja_simple_parquet(wb, cfg, "11_Protocolos_Sector",
                         "fact_v3_benchmarking.parquet",
                         EMPRESAS_AUDITADAS, filtro_tipo="protocolos_urgentes")

    # 12 Dimensiones vs Colombia
    _hoja_simple_parquet(wb, cfg, "12_Dimensiones_Colombia",
                         "fact_v3_benchmarking.parquet",
                         EMPRESAS_AUDITADAS, filtro_tipo="dimension_critica")

    # 13 Ranking Áreas
    _hoja_ranking_areas(wb, cfg, EMPRESAS_AUDITADAS)

    # 14 Fichas Protocolos — protocolo × empresa con N trabajadores urgentes
    _hoja_fichas_protocolos(wb, cfg, EMPRESAS_AUDITADAS)

    # 15 Verificación
    _hoja_calculos_verificacion(wb, cfg)

    wb.close()
    log.info(f"✅ Libro generado: {OUTPUT_PATH}")
    log.info(f"   Hojas: 16 · Empresas: {EMPRESAS_AUDITADAS}")


if __name__ == "__main__":
    main()
