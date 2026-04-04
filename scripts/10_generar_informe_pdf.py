#!/usr/bin/env python3
"""
10_generar_informe_pdf.py — MentalPRO · Informe Diagnóstico de Riesgo Psicosocial
===================================================================================
Genera un informe técnico-científico en PDF para 1 empresa (Res. 2764/2022).

Uso:
    python scripts/10_generar_informe_pdf.py --empresa INDECOL
    python scripts/10_generar_informe_pdf.py --empresa INDECOL --sin-ia

Salida:
    data/final/Informe_RiesgoPsicosocial_INDECOL_YYYYMMDD.pdf
"""

import sys
import logging
import argparse
from pathlib import Path
from datetime import datetime

import io
import pandas as pd
import numpy as np
import yaml

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import cm
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle,
    PageBreak, HRFlowable, Image as RLImage,
)
from reportlab.lib.enums import TA_CENTER, TA_JUSTIFY

try:
    import anthropic
    HAS_ANTHROPIC = True
except ImportError:
    HAS_ANTHROPIC = False

# ── Setup ─────────────────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent.parent
log = logging.getLogger(__name__)

# ── Paleta R11 — inamovible ───────────────────────────────────────────────────
C_NAVY  = colors.HexColor("#0A1628")
C_GOLD  = colors.HexColor("#C9952A")
C_CYAN  = colors.HexColor("#00C2CB")
C_WHITE = colors.white
C_GRAY  = colors.HexColor("#F3F4F6")
C_DARK  = colors.HexColor("#1F2937")
C_MID   = colors.HexColor("#6B7280")

C_RIESGO = {
    1: colors.HexColor("#10B981"),
    2: colors.HexColor("#6EE7B7"),
    3: colors.HexColor("#F59E0B"),
    4: colors.HexColor("#F97316"),
    5: colors.HexColor("#EF4444"),
}

ETIQUETAS = {
    1: "Sin riesgo", 2: "Riesgo bajo", 3: "Riesgo medio",
    4: "Riesgo alto", 5: "Riesgo muy alto",
}

# Colores matplotlib (hex strings) — mismo orden que R11
MPL_RIESGO = {
    "Sin riesgo":    "#10B981",
    "Riesgo bajo":   "#6EE7B7",
    "Riesgo medio":  "#F59E0B",
    "Riesgo alto":   "#F97316",
    "Riesgo muy alto": "#EF4444",
}
MPL_NAVY = "#0A1628"
MPL_GOLD = "#C9952A"
MPL_GRAY = "#F3F4F6"

# ── Helpers globales ──────────────────────────────────────────────────────────
def _pct(v) -> str:
    if v is None or (isinstance(v, float) and np.isnan(v)):
        return "N/D"
    return f"{float(v):.1f}%"


def _hex_c(h) -> colors.Color:
    if isinstance(h, str) and h.startswith("#"):
        return colors.HexColor(h)
    return C_GRAY


def _tbl_base() -> TableStyle:
    return TableStyle([
        ("FONTNAME",      (0, 0), (-1,  0), "Helvetica-Bold"),
        ("FONTNAME",      (0, 1), (-1, -1), "Helvetica"),
        ("FONTSIZE",      (0, 0), (-1, -1), 8.5),
        ("BACKGROUND",    (0, 0), (-1,  0), C_NAVY),
        ("TEXTCOLOR",     (0, 0), (-1,  0), C_WHITE),
        ("ALIGN",         (0, 0), (-1, -1), "CENTER"),
        ("ALIGN",         (0, 1), ( 0, -1), "LEFT"),
        ("ROWBACKGROUNDS",(0, 1), (-1, -1), [C_WHITE, C_GRAY]),
        ("GRID",          (0, 0), (-1, -1), 0.4, colors.HexColor("#D1D5DB")),
        ("TOPPADDING",    (0, 0), (-1, -1), 5),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
        ("LEFTPADDING",   (0, 0), (-1, -1), 6),
        ("RIGHTPADDING",  (0, 0), (-1, -1), 6),
        ("VALIGN",        (0, 0), (-1, -1), "MIDDLE"),
    ])


# ── Estilos ReportLab ─────────────────────────────────────────────────────────
def _build_styles():
    st = getSampleStyleSheet()

    st.add(ParagraphStyle(
        "H1", parent=st["Heading1"], fontName="Helvetica-Bold", fontSize=13,
        textColor=C_NAVY, spaceBefore=16, spaceAfter=8,
    ))
    st.add(ParagraphStyle(
        "H2", parent=st["Heading2"], fontName="Helvetica-Bold", fontSize=10.5,
        textColor=C_NAVY, spaceBefore=10, spaceAfter=6,
    ))
    st.add(ParagraphStyle(
        "Body", parent=st["Normal"], fontName="Helvetica", fontSize=9.5,
        textColor=C_DARK, leading=14, spaceAfter=6, alignment=TA_JUSTIFY,
    ))
    st.add(ParagraphStyle(
        "Caption", parent=st["Normal"], fontName="Helvetica-Oblique", fontSize=8,
        textColor=C_MID, alignment=TA_CENTER, spaceAfter=8,
    ))
    st.add(ParagraphStyle(
        "Alert", parent=st["Normal"], fontName="Helvetica-Bold", fontSize=9,
        textColor=colors.HexColor("#92400E"),
        backColor=colors.HexColor("#FEF3C7"),
        borderPad=6, spaceAfter=8,
    ))
    st.add(ParagraphStyle(
        "Ref", parent=st["Normal"], fontName="Helvetica", fontSize=9,
        textColor=C_DARK, leading=13, spaceAfter=5, leftIndent=12,
        firstLineIndent=-12,
    ))
    return st


# ═══════════════════════════════════════════════════════════════════════════════
class InformeRiesgoPsicosocial:
    """Genera el informe diagnóstico de riesgo psicosocial en PDF."""

    def __init__(self, empresa: str, sin_ia: bool = False):
        self.empresa = empresa.upper().strip()
        self.sin_ia = sin_ia

        with open(BASE_DIR / "config" / "config.yaml", "r", encoding="utf-8") as f:
            self.cfg = yaml.safe_load(f)

        self.proc = BASE_DIR / self.cfg["paths"]["processed"]
        self.out_dir = BASE_DIR / "data" / "final"
        self.out_dir.mkdir(parents=True, exist_ok=True)
        self.st = _build_styles()

        self._load_data()

    # ── Carga de datos ─────────────────────────────────────────────────────────
    def _rp(self, fname: str) -> pd.DataFrame:
        p = self.proc / fname
        if not p.exists():
            log.warning(f"Parquet no encontrado: {fname}")
            return pd.DataFrame()
        return pd.read_parquet(p)

    def _fe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Filter by empresa."""
        if "empresa" in df.columns and not df.empty:
            return df[df["empresa"] == self.empresa].copy()
        return df

    def _load_data(self):
        log.info(f"Cargando datos para {self.empresa}...")
        self.baremo     = self._fe(self._rp("fact_scores_baremo.parquet"))
        self.consolidado= self._fe(self._rp("fact_consolidado.parquet"))
        self.benchmark  = self._fe(self._rp("fact_benchmark.parquet"))
        self.kpis       = self._fe(self._rp("fact_v3_kpis_globales.parquet"))
        self.demo       = self._fe(self._rp("fact_v3_demografia.parquet"))
        self.costos     = self._fe(self._rp("fact_v3_costos.parquet"))
        self.areas      = self._fe(self._rp("fact_v3_ranking_areas.parquet"))
        self.riesgo_emp = self._fe(self._rp("fact_riesgo_empresa.parquet"))
        self.top20      = self._fe(self._rp("fact_top20_comparables.parquet"))
        self.trabajador = self._fe(self._rp("dim_trabajador.parquet"))

        self.n_eval = self.trabajador["cedula"].nunique() if not self.trabajador.empty else 0
        self.sector = (
            self.riesgo_emp["sector_rag"].iloc[0]
            if not self.riesgo_emp.empty else "[DATO NO DISPONIBLE]"
        )
        self.fecha = datetime.today().strftime("%B de %Y").capitalize()
        log.info(f"  Evaluados: {self.n_eval} | Sector: {self.sector}")

    # ── Distribución de riesgo ────────────────────────────────────────────────
    def _dist_riesgo(self, instrumentos: list, nivel_analisis: str) -> pd.DataFrame:
        """Agrega % por etiqueta de riesgo para cada nombre_nivel.

        Retorna columnas: nombre_nivel | Sin riesgo | ... | Riesgo muy alto | %A+MA
        R8: nombre_nivel con n_total < 5 → marcado 'confidencial'
        """
        df = self.baremo[
            (self.baremo["instrumento"].isin(instrumentos)) &
            (self.baremo["nivel_analisis"] == nivel_analisis)
        ].copy()

        if df.empty:
            return pd.DataFrame()

        grp = (
            df.groupby(["nombre_nivel", "nivel_riesgo", "etiqueta_nivel"])
            .agg(n=("cedula", "count"))
            .reset_index()
        )
        tot = grp.groupby("nombre_nivel")["n"].sum().rename("n_total")
        grp = grp.merge(tot, on="nombre_nivel")
        grp["pct"] = (grp["n"] / grp["n_total"] * 100).round(1)

        # Pivot
        piv = grp.pivot_table(
            index=["nombre_nivel", "n_total"],
            columns="etiqueta_nivel",
            values="pct",
            aggfunc="sum",
            fill_value=0,
        ).reset_index()
        piv.columns.name = None

        col_order = [c for c in ETIQUETAS.values() if c in piv.columns]
        piv = piv[["nombre_nivel", "n_total"] + col_order]
        piv["%A+MA"] = (
            piv.get("Riesgo alto", 0) + piv.get("Riesgo muy alto", 0)
        ).round(1)
        piv["confidencial"] = piv["n_total"] < 5
        return piv.sort_values("%A+MA", ascending=False).reset_index(drop=True)

    # ── Elementos visuales ────────────────────────────────────────────────────
    def _sp(self, h=0.3) -> Spacer:
        return Spacer(1, h * cm)

    def _hr(self) -> HRFlowable:
        return HRFlowable(width="100%", thickness=0.5, color=C_NAVY, spaceAfter=6)

    def _h1(self, t: str) -> Paragraph:
        return Paragraph(f"<b>{t}</b>", self.st["H1"])

    def _h2(self, t: str) -> Paragraph:
        return Paragraph(t, self.st["H2"])

    def _body(self, t: str) -> Paragraph:
        return Paragraph(t, self.st["Body"])

    def _caption(self, t: str) -> Paragraph:
        return Paragraph(t, self.st["Caption"])

    def _alert(self, t: str) -> Paragraph:
        return Paragraph(f"⚠ {t}", self.st["Alert"])

    def _tbl(self, data: list, widths=None, extra: list = None) -> Table:
        t = Table(data, colWidths=widths)
        style = _tbl_base()
        if extra:
            for cmd in extra:
                style.add(*cmd)
        t.setStyle(style)
        return t

    def _semaforo_extra(self, pct: float, row: int, col: int = -1) -> list:
        """Devuelve comandos de estilo semáforo para una celda."""
        cmds = []
        if pct > 35:
            cmds.append(("BACKGROUND", (col, row), (col, row), C_RIESGO[5]))
            cmds.append(("TEXTCOLOR",  (col, row), (col, row), C_WHITE))
        elif pct >= 15:
            cmds.append(("BACKGROUND", (col, row), (col, row), C_RIESGO[3]))
        else:
            cmds.append(("BACKGROUND", (col, row), (col, row), C_RIESGO[1]))
        return cmds

    # ── Gráficas matplotlib → ReportLab Image ────────────────────────────────
    def _fig_to_rl(self, fig, width_cm: float = 17) -> RLImage:
        """Convierte figura matplotlib a objeto Image de ReportLab."""
        buf = io.BytesIO()
        fig.savefig(buf, format="png", dpi=150, bbox_inches="tight",
                    facecolor="white")
        plt.close(fig)
        buf.seek(0)
        ratio = fig.get_size_inches()[1] / fig.get_size_inches()[0]
        w = width_cm * cm
        return RLImage(buf, width=w, height=w * ratio)

    def _chart_stacked_h(self, df_src: pd.DataFrame,
                         titulo: str = "", width_cm: float = 17) -> "RLImage | None":
        """Barras horizontales apiladas (distribución 5 niveles de riesgo)."""
        df = df_src[~df_src["confidencial"]].copy()
        col_order = [c for c in ETIQUETAS.values() if c in df.columns]
        if df.empty or not col_order:
            return None

        nombres = df["nombre_nivel"].tolist()
        n = len(nombres)
        fig_h = max(2.2, n * 0.55 + 0.8)
        fig, ax = plt.subplots(figsize=(8, fig_h))

        left = np.zeros(n)
        for col in col_order:
            vals = df[col].fillna(0).values
            bars = ax.barh(nombres, vals, left=left,
                           color=MPL_RIESGO[col], label=col, height=0.55)
            # Etiqueta dentro de la barra si hay espacio
            for bar, v in zip(bars, vals):
                if v >= 8:
                    ax.text(
                        bar.get_x() + bar.get_width() / 2,
                        bar.get_y() + bar.get_height() / 2,
                        f"{v:.0f}%", ha="center", va="center",
                        fontsize=6.5, color="white" if col in ("Riesgo alto", "Riesgo muy alto") else MPL_NAVY,
                        fontweight="bold",
                    )
            left += vals

        ax.axvline(35, color="#EF4444", linestyle="--", linewidth=0.9,
                   alpha=0.75, label="Umbral crítico 35%")
        ax.set_xlim(0, 100)
        ax.set_xlabel("% de trabajadores", fontsize=8)
        ax.tick_params(axis="y", labelsize=8)
        ax.tick_params(axis="x", labelsize=7)
        ax.spines[["top", "right"]].set_visible(False)
        if titulo:
            ax.set_title(titulo, fontsize=9, fontweight="bold", color=MPL_NAVY, pad=6)
        ax.legend(loc="lower right", fontsize=6.5, framealpha=0.8)
        fig.tight_layout(pad=0.5)
        return self._fig_to_rl(fig, width_cm)

    def _chart_hbar(self, nombres: list, valores: list,
                    titulo: str = "", width_cm: float = 17,
                    umbral: float = 35.0) -> "RLImage | None":
        """Barras horizontales simples con coloreado por semáforo."""
        if not nombres:
            return None
        n = len(nombres)
        fig_h = max(2.0, n * 0.6 + 0.8)
        fig, ax = plt.subplots(figsize=(8, fig_h))

        colores = []
        for v in valores:
            if v > 50:
                colores.append(MPL_RIESGO["Riesgo muy alto"])
            elif v > 35:
                colores.append(MPL_RIESGO["Riesgo alto"])
            elif v >= 15:
                colores.append(MPL_RIESGO["Riesgo medio"])
            else:
                colores.append(MPL_RIESGO["Sin riesgo"])

        bars = ax.barh(nombres, valores, color=colores, height=0.55)
        for bar, v in zip(bars, valores):
            ax.text(bar.get_width() + 1, bar.get_y() + bar.get_height() / 2,
                    f"{v:.1f}%", va="center", fontsize=8,
                    color=MPL_NAVY, fontweight="bold")

        ax.axvline(umbral, color="#EF4444", linestyle="--",
                   linewidth=0.9, alpha=0.75, label=f"Umbral {umbral:.0f}%")
        ax.set_xlim(0, max(valores) * 1.18 + 5)
        ax.set_xlabel("% Riesgo Alto + Muy Alto", fontsize=8)
        ax.tick_params(axis="y", labelsize=8)
        ax.tick_params(axis="x", labelsize=7)
        ax.spines[["top", "right"]].set_visible(False)
        ax.legend(fontsize=6.5, framealpha=0.8)
        if titulo:
            ax.set_title(titulo, fontsize=9, fontweight="bold", color=MPL_NAVY, pad=6)
        fig.tight_layout(pad=0.5)
        return self._fig_to_rl(fig, width_cm)

    def _chart_demo_bar(self, variable: str, titulo: str,
                        width_cm: float = 14) -> "RLImage | None":
        """Barras horizontales para una variable demográfica."""
        df = self.demo
        if df.empty:
            return None
        sub = df[(df["variable"] == variable) & (~df["confidencial"].fillna(False))].copy()
        if sub.empty or len(sub) < 2:
            return None

        nombres = sub["categoria"].tolist()
        valores = sub["pct"].fillna(0).tolist()

        fig_h = max(1.8, len(nombres) * 0.5 + 0.6)
        fig, ax = plt.subplots(figsize=(7, fig_h))
        bars = ax.barh(nombres, valores, color=MPL_NAVY, height=0.5)
        for bar, v in zip(bars, valores):
            ax.text(bar.get_width() + 0.5, bar.get_y() + bar.get_height() / 2,
                    f"{v:.1f}%", va="center", fontsize=8, color=MPL_NAVY)
        ax.set_xlabel("% de trabajadores", fontsize=8)
        ax.tick_params(axis="y", labelsize=8)
        ax.tick_params(axis="x", labelsize=7)
        ax.spines[["top", "right"]].set_visible(False)
        if titulo:
            ax.set_title(titulo, fontsize=9, fontweight="bold", color=MPL_NAVY, pad=6)
        fig.tight_layout(pad=0.5)
        return self._fig_to_rl(fig, width_cm)

    # ══════════════════════════════════════════════════════════════════════════
    # SECCIONES
    # ══════════════════════════════════════════════════════════════════════════

    def _s0_portada(self) -> list:
        """S0: Portada."""
        inner_style = ParagraphStyle(
            "CoverP", fontName="Helvetica", fontSize=10,
            textColor=C_WHITE, alignment=TA_CENTER,
        )
        content = Paragraph(
            "<br/><br/><br/>"
            f"<font size='9' color='#C9952A'>AVANTUM CONSULTING</font><br/><br/>"
            f"<font size='22'><b>Informe de Resultados</b></font><br/>"
            f"<font size='16'>Evaluación de Factores de Riesgo Psicosocial</font><br/><br/>"
            f"<font size='14' color='#C9952A'><b>{self.empresa}</b></font><br/><br/>"
            f"<font size='9' color='#9CA3AF'>"
            f"Sector: {self.sector} · {self.n_eval} evaluados · {self.fecha}"
            f"</font><br/><br/>"
            f"<font size='8' color='#6B7280'>Versión 1.0 — Confidencial</font><br/>"
            f"<font size='8' color='#6B7280'>"
            f"Resolución 2764 de 2022 — Ministerio de Trabajo Colombia"
            f"</font><br/><br/><br/>",
            inner_style,
        )
        cover = Table([[content]], colWidths=[17 * cm], rowHeights=[22 * cm])
        cover.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, -1), C_NAVY),
            ("VALIGN",     (0, 0), (-1, -1), "MIDDLE"),
            ("ALIGN",      (0, 0), (-1, -1), "CENTER"),
        ]))
        return [cover, PageBreak()]

    def _s1_toc(self) -> list:
        """S1: Tabla de contenido."""
        elems = [self._h1("TABLA DE CONTENIDO"), self._sp(0.2)]
        toc = [
            ("Sección 2", "Ficha Técnica"),
            ("Sección 3", "Análisis Sociodemográfico (ASIS)"),
            ("Sección 4", "Resultados Intralaborales"),
            ("Sección 5", "Resultados Extralaborales y Estrés"),
            ("Sección 6", "Análisis por Áreas — Mapa de Calor"),
            ("Sección 7", "Ausentismo y Correlación CIE-10"),
            ("Sección 8", "Conclusiones"),
            ("Sección 9", "Plan de Intervención"),
            ("Sección 10", "Referencias Bibliográficas"),
            ("Sección 11", "Anexos"),
        ]
        data = [["Sección", "Contenido"]] + list(toc)
        elems.append(self._tbl(data, [3.5 * cm, 13.5 * cm]))
        elems.append(PageBreak())
        return elems

    def _s2_ficha_tecnica(self) -> list:
        """S2: Ficha técnica."""
        elems = [self._h1("SECCIÓN 2 — FICHA TÉCNICA"), self._hr()]

        n_a = len(self.trabajador[self.trabajador["forma_intra"] == "A"]) if not self.trabajador.empty else 0
        n_b = len(self.trabajador[self.trabajador["forma_intra"] == "B"]) if not self.trabajador.empty else 0

        ficha = [
            ["Campo", "Descripción"],
            ["Entidad evaluadora", "Avantum Consulting"],
            ["Empresa evaluada", self.empresa],
            ["Sector económico", self.sector],
            ["Marco normativo",
             "Resolución 2764/2022 y Resolución 2646/2008 — MinTrabajo Colombia"],
            ["Batería utilizada",
             "Batería de Instrumentos para la Evaluación de Factores de Riesgo "
             "Psicosocial (MinTrabajo 2010, actualizada 2022)"],
            ["Instrumentos aplicados",
             "Forma A (directivos/profesionales con personal a cargo), "
             "Forma B (operarios/auxiliares), Cuestionario Extralaboral, "
             "Cuestionario de Estrés"],
            ["Total evaluados",
             f"{self.n_eval} personas  ·  {n_a} Forma A  ·  {n_b} Forma B"],
            ["Baremos aplicados",
             "Nacionales por tipo de cargo y sexo (2010 / 2022)"],
            ["Escala de clasificación",
             "Sin riesgo · Riesgo bajo · Riesgo medio · Riesgo alto · Riesgo muy alto"],
            ["Referente nacional",
             "ENCST 2013–2021 (II y III Encuesta Nacional de Condiciones de "
             "Salud y Trabajo)"],
            ["Período de evaluación", self.fecha],
            ["Versión del informe", "V1.0 — Confidencial"],
        ]
        extra = [
            ("ALIGN",    (0, 1), (0, -1), "RIGHT"),
            ("FONTNAME", (0, 1), (0, -1), "Helvetica-Bold"),
            ("TEXTCOLOR",(0, 1), (0, -1), C_NAVY),
        ]
        elems.append(self._tbl(ficha, [5 * cm, 12 * cm], extra))

        reevaluar = (
            self.riesgo_emp["debe_reevaluar_1año"].any()
            if not self.riesgo_emp.empty and "debe_reevaluar_1año" in self.riesgo_emp.columns
            else False
        )
        if reevaluar:
            elems += [self._sp(), self._alert(
                "Conforme a la Resolución 2764/2022, dado el nivel de riesgo identificado, "
                "la empresa debe realizar una reevaluación en plazo no mayor a 12 meses."
            )]
        elems.append(PageBreak())
        return elems

    def _s3_asis(self) -> list:
        """S3: Análisis sociodemográfico."""
        elems = [self._h1("SECCIÓN 3 — ANÁLISIS SOCIODEMOGRÁFICO (ASIS)"), self._hr()]

        df = self.demo
        if df.empty:
            elems += [self._body("[DATO NO DISPONIBLE]"), PageBreak()]
            return elems

        def _subtabla(variable: str, titulo: str) -> list:
            sub = df[df["variable"] == variable][["categoria", "n", "pct", "confidencial"]].copy()
            if sub.empty:
                return []
            data = [["Categoría", "N", "%"]]
            for _, r in sub.iterrows():
                if r.get("confidencial", False):
                    data.append([r["categoria"], "–", "Confidencial"])
                else:
                    data.append([r["categoria"], int(r["n"]), _pct(r["pct"])])
            return [self._h2(titulo), self._tbl(data, [8 * cm, 3 * cm, 3.5 * cm]), self._sp(0.2)]

        for var, tit in [
            ("piramide_poblacional",  "3.1 Distribución por Sexo y Grupo de Edad"),
            ("nivel_escolaridad",     "3.2 Nivel de Escolaridad"),
            ("estado_civil",          "3.3 Estado Civil"),
            ("antiguedad_empresa",    "3.4 Antigüedad en la Empresa"),
            ("categoria_cargo",       "3.5 Categoría de Cargo"),
            ("forma_intralaboral",    "3.6 Distribución por Forma Intralaboral"),
            ("area_departamento",     "3.7 Distribución por Área / Departamento"),
        ]:
            elems.extend(_subtabla(var, tit))

        # Gráfica: distribución por categoría de cargo
        img_cargo = self._chart_demo_bar("categoria_cargo", "Figura 3.1 — Distribución por categoría de cargo")
        if img_cargo:
            elems += [img_cargo, self._caption("Figura 3.1 — % de trabajadores por categoría de cargo"), self._sp(0.2)]

        # Gráfica: antigüedad
        img_antig = self._chart_demo_bar("antiguedad_empresa", "Figura 3.2 — Antigüedad en la empresa")
        if img_antig:
            elems += [img_antig, self._caption("Figura 3.2 — % de trabajadores por antigüedad en la empresa"), self._sp(0.2)]

        # Cobertura
        n_planta = self.n_eval
        if not self.costos.empty and "n_planta" in self.costos.columns:
            n_planta = int(self.costos["n_planta"].iloc[0])
        cobertura = round(self.n_eval / n_planta * 100, 1) if n_planta > 0 else 0

        elems += [
            self._sp(), self._h2("3.8 Cobertura de la Evaluación"),
            self._tbl(
                [["Indicador", "Valor"],
                 ["Total trabajadores (planta)", n_planta],
                 ["Total evaluados", self.n_eval],
                 ["Cobertura (%)", f"{cobertura:.1f}%"]],
                [9 * cm, 6 * cm],
            ),
            PageBreak(),
        ]
        return elems

    def _s4_intralaboral(self) -> list:
        """S4: Resultados intralaborales."""
        elems = [self._h1("SECCIÓN 4 — RESULTADOS INTRALABORALES"), self._hr()]

        # 4.1 Resumen ejecutivo
        elems.append(self._h2("4.1 Resumen Ejecutivo"))
        if not self.riesgo_emp.empty:
            data = [["Instrumento", "N evaluados", "Puntaje transformado (0-100)",
                     "Nivel de riesgo", "Reevaluación en 12 meses"]]
            extra = []
            for i, (_, r) in enumerate(self.riesgo_emp.iterrows(), 1):
                nr = int(r.get("nivel_riesgo_empresa", 0))
                data.append([
                    r["instrumento"],
                    int(r["n_trabajadores"]),
                    f"{r['puntaje_transformado']:.1f}",
                    ETIQUETAS.get(nr, str(nr)),
                    "Sí" if r.get("debe_reevaluar_1año") else "No",
                ])
                if nr in C_RIESGO:
                    extra += [
                        ("BACKGROUND", (3, i), (3, i), C_RIESGO[nr]),
                        ("TEXTCOLOR",  (3, i), (3, i), C_WHITE),
                    ]
            elems += [
                self._tbl(data, [3.5*cm, 3*cm, 5*cm, 3.5*cm, 3*cm], extra),
                self._caption("Tabla 4.1 — Nivel de riesgo global por instrumento intralaboral"),
                self._sp(),
            ]

        # 4.2 Distribución por dominios
        elems.append(self._h2("4.2 Resultados por Dominio"))
        df_dom = self._dist_riesgo(["IntraA", "IntraB"], "dominio")
        if not df_dom.empty:
            col_data = [c for c in ETIQUETAS.values() if c in df_dom.columns]
            headers = ["Dominio"] + col_data + ["% Alto+Muy Alto"]
            data = [headers]
            extra = []
            for i, (_, r) in enumerate(df_dom.iterrows(), 1):
                if r["confidencial"]:
                    data.append([r["nombre_nivel"]] + ["Confidencial"] * (len(col_data) + 1))
                    continue
                fila = [r["nombre_nivel"]]
                for c in col_data:
                    fila.append(_pct(r.get(c, 0)))
                fila.append(_pct(r["%A+MA"]))
                data.append(fila)
                extra += self._semaforo_extra(r["%A+MA"], i)
            ncw = len(headers)
            w = [5*cm] + [((17 - 5) / (ncw - 1))*cm] * (ncw - 1)
            elems += [
                self._tbl(data, w, extra),
                self._caption(
                    "Tabla 4.2 — Distribución por dominio. "
                    "Semáforo: >35% rojo · 15–34% amarillo · <15% verde"
                ),
                self._sp(0.3),
            ]
            # Gráfica dominios
            img_dom = self._chart_stacked_h(df_dom, "Figura 4.1 — Distribución de riesgo por dominio intralaboral")
            if img_dom:
                elems += [img_dom, self._caption("Figura 4.1 — Barras apiladas: distribución de los 5 niveles de riesgo por dominio"), self._sp()]

        # 4.3 Top 5 dimensiones críticas
        elems.append(self._h2("4.3 Top 5 Dimensiones Críticas"))
        df_dim = self._dist_riesgo(["IntraA", "IntraB"], "dimension")
        if not df_dim.empty:
            top5 = df_dim[~df_dim["confidencial"]].head(5)
            data = [["Rango", "Dimensión", "N evaluados", "% Alto + Muy Alto"]]
            extra = []
            for i, (_, r) in enumerate(top5.iterrows(), 1):
                data.append([str(i), r["nombre_nivel"], int(r["n_total"]), _pct(r["%A+MA"])])
                extra += self._semaforo_extra(r["%A+MA"], i, col=3)
            elems += [
                self._tbl(data, [1.5*cm, 8.5*cm, 3*cm, 4*cm], extra),
                self._caption("Tabla 4.3 — Dimensiones con mayor porcentaje de Riesgo Alto y Muy Alto"),
                self._sp(0.3),
            ]
            # Gráfica top dimensiones
            img_top = self._chart_hbar(
                top5["nombre_nivel"].tolist(),
                top5["%A+MA"].tolist(),
                "Figura 4.2 — Top 5 dimensiones criticas (% Riesgo Alto + Muy Alto)",
            )
            if img_top:
                elems += [img_top, self._caption("Figura 4.2 — Dimensiones intralaborales con mayor exposicion a riesgo"), self._sp()]

        # 4.4 Comparativo Forma A vs. Forma B
        elems.append(self._h2("4.4 Comparativo Forma A vs. Forma B"))
        df_formas = self.baremo[
            (self.baremo["instrumento"].isin(["IntraA", "IntraB"])) &
            (self.baremo["nivel_analisis"] == "dimension")
        ].copy()

        if not df_formas.empty:
            grp = (
                df_formas.groupby(["instrumento", "nombre_nivel", "nivel_riesgo"])
                .agg(n=("cedula", "count")).reset_index()
            )
            tot = (
                grp.groupby(["instrumento", "nombre_nivel"])["n"]
                .sum().rename("n_total").reset_index()
            )
            grp = grp.merge(tot, on=["instrumento", "nombre_nivel"])
            grp["pct"] = grp["n"] / grp["n_total"] * 100
            ama = (
                grp[grp["nivel_riesgo"] >= 4]
                .groupby(["instrumento", "nombre_nivel"])["pct"]
                .sum().reset_index()
            )
            ama.columns = ["instrumento", "nombre_nivel", "%A+MA"]
            ama = ama.merge(tot, on=["instrumento", "nombre_nivel"])
            ama["confidencial"] = ama["n_total"] < 5

            piv = ama.pivot_table(
                index="nombre_nivel", columns="instrumento",
                values="%A+MA", fill_value=0,
            ).reset_index()
            piv.columns.name = None
            formas = [c for c in ["IntraA", "IntraB"] if c in piv.columns]

            data = [["Dimensión"] + formas]
            for _, r in piv.iterrows():
                fila = [r["nombre_nivel"]]
                for f in formas:
                    fila.append(_pct(r.get(f, 0)))
                data.append(fila)

            w_f = [8.5*cm] + [(8.5 / len(formas))*cm] * len(formas)
            elems += [
                self._tbl(data, w_f),
                self._caption(
                    "Tabla 4.4 — % Riesgo Alto+Muy Alto por dimensión según forma intralaboral"
                ),
            ]

        elems.append(PageBreak())
        return elems

    def _s5_extralaboral_estres(self) -> list:
        """S5: Extralaboral y Estrés."""
        elems = [self._h1("SECCIÓN 5 — EXTRALABORALES Y ESTRÉS"), self._hr()]

        for instrumento, numero, titulo in [
            (["Extralaboral"], "5.1", "5.1 Dimensiones Extralaborales"),
            (["Estres"],       "5.2", "5.2 Síntomas de Estrés"),
        ]:
            elems.append(self._h2(titulo))
            df = self._dist_riesgo(instrumento, "dimension")
            if df.empty:
                elems.append(self._body(
                    f"[DATO NO DISPONIBLE] — No se aplicó cuestionario de "
                    f"{'Estrés' if 'Estres' in instrumento else 'Extralaboral'}."
                ))
            else:
                col_data = [c for c in ETIQUETAS.values() if c in df.columns]
                headers = ["Dimensión"] + col_data + ["% Alto+Muy Alto"]
                data = [headers]
                extra = []
                for i, (_, r) in enumerate(df.iterrows(), 1):
                    if r["confidencial"]:
                        data.append([r["nombre_nivel"]] + ["Confidencial"] * (len(col_data) + 1))
                        continue
                    fila = [r["nombre_nivel"]] + [_pct(r.get(c, 0)) for c in col_data]
                    fila.append(_pct(r["%A+MA"]))
                    data.append(fila)
                    extra += self._semaforo_extra(r["%A+MA"], i)
                ncw = len(headers)
                w = [5.5*cm] + [((17 - 5.5) / (ncw - 1))*cm] * (ncw - 1)
                elems += [
                    self._tbl(data, w, extra),
                    self._caption(f"Tabla {numero} — Distribución de niveles de riesgo"),
                    self._sp(0.3),
                ]
                # Gráfica
                img_e = self._chart_stacked_h(df, f"Figura — Distribución de riesgo: {titulo}")
                if img_e:
                    elems += [img_e, self._caption(f"Figura — Distribución de los 5 niveles de riesgo: {titulo}"), self._sp()]

        elems.append(PageBreak())
        return elems

    def _s6_mapa_calor(self) -> list:
        """S6: Análisis por áreas."""
        elems = [self._h1("SECCIÓN 6 — ANÁLISIS POR ÁREAS (MAPA DE CALOR)"), self._hr()]

        df = self.areas
        if df.empty:
            elems += [self._body("[DATO NO DISPONIBLE]"), PageBreak()]
            return elems

        data = [["Área / Departamento", "N eval.", "% Alto+Muy Alto",
                 "Nivel predominante", "Dimensión crítica"]]
        extra = []
        df_s = df.sort_values("pct_ama", ascending=False)
        for i, (_, r) in enumerate(df_s.iterrows(), 1):
            if r.get("confidencial", False):
                data.append([r["area_departamento"], int(r["n_evaluados"]),
                              "Confidencial", "–", "–"])
            else:
                data.append([
                    r["area_departamento"],
                    int(r["n_evaluados"]),
                    _pct(r["pct_ama"]),
                    r.get("nivel_predominante", ""),
                    r.get("dimension_critica", ""),
                ])
                extra += self._semaforo_extra(r["pct_ama"], i, col=2)

        elems += [
            self._tbl(data, [4.5*cm, 2*cm, 3.5*cm, 4*cm, 4*cm], extra),
            self._caption(
                "Tabla 6.1 — Ranking de áreas por nivel de riesgo. "
                "Criterio crítico: >40% riesgo alto/muy alto"
            ),
        ]

        # Gráfica áreas
        df_vis = df_s[~df_s.get("confidencial", pd.Series(False)).fillna(False)]
        if not df_vis.empty:
            img_areas = self._chart_hbar(
                df_vis["area_departamento"].tolist(),
                df_vis["pct_ama"].tolist(),
                "Figura 6.1 — % Riesgo Alto+Muy Alto por area",
                umbral=40.0,
            )
            if img_areas:
                elems += [img_areas, self._caption("Figura 6.1 — Porcentaje de trabajadores en riesgo Alto o Muy Alto por area. Umbral critico: 40%"), self._sp(0.3)]

        criticas = df_s[
            (df_s["pct_ama"] > 40) & (~df_s.get("confidencial", pd.Series(False)).fillna(False))
        ]
        if not criticas.empty:
            areas_str = ", ".join(criticas["area_departamento"].tolist())
            elems += [self._sp(), self._alert(
                f"Se identificaron {len(criticas)} área(s) con >40% de trabajadores en "
                f"Riesgo Alto o Muy Alto: {areas_str}. "
                "Se requiere plan de intervención inmediata."
            )]

        elems.append(PageBreak())
        return elems

    def _s7_ausentismo(self) -> list:
        """S7: Ausentismo y correlación CIE-10."""
        elems = [self._h1("SECCIÓN 7 — AUSENTISMO Y CORRELACIÓN CIE-10"), self._hr()]

        df = self.costos
        if df.empty:
            elems += [self._body("[DATO NO DISPONIBLE]"), PageBreak()]
            return elems

        elems.append(self._h2("7.1 Indicadores de Ausentismo y Costo Económico"))
        data = [["Paso", "Indicador", "Valor", "Unidad"]]
        for _, r in df.sort_values("paso").iterrows():
            data.append([
                str(r["paso"]),
                r.get("nombre_paso", ""),
                str(r.get("valor", "")),
                str(r.get("unidad", "")),
            ])
        elems += [
            self._tbl(data, [1.5*cm, 7.5*cm, 4.5*cm, 3.5*cm]),
            self._caption(
                "Tabla 7.1 — Fórmula de costo económico del ausentismo (Metodología R10 — Avantum)"
            ),
            self._sp(),
        ]

        elems.append(self._h2("7.2 Correlación con Diagnósticos CIE-10"))
        pct_aus = df["pct_ausentismo"].iloc[0] if "pct_ausentismo" in df.columns else 0
        dif = df["diferencia_pp_vs_pais"].iloc[0] if "diferencia_pp_vs_pais" in df.columns else 0

        txt = (
            f"La tasa de ausentismo registrada para {self.empresa} es del {_pct(pct_aus)}, "
            f"presentando una diferencia de {dif:+.1f} puntos porcentuales respecto "
            f"al referente nacional. "
        )
        if pct_aus == 0:
            txt += (
                "No se registraron días de incapacidad laboral en el período evaluado. "
                "Esta condición impide la correlación cuantitativa con diagnósticos CIE-10 "
                "(Capítulo F — Trastornos mentales y del comportamiento; grupo osteomuscular). "
                "[DATO NO DISPONIBLE: se requieren registros de incapacidad médica para el análisis de correlación.]"
            )
        else:
            txt += (
                "Se recomienda analizar la distribución diagnóstica (CIE-10, Capítulo F y grupo "
                "osteomuscular) en relación con las dimensiones de riesgo psicosocial identificadas, "
                "con el fin de establecer hipótesis de causalidad orientadas a la intervención."
            )
        elems.append(self._body(txt))
        elems.append(PageBreak())
        return elems

    def _s8_conclusiones(self) -> list:
        """S8: Conclusiones (narrativa IA o placeholder)."""
        elems = [self._h1("SECCIÓN 8 — CONCLUSIONES"), self._hr()]

        if self.sin_ia or not HAS_ANTHROPIC:
            elems += [
                self._alert(
                    "Narrativa pendiente. Ejecute sin --sin-ia para generar "
                    "automáticamente con Claude (requiere ANTHROPIC_API_KEY)."
                ),
                self._body("[NARRATIVA POR GENERAR]"),
                PageBreak(),
            ]
            return elems

        ctx = self._build_ctx()
        narrativa = self._llamar_claude(ctx)
        for parrafo in narrativa.split("\n\n"):
            p = parrafo.strip()
            if p:
                elems += [self._body(p), self._sp(0.15)]

        elems.append(PageBreak())
        return elems

    def _s9_plan_intervencion(self) -> list:
        """S9: Plan de intervención."""
        elems = [self._h1("SECCIÓN 9 — PLAN DE INTERVENCIÓN"), self._hr(),
                 self._h2("Tabla de Intervención Prioritaria (≥35% Riesgo Alto+Muy Alto)")]

        # Recopilar dimensiones críticas de todos los instrumentos
        dimensiones: list[tuple[str, float, str]] = []
        for instrs, instrumento in [
            (["IntraA", "IntraB"], "Intralaboral"),
            (["Extralaboral"],     "Extralaboral"),
            (["Estres"],           "Estrés"),
        ]:
            df_src = self._dist_riesgo(instrs, "dimension")
            if not df_src.empty:
                for _, r in df_src[~df_src["confidencial"]].iterrows():
                    if r["%A+MA"] >= 35:
                        dimensiones.append((r["nombre_nivel"], r["%A+MA"], instrumento))

        # Estrategias base (extendibles)
        ESTRATEGIAS = {
            "Demandas cuantitativas":
                ("Redistribución de carga laboral y rediseño de procesos",
                 "RRHH / Gerencia", "0–3 meses"),
            "Demandas emocionales":
                ("Talleres de regulación emocional y manejo del estrés",
                 "SST / RRHH", "0–3 meses"),
            "Demandas de la jornada de trabajo":
                ("Revisión de turnos y esquema de descansos",
                 "RRHH / Gerencia", "0–3 meses"),
            "Control sobre el trabajo":
                ("Programa de autonomía, participación y toma de decisiones",
                 "Gerencia / Jefaturas", "3–6 meses"),
            "Características del liderazgo":
                ("Formación en liderazgo transformacional y habilidades blandas",
                 "RRHH", "3–6 meses"),
            "Consistencia del rol":
                ("Clarificación de roles, funciones y responsabilidades",
                 "RRHH / Jefaturas", "0–3 meses"),
            "Claridad de rol":
                ("Actualización de perfiles de cargo y manual de funciones",
                 "RRHH", "0–3 meses"),
            "Retroalimentación del desempeño":
                ("Implementación de sistema de evaluación y retroalimentación continua",
                 "RRHH / Gerencia", "3–6 meses"),
            "Reconocimiento y compensación":
                ("Revisión del esquema salarial y programa de reconocimiento no monetario",
                 "Gerencia / RRHH", "3–12 meses"),
            "Relaciones sociales en el trabajo":
                ("Programa de convivencia laboral y resolución de conflictos",
                 "SST / RRHH", "3–6 meses"),
            "Balance entre la vida laboral y familiar":
                ("Flexibilización horaria y actividades de bienestar familiar",
                 "RRHH", "3–6 meses"),
            "Relaciones familiares":
                ("Escuela de familia y apoyo psicosocial externo",
                 "SST", "3–6 meses"),
            "Situación económica del grupo familiar":
                ("Orientación financiera y vinculación con Caja de Compensación",
                 "RRHH / Gerencia", "3–12 meses"),
        }
        DEFAULT = (
            "Intervención específica — definir con equipo SST y RRHH",
            "SST / RRHH",
            "3–6 meses",
        )

        if not dimensiones:
            elems.append(self._body(
                "No se identificaron dimensiones con porcentaje de Riesgo Alto+Muy Alto "
                "igual o superior al 35%. Se recomienda mantener el programa de vigilancia "
                "epidemiológica y fortalecer actividades de promoción de la salud mental."
            ))
        else:
            dimensiones.sort(key=lambda x: x[1], reverse=True)
            data = [["Dimensión", "Instrumento", "% A+MA",
                     "Estrategia de intervención", "Responsable", "Plazo"]]
            extra = []
            for i, (dim, pct, instr) in enumerate(dimensiones, 1):
                nivel = "CRÍTICO" if pct > 50 else "PRIORITARIO"
                est = ESTRATEGIAS.get(dim, DEFAULT)
                data.append([f"{dim}\n[{nivel}]", instr, _pct(pct), est[0], est[1], est[2]])
                bg = C_RIESGO[5] if pct > 50 else C_RIESGO[4]
                extra += [
                    ("BACKGROUND", (0, i), (0, i), bg),
                    ("TEXTCOLOR",  (0, i), (0, i), C_WHITE),
                ]
            extra.append(("FONTSIZE", (0, 1), (-1, -1), 8))
            extra.append(("LEADING",  (0, 1), (-1, -1), 10))
            elems += [
                self._tbl(data, [4.5*cm, 2.5*cm, 2*cm, 4*cm, 2.5*cm, 2*cm], extra),
                self._caption(
                    "Tabla 9.1 — Plan de intervención. CRÍTICO: >50% · PRIORITARIO: 35–50%. "
                    "Plazos: Muy Alto 0–3 m · Alto 3–6 m · Medio/estructural 3–12 m."
                ),
            ]

        elems.append(PageBreak())
        return elems

    def _s10_referencias(self) -> list:
        """S10: Referencias APA 7."""
        elems = [self._h1("SECCIÓN 10 — REFERENCIAS BIBLIOGRÁFICAS"), self._hr()]

        refs = [
            "Ministerio de Trabajo de Colombia. (2022). <i>Resolución 2764 del 25 de julio de "
            "2022</i>. Por la cual se adopta la Batería de Instrumentos para la Evaluación de "
            "Factores de Riesgo Psicosocial. MinTrabajo.",

            "Ministerio de la Protección Social. (2010). <i>Batería de instrumentos para la "
            "evaluación de factores de riesgo psicosocial</i>. Ministerio de la Protección "
            "Social — Pontificia Universidad Javeriana.",

            "Ministerio de la Protección Social. (2008). <i>Resolución 2646 de 2008</i>. "
            "Por la cual se establecen disposiciones y se definen responsabilidades para la "
            "identificación, evaluación, prevención, intervención y monitoreo permanente de la "
            "exposición a factores de riesgo psicosocial. MinTrabajo.",

            "Ministerio del Trabajo y Ministerio de Salud y Protección Social. (2013). "
            "<i>II Encuesta Nacional de Condiciones de Salud y Trabajo en el Sistema General "
            "de Riesgos Laborales (ENCST)</i>. MinTrabajo — MinSalud.",

            "Ministerio del Trabajo y Ministerio de Salud y Protección Social. (2021). "
            "<i>III Encuesta Nacional de Condiciones de Salud y Trabajo en el Sistema General "
            "de Riesgos Laborales (ENCST)</i>. MinTrabajo — MinSalud.",

            "Organización Internacional del Trabajo — OIT. (2022). "
            "<i>Riesgos psicosociales y estrés en el trabajo</i>. OIT.",

            "World Health Organization — WHO. (2022). <i>World mental health report: "
            "Transforming mental health for all</i>. WHO Press.",
        ]
        for r in refs:
            elems += [Paragraph(r, self.st["Ref"]), self._sp(0.1)]

        elems.append(PageBreak())
        return elems

    def _s11_anexos(self) -> list:
        """S11: Anexos."""
        elems = [self._h1("SECCIÓN 11 — ANEXOS"), self._hr()]

        # Anexo A: Benchmarking vs Colombia
        elems.append(self._h2("Anexo A — Benchmarking vs. Colombia (ENCST)"))
        df_bm = self.benchmark
        if not df_bm.empty:
            df_dom = df_bm[df_bm["nivel_analisis"] == "dominio"]
            if not df_dom.empty:
                data = [["Dominio / Dimensión", "Forma", "% Empresa",
                          "% Referente", "Diferencia pp", "Semáforo"]]
                extra = []
                for i, (_, r) in enumerate(df_dom.iterrows(), 1):
                    sem = str(r.get("semaforo", ""))
                    sem_label = {"rojo": "Rojo ▲", "amarillo": "Amarillo →", "verde": "Verde ✓"}.get(sem, sem)
                    data.append([
                        r["nombre_nivel"],
                        r.get("forma_intra", "–"),
                        _pct(r.get("pct_empresa")),
                        _pct(r.get("pct_referencia")),
                        f"{r['diferencia_pp']:+.1f}",
                        sem_label,
                    ])
                    bg = {"rojo": C_RIESGO[5], "amarillo": C_RIESGO[3], "verde": C_RIESGO[1]}.get(sem)
                    if bg:
                        extra.append(("BACKGROUND", (5, i), (5, i), bg))
                        if sem == "rojo":
                            extra.append(("TEXTCOLOR", (5, i), (5, i), C_WHITE))
                elems += [
                    self._tbl(data, [5*cm, 2*cm, 2.5*cm, 2.5*cm, 2.5*cm, 2.5*cm], extra),
                    self._caption("Tabla A.1 — Comparativo % Riesgo Alto+Muy Alto empresa vs. referente sectorial/nacional"),
                    self._sp(),
                ]

        # Anexo B: Glosario
        elems.append(self._h2("Anexo B — Glosario de Términos"))
        glosario = [
            ["Término", "Definición"],
            ["Batería de Riesgo Psicosocial",
             "Conjunto de instrumentos del MinTrabajo para medir factores intralaborales, "
             "extralaborales y de estrés (Res. 2764/2022)."],
            ["Forma A",
             "Instrumento intralaboral para personal con personal a cargo "
             "(directivos, jefes, profesionales)."],
            ["Forma B",
             "Instrumento intralaboral para personal sin personal a cargo "
             "(operarios, auxiliares, asistentes)."],
            ["Riesgo muy alto (nivel 5)",
             "Máxima exposición. Intervención obligatoria en plazo no mayor a 3 meses."],
            ["Riesgo alto (nivel 4)",
             "Alta exposición. Intervención en plazo no mayor a 6 meses."],
            ["ENCST",
             "Encuesta Nacional de Condiciones de Salud y Trabajo — referente nacional "
             "colombiano (2013 y 2021)."],
            ["CIE-10",
             "Clasificación Internacional de Enfermedades, 10.ª revisión. "
             "Capítulo F: trastornos mentales y del comportamiento."],
            ["ARL",
             "Administradora de Riesgos Laborales. Entidad responsable del SGRP en Colombia."],
            ["R8 (Confidencialidad)",
             "Grupos con menos de 5 personas: datos individuales protegidos, "
             "se reporta 'Confidencial'."],
        ]
        elems.append(self._tbl(glosario, [5*cm, 12*cm]))
        return elems

    # ── IA: contexto y llamada ────────────────────────────────────────────────
    def _build_ctx(self) -> dict:
        ctx: dict = {
            "empresa": self.empresa,
            "sector": self.sector,
            "n_evaluados": self.n_eval,
        }
        if not self.riesgo_emp.empty:
            ctx["riesgo_global"] = self.riesgo_emp[
                ["instrumento", "puntaje_transformado", "nivel_riesgo_empresa"]
            ].to_dict("records")

        if not self.kpis.empty:
            ctx["kpis_ama"] = (
                self.kpis[self.kpis["nivel_num"] >= 4]
                .groupby("kpi_grupo")["pct_alto_muy_alto"].max()
                .to_dict()
            )

        df_dim = self._dist_riesgo(["IntraA", "IntraB"], "dimension")
        if not df_dim.empty:
            ctx["top5_dimensiones"] = (
                df_dim[~df_dim["confidencial"]]
                .head(5)[["nombre_nivel", "%A+MA"]]
                .to_dict("records")
            )

        if not self.areas.empty:
            ctx["areas"] = self.areas[
                ["area_departamento", "pct_ama", "nivel_predominante"]
            ].to_dict("records")

        if not self.costos.empty and "pct_ausentismo" in self.costos.columns:
            ctx["pct_ausentismo"] = float(self.costos["pct_ausentismo"].iloc[0])

        return ctx

    def _llamar_claude(self, ctx: dict) -> str:
        try:
            sys_path = BASE_DIR / "docs" / "system_instructions.md"
            system_prompt = (
                sys_path.read_text(encoding="utf-8") if sys_path.exists() else ""
            )
            user_prompt = (
                f"Redacta las CONCLUSIONES del informe de riesgo psicosocial para "
                f"la empresa {ctx['empresa']} (sector {ctx['sector']}, "
                f"{ctx['n_evaluados']} evaluados).\n\n"
                f"**Datos estadísticos:**\n"
                f"- Riesgo global: {ctx.get('riesgo_global', '[DATO NO DISPONIBLE]')}\n"
                f"- % Alto+Muy Alto por instrumento: {ctx.get('kpis_ama', '[DATO NO DISPONIBLE]')}\n"
                f"- Top 5 dimensiones críticas: {ctx.get('top5_dimensiones', '[DATO NO DISPONIBLE]')}\n"
                f"- Áreas evaluadas: {ctx.get('areas', '[DATO NO DISPONIBLE]')}\n"
                f"- Tasa de ausentismo: {_pct(ctx.get('pct_ausentismo', 0))}\n\n"
                "**Instrucciones de redacción:**\n"
                "- Extensión: 400–600 palabras\n"
                "- Estructura: párrafo de síntesis → hallazgos críticos (2–3 párrafos) "
                "→ factores protectores → recomendación final con mención a Res. 2764/2022\n"
                "- Tono: académico, técnico, tercera persona impersonal\n"
                "- Citar niveles de riesgo específicos con los datos proporcionados\n"
                "- No mencionar nombres propios de trabajadores\n"
            )
            client = anthropic.Anthropic()
            response = client.messages.create(
                model="claude-opus-4-6",
                max_tokens=1024,
                system=system_prompt,
                messages=[{"role": "user", "content": user_prompt}],
            )
            return response.content[0].text
        except Exception as e:
            log.error(f"Error Claude API: {e}")
            return (
                f"[ERROR AL GENERAR NARRATIVA IA: {e}]\n\n"
                "Por favor redacte manualmente esta sección o vuelva a ejecutar el script."
            )

    # ── Generador principal ───────────────────────────────────────────────────
    def generar(self) -> Path:
        hoy = datetime.today().strftime("%Y%m%d")
        out = self.out_dir / f"Informe_RiesgoPsicosocial_{self.empresa}_{hoy}.pdf"

        log.info(f"Generando informe PDF para {self.empresa}...")
        doc = SimpleDocTemplate(
            str(out),
            pagesize=A4,
            rightMargin=2*cm, leftMargin=2*cm,
            topMargin=2.5*cm, bottomMargin=2.5*cm,
            title=f"Informe Riesgo Psicosocial — {self.empresa}",
            author="Avantum Consulting",
            subject="Evaluación de Factores de Riesgo Psicosocial — Res. 2764/2022",
        )

        story = []
        for seccion in [
            self._s0_portada,
            self._s1_toc,
            self._s2_ficha_tecnica,
            self._s3_asis,
            self._s4_intralaboral,
            self._s5_extralaboral_estres,
            self._s6_mapa_calor,
            self._s7_ausentismo,
            self._s8_conclusiones,
            self._s9_plan_intervencion,
            self._s10_referencias,
            self._s11_anexos,
        ]:
            story.extend(seccion())

        doc.build(story)
        log.info(f"PDF generado: {out}")
        return out


# ── CLI ───────────────────────────────────────────────────────────────────────
def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s  %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )
    parser = argparse.ArgumentParser(
        description="Genera informe de riesgo psicosocial (Res. 2764/2022) para 1 empresa."
    )
    parser.add_argument("--empresa", required=True,
                        help="Nombre de la empresa tal como aparece en los datos (ej: INDECOL)")
    parser.add_argument("--sin-ia", action="store_true",
                        help="Omitir generación de narrativa IA (Sección 8 queda como placeholder)")
    args = parser.parse_args()

    informe = InformeRiesgoPsicosocial(empresa=args.empresa, sin_ia=args.sin_ia)
    out = informe.generar()
    print(f"\n{'='*60}")
    print(f"  Informe generado exitosamente:")
    print(f"  {out}")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
