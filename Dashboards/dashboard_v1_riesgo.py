"""
dashboard_v1_riesgo.py
======================
Visualizador 1 — Resultados de Riesgo Psicosocial (Res. 2764/2022)
Exporta HTML standalone Plotly (sin servidor).

Secciones:
  S1  Resultados globales: barras apiladas 5 niveles + gauge + KPIs + alerta reevaluación
  S2  Distribución 0-100: boxplot scores transformados por instrumento
  S3  Benchmarking sector: % Alto+MuyAlto IntraA y B vs sector ENCST
  S4  Dominios vs Colombia: heatmap % riesgo dominios empresa vs referencia
  S5  Dimensiones vs Colombia: tabla semaforizada empresa vs ENCST
  S6  Preguntas clave: Top 20 preguntas con mayor diferencia vs Colombia
  S7  Empresa vs Áreas (Intra+Extra+Estrés): heatmap área × dimensión
  S8  Empresa vs Áreas (Individual): heatmap área × dimensión individual

Filtros interactivos (Plotly dropdowns):
  - Empresa | Forma (A/B/Ambas)

Especificaciones técnicas:
  - Plotly HTML estático: write_html(full_html=True, include_plotlyjs='cdn')
  - Colores inamovibles R11 (AVANTUM)
  - R8: grupos < 5 → "Confidencial"
  - Output: output/dashboard_v1_riesgo.html
"""

import logging
import sys
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
import plotly.io as pio
import yaml
from plotly.subplots import make_subplots

ROOT = Path(__file__).resolve().parents[1]
CONFIG_PATH = ROOT / "config" / "config.yaml"
OUTPUT_DIR = ROOT / "output"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)

# ── Paleta R11 (inamovible) ──────────────────────────────────────────────────
COLORES_RIESGO = {
    1: "#10B981",  # Sin riesgo
    2: "#6EE7B7",  # Bajo
    3: "#F59E0B",  # Medio
    4: "#F97316",  # Alto
    5: "#EF4444",  # Muy alto
}
LABELS_RIESGO = {1: "Sin riesgo", 2: "Bajo", 3: "Medio", 4: "Alto", 5: "Muy alto"}

COLORES_PROTECCION = {
    1: "#EF4444",  # Muy bajo
    2: "#F97316",  # Bajo
    3: "#F59E0B",  # Medio
    4: "#6EE7B7",  # Alto
    5: "#10B981",  # Muy alto
}

NAVY  = "#0A1628"
GOLD  = "#C9952A"
CYAN  = "#00C2CB"
WHITE = "#FFFFFF"
GRAY  = "#F3F4F6"

FONT_FAMILY = "Inter, Arial, sans-serif"

SEMAFORO_COLOR = {
    "verde":        "#10B981",
    "rojo":         "#EF4444",
    "insuficiente": "#9CA3AF",
}


def cargar_config() -> dict:
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


# ═══════════════════════════════════════════════════════════════════════════════
# Helpers
# ═══════════════════════════════════════════════════════════════════════════════

def filtrar(df: pd.DataFrame, empresa: str, forma: str | None = None) -> pd.DataFrame:
    out = df[df["empresa"] == empresa].copy()
    if forma and forma != "Ambas":
        if "forma_intra" in out.columns:
            out = out[out["forma_intra"] == forma]
    return out


def confidencial(n: int, n_min: int) -> bool:
    return n < n_min


def distribucion_niveles(
    df_baremo: pd.DataFrame, nivel_analisis: str, nombre_nivel: str, forma: str
) -> dict[int, float]:
    """% de personas por cada nivel 1-5 para un instrumento dado."""
    sub = df_baremo[
        (df_baremo["nivel_analisis"] == nivel_analisis) &
        (df_baremo["nombre_nivel"] == nombre_nivel)
    ]
    if forma != "Ambas":
        sub = sub[sub["forma_intra"] == forma]
    if sub.empty:
        return {i: 0.0 for i in range(1, 6)}
    total = sub["cedula"].nunique()
    return {
        lvl: round(sub[sub["nivel_riesgo"] == lvl]["cedula"].nunique() / total * 100, 1)
        for lvl in range(1, 6)
    }


# ═══════════════════════════════════════════════════════════════════════════════
# Estilos base de figuras
# ═══════════════════════════════════════════════════════════════════════════════

def base_layout(title: str, height: int = 500) -> dict:
    return dict(
        title=dict(text=title, font=dict(family=FONT_FAMILY, size=16, color=NAVY), x=0.02),
        paper_bgcolor=WHITE,
        plot_bgcolor=GRAY,
        font=dict(family=FONT_FAMILY, color=NAVY),
        height=height,
        margin=dict(l=60, r=40, t=60, b=60),
    )


# ═══════════════════════════════════════════════════════════════════════════════
# S1 — Resultados globales
# ═══════════════════════════════════════════════════════════════════════════════

def build_s1(baremo: pd.DataFrame, riesgo_empresa: pd.DataFrame,
             empresa: str, forma: str, n_min: int) -> go.Figure:
    """Barras apiladas 5 niveles para cada instrumento/factor + gauge nivel empresa."""
    instrumentos = ["IntraA", "IntraB", "Extralaboral", "Estres", "Individual"]
    instrumento_labels = {
        "IntraA": "Intralaboral A", "IntraB": "Intralaboral B",
        "Extralaboral": "Extralaboral", "Estres": "Estrés", "Individual": "Individual",
    }

    b_emp = filtrar(baremo, empresa, forma)
    factores = b_emp[b_emp["nivel_analisis"] == "factor"].copy()

    fig = go.Figure()

    for lvl in range(1, 6):
        pcts = []
        for instr in instrumentos:
            sub = factores[factores["nombre_nivel"] == instr]
            total = sub["cedula"].nunique()
            n_lvl = sub[sub["nivel_riesgo"] == lvl]["cedula"].nunique()
            pcts.append(round(n_lvl / total * 100, 1) if total >= n_min else 0)

        fig.add_trace(go.Bar(
            name=LABELS_RIESGO[lvl],
            x=[instrumento_labels.get(i, i) for i in instrumentos],
            y=pcts,
            marker_color=COLORES_RIESGO[lvl],
            text=[f"{p:.1f}%" for p in pcts],
            textposition="inside",
        ))

    # Alerta reevaluación
    re = riesgo_empresa[riesgo_empresa["empresa"] == empresa]
    alerta_txt = ""
    for _, row in re.iterrows():
        if row.get("debe_reevaluar_1año"):
            alerta_txt += f"⚠️ {row['instrumento']}: Nivel {row['nivel_riesgo_empresa']} — Reevaluar en 1 año<br>"

    annotations = []
    if alerta_txt:
        annotations.append(dict(
            text=alerta_txt, x=0.5, y=-0.18, xref="paper", yref="paper",
            showarrow=False, font=dict(color="#EF4444", size=12), align="center",
        ))

    fig.update_layout(
        **base_layout(f"S1 — Distribución de Riesgo por Factor | {empresa}", height=480),
        barmode="stack",
        xaxis_title="Instrumento",
        yaxis_title="% Trabajadores",
        yaxis=dict(range=[0, 100]),
        legend=dict(orientation="h", y=-0.12),
        annotations=annotations,
    )
    return fig


# ═══════════════════════════════════════════════════════════════════════════════
# S2 — Distribución 0-100 (Boxplot)
# ═══════════════════════════════════════════════════════════════════════════════

def build_s2(baremo: pd.DataFrame, empresa: str, forma: str) -> go.Figure:
    b_emp = filtrar(baremo, empresa, forma)
    factores = b_emp[b_emp["nivel_analisis"] == "factor"].copy()

    instrumentos = factores["nombre_nivel"].unique().tolist()
    fig = go.Figure()

    for instr in instrumentos:
        sub = factores[factores["nombre_nivel"] == instr]
        tipo = sub["tipo_baremo"].iloc[0] if not sub.empty else "riesgo"
        color = COLORES_RIESGO[3] if tipo == "riesgo" else COLORES_PROTECCION[3]
        fig.add_trace(go.Box(
            y=sub["puntaje_transformado"],
            name=instr,
            marker_color=color,
            boxmean="sd",
        ))

    fig.update_layout(
        **base_layout(f"S2 — Distribución Puntajes Transformados (0-100) | {empresa}", height=420),
        yaxis_title="Puntaje Transformado (0-100)",
        showlegend=False,
    )
    return fig


# ═══════════════════════════════════════════════════════════════════════════════
# S3 — Benchmarking sector
# ═══════════════════════════════════════════════════════════════════════════════

def build_s3(benchmark: pd.DataFrame, cfg: dict, empresa: str) -> go.Figure:
    bench_sector = cfg.get("benchmark_sector", {})
    prom_gral = bench_sector.get("_promedio_general", 39.69)

    b_emp = benchmark[
        (benchmark["empresa"] == empresa) &
        (benchmark["nivel_analisis"] == "factor") &
        (benchmark["nombre_nivel"].isin(["IntraA", "IntraB"]))
    ].copy()

    if b_emp.empty:
        fig = go.Figure()
        fig.update_layout(**base_layout(f"S3 — Benchmarking Sector | {empresa} (sin datos)"))
        return fig

    sector = b_emp["sector_rag"].iloc[0] if "sector_rag" in b_emp.columns else "No clasificado"
    pct_sector = bench_sector.get(sector, prom_gral)

    fig = go.Figure()

    for _, row in b_emp.iterrows():
        forma = row.get("forma_intra", "")
        label = f"Intralaboral {forma}"
        fig.add_trace(go.Bar(
            name=label,
            x=[label],
            y=[row["pct_empresa"]],
            marker_color=COLORES_RIESGO[4] if row["pct_empresa"] > pct_sector else COLORES_RIESGO[2],
            text=[f"{row['pct_empresa']:.1f}%"],
            textposition="outside",
        ))

    # Línea de referencia sector
    fig.add_hline(
        y=pct_sector, line_dash="dash", line_color=GOLD,
        annotation_text=f"Sector {sector}: {pct_sector:.1f}%",
        annotation_position="top right",
    )
    # Línea promedio general Colombia
    fig.add_hline(
        y=prom_gral, line_dash="dot", line_color=CYAN,
        annotation_text=f"Colombia general: {prom_gral:.1f}%",
        annotation_position="top left",
    )

    fig.update_layout(
        **base_layout(f"S3 — % Alto+Muy Alto vs Sector ({sector}) | {empresa}", height=420),
        yaxis_title="% Trabajadores con Riesgo Alto o Muy Alto",
        yaxis=dict(range=[0, 100]),
        showlegend=False,
    )
    return fig


# ═══════════════════════════════════════════════════════════════════════════════
# S4 — Dominios vs Colombia (barras comparativas)
# ═══════════════════════════════════════════════════════════════════════════════

def build_s4(benchmark: pd.DataFrame, empresa: str, forma: str) -> go.Figure:
    b_emp = benchmark[
        (benchmark["empresa"] == empresa) &
        (benchmark["nivel_analisis"] == "dominio") &
        (benchmark["tipo_referencia"] == "colombia")
    ].copy()

    if forma != "Ambas":
        b_emp = b_emp[b_emp["forma_intra"].isin([forma, "AB"])]

    if b_emp.empty:
        fig = go.Figure()
        fig.update_layout(**base_layout(f"S4 — Dominios vs Colombia | {empresa} (sin datos)"))
        return fig

    b_grouped = (
        b_emp.groupby("nombre_nivel", as_index=False)
        .agg(pct_empresa=("pct_empresa", "mean"), pct_referencia=("pct_referencia", "first"))
    )
    b_grouped = b_grouped.sort_values("pct_empresa", ascending=True)

    fig = go.Figure()
    fig.add_trace(go.Bar(
        name="Empresa",
        y=b_grouped["nombre_nivel"],
        x=b_grouped["pct_empresa"],
        orientation="h",
        marker_color=[
            COLORES_RIESGO[4] if (e - r) > 0 else COLORES_RIESGO[2]
            for e, r in zip(b_grouped["pct_empresa"], b_grouped["pct_referencia"])
        ],
        text=[f"{v:.1f}%" for v in b_grouped["pct_empresa"]],
        textposition="outside",
    ))
    fig.add_trace(go.Scatter(
        name="Colombia ENCST",
        y=b_grouped["nombre_nivel"],
        x=b_grouped["pct_referencia"],
        mode="markers",
        marker=dict(symbol="diamond", size=10, color=GOLD),
    ))

    fig.update_layout(
        **base_layout(f"S4 — % Alto+Muy Alto por Dominio vs Colombia | {empresa}", height=500),
        xaxis_title="% Trabajadores",
        xaxis=dict(range=[0, 100]),
        barmode="overlay",
        legend=dict(orientation="h", y=-0.12),
    )
    return fig


# ═══════════════════════════════════════════════════════════════════════════════
# S5 — Dimensiones vs Colombia (tabla semaforizada)
# ═══════════════════════════════════════════════════════════════════════════════

def build_s5(benchmark: pd.DataFrame, empresa: str, forma: str) -> go.Figure:
    b_emp = benchmark[
        (benchmark["empresa"] == empresa) &
        (benchmark["nivel_analisis"] == "dimension") &
        (benchmark["tipo_referencia"] == "colombia")
    ].copy()

    if forma != "Ambas":
        b_emp = b_emp[b_emp["forma_intra"].isin([forma, "AB"])]

    if b_emp.empty:
        fig = go.Figure()
        fig.update_layout(**base_layout(f"S5 — Dimensiones vs Colombia | {empresa} (sin datos)"))
        return fig

    b_grouped = (
        b_emp.groupby("nombre_nivel", as_index=False)
        .agg(
            pct_empresa=("pct_empresa", "mean"),
            pct_referencia=("pct_referencia", "first"),
            diferencia_pp=("diferencia_pp", "mean"),
            semaforo=("semaforo", "first"),
        )
        .sort_values("diferencia_pp", ascending=False)
    )

    cell_colors = [
        [SEMAFORO_COLOR.get(s, WHITE) for s in b_grouped["semaforo"]],
    ]
    white_cols = [[WHITE] * len(b_grouped)] * 3

    fig = go.Figure(go.Table(
        header=dict(
            values=["<b>Dimensión</b>", "<b>Empresa %</b>", "<b>Colombia %</b>", "<b>Diff pp</b>", "<b>Semáforo</b>"],
            fill_color=NAVY, font=dict(color=WHITE, size=12),
            align="left",
        ),
        cells=dict(
            values=[
                b_grouped["nombre_nivel"],
                [f"{v:.1f}%" for v in b_grouped["pct_empresa"]],
                [f"{v:.1f}%" for v in b_grouped["pct_referencia"]],
                [f"{v:+.1f} pp" for v in b_grouped["diferencia_pp"]],
                b_grouped["semaforo"].str.upper(),
            ],
            fill_color=white_cols + white_cols[:2] + [cell_colors[0]],  # type: ignore
            align="left",
            font=dict(size=11),
        ),
    ))

    fig.update_layout(
        **base_layout(f"S5 — Dimensiones vs Colombia | {empresa}", height=max(400, len(b_grouped) * 28 + 80)),
    )
    return fig


# ═══════════════════════════════════════════════════════════════════════════════
# S6 — Top 20 preguntas clave vs Colombia
# ═══════════════════════════════════════════════════════════════════════════════

def build_s6(top20: pd.DataFrame, dim_pregunta: pd.DataFrame, empresa: str, forma: str) -> go.Figure:
    sub = top20[top20["empresa"] == empresa].copy()
    if forma != "Ambas":
        sub = sub[sub["forma_intra"] == forma]

    sub = sub[sub["top20_flag"] == True].sort_values("diferencia_pp", ascending=False).head(20)

    if sub.empty:
        fig = go.Figure()
        fig.update_layout(**base_layout(f"S6 — Top 20 Preguntas Clave | {empresa} (sin datos)"))
        return fig

    # Enriquecer con texto pregunta
    if "texto_pregunta" in dim_pregunta.columns:
        sub = sub.merge(
            dim_pregunta[["id_pregunta", "texto_pregunta"]].drop_duplicates("id_pregunta"),
            on="id_pregunta", how="left",
        )
        sub["label"] = sub.apply(
            lambda r: f"{r['id_pregunta']}: {str(r.get('texto_pregunta',''))[:50]}...", axis=1
        )
    else:
        sub["label"] = sub["id_pregunta"]

    sub = sub.sort_values("diferencia_pp", ascending=True)

    fig = go.Figure(go.Bar(
        y=sub["label"],
        x=sub["diferencia_pp"],
        orientation="h",
        marker_color=[
            COLORES_RIESGO[4] if d > 10 else COLORES_RIESGO[3] if d > 0 else COLORES_RIESGO[2]
            for d in sub["diferencia_pp"]
        ],
        text=[f"+{d:.1f} pp" for d in sub["diferencia_pp"]],
        textposition="outside",
        customdata=sub[["pct_empresa", "pct_pais_encst", "dimension"]].values,
        hovertemplate=(
            "<b>%{y}</b><br>"
            "Empresa: %{customdata[0]:.1f}%<br>"
            "Colombia: %{customdata[1]:.1f}%<br>"
            "Dimensión: %{customdata[2]}<br>"
            "<extra></extra>"
        ),
    ))

    fig.update_layout(
        **base_layout(f"S6 — Top 20 Preguntas con Mayor Diferencia vs Colombia | {empresa}", height=600),
        xaxis_title="Diferencia en puntos porcentuales (empresa − Colombia)",
        margin=dict(l=350, r=60, t=60, b=60),
    )
    return fig


# ═══════════════════════════════════════════════════════════════════════════════
# S7 — Empresa vs Áreas (Intra + Extra + Estrés) — Heatmap
# ═══════════════════════════════════════════════════════════════════════════════

def build_s7(consolidado: pd.DataFrame, empresa: str, forma: str, n_min: int) -> go.Figure:
    sub = filtrar(consolidado, empresa, forma)
    # Solo instrumentos de riesgo (intra, extra, estrés)
    sub = sub[sub["instrumento"].isin(["IntraA", "IntraB", "Extralaboral", "Estres"])]
    sub = sub[sub["nivel_analisis"] == "dominio"]

    if sub.empty:
        fig = go.Figure()
        fig.update_layout(**base_layout(f"S7 — Áreas vs Dominios | {empresa} (sin datos)"))
        return fig

    # % Alto+MuyAlto por area × dominio
    areas = [a for a in sub["area_departamento"].unique() if a != "Sin área"]
    dominios = sub["nombre_nivel"].unique().tolist()

    z = []
    text_z = []
    for area in areas:
        row_z = []
        row_t = []
        for dom in dominios:
            sub2 = sub[
                (sub["area_departamento"] == area) &
                (sub["nombre_nivel"] == dom)
            ]
            n_total = sub2["cedula"].nunique()
            if n_total < n_min:
                row_z.append(None)
                row_t.append("Confidencial")
            else:
                n_alto = sub2[sub2["nivel_riesgo"] >= 4]["cedula"].nunique()
                pct = round(n_alto / n_total * 100, 1)
                row_z.append(pct)
                row_t.append(f"{pct:.1f}%<br>(n={n_total})")
        z.append(row_z)
        text_z.append(row_t)

    fig = go.Figure(go.Heatmap(
        z=z,
        x=dominios,
        y=areas,
        text=text_z,
        texttemplate="%{text}",
        colorscale=[
            [0.0, COLORES_RIESGO[1]],
            [0.25, COLORES_RIESGO[2]],
            [0.50, COLORES_RIESGO[3]],
            [0.75, COLORES_RIESGO[4]],
            [1.0, COLORES_RIESGO[5]],
        ],
        zmin=0, zmax=100,
        colorbar=dict(title="% Alto+Muy Alto"),
    ))

    fig.update_layout(
        **base_layout(f"S7 — % Riesgo Alto+Muy Alto por Área y Dominio | {empresa}", height=max(400, len(areas) * 35 + 120)),
        xaxis=dict(tickangle=-30),
    )
    return fig


# ═══════════════════════════════════════════════════════════════════════════════
# S8 — Empresa vs Áreas (Individual) — Heatmap
# ═══════════════════════════════════════════════════════════════════════════════

def build_s8(consolidado: pd.DataFrame, empresa: str, forma: str, n_min: int) -> go.Figure:
    sub = filtrar(consolidado, empresa, forma)
    sub = sub[sub["instrumento"] == "Individual"]
    sub = sub[sub["nivel_analisis"] == "dimension"]

    if sub.empty:
        fig = go.Figure()
        fig.update_layout(**base_layout(f"S8 — Áreas vs Dimensiones Individuales | {empresa} (sin datos)"))
        return fig

    areas = [a for a in sub["area_departamento"].unique() if a != "Sin área"]
    dims = sub["nombre_nivel"].unique().tolist()

    z = []
    text_z = []
    for area in areas:
        row_z = []
        row_t = []
        for dim in dims:
            sub2 = sub[
                (sub["area_departamento"] == area) &
                (sub["nombre_nivel"] == dim)
            ]
            n_total = sub2["cedula"].nunique()
            if n_total < n_min:
                row_z.append(None)
                row_t.append("Confidencial")
            else:
                # Individual: nivel 1-2 = vulnerabilidad (bajo/muy bajo)
                n_vuln = sub2[sub2["nivel_riesgo"] <= 2]["cedula"].nunique()
                pct = round(n_vuln / n_total * 100, 1)
                row_z.append(pct)
                row_t.append(f"{pct:.1f}%<br>(n={n_total})")
        z.append(row_z)
        text_z.append(row_t)

    fig = go.Figure(go.Heatmap(
        z=z,
        x=dims,
        y=areas,
        text=text_z,
        texttemplate="%{text}",
        # Invertido: más % vulnerabilidad = más rojo
        colorscale=[
            [0.0, COLORES_PROTECCION[5]],
            [0.5, COLORES_PROTECCION[3]],
            [1.0, COLORES_PROTECCION[1]],
        ],
        zmin=0, zmax=100,
        colorbar=dict(title="% Vulnerabilidad"),
    ))

    fig.update_layout(
        **base_layout(f"S8 — % Vulnerabilidad Individual por Área | {empresa}", height=max(400, len(areas) * 35 + 120)),
        xaxis=dict(tickangle=-30),
    )
    return fig


# ═══════════════════════════════════════════════════════════════════════════════
# Ensamblaje HTML con selector de empresa y forma
# ═══════════════════════════════════════════════════════════════════════════════

def build_selector_html(empresas: list[str]) -> str:
    """Genera el bloque HTML del selector de empresa y forma."""
    opts_empresa = "".join(f'<option value="{e}">{e}</option>' for e in sorted(empresas))
    return f"""
<div id="controles" style="
    background:{NAVY}; padding:16px 24px; display:flex; gap:24px;
    align-items:center; font-family:{FONT_FAMILY}; position:sticky; top:0; z-index:999;
    box-shadow: 0 2px 8px rgba(0,0,0,0.4);">
  <img src="" alt="AVANTUM" style="height:32px; opacity:0.9;" onerror="this.style.display='none'"/>
  <span style="color:{GOLD}; font-size:18px; font-weight:700;">
    MentalPRO — Dashboard V1: Riesgo Psicosocial
  </span>
  <label style="color:{WHITE}; font-size:13px;">Empresa:</label>
  <select id="sel_empresa" onchange="filtrarDash()" style="
    padding:6px 12px; border-radius:6px; border:none;
    background:#1E3A5F; color:{WHITE}; font-size:13px;">
    {opts_empresa}
  </select>
  <label style="color:{WHITE}; font-size:13px;">Forma:</label>
  <select id="sel_forma" onchange="filtrarDash()" style="
    padding:6px 12px; border-radius:6px; border:none;
    background:#1E3A5F; color:{WHITE}; font-size:13px;">
    <option value="Ambas">Ambas</option>
    <option value="A">Forma A</option>
    <option value="B">Forma B</option>
  </select>
  <span style="color:{GRAY}; font-size:11px; margin-left:auto;">
    Fuente: Res. 2764/2022 | ENCST II-III | AVANTUM
  </span>
</div>
"""


SECTION_STYLE = f"""
    background:{WHITE};
    border-radius:12px;
    box-shadow:0 2px 12px rgba(0,0,0,0.08);
    margin:16px;
    padding:8px;
    border-left: 4px solid {GOLD};
"""

SECTION_TITLE_STYLE = f"""
    font-family:{FONT_FAMILY};
    color:{NAVY};
    font-size:14px;
    font-weight:600;
    padding:8px 16px 4px;
    border-bottom:1px solid {GRAY};
    margin-bottom:8px;
"""

FOOTER = f"""
<div style="text-align:center; padding:24px; color:#9CA3AF; font-family:{FONT_FAMILY}; font-size:11px;">
  R8: Grupos con menos de 5 personas no se muestran por confidencialidad. |
  Fuente: Res. 2764/2022 — Ministerio de Salud Colombia | AVANTUM © 2025
</div>
"""


def section_div(sec_id: str, title: str, plot_html: str) -> str:
    return f"""
<div id="{sec_id}" style="{SECTION_STYLE}">
  <div style="{SECTION_TITLE_STYLE}">{title}</div>
  {plot_html}
</div>
"""


def fig_to_html(fig: go.Figure) -> str:
    return fig.to_html(full_html=False, include_plotlyjs=False, config={"responsive": True})


# ═══════════════════════════════════════════════════════════════════════════════
# Main
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    log.info("=" * 60)
    log.info("DASHBOARD V1 — Riesgo Psicosocial")
    log.info("=" * 60)

    cfg = cargar_config()
    proc = ROOT / cfg["paths"]["processed"]
    n_min = cfg.get("confidencialidad_n_min", 5)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # ── Cargar datos ──────────────────────────────────────────────────────────
    log.info("Cargando parquets...")
    baremo      = pd.read_parquet(proc / "fact_scores_baremo.parquet")
    consolidado = pd.read_parquet(proc / "fact_consolidado.parquet")
    benchmark   = pd.read_parquet(proc / "fact_benchmark.parquet")
    riesgo_emp  = pd.read_parquet(proc / "fact_riesgo_empresa.parquet")
    top20       = pd.read_parquet(proc / "fact_top20_comparables.parquet")
    dim_pregunta = pd.read_parquet(proc / "dim_pregunta.parquet")
    dim_trabajador = pd.read_parquet(proc / "dim_trabajador.parquet")

    empresas = sorted(baremo["empresa"].dropna().unique().tolist())
    log.info("Empresas detectadas: %d — %s", len(empresas), empresas)

    # ── Generar figuras por empresa ───────────────────────────────────────────
    # Para cada empresa y forma, generamos las 8 secciones y las almacenamos
    # en divs que el JS de la página muestra/oculta según el selector.
    forma_default = "Ambas"

    secciones_html = ""
    for empresa in empresas:
        e_id = empresa.replace(" ", "_").replace("/", "_")
        log.info("Generando secciones para: %s", empresa)

        for forma in ["Ambas", "A", "B"]:
            f_id = forma
            bloque_id = f"{e_id}_{f_id}"
            display = "block" if (empresa == empresas[0] and forma == forma_default) else "none"

            figs = {
                "s1": build_s1(baremo, riesgo_emp, empresa, forma, n_min),
                "s2": build_s2(baremo, empresa, forma),
                "s3": build_s3(benchmark, cfg, empresa),
                "s4": build_s4(benchmark, empresa, forma),
                "s5": build_s5(benchmark, empresa, forma),
                "s6": build_s6(top20, dim_pregunta, empresa, forma),
                "s7": build_s7(consolidado, empresa, forma, n_min),
                "s8": build_s8(consolidado, empresa, forma, n_min),
            }

            inner = "".join([
                section_div(f"s1_{bloque_id}", "S1 · Distribución de Riesgo por Factor",        fig_to_html(figs["s1"])),
                section_div(f"s2_{bloque_id}", "S2 · Distribución Puntajes 0-100",              fig_to_html(figs["s2"])),
                section_div(f"s3_{bloque_id}", "S3 · Benchmarking Sector Económico",            fig_to_html(figs["s3"])),
                section_div(f"s4_{bloque_id}", "S4 · Dominios vs Colombia",                     fig_to_html(figs["s4"])),
                section_div(f"s5_{bloque_id}", "S5 · Dimensiones vs Colombia",                  fig_to_html(figs["s5"])),
                section_div(f"s6_{bloque_id}", "S6 · Top 20 Preguntas Clave vs Colombia",       fig_to_html(figs["s6"])),
                section_div(f"s7_{bloque_id}", "S7 · Heatmap Áreas × Dominios (Riesgo)",        fig_to_html(figs["s7"])),
                section_div(f"s8_{bloque_id}", "S8 · Heatmap Áreas × Individual (Vulnerabilidad)", fig_to_html(figs["s8"])),
            ])

            secciones_html += f'<div id="bloque_{bloque_id}" style="display:{display}">{inner}</div>\n'

    # ── Script JS para filtrado ───────────────────────────────────────────────
    empresas_js = str([e.replace(" ", "_").replace("/", "_") for e in empresas])
    js = f"""
<script>
const EMPRESAS = {empresas_js};
function filtrarDash() {{
    const emp = document.getElementById('sel_empresa').value.replace(/ /g,'_').replace(/\\//g,'_');
    const forma = document.getElementById('sel_forma').value;
    EMPRESAS.forEach(function(e) {{
        ['Ambas','A','B'].forEach(function(f) {{
            const el = document.getElementById('bloque_' + e + '_' + f);
            if (el) el.style.display = (e === emp && f === forma) ? 'block' : 'none';
        }});
    }});
}}
</script>
"""

    # ── Ensamblar HTML final ───────────────────────────────────────────────────
    plotlyjs_cdn = '<script src="https://cdn.plot.ly/plotly-latest.min.js"></script>'
    html = f"""<!DOCTYPE html>
<html lang="es">
<head>
  <meta charset="UTF-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1.0"/>
  <title>MentalPRO — Dashboard V1: Riesgo Psicosocial</title>
  {plotlyjs_cdn}
  <style>
    * {{ box-sizing: border-box; margin: 0; padding: 0; }}
    body {{ background: #F0F4F8; font-family: {FONT_FAMILY}; }}
    .dashboard-grid {{ max-width: 3000px; margin: 0 auto; }}
  </style>
</head>
<body>
{build_selector_html(empresas)}
<div class="dashboard-grid">
{secciones_html}
{FOOTER}
</div>
{js}
</body>
</html>
"""

    out_path = OUTPUT_DIR / "dashboard_v1_riesgo.html"
    out_path.write_text(html, encoding="utf-8")
    log.info("Dashboard exportado: %s (%.1f MB)", out_path, out_path.stat().st_size / 1e6)
    log.info("=" * 60)
    log.info("V1 COMPLETO — Abrir en navegador: %s", out_path)
    log.info("=" * 60)


if __name__ == "__main__":
    main()
