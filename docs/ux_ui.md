# instrucciones_UX_UI.md — Design System AVANTUM
> Reglas de diseño obligatorias para todos los dashboards MentalPRO | Versión 2.0
> **Los dashboards son HTML estático (sin servidor Dash). Ver R19 en reglas_negocio.md.**

---

## REGLAS CRÍTICAS — NUNCA VIOLAR

| Código | Regla |
|--------|-------|
| RC-01 | Registrar `pio.templates['avantum'] = AVANTUM_TEMPLATE` y `pio.templates.default = 'avantum'` al inicio de cada script dashboard, ANTES de crear cualquier figura. |
| RC-02 | Usar `paper_bgcolor='rgba(0,0,0,0)'` en TODAS las figuras. El fondo lo define el CSS del HTML. |
| RC-03 | Los 5 colores risk_1…risk_5 son normativos (Res. 2764). NUNCA cambiarlos. |
| RC-04 | Export: `fig.to_html(full_html=False, include_plotlyjs=False)`. El HTML principal carga Plotly CDN una sola vez. |
| RC-05 | Todo label, título y leyenda: `color='#FFFFFF'`. Sin excepciones. |
| RC-06 | `hovertemplate` obligatorio en cada `go.Trace`. |
| RC-07 | Nunca omitir `xaxis.title` y `yaxis.title`. Accesibilidad mínima. |
| RC-08 | Alturas mínimas: KPI=280px, análisis=400px, heatmaps=520px |
| RC-09 | Canvas total por dashboard: **3000×2000 px**, orientación vertical. Secciones apiladas verticalmente. |
| RC-10 | Filtros interactivos implementados en **JavaScript vanilla** (no Dash callbacks). Un bloque HTML por empresa×forma, JS muestra/oculta según dropdown. |
| RC-11 | Incluir Plotly CDN una sola vez al inicio del HTML: `<script src="https://cdn.plot.ly/plotly-latest.min.js"></script>` |
| RC-12 | Regla R8 de confidencialidad implementada en el script Python ANTES de generar las figuras. Grupos N<5 → celda "Confidencial" en heatmaps, fila omitida en tablas. |

---

## 1. PALETA AVANTUM — TOKENS OBLIGATORIOS

```python
# assets/theme.py — importar en TODOS los módulos

AVANTUM_COLORS = {
    # Primarios
    'navy':      '#0A1628',    # Fondo principal, headers
    'navy_m':    '#0E2244',    # Fondos secundarios, sidebars
    'navy_l':    '#1A3A6B',    # Bordes énfasis
    'gold':      '#C9952A',    # CTAs, highlights, valor
    'gold_l':    '#E8B84B',    # Hover states
    'gold_s':    '#F5D78E',    # Tags, chips
    'cyan':      '#00C2CB',    # IA, tendencias
    'cyan_l':    '#5CE8EE',    # Elementos secundarios
    'teal':      '#00D4AA',    # Métricas crecimiento
    # Soporte
    'white':     '#FFFFFF',
    'g100':      '#F4F5F7',
    'g200':      '#E2E5EA',
    'g400':      '#9AA3B0',
    'g600':      '#5A6478',
    'g800':      '#2D3445',
    # Sistema / Estado
    'ok':        '#10B981',
    'warn':      '#F59E0B',
    'err':       '#EF4444',
    'orange':    '#F97316',
    'ok_light':  '#6EE7B7',
    # NORMATIVOS — 5 niveles riesgo Res. 2764 — INAMOVIBLES
    'risk_1':    '#10B981',    # Sin riesgo / Muy bajo
    'risk_2':    '#6EE7B7',    # Bajo
    'risk_3':    '#F59E0B',    # Medio
    'risk_4':    '#F97316',    # Alto
    'risk_5':    '#EF4444',    # Muy alto
}

RISK_COLORSCALE = [
    [0.00, '#10B981'], [0.25, '#6EE7B7'], [0.50, '#F59E0B'],
    [0.75, '#F97316'], [1.00, '#EF4444'],
]

CATEGORICAL_PALETTE = [
    '#00C2CB','#C9952A','#10B981','#1A3A6B',
    '#F59E0B','#EF4444','#F5D78E','#5CE8EE',
]
```

---

## 2. TIPOGRAFÍA

```python
AVANTUM_FONTS = {
    'display': 'Montserrat, sans-serif',
    'body':    'Inter, sans-serif',
    'mono':    'JetBrains Mono, monospace',
}

FONT_SCALE = {
    'kpi_value':   52, 'kpi_label':   13,
    'chart_title': 17, 'chart_axis':  12,
    'tooltip':     13, 'table_header':12,
    'table_cell':  12, 'badge':       11,
}
```

---

## 3. TEMPLATE PLOTLY

```python
import plotly.graph_objects as go
import plotly.io as pio

AVANTUM_TEMPLATE = go.layout.Template(
    layout=go.Layout(
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(14,34,68,0.4)',
        font=dict(family='Inter, sans-serif', color='#FFFFFF', size=12),
        title=dict(font=dict(family='Montserrat', size=17, color='#FFFFFF'),
                   x=0.0, xanchor='left', pad=dict(l=4)),
        colorway=CATEGORICAL_PALETTE,
        legend=dict(bgcolor='rgba(10,22,40,0.7)',
                    bordercolor='rgba(255,255,255,0.1)',
                    font=dict(size=11, color='rgba(255,255,255,0.75)')),
        xaxis=dict(gridcolor='rgba(255,255,255,0.07)',
                   linecolor='rgba(255,255,255,0.15)',
                   tickfont=dict(size=11, color='rgba(255,255,255,0.6)'),
                   zeroline=False),
        yaxis=dict(gridcolor='rgba(255,255,255,0.07)',
                   linecolor='rgba(255,255,255,0.15)',
                   tickfont=dict(size=11, color='rgba(255,255,255,0.6)'),
                   zeroline=False),
        margin=dict(l=48, r=20, t=40, b=40),
        hoverlabel=dict(bgcolor='#0A1628', bordercolor='#C9952A',
                        font=dict(size=12, color='#FFFFFF')),
    )
)
pio.templates['avantum'] = AVANTUM_TEMPLATE
pio.templates.default = 'avantum'  # RC-01
```

---

## 4. CSS GLOBAL — assets/styles.css

```css
:root {
  --navy: #0A1628; --navy-m: #0E2244; --navy-l: #1A3A6B;
  --gold: #C9952A; --gold-l: #E8B84B; --gold-s: #F5D78E;
  --cyan: #00C2CB; --teal: #00D4AA;
  --g100: #F4F5F7; --g200: #E2E5EA; --g400: #9AA3B0;
  --g600: #5A6478; --g800: #2D3445;
  --ok: #10B981; --warn: #F59E0B; --err: #EF4444;
  --r: 8px; --rl: 12px;
}
body { font-family: 'Inter', sans-serif; background: var(--navy); color: #fff; }
.app-container { display: flex; min-height: 100vh; background: var(--navy); }
.sidebar-nav { width: 240px; background: rgba(14,34,68,0.95);
               border-right: 1px solid rgba(255,255,255,0.07);
               position: fixed; top: 0; left: 0; bottom: 0; z-index: 100; }
.main-content { margin-left: 240px; flex: 1; padding: 24px 32px; }
.kpi-card { background: rgba(14,34,68,0.6);
            border: 1px solid rgba(255,255,255,0.08);
            border-radius: var(--rl); padding: 20px 24px; }
.kpi-value { font-family: 'Montserrat'; font-size: 42px; font-weight: 900; }
.kpi-label { font-size: 12px; font-weight: 600;
             color: rgba(255,255,255,0.5); text-transform: uppercase; }
.chart-card { background: rgba(14,34,68,0.5);
              border: 1px solid rgba(255,255,255,0.07);
              border-radius: var(--rl); padding: 20px; }
.risk-1 { background: rgba(16,185,129,.15); color: #10B981; }
.risk-2 { background: rgba(110,231,183,.12); color: #6EE7B7; }
.risk-3 { background: rgba(245,158,11,.15); color: #F59E0B; }
.risk-4 { background: rgba(249,115,22,.15); color: #F97316; }
.risk-5 { background: rgba(239,68,68,.15); color: #EF4444; }
.grid-2 { display: grid; grid-template-columns: 1fr 1fr; gap: 16px; }
.grid-3 { display: grid; grid-template-columns: 1fr 1fr 1fr; gap: 16px; }
.grid-4 { display: grid; grid-template-columns: repeat(4,1fr); gap: 14px; }
@media (max-width:900px) {
  .sidebar-nav { width: 56px; }
  .main-content { margin-left: 56px; }
  .grid-2,.grid-3,.grid-4 { grid-template-columns: 1fr; }
}
```

---

## 5. CATÁLOGO DE GRÁFICAS POR CASO DE USO

| Variable | Gráfica | Nota |
|---------|---------|------|
| 1 categoría ≤6 valores | go.Pie (donut) | Colores CATEGORICAL_PALETTE |
| 1 categoría >6 valores | go.Bar horizontal | Ordenar por frecuencia desc. |
| Niveles riesgo 1-5 | go.Bar apilada 100% | Colores normativos OBLIGATORIOS |
| Jerarquía multi-nivel | go.Sunburst / go.Treemap | Colores por calificación |
| Flujo eje→línea→calificación | go.Sankey | V2 sección C2 |
| Cross-tabulation | go.Heatmap | Escala color + texto |
| Distribución 1 variable | go.Histogram + KDE | Línea densidad |
| Comparar grupos | go.Box | Mostrar outliers |
| Perfil múltiple dimensiones | go.Scatterpolar (radar) | Máx 8 ejes |
| Correlaciones | go.Heatmap | Escala divergente navy–gold |
| Benchmarking | go.Indicator bullet | Líneas ref. sector/Colombia |
| K-Means/DBSCAN | Scatter UMAP/PCA | umap-learn |
| SHAP values | Bar global + beeswarm | shap adaptado |