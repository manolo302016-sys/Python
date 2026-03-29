# MentalPRO — Instrucciones de desarrollo para Claude

## Skills activos para este proyecto

Cuando trabajes en cualquier archivo de `Dashboards/`, activa estos skills:
- `/kpi-dashboard-design` — diseño de métricas, layout, visualizaciones
- `/data-storytelling` — narrativa ejecutiva, estructura de insights, presentación a stakeholders

## Stack técnico (inamovible)

- Python 3.11+ / pandas / pyarrow / plotly
- **NO Dash, NO FastAPI** — `plotly.io.write_html(full_html=True, include_plotlyjs='cdn')`
- Config: `config/config.yaml`
- Outputs HTML en `output/`

## Paleta R11 — inamovible

```python
COLORES_RIESGO = {1: "#10B981", 2: "#6EE7B7", 3: "#F59E0B", 4: "#F97316", 5: "#EF4444"}
NAVY  = "#0A1628"
GOLD  = "#C9952A"
CYAN  = "#00C2CB"
WHITE = "#FFFFFF"
GRAY  = "#F3F4F6"
FONT  = "Inter, Arial, sans-serif"
```

## Reglas de negocio obligatorias

- **R8 Confidencialidad**: grupos < 5 personas → mostrar "Confidencial" en lugar del dato
- **Lienzo**: desplazamiento vertical, 3000 × 2000 px, UX/UI alta fidelidad
- **Umbrales semáforo KPI**: >35% = rojo | 15–34% = amarillo | <15% = verde
- **Referente sector**: si el sector no tiene dato, usar referente país (ENCST)
- **1 HTML por empresa** — el dashboard es por empresa, no consolidado

---

## Visualizador 1: `dashboard_v1_riesgo.py`

**Archivos fuente** (parquet en `data/processed/`): resultados baremos, benchmarking, frecuencias preguntas, consolidación demográfica.

### Secciones

| # | Nombre | Contenido clave |
|---|--------|-----------------|
| S0 | Header | Empresa · Sector · #evaluados · %cobertura · Fecha evaluación |
| S1 | Filtros | area_departamento · categoria_cargo · modalidad_trabajo · nombre_jefe · edad_cumplida · antigüedad_empresa |
| S2 | KPIs | Ver tarjetas KPI abajo |
| S3 | Resultados globales | Barras apiladas 5 niveles + gauge + alerta reevaluación |
| S4 | Distribución 0–100 | Boxplot scores transformados por instrumento |
| S5 | Benchmarking sector | % Alto+MuyAlto IntraA y B vs sector ENCST |
| S6 | Dominios vs Colombia | Heatmap % riesgo dominios empresa vs referencia |
| S7 | Dimensiones vs Colombia | Tabla semaforizada empresa vs ENCST |
| S8 | Preguntas clave | Top 20 preguntas con mayor diferencia vs Colombia |
| S9 | Empresa vs Áreas (Intra+Extra+Estrés) | Heatmap área × dimensión |
| S10 | Empresa vs Áreas (Individual) | Heatmap área × dimensión individual |

### Tarjetas KPI (S2)

| ID | Nombre | Estructura | Semáforo |
|----|--------|------------|----------|
| KPI-1 | % Riesgo A-MA Intralaboral | 3 filas: Intra A · Intra B · Sector/País | >35% rojo · 15–34% amarillo · <15% verde |
| KPI-2 | Trabajadores A+B con Estrés A+MA | 3 filas: N · % total evaluados · % país | Semaforizar diferencia pp vs país |
| KPI-3 | Vulnerabilidad psicológica | 3 filas: N · % total evaluados · % país | Semaforizar diferencia pp vs país |
| KPI-4 | % Riesgo A-MA Extralaboral | 3 filas: Extra A · Extra B · País | >35% rojo · 15–34% amarillo · <15% verde |
| KPI-5 | Dimensiones críticas | Conteo dimensiones por encima del referente nacional | Valor único |

---

## Visualizador 2: `dashboard_v2_gestion.py` (pendiente)

Secciones: Resultados por eje/línea de gestión · Indicadores de gestión · Top 10 indicadores × áreas

## Visualizador 3: `dashboard_v3_gerencial.py` (pendiente)

Secciones: Ficha técnica · Resultados globales + benchmarking · 3 ejes de gestión · Ranking áreas top 5 · 5 protocolos prioritarios por sector

## Visualizador 4: `dashboard_v4_asis.py` (pendiente)

Secciones: Distribución demográfica · Perfil laboral · Condiciones de salud/costos (ausentismo CIE, prevalencia, días, costos productividad)

---

## Marco regulatorio

Resolución 2764/2022 — Ministerio de Trabajo Colombia
Referentes nacionales: II y III Encuesta Nacional de Condiciones de Salud y Trabajo (ENCST 2013–2021)
