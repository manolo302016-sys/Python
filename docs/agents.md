# AGENT.md — MentalPRO · Pipeline de Análisis de Riesgo Psicosocial
> **Instrucciones de agente para IDE** | Versión 1.5 | Res. 2764/2022 MinTrabajo Colombia

---

## 1. PROPÓSITO Y CONTEXTO DEL PROYECTO

Este repositorio contiene el pipeline completo de análisis de datos para la **Batería de Riesgo Psicosocial** (Resolución 2764/2022, Ministerio de Trabajo de Colombia). El sistema procesa respuestas de trabajadores, calcula niveles de riesgo normativo, genera scores de gestión por eje del modelo y alimenta 4 dashboards interactivos + un motor RAG de generación de protocolos de intervención.

### Arquitectura de 3 capas

```
CAPA 1: EVALUACIÓN
  Batería PSR Res. 2764/2022 → Excel (Resultado_mentalPRO.xlsx)
  FactRespuestas: 180,617 filas × 10 columnas
  2 formas del instrumento: A (jefes, 125 preguntas) y B (operativos, 97 preguntas)

CAPA 2: ANÁLISIS Y VISUALIZACIÓN  ← ESTE REPOSITORIO
  ETL → Scoring → Baremos → Gestión → Benchmarking → 4 Dashboards
  Stack: Python 3.11+ / pandas / plotly / Dash / scikit-learn / XGBoost

CAPA 3: MOTOR RAG (sistema externo)
  Corpus v5: 4,178 chunks / 20 protocolos PROT-01…PROT-20
  Input: JSON_RAG_OUTPUT generado por este pipeline
  Output: Planes de gestión PHVA personalizados por empresa
```

---

## 2. SCRIPTS PYTHON (orden de ejecución)

```
01_etl_star_schema.py          → ETL, validación, homologación sector
02a_scoring_bateria.py         → Codificación + inversión + agrupación ítems
02b_baremos.py                 → Puntaje transformado + clasificación 5 niveles
03_scoring_gestion.py          → Media ponderada indicadores/líneas/ejes
04_categorias_gestion.py       → Clasificación categórica gestión (5 niveles)
05_prioridades_protocolos.py   → Ranking lesividad + KPIs + protocolos por sector
06_benchmarking.py             → Comparativos vs Colombia + sector (ENCST)
07_frecuencias_preguntas.py    → Distribución frecuencias + top 20 preguntas críticas
08_consolidado_demografico.py  → Base consolidada 1 fila/trabajador + 19 KPIs
09_costo_ausentismo.py         → Costo económico ausentismo (6 pasos)
dashboard_v1_riesgo.py         → Visualizador 1: Resultados Riesgo Psicosocial
dashboard_v2_gestion.py        → Visualizador 2: Gestión Salud Mental y Bienestar
dashboard_v3_gerencial.py      → Visualizador 3: Dashboard Gerencial / Ejecutivo
dashboard_v4_asis.py           → Visualizador 4: Perfil Demográfico y Salud (ASIS)
```

---

## 3. REGLAS CRÍTICAS — NUNCA VIOLAR

| Código | Regla |
|--------|-------|
| R1 | PK triple obligatoria: cedula + forma_intra + id_pregunta. Nunca solo cedula. |
| R2 | Baremos diferenciados A/B: IntraA (transf. 492) ≠ IntraB (transf. 388). |
| R3 | No hardcodear rutas: usar rutas relativas o config.yaml. |
| R4 | fact_respuestas es inmutable. Scores en fact_scores_baremo y fact_gestion_scores. |
| R5 | 5 niveles de riesgo normativos: Sin riesgo/Bajo/Medio/Alto/Muy alto. No reducir. |
| R6 | LEFT JOIN en ausentismo: dim_ausentismo ~17 registros. Preservar todos los trabajadores. |
| R7 | Colores AVANTUM inamovibles: risk_1=#10B981, risk_2=#6EE7B7, risk_3=#F59E0B, risk_4=#F97316, risk_5=#EF4444. |
| R8 | ASIGNAR = empresa real. No filtrar ni excluir. |
| R9 | Sector map: Comercio → Comercio/financiero (único que cambia). |
| R10 | Media ponderada en gestión: NUNCA promedio simple en fact_gestion_scores. |

---

## 4. ESTRUCTURA DEL REPOSITORIO

```
mentalPRO/
├── data/
│   ├── raw/                    # Excel originales (no en git)
│   │   ├── Resultado_mentalPRO.xlsx
│   │   └── datasets.xlsx
│   ├── processed/              # Parquets generados
│   └── reference/              # Benchmarks Colombia/sector
├── scripts/                    # 01_etl → 09_costo
├── dashboards/
│   ├── app.py
│   ├── pages/                  # riesgo, gestion, gerencial, asis
│   ├── components/             # sidebar, topbar, kpi_card, table_styles
│   ├── data/loader.py
│   └── assets/theme.py + styles.css
├── models/                     # .pkl serializados
├── reports/                    # PDF e informes
├── config/config.yaml
└── docs/                       # Estos archivos .md
```

---

## 5. REQUIREMENTS.TXT

```
pandas>=2.1.0
numpy>=1.26.0
openpyxl>=3.1.0
plotly>=5.18.0
dash>=2.16.0
dash-bootstrap-components>=1.5.0
dash-iconify>=0.1.6
scikit-learn>=1.4.0
xgboost>=2.0.0
lightgbm>=4.2.0
shap>=0.44.0
scipy>=1.12.0
statsmodels>=0.14.0
imbalanced-learn>=0.12.0
prince>=0.13.0
optuna>=3.5.0
fastapi>=0.108.0
uvicorn>=0.25.0
sqlalchemy>=2.0.0
jinja2>=3.1.0
weasyprint>=61.0
pyarrow>=14.0.0
pydantic>=2.5.0
```

---

## 6. ORDEN DE EJECUCIÓN

```bash
python scripts/01_etl_star_schema.py
python scripts/02a_scoring_bateria.py
python scripts/02b_baremos.py
python scripts/03_scoring_gestion.py
python scripts/04_categorias_gestion.py
python scripts/05_prioridades_protocolos.py
python scripts/06_benchmarking.py
python scripts/07_frecuencias_preguntas.py
python scripts/08_consolidado_demografico.py
python scripts/09_costo_ausentismo.py
python dashboards/app.py
# Abrir: http://localhost:8050/riesgo
```

---

## 7. CONFIG/CONFIG.YAML

```yaml
paths:
  raw_fact: "data/raw/Resultado_mentalPRO.xlsx"
  raw_dim:  "data/raw/datasets.xlsx"
  processed: "data/processed/"
  models:   "models/"
  reports:  "reports/"

empresa:
  smlv_mensual: 2800000
  presentismo_factor: 1.40
  costo_empleador_pct: 0.60
  psicosocial_pct: 0.30

scoring:
  formas_validas: ["A", "B"]
  niveles_riesgo: ["Sin riesgo", "Bajo", "Medio", "Alto", "Muy alto"]

rag:
  corpus_version: "v5.0"
  chunks_por_protocolo: 15
  umbral_critico_pct: 40
  umbral_prioritario_pct: 20
```

---

## 8. CONVENCIONES DE CÓDIGO

- Español para variables de dominio (sector_rag, nivel_riesgo), inglés para técnicas (DataFrame, merge)
- logging estándar Python: INFO=progreso, WARNING=anomalías, ERROR=fallos críticos
- Outputs siempre en .parquet (no CSV) para preservar tipos de datos
- Funciones máximo 40 líneas con docstrings en español
- Cada script debe tener al menos una función validar_*() → (bool, pd.DataFrame)