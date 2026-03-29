# AGENT.md — MentalPRO · Pipeline de Análisis de Riesgo Psicosocial
> **Instrucciones de agente para IDE** | Versión 2.0 | Res. 2764/2022 MinTrabajo Colombia

---

## 1. PROPÓSITO Y CONTEXTO DEL PROYECTO

Este repositorio contiene el pipeline completo de análisis de datos para la **Batería de Riesgo Psicosocial** (Resolución 2764/2022, Ministerio de Trabajo de Colombia). El sistema procesa respuestas de trabajadores, calcula niveles de riesgo normativo, genera scores de gestión por eje del modelo y alimenta 4 dashboards interactivos + un motor RAG de generación de protocolos de intervención.

### Arquitectura de 3 capas

```
CAPA 1: EVALUACIÓN
  Batería PSR Res. 2764/2022 → Excel (Resultado_mentalPRO.xlsx)
  FactRespuestas: 180,617 filas × 10 columnas
  2 formas del instrumento: A (jefes, 125 preguntas) y B (operativos, 97 preguntas)

CAPA 2: BACKEND Y API (PYTHON)  ← ESTE REPOSITORIO
  ETL → Scoring → Baremos → Gestión → Benchmarking → Parquets
  Stack: Python 3.11+ / pandas / scikit-learn / FastAPI (Para consumir Parquets)

CAPA 3: FRONTEND Y VISUALIZADORES (SISTEMA EXTERNO / NEXT.JS)
  Input: Endpoints JSON expuestos por la Capa 2
  Output: Dashboards Dinámicos, Componentes interactivos, Motor RAG PHVA
```

---

## 2. SCRIPTS PYTHON (orden de ejecución)

```
scripts/01_etl_star_schema.py          → ETL, validación, homologación sector (30+ aliases)
scripts/02a_scoring_bateria.py         → Codificación + inversión + agrupación ítems
scripts/02b_baremos.py                 → Puntaje transformado + clasificación 5 niveles
scripts/03_scoring_gestion.py          → Media ponderada indicadores/líneas/ejes
scripts/04_categorias_gestion.py       → Clasificación categórica gestión (5 niveles)
scripts/05_prioridades_protocolos.py   → Ranking lesividad + KPIs + protocolos por sector
scripts/06_benchmarking.py             → Comparativos vs Colombia + sector (ENCST) + alerta reevaluación
scripts/07_frecuencias_preguntas.py    → Distribución frecuencias + top 20 preguntas críticas
scripts/08_consolidacion.py            → Base consolidada long format + joins demográficos
scripts/09_asis_costos.py              → ASIS demográfico + costo económico ausentismo (6 pasos)
Dashboards/dashboard_v1_riesgo.py      → Visualizador 1 → output/dashboard_v1_riesgo.html
Dashboards/dashboard_v2_gestion.py     → Visualizador 2 → output/dashboard_v2_gestion.html
Dashboards/dashboard_v3_gerencial.py   → Visualizador 3 → output/dashboard_v3_gerencial.html
Dashboards/dashboard_v4_asis.py        → Visualizador 4 → output/dashboard_v4_asis.html
```

---

## 3. REGLAS CRÍTICAS — NUNCA VIOLAR

| Código | Regla |
|--------|-------|
| R1 | PK triple obligatoria: cedula + forma_intra + id_pregunta. Nunca solo cedula. |
| R2 | Baremos diferenciados A/B: IntraA (transf. 492) ≠ IntraB (transf. 388). |
| R3 | No hardcodear rutas: usar rutas relativas o config.yaml. |
| R4 | fact_respuestas es inmutable. Scores en fact_scores_baremo y fact_gestion_scores. |
| R5 | 5 niveles normativos. Etiqueta depende de tipo_baremo: riesgo→"Sin riesgo/Riesgo bajo/…/Riesgo muy alto"; afrontamiento_dim→"Muy inadecuado…Muy adecuado"; capitalpsicologico_dim→"Muy bajo capital psicológico…Muy alto"; individual/proteccion→"Muy bajo…Muy alto". |
| R6 | LEFT JOIN en ausentismo: dim_ausentismo ~17 registros. Preservar todos los trabajadores. |
| R7 | Colores AVANTUM inamovibles: risk_1=#10B981, risk_2=#6EE7B7, risk_3=#F59E0B, risk_4=#F97316, risk_5=#EF4444. |
| R8 | ASIGNAR = empresa real. No filtrar ni excluir. |
| R9 | Sector map: 30+ aliases normalizados en 01_etl_star_schema.py → SECTOR_MAP. Desconocido → 'No clasificado'. |
| R10 | Media ponderada en gestión: NUNCA promedio simple en fact_gestion_scores. |
| R11 | Frontend desacoplado. Backend expone APIs. NO generar gráficos Plotly estáticos en HTML. |
| R12 | Diseño responsivo: Delegado completamente a Tailwind / Next.js en la Capa 3. |

---

## 4. ESTRUCTURA DEL REPOSITORIO

```
mentalPRO/
├── data/
│   ├── raw/                    # Excel originales (no en git)
│   │   ├── Resultado_mentalPRO.xlsx
│   │   └── datasets.xlsx
│   └── processed/              # Parquets generados por los scripts
├── scripts/                    # 01_etl_star_schema.py → 09_asis_costos.py
├── api/                        # Proximos Endpoints FastAPI
├── output/                     # Reservado / Deprecado
├── config/config.yaml
└── docs/                       # Documentación del proyecto
```

---

## 5. REQUIREMENTS.TXT

```
# Pipeline ETL + Scoring (obligatorio)
pandas>=2.1.0
numpy>=1.26.0
openpyxl>=3.1.0
pyarrow>=14.0.0
pyyaml>=6.0.0
plotly>=5.18.0

# Análisis estadístico (opcional, para scripts avanzados)
scipy>=1.12.0
scikit-learn>=1.4.0

# API y Backend Web (Opción 3)
fastapi>=0.108.0
uvicorn>=0.27.0
pydantic>=2.5.0
# NO requerido localmente: dash, react (se manejan en frontend)

---

## 6. ORDEN DE EJECUCIÓN

```bash
# Desde la raíz del proyecto (c:\Users\Avant\Documents\GitHub\Python)
python scripts/01_etl_star_schema.py        # → data/processed/fact_respuestas.parquet + dim_*.parquet
python scripts/02a_scoring_bateria.py       # → data/processed/fact_scores_brutos.parquet
python scripts/02b_baremos.py               # → data/processed/fact_scores_baremo.parquet
python scripts/03_scoring_gestion.py        # → data/processed/fact_gestion_scores.parquet
python scripts/04_categorias_gestion.py     # → agrega columnas a fact_gestion_scores
python scripts/05_prioridades_protocolos.py # → data/processed/fact_prioridades.parquet
python scripts/06_benchmarking.py           # → data/processed/fact_benchmark.parquet + fact_riesgo_empresa.parquet
python scripts/07_frecuencias_preguntas.py  # → data/processed/fact_frecuencias.parquet + fact_top20_comparables.parquet
python scripts/08_consolidacion.py          # → data/processed/fact_consolidado.parquet
python scripts/09_asis_costos.py            # → data/processed/fact_asis.parquet + fact_costo_ausentismo.parquet

# API Endpoints (Reemplazan Dashboards)
# Próximamente: uvicorn api.main:app --reload
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
  niveles_riesgo: [1, 2, 3, 4, 5]   # int; etiqueta depende de tipo_baremo (ver R4)
  etiquetas_riesgo:          ["Sin riesgo", "Riesgo bajo", "Riesgo medio", "Riesgo alto", "Riesgo muy alto"]
  etiquetas_afrontamiento:   ["Muy inadecuado", "Inadecuado", "Algo adecuado", "Adecuado", "Muy adecuado"]
  etiquetas_capitalpsic:     ["Muy bajo capital psicológico", "Bajo capital psicológico", "Medio capital psicológico", "Alto capital psicológico", "Muy alto capital psicológico"]
  etiquetas_individual:      ["Muy bajo", "Bajo", "Medio", "Alto", "Muy alto"]
  redondeo_puntaje: 1        # puntaje_transformado = round(bruto/max*100, 1) SIEMPRE

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