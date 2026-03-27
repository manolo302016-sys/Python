# skill_psicosocial.md — Routing, Comandos y Reglas del Sistema
> Instrucciones de skill para el pipeline MentalPRO | Versión 1.5 | CRISP-DM + Res. 2764/2022

---

## 1. ROUTING TABLE — Comandos disponibles

| Comando | Script | Descripción | Insumos requeridos |
|---------|--------|-------------|-------------------|
| /etl | 01_etl_star_schema.py | Cargar y validar FactRespuestas | Resultado_mentalPRO.xlsx + datasets.xlsx |
| /scoring | 02a_scoring_bateria.py | Codificar + invertir + agrupar ítems | fact_respuestas_clean |
| /baremos | 02b_baremos.py | Puntaje transformado + 5 niveles | fact_scores_brutos + dim_baremo |
| /gestion | 03_scoring_gestion.py | Score ponderado eje/línea/indicador | fact_scores_brutos |
| /categorias | 04_categorias_gestion.py | Clasificar niveles gestión | fact_gestion_scores |
| /prioridades | 05_prioridades_protocolos.py | Ranking lesividad + protocolos | fact_gestion_scores + sector_rag |
| /benchmark | 06_benchmarking.py | Comparativos vs Colombia + sector | fact_scores_baremo + ref ENCST |
| /frecuencias | 07_frecuencias_preguntas.py | Distribución + Top 20 críticas | fact_scores_brutos |
| /consolidado | 08_consolidado_demografico.py | Base 1 fila/trabajador + 19 KPIs | fact_scores_baremo + dims |
| /costos | 09_costo_ausentismo.py | Costo económico 6 pasos | dim_ausentismo + SMLV |
| /dashboard-v1 | dashboard_v1_riesgo.py | Visualizador 1 interactivo | fact_consolidado |
| /dashboard-v2 | dashboard_v2_gestion.py | Visualizador 2 interactivo | fact_gestion_scores |
| /dashboard-v3 | dashboard_v3_gerencial.py | Visualizador 3 ejecutivo | fact_kpis_gerenciales |
| /dashboard-v4 | dashboard_v4_asis.py | Visualizador 4 ASIS | fact_consolidado + fact_costo |
| /pipeline | Todos los scripts | Ejecutar pipeline completo en orden | Todos los insumos |
| /audit | validar_*() en cada script | Validar integridad de datos | Cualquier tabla |

---

## 2. FLUJO DE TRABAJO PRINCIPAL

### Paso 1 — Validación de insumos
```
Verificar archivos requeridos:
  - Resultado_mentalPRO.xlsx → hojas: FactRespuestas (180,617 filas × 10 cols)
  - datasets.xlsx → hojas: dim_trabajador, dim_pregunta, dim_respuesta,
                            dim_baremo, dim_demografia, dim_ausentismo, categorias_analisis

Columnas FactRespuestas (verificar exactamente):
  cedula | nombre | forma_intra | empresa | sector_economico |
  pct_ausentismo | dias_ausentismo | accidente_laboral | id_pregunta | id_respuesta

Valores únicos sector_economico (6 valores esperados):
  Agricultura | Comercio | Construcción | Manufactura | Servicios | Transporte

Empresas activas (11 empresas, incluyendo ASIGNAR y EVENTUAL):
  NO excluir ninguna empresa. ASIGNAR es nombre de empresa real.

Formas válidas: A (jefes, 125 preguntas) | B (operativos, 97 preguntas)
```

### Paso 2 — Homologación sector
```
Standardizar sector_economico → sector_rag:
  'Agricultura'  → 'Agricultura'
  'Comercio'     → 'Comercio/financiero'   # único que cambia
  'Construcción' → 'Construcción'
  'Manufactura'  → 'Manufactura'
  'Servicios'    → 'Servicios'
  'Transporte'   → 'Transporte'

Si hay valores diferentes a los 6 esperados:
  → logging.WARNING con lista de valores no mapeados
  → sector_rag = 'No clasificado' para esos registros
```

### Paso 3 — Inversión de ítems (2 niveles)
```
NIVEL 1 (02a_scoring_bateria.py — V1-Paso2):
  Recodificación directa: para ítems en las listas de invertidos
  Siempre=4→0 | Casi siempre=3→1 | Algunas veces=2→2 | Casi nunca=1→3 | Nunca=0→4
  Listas documentadas en AGENT.md y pipeline.md

NIVEL 2 (03_scoring_gestion.py — V2-Paso3):
  Inversión de indicadores de riesgo: score = 1 - score_0a1
  Aplicar ANTES de calcular score de la línea de gestión
  Indicadores a invertir: Autonegación, Evitación cognitiva, Evitación conductual,
    Accesibilidad entorno, Apoyo social, Condiciones vivienda,
    Alteraciones cognitivas, Desgaste emocional, Pérdida de sentido
```

### Paso 4 — Clasificación de riesgo (5 niveles normativos)
```
NUNCA reducir a 3 niveles. Los 5 niveles son obligatorios (Res. 2764/2022):
  1 Sin riesgo    — color: #10B981 (risk_1)
  2 Bajo          — color: #6EE7B7 (risk_2)
  3 Medio         — color: #F59E0B (risk_3)
  4 Alto          — color: #F97316 (risk_4)
  5 Muy alto      — color: #EF4444 (risk_5)

Re-evaluación obligatoria si factor intralaboral A o B = Alto o Muy alto.
```

### Paso 5 — Confidencialidad
```
Regla: grupos / áreas con < 5 trabajadores NO reportar individualmente
Mostrar: 'No se muestra por confidencialidad'
Aplica en: fact_consolidado, fact_kpis_gerenciales, layout V1/V2/V3/V4

Líderes con < 5 personas a cargo:
  NO incluir en análisis individual de 'Ecosistema liderazgo'
```

---

## 3. STACK TÉCNICO

```
Python 3.11+
pandas 2.1+              # ETL, transformaciones
numpy 1.26+              # cálculos numéricos
openpyxl 3.1+            # lectura Excel
plotly 5.18+             # visualizaciones
dash 2.16+               # dashboards interactivos
dash-bootstrap-components 1.5+
scikit-learn 1.4+        # KMeans, PCA
xgboost 2.0+             # modelos predictivos
lightgbm 4.2+            # gradient boosting alternativo
shap 0.44+               # interpretabilidad ML
scipy 1.12+              # estadísticas
statsmodels 0.14+        # regresión, ANOVA
umap-learn               # reducción dimensional
prince 0.13+             # MCA (variables categóricas)
pyarrow 14+              # formato parquet
fastapi 0.108+           # API para RAG endpoint
jinja2 3.1+              # templates reportes PDF
weasyprint 61+           # render PDF
pydantic 2.5+            # validación de datos
```

---

## 4. INSUMOS I1-I12 — ESTADO FINAL

| Código | Insumo | Estado | Ubicación |
|--------|--------|--------|-----------|
| I1 | FactRespuestas (180,617 filas × 10 cols) | RESUELTO | Resultado_mentalPRO.xlsx |
| I2 | Clasificación 5 niveles riesgo | RESUELTO | dim_baremo en datasets.xlsx |
| I3 | Baremos dimensiones/dominios/factor Res.2764 | RESUELTO | dim_baremo en datasets.xlsx |
| I4 | Benchmarks ENCST (sectorial + Colombia) | RESUELTO | dim_benchmarks (referencia) |
| I5 | Modelo gestión AVANTUM (eje/línea/indicador) | RESUELTO | categorias_analisis + V2 |
| I6 | Sector económico en FactRespuestas | RESUELTO | FactRespuestas columna E |
| I7 | Ponderaciones 3 niveles V2-Paso3 | RESUELTO | V2-Paso3 (tablas pesos) |
| I8 | Lista ítems invertidos V1-Paso2 | RESUELTO | V1-Paso2 (73+68+23 items) |
| I9 | Mapa ítem → indicador → línea → eje | RESUELTO | V2-Paso2 (filas 70-381) |
| I10 | Baremos AVANTUM individual | RESUELTO | V1-Paso11/13/15 |
| I11 | 19 KPIs gerenciales V3-Paso1 | RESUELTO | V3-Paso1 |
| I12 | Fórmula costo ausentismo V4-Paso1 | RESUELTO | V4-Paso1 (6 pasos) |

Todos los insumos están resueltos. Pipeline 100% desbloqueado.

---

## 5. DECISIONES CLAVE (D1-D5)

| Código | Decisión | Impacto |
|--------|----------|---------|
| D1 | 5 niveles de riesgo OBLIGATORIOS (nunca 3) | Todos los baremos y dashboards |
| D2 | Sectorial = solo intralaboral total; Colombia = factor+dominios+dimensiones | 06_benchmarking.py |
| D3 | sector_economico YA EXISTE en FactRespuestas columna E, NO hacer JOIN externo | 01_etl_star_schema.py |
| D4 | 2 archivos: Resultado_mentalPRO.xlsx (facts) + datasets.xlsx (dims) | Toda la arquitectura |
| D5 | Costo incluye ×1.40 por presentismo (40% adicional) | 09_costo_ausentismo.py Paso 5 |