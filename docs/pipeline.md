# pipeline.md — Mapa Completo del Pipeline MentalPRO
> Todos los pasos documentados en V1 (21 pasos), V2 (8 pasos), V3 (Paso 1) y V4 (Paso 1) mapeados a sus scripts Python.

---

## MAPA CRUZADO: Script → Pasos de Visualizadores

| Script | Pasos que implementa | Fact table producida |
|--------|---------------------|---------------------|
| 01_etl_star_schema.py | V1-Paso1 (parcial) · Homologación sector · Validación PK | fact_respuestas_clean |
| 02a_scoring_bateria.py | V1-Paso1 (completo) · V1-Paso2 · V1-Paso3 · V1-Paso4 · V1-Paso5 · V1-Paso6 · V1-Paso7 · V1-Paso8 | fact_scores_brutos |
| 02b_baremos.py | V1-Paso9 · V1-Paso10 · V1-Paso11 · V1-Paso12 · V1-Paso13 · V1-Paso14 · V1-Paso14.1 · V1-Paso15 | fact_scores_baremo |
| 03_scoring_gestion.py | V2-Paso1 · V2-Paso2 · V2-Paso3 (3 niveles + inversión) | fact_gestion_scores |
| 04_categorias_gestion.py | V2-Paso4 · V2-Paso5 | fact_gestion_scores (+ columnas) |
| 05_prioridades_protocolos.py | V2-Paso6 · V2-Paso7 | fact_prioridades |
| 06_benchmarking.py | V1-Paso16 · V1-Paso17 · V1-Paso18 · V1-Paso19 | fact_benchmark |
| 07_frecuencias_preguntas.py | V1-Paso20 | fact_frecuencias |
| 08_consolidacion.py | V1-Paso21 · V3-Paso1 (19 KPIs) | fact_consolidado + fact_kpis_gerenciales |
| 09_asis_gerencial.py | V3-Pasos 1-5 · V4-Paso1 (fórmula R10) | fact_v3_kpis_globales · fact_v3_demografia · fact_v3_costos · fact_v3_benchmarking · fact_v3_ranking_areas |
| generar_auditoria_v3.py | Auditoría Excel V3 (consultel + indecol) | output/auditoria_v3_consultel_indecol.xlsx |
| api/routers/v3_gerencial_asis.py | Endpoints FastAPI /v3 (7 endpoints) | JSON · 7 rutas S0-S5 |

---

## VISUALIZADOR 1 — Resultados de Riesgo Psicosocial (21 Pasos)

### 01_etl_star_schema.py
#### V1-Paso1 (parcial) — Carga tabla de codificación texto→número

### 02a_scoring_bateria.py
#### V1-Paso1 — Codificación texto → número (completo)
```
Escala Likert estándar (IntraA, IntraB, Extralaboral, Estrés, Cap.Psicológico):
  Siempre=4 | Casi siempre=3 | Algunas veces=2 | Casi nunca=1 | Nunca=0
Dicotómica (ítem 106 IntraA, ítem 116 IntraA): Sí=1 | No=0
Afrontamiento ítems 5-8: Nunca=1 | A veces=0.7 | Frecuentemente=0.5 | Siempre=0
```

#### V1-Paso2 — Ítems invertidos NIVEL 1 (recodificación directa)
```
Intralaboral_A (73 ítems — invertir Siempre→0, Nunca→4):
  4,5,6,9,12,14,32,34,39,40,41,42,43,44,45,46,47,48,49,50,51,53,54,55,
  56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,72,73,74,75,76,77,78,
  79,81,82,83,84,85,86,87,88,89,90,91,92,93,94,95,96,97,98,99,100,101,
102,103,104,105

Intralaboral_B (68 ítems):
  4,5,6,9,12,14,22,24,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,
  45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,67,68,
  69,70,71,72,73,74,75,76,77,78,79,80,81,82,83,84,85,86,87,88,98

Extralaboral (23 ítems):
  1,4,5,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,25,27,29
```

#### V1-Paso3 — Agrupación IntraA → Dimensión
```
Liderazgo y relaciones:
  Características liderazgo: 63-75 | Relaciones sociales: 76-89
  Retroalimentación: 90-94 | Relación colaboradores: 117-125
Control sobre el trabajo:
  Claridad de rol: 53-59 | Capacitación: 60-62
  Participación cambio: 48-51 | Oportunidades: 39-42 | Control autonomía: 44-46
Demandas del trabajo:
  Ambientales: 1-12 | Emocionales: 107-115 | Cuantitativas: 13,14,15,32,43,47
  Influencia extralaboral: 35-38 | Exigencias resp: 19,22,23,24,25,26
  Carga mental: 16,17,18,20,21 | Consistencia rol: 27,28,29,30,52
  Demandas jornada: 31,33,34
Recompensas:
  Recompensas pertenencia: 95,102-105 | Reconocimiento: 96-101
```

#### V1-Paso4 — Agrupación IntraB → Dimensión
```
Liderazgo y relaciones: 49-61 (lid) | 62-73 (relac) | 74-78 (retro)
Control: 41-45 (claridad) | 46-48 (cap) | 38-40 (particip) | 29-32 (oport) | 34-36 (auto)
Demandas: 1-12 (amb) | 90-98 (emoc) | 13-15 (cuant) | 25-28 (extral)
Recompensas: 85-88 (perten) | 79-84 (reconoc)
```

#### V1-Paso5 — Agrupación Extralaboral → Dimensión
```
Balance vida-laboral: 14-17 | Relaciones familiares: 22,25,27
Comunicación interpersonal: 18,19,20,21,23 | Situación económica: 29,30,31
Características vivienda: 5-13 | Influencia entorno: 24,26,28
Desplazamiento: 1,2,3,4
```

#### V1-Paso6/7/8 — Agrupación Estrés / Afrontamiento / Capital Psicológico
```
Estrés: 1 al 31 (aplica A y B)
Afrontamiento_activo_planificacion: 1-4 | Pasivo_negacion: 5-8 | Activo_soporte: 9-12
Capital_psicologico: Optimismo 1-3 | Esperanza 4-6 | Resiliencia 7-9 | Autoeficacia 9-12
```

---

### 02b_baremos.py

#### V1-Paso9 al V1-Paso15 — Baremos Res.2764 + AVANTUM
```
Proceso: Sumatoria respuestas → puntaje bruto → puntaje transformado (1 decimal) → 5 niveles

REGLA DE REDONDEO: puntaje_transformado = round(bruto / max × 100, 1)
  El redondeo a 1 decimal es OBLIGATORIO para evitar casos borde en las clasificaciones.

Etiquetas por tipo_baremo (columna etiqueta_nivel):
  tipo "riesgo"               → Sin riesgo | Riesgo bajo | Riesgo medio | Riesgo alto | Riesgo muy alto
  tipo "afrontamiento_dim"    → Muy inadecuado | Inadecuado | Algo adecuado | Adecuado | Muy adecuado
  tipo "capitalpsicologico_dim" → Muy bajo capital psicológico | Bajo capital psicológico |
                                  Medio capital psicológico | Alto capital psicológico | Muy alto capital psicológico
  tipo "individual"           → Muy bajo | Bajo | Medio | Alto | Muy alto
  tipo "proteccion"           → Muy bajo | Bajo | Medio | Alto | Muy alto

Pasos 9-10: Dimensiones IntraA / IntraB / Extralaboral
  Forma A: 19 dimensiones (max variable por dimensión, derivado de datos)
  Forma B: 16 dimensiones (sin Consistencia del rol, Exigencias resp., Relación colaboradores)
  Extralaboral A/B: 7 dimensiones con puntos de corte independientes por forma
  tipo_baremo = "riesgo"

Paso 11: Dimensiones Afrontamiento y Capital Psicológico (instrumento Individual)
  Afrontamiento: 3 subdimensiones (max=4), tipo_baremo = "afrontamiento_dim"
  Capital Psicológico: 4 subdimensiones (max=3), tipo_baremo = "capitalpsicologico_dim"

Pasos 12-13: Dominios IntraA / IntraB / Afrontamiento
  IntraA: 4 dominios | IntraB: 4 dominios
  Dominio Afrontamiento (Estrategias de Afrontamiento): media ponderada, max=4, tipo="proteccion"
  Extralaboral NO tiene subdominios (el factor = el único dominio)

Paso 14: Factor IntraA + IntraB + Extralaboral + Estrés
  IntraA: max=492 | cortes: 19.7 / 25.8 / 31.5 / 38.8
  IntraB: max=388 | cortes: 20.6 / 26.0 / 31.2 / 38.7
  Extralaboral A: max=124 | cortes: 11.3 / 16.9 / 22.6 / 29.0
  Extralaboral B: max=124 | cortes: 12.9 / 17.7 / 24.2 / 32.3
  IntraA+Extralaboral: max=616 | cortes: 18.8 / 24.4 / 29.5 / 35.4
  IntraB+Extralaboral: max=512 | cortes: 19.9 / 24.8 / 29.5 / 35.4

Paso 14.1: Factor Estrés — fórmula de 4 promedios ponderados (NO suma simple)
  PP1 = promedio(ítems 1-8)  × 4    (max PP1 = 25.50)
  PP2 = promedio(ítems 9-12) × 3    (max PP2 = 18.00)
  PP3 = promedio(ítems 13-22)× 2    (max PP3 = 12.00)
  PP4 = promedio(ítems 23-31)× 1    (max PP4 = 5.667)
  puntaje_bruto = PP1 + PP2 + PP3 + PP4   (max teórico = 61.16)
  Estrés A: cortes 7.8 / 12.6 / 14.7 / 25.0
  Estrés B: cortes 6.5 / 11.8 / 17.0 / 23.4

Paso 15: Factor Individual — Afrontamiento + Capital Psicológico combinados
  Suma de todos los ítems de afrontamiento (12) + capitalpsicologico (12) por trabajador
  max = 24.0 | cortes A y B: 29.0 / 51.0 / 69.0 / 89.0 | tipo_baremo = "individual"

% vulnerabilidad psicológica = (nivel muy_bajo + bajo) / total evaluados × 100
```

---

### 06_benchmarking.py

#### V1-Paso16 — Riesgo total empresa
```
Proceso:
  1. fact_scores_baremo[nivel_analisis='factor', instrumento in (IntraA, IntraB)]
  2. Promedio puntaje_bruto por empresa × instrumento
  3. puntaje_transformado = round(promedio / max × 100, 1)   ← 1 decimal obligatorio
  4. Clasificar con baremos del Paso 14:
     IntraA: max=492 | cortes 19.7 / 25.8 / 31.5 / 38.8
     IntraB: max=388 | cortes 20.6 / 26.0 / 31.2 / 38.7
  5. nivel_riesgo_empresa >= 4 → debe_reevaluar = True

Output: fact_riesgo_empresa.parquet (1 fila por empresa × instrumento)
```

#### V1-Paso17 — %Alto+MuyAlto IntraA/B vs sector (III ENCST 2021)
```
Proceso:
  % trabajadores con nivel_riesgo >= 4 en factor IntraA (forma A) y IntraB (forma B)
  Comparar empresa_pct vs pct_sector (config.yaml → benchmark_sector)
  diferencia_pp = empresa_pct - pct_sector
  Semáforo: rojo si diferencia_pp > 0 (empresa peor que sector) | verde si <= 0
  Grupos < 5 → semaforo = 'insuficiente' (R8)

Referencias sectoriales (III ENCST 2021):
  Agricultura: 31.0% | Manufactura: 44.8% | Servicios: 37.2%
  Construcción: 42.2% | Comercio/financiero: 36.4% | Minas y canteras: 42.9%
  Adm. pública: 36.0% | Educación: 40.4% | Salud: 40.3%
  Transporte: 37.2% (usa Servicios — sin dato ENCST)
  No clasificado: 39.7% (promedio general)

NOTA: Benchmarking sectorial SOLO para total intralaboral — NO para dominios/dimensiones
```

#### V1-Paso18 — %Alto+MuyAlto dominios vs Colombia (II+III ENCST 2013-2021)
```
Proceso:
  Dominios IntraA/B: filter nivel_analisis='dominio', instrumento=IntraA/IntraB
    → % nivel_riesgo >= 4 por empresa × forma
  Extralaboral: filter nivel_analisis='factor', instrumento='Extralaboral'
    → % nivel_riesgo >= 4 (aplica A y B combinados)
  Estrés: filter nivel_analisis='factor', instrumento='Estres'
    → % nivel_riesgo >= 4 (aplica A y B combinados)
  Vulnerabilidad: filter nivel_analisis='factor', instrumento='Individual'
    → % nivel_riesgo <= 2 (muy_bajo + bajo = vulnerabilidad psicológica)

Referencias Colombia (II+III ENCST 2013-2021):
  Nombre en datos (nombre_nivel)        Referencia
  "Demandas del trabajo"                43.9%
  "Control sobre el trabajo"            16.9%
  "Liderazgo y relaciones sociales"     13.3%
  "Recompensas"                          3.3%
  "Extralaboral" (factor)               26.3%
  "Estres" (factor)                     32.9%
  "Vulnerabilidad" (Individual nivel<=2) 4.2%
```

#### V1-Paso19 — %Alto+MuyAlto dimensiones vs Colombia (II+III ENCST 2013-2021)
```
Proceso:
  filter nivel_analisis='dimension', calcular por empresa × forma_intra
  → % nivel_riesgo >= 4 vs referencia Colombia
  Semáforo: rojo si diferencia_pp > 0 | verde si <= 0 | insuficiente si n < 5 (R8)

  EXCEPCIÓN: "Claridad de rol" usa referencia diferenciada por forma:
    Forma A: 20.5% | Forma B: 5.8%

11 dimensiones comparables (nombre_nivel exacto en fact_scores_baremo):
  "Demandas de carga mental"                                   58.2%
  "Demandas emocionales"                                       49.4%
  "Demandas cuantitativas"                                     39.2%
  "Características del liderazgo"                              25.9%
  "Control y autonomía sobre el trabajo"                       22.1%
  "Influencia del trabajo sobre el entorno extra"              21.5%  (IntraA/B)
  "Influencia del entorno extralaboral sobre el trabajo"       21.5%  (Extralaboral)
  "Claridad de rol"                                            20.5% A / 5.8% B
  "Oportunidades de desarrollo y uso de habilidad"             18.4%
  "Capacitación"                                               11.0%
  "Relaciones sociales en el trabajo"                          10.1%

Nota: "Supuesto acoso laboral" no presente en este dataset → no incluido
```

---

### 07_frecuencias_preguntas.py

#### V1-Paso20 — Distribución frecuencias + Top 20 preguntas críticas
```
Parte A — Frecuencias generales:
  Por empresa × forma_intra × id_pregunta × opcion_respuesta:
    n_personas = trabajadores únicos que eligieron esa opción
    pct_empresa = n_personas / n_total × 100
    R8: pct=None si n_total < 5

Parte B — Comparables Colombia (39 preguntas clave):
  Discriminado por forma A y B (id_pregunta diferente por forma para ítems intra)
  Fórmula "alta presencia":
    likert        → "siempre" + "casi siempre"
    afrontamiento → "frecuentemente hago eso" + "siempre hago eso"
  diferencia_pp = pct_empresa - pct_pais_encst
  Top 20: top 20 con diferencia_pp > 0 (empresa peor que Colombia), por empresa × forma

Outputs:
  fact_frecuencias.parquet        — frecuencias generales todos los ítems
  fact_top20_comparables.parquet  — 39 preguntas + top20_flag por empresa × forma
```

---

### 08_consolidacion.py

#### V1-Paso21 — Base consolidada + layout V1
```
JOIN: fact_scores_baremo × dim_trabajador × dim_demografia × dim_ausentismo (LEFT)
Resultado: 1 fila por trabajador × todos los scores dimensiones/dominios/factores/ejes

CONFIDENCIALIDAD: áreas/grupos < 5 trabajadores → 'No se muestra por confidencialidad'

Layout V1 (8 secciones):
1. Resultados globales: % en 5 niveles + gauge + 4 KPIs
2. Distribución 0-100: scatter/boxplot por instrumento
3. Benchmarking sector: % alto+muy_alto IntraA+B vs sectores ENCST 2021
4. Benchmarking dominios: % alto+muy_alto por dominio vs Colombia
5. Benchmarking dimensiones: tabla semaforizada empresa vs Colombia
6. Top preguntas críticas: tabla semaforizada empresa vs ENCST
7. Empresa vs áreas (Intra+Extra+Estrés): heatmap área × dimensión
8. Empresa vs áreas (Individual): heatmap área × dimensión individual
```

---

## VISUALIZADOR 2 — Gestión Salud Mental y Bienestar (8 Pasos)

### 03_scoring_gestion.py

#### V2-Paso1 — Estandarización 0-1
```
Score numérico → escala 0 a 1:
  4→1.00 | 3→0.75 | 2→0.50 | 1→0.25 | 0→0.00
  Sí/1→1.00 | No/0→0.00
```

#### V2-Paso2 — Agrupación ítem → Eje / Línea / Indicador
```
Mapeo id_pregunta → (eje_modelo, linea_gestion, indicador)
Discriminado por forma_intra (A / B / A_y_B)
Fuente: V2 filas 70-381
```

#### V2-Paso3 — Calificación ponderada + Inversión Nivel 2
```
NIVEL 1: ítem → indicador
  score_indicador = Σ(score_0a1_item × peso_item) / Σ(pesos)
  Pesos documentados en columna C de V2-Paso3

  INVERSIÓN NIVEL 2 (aplicar ANTES de calcular indicador):
  score = 1 - score_0a1 para indicadores de riesgo:
    Autonegación | Evitación cognitiva | Evitación conductual
    Accesibilidad entorno | Apoyo social | Condiciones vivienda
    Alteraciones cognitivas | Desgaste emocional | Pérdida de sentido

NIVEL 2: indicador → línea
  score_linea = Σ(score_indicador × peso_indicador) / Σ(pesos)

NIVEL 3: línea → eje
  score_eje = Σ(score_linea × peso_linea) / Σ(pesos)
  Afrontamiento×0.15 | Emocional×0.20 | Cognitivo×0.05 | Extralaboral×0.10
```

### 04_categorias_gestion.py

#### V2-Paso4 — Clasificación categórica ejes y líneas
```
score > 0.79  → Gestión prorrogable
score > 0.65  → Gestión preventiva
score > 0.45  → Gestión de mejora selectiva
score > 0.29  → Intervención correctiva
score ≤ 0.29  → Intervención urgente
```

#### V2-Paso5 — Enfoque de gestión (texto interpretativo)
```
Gestión prorrogable  → Promoción: Reforzar y mantener factores protectores
Gestión preventiva   → Educación y prevención: Formación para autocuidado
Mejora selectiva     → Ajuste y mejora: Intervención en indicadores específicos
Intervención correctiva → Control correctivo + SVE psicosocial
Intervención urgente → Intervención urgente ≤6 meses + PVE
```

### 05_prioridades_protocolos.py

#### V2-Paso6 — Jerarquía por lesividad + 20 KPIs
```
Ranking indicadores: Alta / Media / Baja prioridad de impacto
Alta: Autoeficacia | Locus de control | Evitación conductual |
  Desgaste emocional | Pérdida de sentido | Apoyo social |
  Disonancia emocional | Hostilidad y violencia | Riesgos biomecánicos

20 KPIs: (N trabajadores score≥umbral / Total evaluados) × 100
Meta universal: 90%
Líderes: promedio equipo en línea 'Ecosistema liderazgo'
Confidencialidad: líderes con < 5 personas → no reportar individualmente
PROT_ID asignado a cada línea (PROT-01 a PROT-20)
```

#### V2-Paso7 — Prioridades por sector económico
```
Agricultura:         PROT-08, PROT-06, PROT-07, PROT-16
Comercio/financiero: PROT-09, PROT-15, PROT-20, PROT-07
Construcción:        PROT-08, PROT-14, PROT-16, PROT-10
Manufactura:         PROT-14, PROT-08, PROT-15, PROT-16
Servicios:           PROT-09, PROT-18, PROT-13, PROT-03
Transporte:          PROT-16, PROT-08, PROT-14, PROT-09
Salud:               PROT-04, PROT-05, PROT-13, PROT-10
Educación:           PROT-18, PROT-13, PROT-02, PROT-11
Admón. Pública:      PROT-17, PROT-11, PROT-19, PROT-20
Minas/energía:       PROT-08, PROT-14, PROT-10, PROT-05
```

#### V2-Paso8 — Layout Dashboard V2
```
[Filtros globales]
A: KPIs por eje (tarjetas coloreadas)
B: B1 Sunburst/Treemap Eje→Línea→Indicador | B2 Heatmap calificación eje × área
C: C1 Bar horizontal líneas gestión | C2 Sankey Eje→Línea→Calificación
   C3 Waffle/Donut distribución calificaciones + líderes
D: D1 Scatter PCA/UMAP clusters | D2 Radar chart por cluster | D3 Tabla Apriori
E: Gantt/grouped bar por formulación × eje
F: Tabla gestión + protocolos por sector + endpoint RAG
```

---

## VISUALIZADOR 3 — Dashboard Gerencial / Ejecutivo

### 08_consolidacion.py + dashboard_v3_gerencial.py

#### V3-Paso1 — 19 KPIs Gerenciales
```
IAEE  (N score≥70 / Total) × 100        — Meta 90%
IBET  (N nivel≥75 / Total) × 100
IBFT  (N >70 / Total) × 100
IBFHS (N >60 / Total) × 100
IBE   (N ≥65 / Total) × 100
ICVT  (N >70 / Total) × 100
IML   (N >70 / Total) × 100
IBC   (N >50 / Total) × 100
TCR   (N saludables>80 / Total) × 100
IGDE  (N cond.emoc>70 / Total atiende público) × 100
ICTS  (N >70 / Total) × 100
IJTS  (N >60 / Total) × 100
ICRR  (N >70 / Total) × 100
ICFS  (N >80 / Total) × 100
IEO   (N >80 / Total) × 100
IDCA  (N ≥80% / Total) × 100
IGSCO (N >70 / Total) × 100
ILIPP (N líderes>80 / Total líderes) × 100
TCLC  (N >70 / Total) × 100
```

---

## VISUALIZADOR 4 — Perfil Demográfico y Salud (ASIS)

### 09_costo_ausentismo.py + dashboard_v4_asis.py

#### V4-Paso1 — Fórmula Costo Económico (6 pasos)
```
Paso 1: SMLV_anual = 2,800,000 × 12 = 33,600,000 COP
Paso 2: capacidad_anual = n_trabajadores × SMLV_anual
Paso 3: perdida_ausentismo = (pct_ausentismo / 100) × capacidad_anual
Paso 4: perdida_empleador = perdida_ausentismo × 0.60
Paso 5: perdida_productividad = perdida_empleador × 1.40  # PRESENTISMO
Paso 6: perdida_psicosocial = perdida_productividad × 0.30

NOTA: Si salario promedio > SMLV, ajustar en Paso 1.
```

---

## TABLAS FACT PRODUCIDAS

| Tabla | PK | Descripción |
|-------|----|-------------|
| fact_respuestas_clean | cedula + forma_intra + id_pregunta | FactRespuestas validada |
| fact_scores_brutos | cedula + forma_intra + id_pregunta | Score numérico + tipo_escala |
| fact_scores_baremo | cedula + forma_intra + nivel_analisis + nombre_nivel | Score transformado (1 decimal) + nivel_riesgo + etiqueta_nivel |
| fact_riesgo_empresa | empresa + instrumento | Nivel riesgo empresa + debe_reevaluar |
| fact_benchmark | empresa + nivel_analisis + nombre_nivel + forma_intra | empresa_pct + pct_referencia + diferencia_pp + semaforo |
| fact_frecuencias | empresa + forma_intra + id_pregunta + opcion_respuesta | n_personas + pct_empresa |
| fact_top20_comparables | empresa + forma_intra + id_pregunta + ref_idx | pct_empresa + pct_pais_encst + diferencia_pp + top20_flag |
| fact_consolidado | cedula + nivel_analisis + nombre_nivel | fact_scores_baremo enriquecida con dim_trabajador + dim_demografia + dim_ausentismo |
| fact_gestion_scores | cedula + forma_intra + eje + linea | Score 0-1 + nivel_gestion |
| fact_prioridades | empresa + prot_id | Prioridad protocolo |
| fact_kpis_gerenciales | empresa | 19 KPIs calculados |
| fact_costo_ausentismo | empresa | 6 pasos costo económico |