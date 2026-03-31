# Visualizador 2: Gestion de Salud Mental — Especificacion para Frontend

> Documento de referencia para el equipo de frontend (Next.js/React).
> Describe las secciones, KPIs, metricas, narrativa y fuentes de datos del dashboard V2.
> Marco regulatorio: Resolucion 2764/2022 — Ministerio de Trabajo, Colombia.
> Modelo conceptual propio de gestion psicosocial (Avantum).

---

## 1. Diferencia conceptual V1 vs V2

| Aspecto | V1 (Riesgo Psicosocial) | V2 (Gestion Salud Mental) |
|---------|------------------------|--------------------------|
| Enfoque | Diagnostico de riesgo | Gestion y plan de accion |
| Jerarquia | Pregunta -> Dimension -> Dominio -> Factor | Pregunta -> Indicador -> Linea de gestion -> Eje de gestion |
| Escala | 0-100 (puntaje transformado) | 0-1 (score ponderado normalizado) |
| Niveles | 5 niveles de riesgo (sin riesgo a muy alto) | 5 niveles de gestion (prorrogable a urgente) |
| Orientacion | A mayor score = mayor riesgo | A mayor score = mejor gestion |
| Referente | ENCST (pais/sector) | Protocolos Res. 2764 por sector |
| Narrativa | "X% tiene riesgo Alto+MuyAlto" | "X% requiere intervencion correctiva o urgente" |

---

## 2. Arquitectura de datos V2

### Parquets generados por el ETL V2

| Archivo | Contenido | Columnas clave |
|---------|-----------|---------------|
| `fact_gestion_indicadores.parquet` | Score ponderado por trabajador x indicador | cedula, nombre_trabajador, empresa, sector_economico, forma_intra, indicador, linea_gestion, eje_gestion, score_indicador, score_ind_inv |
| `fact_gestion_lineas.parquet` | Score ponderado por trabajador x linea de gestion | cedula, empresa, linea_gestion, eje_gestion, score_linea |
| `fact_gestion_ejes.parquet` | Score ponderado por trabajador x eje de gestion | cedula, empresa, eje_gestion, score_eje |
| `fact_gestion_04_niveles_indicadores.parquet` | Score + nivel de gestion por indicador | cedula, empresa, indicador, score_ind_inv, nivel_gestion, etiqueta_gestion, orden_nivel |
| `fact_gestion_04_niveles_lineas.parquet` | Score + nivel de gestion por linea | cedula, empresa, linea_gestion, score_linea, nivel_gestion, orden_nivel |
| `fact_gestion_04_niveles_ejes.parquet` | Score + nivel de gestion por eje | cedula, empresa, eje_gestion, score_eje, nivel_gestion, orden_nivel |
| `fact_gestion_04_resumen_empresa_ejes.parquet` | Resumen empresa x eje: score stats + % por nivel | empresa, eje_gestion, n_trabajadores, score_mean, nivel_predominante |
| `fact_gestion_04_resumen_empresa_lineas.parquet` | Resumen empresa x linea: score stats + % por nivel | empresa, linea_gestion, n_trabajadores, score_mean, nivel_predominante |
| `dim_protocolos_lineas.parquet` | Tabla de relacion linea <-> protocolo con metadatos | linea_gestion, protocolo_id, protocolo_nombre, objetivo, resultado_esperado |
| `fact_gestion_05_prioridades_protocolos.parquet` | Ranking de protocolos por empresa: urgencia + sector | empresa, protocolo_id, protocolo_nombre, pct_critico_max, rango_sector, score_prioridad, rango_empresa, es_prioritario_sector |
| `fact_gestion_06_vigilancia_resumen.parquet` | Resumen VEP: empresa x indicador con n_casos y % | empresa, vig_id, indicador, fuente, n_total, n_casos, pct_casos, soporte_legal, enfoque |
| `fact_gestion_06_vigilancia_trabajadores.parquet` | Listado nominativo: trabajador x criterios cumplidos | cedula, nombre_trabajador, empresa, area_departamento, categoria_cargo, criterios_cumplidos, n_criterios |
| `fact_gestion_06_vigilancia_ranking.parquet` | Ranking de trabajadores por n_criterios DESC | empresa, cedula, nombre_trabajador, n_criterios, criterios_cumplidos |

### Parquets reutilizados del V1

| Archivo | Uso en V2 |
|---------|-----------|
| `dim_trabajador.parquet` | Header (empresa, sector), filtros (area, cargo, jefe) |
| `dim_demografia.parquet` | Filtros (edad, antiguedad), fecha evaluacion |
| `categorias_analisis.parquet` | Metadata de agrupacion pregunta->indicador->linea->eje->protocolo |

---

## 3. Secciones del Dashboard V2

### S0 — Header (igual V1)

| Campo | Valor | Fuente |
|-------|-------|--------|
| Empresa | Nombre | `dim_trabajador.empresa` |
| Sector | Sector economico | `dim_trabajador.sector_economico` |
| Total evaluados | N trabajadores | `dim_trabajador.cedula.nunique()` |
| % Cobertura | Evaluados / planta | Calculado |
| Fecha evaluacion | Fecha de aplicacion | `dim_demografia.fecha_aplicacion` |

### S1 — Filtros de segmentacion (igual V1)

| Filtro | Columna | Tabla |
|--------|---------|-------|
| Area/Departamento | `area_departamento` | dim_trabajador |
| Categoria de cargo | `categoria_cargo` | dim_trabajador |
| Modalidad de trabajo | `modalidad_de_trabajo` | dim_trabajador |
| Nombre del jefe | `nombre_jefe` | dim_trabajador |
| Edad cumplida | `edad_cumplida` | dim_demografia |
| Antiguedad empresa | `antiguedad_empresa` | dim_demografia |

**R8:** Grupos < 5 personas = "Confidencial".

### S2 — Tarjetas KPI de gestion (6 tarjetas)

#### KPI-G1: Indice Global de Gestion Psicosocial (IGGP)

```
+--------------------------------------------+
|  INDICE GLOBAL DE GESTION PSICOSOCIAL      |
|  ----------------------------------------- |
|           0.62                              |
|      Gestion de mejora selectiva            |
|  [============================-------]      |
|  <-urgente  correct  mejora  prev  prorr->  |
|                                             |
|  Promedio ponderado de los 3 ejes           |
+--------------------------------------------+
```

| Dato | Calculo | Fuente |
|------|---------|--------|
| Score global | Promedio ponderado de los 3 ejes: Bienestar (0.45) + Condiciones (0.35) + Entorno (0.20) | `fact_gestion_categorias` donde nivel_calculo=eje, promedio por empresa |
| Nivel | Aplicar puntos de corte (ver seccion 5) | Calculado |
| Barra de progreso | Visualizar score en escala 0-1 con colores por zona | UI |

**Semaforo:** Verde (>0.79), Amarillo (0.45-0.79), Rojo (<=0.45)

#### KPI-G2: Distribucion de trabajadores por nivel de gestion

```
+--------------------------------------------+
|  DISTRIBUCION NIVEL DE GESTION             |
|  ----------------------------------------- |
|  Prorrogable     ============ 32%          |
|  Preventiva      ========    22%           |
|  Mejora select.  ==========  28%           |
|  Correctiva      ====        12%           |
|  Urgente         ==           6%           |
|                                            |
|  ! 18% requiere intervencion              |
+--------------------------------------------+
```

| Dato | Calculo | Fuente |
|------|---------|--------|
| % por nivel | Contar trabajadores por nivel_gestion / total * 100 | `fact_gestion_categorias` donde nivel_calculo=eje (promedio global del trabajador) |
| Alerta | Si % correctiva+urgente > 20% -> alerta roja | Calculado |

#### KPI-G3: Score por eje de gestion

```
+--------------------------------------------+
|  EJES DE GESTION                           |
|  ----------------------------------------- |
|                                            |
|  Bienestar biopsicosocial      0.58  *--   |
|  Condiciones trabajo saludable 0.65  --*-  |
|  Entorno y clima               0.71  ---*  |
|                                            |
|  * Score empresa  (escala 0-1)             |
|  Linea de corte mejora selectiva: 0.65     |
+--------------------------------------------+
```

| Dato | Calculo | Fuente |
|------|---------|--------|
| Score por eje | Promedio del score de todos los trabajadores en cada eje | `fact_gestion_categorias` donde nivel_calculo=eje |
| Linea de corte | 0.65 (frontera mejora selectiva / preventiva) | Constante |

**Semaforo por eje:** Mismo sistema de puntos de corte.

#### KPI-G4: Lineas de gestion criticas

```
+--------------------------------------------+
|  LINEAS DE GESTION CRITICAS                |
|  ----------------------------------------- |
|                                            |
|  N lineas en intervencion: 4/19            |
|                                            |
|  [R] Bienestar emocional          0.38    |
|  [R] Carga de trabajo saludable   0.41    |
|  [N] Bienestar fisico             0.44    |
|  [N] Jornadas de trabajo          0.43    |
|                                            |
|  Conteo de lineas con score <= 0.45        |
+--------------------------------------------+
```

| Dato | Calculo | Fuente |
|------|---------|--------|
| N criticas | Conteo de lineas con score medio <= 0.45 | `fact_gestion_categorias` donde nivel_calculo=linea, promedio por linea |
| Lista | Las lineas criticas ordenadas por score ascendente | Calculado |

#### KPI-G5: Protocolos prioritarios a activar

```
+--------------------------------------------+
|  PROTOCOLOS PRIORITARIOS                   |
|  ----------------------------------------- |
|                                            |
|  1. PROT-04 Bienestar emocional    38% [L] |
|  2. PROT-15 Carga de trabajo       35%     |
|  3. PROT-08 Bienestar fisico       28%     |
|                                            |
|  % = trabajadores en interv. correctiva    |
|     + urgente en la linea del protocolo    |
|  [L] = Exigencia legal del sector          |
+--------------------------------------------+
```

| Dato | Calculo | Fuente |
|------|---------|--------|
| Top 3 protocolos | Ordenados por pct_intervencion descendente | `fact_prioridades_protocolos` |
| Marcador legal | Si protocolo esta en lista de exigencia del sector | `fact_prioridades_protocolos.exigencia_legal_sector` |

#### KPI-G6: Comparativa forma A vs B por eje

```
+--------------------------------------------+
|  FORMA A (JEFES) vs B (OPERATIVOS)         |
|  ----------------------------------------- |
|              Forma A    Forma B    Delta    |
|  Bienestar    0.55       0.61    +0.06     |
|  Condiciones  0.62       0.67    +0.05     |
|  Entorno      0.68       0.73    +0.05     |
|                                            |
|  Forma A consistentemente mas bajo          |
+--------------------------------------------+
```

| Dato | Calculo | Fuente |
|------|---------|--------|
| Score A por eje | Promedio score trabajadores forma A en cada eje | `fact_gestion_categorias` filtrado forma_intra=A |
| Score B por eje | Promedio score trabajadores forma B en cada eje | `fact_gestion_categorias` filtrado forma_intra=B |
| Delta | Score B - Score A | Calculado |

#### KPI-G7: Vigilancia Epidemiologica

```
+--------------------------------------------+
|  VIGILANCIA EPIDEMIOLOGICA (PVE)           |
|  ----------------------------------------- |
|  Trabajadores con 1+ criterio: N  (X%)     |
|  ================================           |
|  Criterios mas frecuentes:                 |
|  [rojo]  VIG-10 Bienestar financiero  28%  |
|  [rojo]  VIG-09 Interferencia temps.  22%  |
|  [naranja] VIG-01 Convivencia         15%  |
|                                            |
|  [!] N trabajadores con 3+ criterios       |
+--------------------------------------------+
```

| Dato | Calculo | Fuente |
|------|---------|--------|
| N con 1+ criterio | Conteo de trabajadores con n_criterios >= 1 | `fact_gestion_06_vigilancia_ranking` |
| % con 1+ criterio | N/total_trabajadores * 100 | Calculado |
| Top 3 criterios | Los 3 VIG-ID con mayor pct_casos | `fact_gestion_06_vigilancia_resumen` |
| N con 3+ criterios | Conteo n_criterios >= 3 | `fact_gestion_06_vigilancia_ranking` |

**Semaforo KPI-G7:**

| Umbral | Color | Accion sugerida |
|--------|-------|-----------------|
| < 5% | Verde | Monitoreo regular |
| 5 - 14.9% | Amarillo | Revision periodica |
| 15 - 29.9% | Naranja | Activar valoraciones individuales |
| >= 30% | Rojo | Activar PVE urgente |

**Badge alerta:** Si algun trabajador cumple >= 3 criterios -> mostrar conteo destacado en rojo.

---

### S3 — Vista global por ejes

**Componente 1: 3 Gauges o Donuts (uno por eje)**
- Cada gauge muestra el score medio empresa del eje en escala 0-1
- Color segun nivel de gestion asignado
- Fuente: `fact_gestion_categorias` donde nivel_calculo=eje

**Componente 2: Barras apiladas horizontales**
- Una barra por eje (3 barras)
- Cada barra dividida en 5 segmentos: % trabajadores en cada nivel de gestion
- Colores: prorrogable=#10B981, preventiva=#6EE7B7, mejora=#F59E0B, correctiva=#F97316, urgente=#EF4444
- Fuente: `fact_gestion_categorias` agrupado por eje y nivel_gestion

**Narrativa S3:**
> "Los 3 ejes de gestion muestran el siguiente panorama. El eje de [nombre eje con menor score] presenta el mayor desafio con un score de [X], clasificado como [nivel]."

### S4 — Resultados por lineas de gestion

**Heatmap o tabla semaforizada**: 19 lineas x 5 niveles.
- Filas: 19 lineas de gestion, agrupadas por eje
- Columnas: % trabajadores en cada nivel de gestion
- Celda: coloreada segun nivel predominante
- Fuente: `fact_gestion_categorias` donde nivel_calculo=linea

**Alternativa tabla:**

| Eje | Linea de gestion | Score medio | Nivel | % Correctiva+Urgente |
|-----|-----------------|-------------|-------|---------------------|
| Bienestar | Bienestar emocional | 0.38 | Correctiva | 42% |
| Bienestar | Bienestar fisico | 0.44 | Correctiva | 35% |
| ... | ... | ... | ... | ... |

**Narrativa S4:**
> "Las lineas de gestion con mayor necesidad de intervencion son [top 3]. En estas lineas, [X]% de los trabajadores se encuentran en nivel de intervencion correctiva o urgente."

### S5 — Top 10 indicadores criticos

**Tabla ordenada** de los 10 indicadores con mayor % de trabajadores en intervencion correctiva+urgente.

| # | Indicador | Linea de gestion | Eje | Score medio | % Correctiva+Urgente |
|---|-----------|-----------------|-----|-------------|---------------------|
| 1 | Desgaste emocional | Bienestar emocional | Bienestar | 0.32 | 48% |
| 2 | Somatizacion y fatiga | Bienestar fisico | Bienestar | 0.35 | 45% |
| ... | ... | ... | ... | ... | ... |

- Fuente: `fact_gestion_categorias` donde nivel_calculo=indicador
- Ordenar por pct correctiva+urgente descendente
- Mostrar top 10

**Narrativa S5:**
> "Los indicadores que mas contribuyen al nivel de riesgo son [top 3]. El indicador [X] tiene un [Y]% de trabajadores en nivel urgente."

### S6 — Top 10 indicadores x areas

**Heatmap**: area x indicador (top 10 indicadores criticos de S5).
- Filas: areas de la empresa
- Columnas: top 10 indicadores
- Celdas: score medio del indicador en esa area (escala 0-1, coloreado por nivel)
- **R8:** Areas con < 5 personas = "Confidencial"
- Fuente: `fact_gestion_categorias` cruzado con `dim_trabajador.area_departamento`

**Narrativa S6:**
> "Al segmentar por areas, se observa que [area X] presenta los scores mas bajos en [indicador Y], con un promedio de [Z]."

### S7 — Priorizacion de protocolos

**Tabla de ranking** de protocolos a implementar.

| Ranking | Protocolo | Linea de gestion | % Intervencion | Exigencia legal | Prioridad |
|---------|-----------|-----------------|----------------|-----------------|-----------|
| 1 | PROT-04: Bienestar emocional | Bienestar emocional | 42% | Si | Alta |
| 2 | PROT-15: Gestion carga de trabajo | Carga trabajo saludable | 38% | No | Alta |
| ... | ... | ... | ... | ... | ... |

- Fuente: `fact_prioridades_protocolos`
- Ordenar por: 1) exigencia legal del sector, 2) pct_intervencion descendente
- Marcar con icono de balanza los protocolos de exigencia legal

**Narrativa S7:**
> "Con base en los resultados y la exigencia regulatoria del sector [X], los protocolos a activar prioritariamente son [PROT-XX] y [PROT-YY]. El [Z]% de los trabajadores requiere intervencion en estas lineas."

### S8 — Ficha por protocolo

**Vista de detalle** cuando se selecciona un protocolo de S7.

| Campo | Contenido |
|-------|-----------|
| Protocolo ID | PROT-XX |
| Nombre | Nombre completo del protocolo |
| Eje de gestion | Eje al que pertenece |
| Linea de gestion | Linea asociada |
| Objetivo | Texto descriptivo del objetivo |
| KPI del protocolo | Nombre del KPI de resultado esperado |
| Linea base KPI | Score medio actual de la linea (0-1) |
| Poblacion a intervenir | N y % trabajadores en correctiva+urgente |
| Enfoque de gestion | Segun nivel predominante |

- Fuente: `fact_prioridades_protocolos` + `categorias_analisis` + metadata de protocolos
- Los textos de objetivo y KPI esperado vienen del doc V2 (Paso 4.3)

**Narrativa S8:**
> "El protocolo [nombre] tiene como objetivo [objetivo]. Actualmente, [N] trabajadores ([X]%) se encuentran en nivel de intervencion. La implementacion de este protocolo busca mejorar el indicador [KPI] desde su linea base actual de [score]."

### S9 — Vigilancia Epidemiologica (PVE)

**Proposito:** Identificar trabajadores que cumplen criterios de caso sospechoso para el Programa de Vigilancia Epidemiologica Psicosocial.

#### Componente 1: Tabla resumen VEP (11 indicadores)

| Columna | Contenido |
|---------|-----------|
| VIG-ID | Identificador del indicador (VIG-01 a VIG-11) |
| Indicador | Nombre del indicador de vigilancia |
| Fuente | Instrumento de origen (intralaboral, estres, extralaboral, ausentismo) |
| Definicion | Descripcion del caso probable |
| N casos | Numero de trabajadores que cumplen el criterio sospechoso |
| % casos | Porcentaje sobre total evaluados |
| Semaforo | Verde/Amarillo/Naranja/Rojo segun umbrales |
| Enfoque | Cuidado de la salud / Promocion del bienestar |
| Soporte legal | Normativa aplicable |

- Fuente: `fact_gestion_06_vigilancia_resumen`
- R8 Confidencialidad: si n_total < 5 para algun grupo -> no mostrar dato individual

**Grafico de barras horizontales:** % casos por indicador, coloreado por enfoque (azul = cuidado salud, verde = promocion bienestar).

#### Componente 2: Listado nominativo

Tabla paginada y filtrable de trabajadores con criterios cumplidos.

| Columna | Contenido |
|---------|-----------|
| Cedula | Identificador del trabajador (visible solo con permiso) |
| Nombre | Nombre completo |
| Area | Area/departamento |
| Cargo | Categoria de cargo |
| Criterios cumplidos | Lista de VIG-IDs (ej. VIG-01\|VIG-06\|VIG-10) |
| N criterios | Numero total de criterios cumplidos |

- Ordenado por n_criterios DESC (mayor urgencia primero)
- Fuente: `fact_gestion_06_vigilancia_trabajadores`
- **R8:** Solo visible para roles con permiso (RR.HH., medico laboral); en vistas publicas mostrar unicamente conteos anonimizados

#### Narrativa S9

> "El [X]% de los trabajadores cumple al menos un criterio de caso sospechoso de vigilancia epidemiologica. Los indicadores con mayor prevalencia son [top 3]. Se identificaron [N] trabajadores con 3 o mas criterios simultaneos, quienes requieren valoracion psicosocial individual prioritaria."

**Transicion S8 -> S9:**
> "Ademas del plan de accion por protocolos, el modelo identifica trabajadores con criterios de vigilancia epidemiologica que requieren atencion individualizada..."

**Llamado a accion:**
> "Los [N] trabajadores listados deben ser priorizados para: (1) valoracion psicosocial individual, (2) derivacion al medico laboral si criterios VIG-03/VIG-04, (3) reporte al comite de convivencia si criterio VIG-01."

**Audiencias diferenciadas:**

| Audiencia | Indicadores clave | Accion |
|-----------|-----------------|--------|
| RR.HH. | Todos - seguimiento y gestion | Listado nominativo completo |
| Medico laboral | VIG-03, VIG-04, multi-criterio (>=3) | Derivacion a valoracion medica |
| Comite de convivencia | VIG-01 (Convivencia y respeto) | Apertura de caso CCL si reiterado |

---

## 4. Narrativa y storytelling

### Arco narrativo del dashboard

```
1. HOOK (S2 — KPI-G1)
   "El Indice Global de Gestion Psicosocial de [Empresa] es de 0.XX,
    clasificado como [nivel]. X% de los trabajadores requiere
    intervencion correctiva o urgente."

2. CONTEXTO (S3)
   "Los 3 ejes de gestion muestran el siguiente panorama..."
   -> Gauges + barras apiladas

3. PROFUNDIZACION (S4-S5)
   "Las lineas de gestion con mayor necesidad de intervencion son..."
   -> Heatmap lineas + Top 10 indicadores

4. SEGMENTACION (S6)
   "Al segmentar por areas, se observa que..."
   -> Heatmap area x indicador

5. PLAN DE ACCION (S7-S8)
   "Los protocolos prioritarios para [Empresa] (sector [X]) son..."
   -> Ranking + fichas por protocolo

6. VIGILANCIA EPIDEMIOLOGICA (S9)
   "Ademas del plan organizacional, se identificaron [N] trabajadores
    con criterios de caso sospechoso que requieren atencion individual."
   -> Tabla VEP + listado nominativo (R8)

7. LLAMADO A LA ACCION INTEGRADO
   "Implementar [PROT-XX] para la organizacion + derivar [N] trabajadores
    con >=3 criterios para valoracion psicosocial individual."
```

### Transiciones entre secciones

| Transicion | Frase |
|-----------|-------|
| S2 -> S3 | "Para entender este indice global, veamos como se descompone en los 3 ejes de gestion del modelo..." |
| S3 -> S4 | "Dentro de cada eje, las 19 lineas de gestion revelan donde estan los focos especificos de intervencion..." |
| S4 -> S5 | "Los indicadores que mas contribuyen al nivel critico en estas lineas son..." |
| S5 -> S6 | "Al cruzar estos indicadores con las areas de la empresa, se identifican los grupos poblacionales prioritarios..." |
| S6 -> S7 | "Con base en estos resultados y la exigencia regulatoria del sector, los protocolos a activar son..." |
| S7 -> S8 | "Cada protocolo tiene un KPI de linea base medible que permitira monitorear la efectividad de la intervencion..." |
| S8 -> S9 | "Ademas del plan organizacional por protocolos, el modelo identifica trabajadores con criterios de vigilancia epidemiologica que requieren atencion individualizada..." |

### Interpretaciones por nivel de gestion

| Nivel | Etiqueta | Enfoque | Frase narrativa | Temporalidad |
|-------|----------|---------|-----------------|-------------|
| Gestion prorrogable | Promocion | Reforzar factores protectores | "Fortaleza organizacional — mantener y potenciar" | Sin urgencia |
| Gestion preventiva | Educacion y prevencion | Formar en autocuidado | "Oportunidad de prevencion — actuar antes de que escale" | Proximo ciclo |
| Mejora selectiva | Ajuste y mejora | Intervenir focos especificos | "Focos de atencion — corregir selectivamente" | 12 meses |
| Intervencion correctiva | Control correctivo | Protocolos estructurados, articular con SVE | "Riesgo activo — intervenir con protocolo" | 6-12 meses |
| Intervencion urgente | Intervencion urgente | Implementar en 6 meses, conexion al PVE | "Prioridad inmediata — activar dentro de 6 meses" | 0-6 meses |

---

## 5. Puntos de corte (niveles de gestion)

Aplican uniformemente a ejes y lineas de gestion.

| Nivel | Rango score | Color | Hex |
|-------|-------------|-------|-----|
| Gestion prorrogable | > 0.79 | Verde | `#10B981` |
| Gestion preventiva | 0.65 < score <= 0.79 | Verde claro | `#6EE7B7` |
| Mejora selectiva | 0.45 < score <= 0.65 | Amarillo | `#F59E0B` |
| Intervencion correctiva | 0.29 < score <= 0.45 | Naranja | `#F97316` |
| Intervencion urgente | <= 0.29 | Rojo | `#EF4444` |

**Escala invertida:** A mayor score = mejor gestion. La paleta va de verde (bien) a rojo (mal), igual que V1.

---

## 6. Paleta de colores (R11 — inamovible)

Identica al V1. Ver `docs/v1_dashboard_spec.md` seccion 3.

```
Gestion: prorrogable=#10B981, preventiva=#6EE7B7, mejora=#F59E0B, correctiva=#F97316, urgente=#EF4444
Brand: navy=#0A1628, gold=#C9952A, cyan=#00C2CB, white=#FFFFFF, gray=#F3F4F6
Font: Inter, Arial, sans-serif
```

---

## 7. Reglas de negocio

- **R8 Confidencialidad:** Grupos < 5 personas = "Confidencial". Aplica a S6 (heatmap area x indicador) y S9 (listado nominativo — solo visible con rol autorizado).
- **Semaforo KPI-G1:** Verde (>0.79), Amarillo (0.45-0.79), Rojo (<=0.45).
- **Semaforo KPI-G7:** Verde (<5%), Amarillo (5-14.9%), Naranja (15-29.9%), Rojo (>=30%).
- **Alerta intervencion:** Si % correctiva+urgente > 20% del total de trabajadores -> alerta roja.
- **Alerta VEP multi-criterio:** Si algun trabajador cumple >= 3 criterios -> badge rojo con conteo en KPI-G7.
- **Exigencia legal:** Los protocolos prioritarios por sector se marcan con icono de balanza/ley.
- **Forma A vs B:** El dashboard debe permitir ver resultados globales (A+B) y discriminados.
- **Ausentismo opcional:** VIG-03 y VIG-04 requieren ausentismo_12meses.parquet. Si no existe, marcar como "Sin datos" en S9.

---

## 8. Estructura de navegacion sugerida

```
[Header S0]
[Filtros S1]
--------------------------------------------------
[KPI-G1]  [KPI-G2]  [KPI-G3]                 <- S2
[KPI-G4]  [KPI-G5]  [KPI-G6]  [KPI-G7]      <- S2
--------------------------------------------------
[Gauges 3 ejes]  [Barras apiladas niveles]    <- S3
--------------------------------------------------
[Heatmap/Tabla 19 lineas x 5 niveles]         <- S4
--------------------------------------------------
[Top 10 indicadores criticos]                  <- S5
--------------------------------------------------
[Heatmap area x top 10 indicadores]            <- S6
--------------------------------------------------
[Ranking protocolos]                            <- S7
--------------------------------------------------
[Ficha protocolo seleccionado]                  <- S8
--------------------------------------------------
[Tabla resumen VEP 11 indicadores]             <- S9
[Listado nominativo trabajadores (R8)]         <- S9
```

Lienzo de desplazamiento vertical. Minimo 3000 x 2000 px. Diseno responsivo.

---

## 9. Protocolos de gestion (referencia)

### Protocolos por eje

| Eje | Linea de gestion | Protocolo ID | Nombre | KPI esperado |
|-----|-----------------|-------------|--------|-------------|
| Bienestar biopsicosocial | Afrontamiento del estres y recursos psicologicos | PROT-01 | Afrontamiento del estres y recursos psicologicos | Indice de Afrontamiento Eficaz del Estres (IAEE) |
| Bienestar biopsicosocial | Bienestar cognitivo | PROT-02 | Bienestar cognitivo | Indice de Bienestar Cognitivo (IBC) |
| Bienestar biopsicosocial | Bienestar emocional y trascendente | PROT-03 | Promocion salud mental positiva | Indice de Bienestar Emocional y Trascendente (IBET) |
| Bienestar biopsicosocial | Bienestar emocional y trascendente | PROT-04 | Intervencion en trastornos | Indice de Bienestar Emocional y Trascendente (IBET) |
| Bienestar biopsicosocial | Bienestar emocional y trascendente | PROT-05 | Crisis, duelo y emergencia | Indice de Bienestar Emocional y Trascendente (IBET) |
| Bienestar biopsicosocial | Bienestar extralaboral | PROT-06 | Bienestar extralaboral | Indice de Bienestar Extralaboral (IBE) |
| Bienestar biopsicosocial | Bienestar financiero | PROT-07 | Bienestar financiero | Indice de Bienestar Financiero del Trabajador (IBFT) |
| Bienestar biopsicosocial | Bienestar fisico | PROT-08 | Bienestar fisico y habitos saludables | Indice de Bienestar Fisico y Habitos Saludables (IBFHS) |
| Bienestar biopsicosocial | Bienestar vida-trabajo | PROT-09 | Conciliacion trabajo-familia | Indice de Conciliacion Vida-Trabajo (ICVT) |
| Bienestar biopsicosocial | Comportamientos seguros | PROT-10 | Prevencion de conductas de riesgo | Tasa de Comportamientos seguros (TCR) |
| Bienestar biopsicosocial | Motivacion laboral | PROT-20 | Gestion del Engagement organizacional | Indice de Motivacion Laboral (IML) |
| Condiciones de trabajo saludable | Arquitectura de roles | PROT-11 | Arquitectura de roles y responsabilidades | Indice de Claridad de Roles (ICRR) |
| Condiciones de trabajo saludable | Aprendizaje y desarrollo | PROT-12 | Estrategia L&D | Indice de Desarrollo de Competencias (IDCA) |
| Condiciones de trabajo saludable | Condiciones emocionales | PROT-13 | Gestion de demandas emocionales | Indice de Gestion Demandas Emocionales (IGDE) |
| Condiciones de trabajo saludable | Condiciones fisicas | PROT-14 | Gestion condiciones fisicas y ergonomia | Indice de Condiciones Fisicas Saludables (ICFS) |
| Condiciones de trabajo saludable | Carga de trabajo | PROT-15 | Gestion de la carga de trabajo | Indice de Carga de Trabajo Saludable (ICTS) |
| Condiciones de trabajo saludable | Jornadas de trabajo | PROT-16 | Gestion de jornadas de trabajo | Indice de Jornadas de Trabajo Saludables (IJTS) |
| Condiciones de trabajo saludable | Cambio organizacional | PROT-17 | Gestion del cambio organizacional | Indice de Gestion del Cambio (IGSCO) |
| Condiciones de trabajo saludable | Engagement organizacional | PROT-20 | Gestion del Engagement organizacional | Indice de Engagement Organizacional (IEO) |
| Entorno y clima | Convivencia y relacionamiento | PROT-18 | Convivencia y prevencion de violencia | Tasa de convivencia y respeto (TCLC) |
| Entorno y clima | Ecosistema de liderazgo | PROT-19 | Ecosistema de liderazgo psicosocial | Indice de Liderazgo Psicosocial Positivo (ILIPP) |

### Protocolos prioritarios por sector economico

| Sector | Protocolos en orden de prioridad legal |
|--------|---------------------------------------|
| Salud | PROT-04, PROT-05, PROT-13, PROT-10 |
| Educacion | PROT-18, PROT-13, PROT-02, PROT-11 |
| Administracion publica | PROT-17, PROT-11, PROT-19, PROT-20 |
| Comercio/Financiero | PROT-09, PROT-15, PROT-20, PROT-07 |
| Construccion | PROT-08, PROT-14, PROT-16, PROT-10 |
| Servicios | PROT-09, PROT-18, PROT-13, PROT-03 |
| Manufactura | PROT-14, PROT-08, PROT-15, PROT-16 |
| Transporte | PROT-16, PROT-08, PROT-14, PROT-09 |
| Minas/Energia | PROT-08, PROT-14, PROT-10, PROT-05 |
| Agricultura | PROT-08, PROT-06, PROT-07, PROT-16 |
