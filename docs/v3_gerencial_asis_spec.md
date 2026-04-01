# Visualizador 3 — Gerencial + ASIS Consolidado
## MentalPRO · Especificación Técnica v1.0

**Resolución 2764/2022 — Ministerio de Trabajo Colombia**
**Audiencia:** Alta dirección · Gerencias · Talento Humano estratégico
**Propósito:** Consolidar diagnóstico de riesgo psicosocial + perfil demográfico/ocupacional + impacto económico del ausentismo para decisiones ejecutivas y retorno de inversión en salud mental.

---

## Stack y dependencias

| Capa | Tecnología |
|------|------------|
| ETL / Cálculos | Python 3.11+ · pandas · pyarrow |
| API | FastAPI · router `/v3` |
| Frontend | Next.js · TailwindCSS · Recharts · Tremor |
| Parquets fuente | Ver tabla de fuentes por sección |

---

## Paleta R11 (inamovible)

```python
COLORES_RIESGO = {1: "#10B981", 2: "#6EE7B7", 3: "#F59E0B", 4: "#F97316", 5: "#EF4444"}
NAVY  = "#0A1628"
GOLD  = "#C9952A"
CYAN  = "#00C2CB"
WHITE = "#FFFFFF"
GRAY  = "#F3F4F6"
FONT  = "Inter, Arial, sans-serif"
```

---

## Reglas de negocio aplicables

| Regla | Descripción |
|-------|-------------|
| R8 | Grupos < 5 personas → enviar "Confidencial" |
| R10 | Fórmula costo económico (6 pasos con presentismo) |
| R11 | Paleta de colores inamovible |
| R17 | Homologación sector (mapeado en config.yaml) |
| R18 | Escalable: N empresas filtradas por `empresa` + `fecha_evaluacion` |

---

## Secciones

### S0 — Identificadores

**Propósito narrativo:** Fijar el contexto evaluativo para que el directivo comprenda el universo analizado.

| Campo | Fuente | Cálculo |
|-------|--------|---------|
| Nombre empresa | dim_trabajador → `empresa` | Valor único |
| Sector económico | config.yaml homologación | Sector canónico |
| N evaluados | fact_consolidado → count(distinct cedula) | Conteo |
| N planta | dim_trabajador → `total_planta` | Campo directo |
| % cobertura | (N evaluados / N planta) × 100 | Calculado |
| Fecha evaluación | fact_consolidado → max(fecha_evaluacion) | Max fecha |
| Instrumento | config → forma_intra disponibles | A / B / A+B |

**Endpoint:** `GET /v3/encabezado?empresa=X&fecha_evaluacion=Y`

---

### S1 — Tarjetas KPI Globales

**Propósito narrativo:** De un vistazo, la dirección ve el volumen de riesgo y la población que requiere acción inmediata.

#### Grupo 1 — Distribución de Riesgo (5 niveles)

6 tarjetas en 2 filas (A y B):

| Tarjeta | Instrumento | Población | Semáforo |
|---------|-------------|-----------|----------|
| KPI-1a | Intralaboral A | Forma A | % Alto+MuyAlto: >35% R · 15-34% A · <15% V |
| KPI-1b | Intralaboral B | Forma B | ídem |
| KPI-2a | Extralaboral A | Forma A | ídem |
| KPI-2b | Extralaboral B | Forma B | ídem |
| KPI-3a | Estrés A | Forma A | ídem |
| KPI-3b | Estrés B | Forma B | ídem |

**Estructura de cada tarjeta (3 filas):**
```
Fila 1: % nivel SIN RIESGO | BAJO | MEDIO | ALTO | MUY ALTO
Fila 2: N trabajadores en cada nivel
Fila 3: Diferencia vs referente sector/país (pp) — semaforizado
```

#### Grupo 2 — Factor Individual (Vulnerabilidad)

| KPI | Nombre | Contenido | Semáforo |
|-----|--------|-----------|----------|
| KPI-4 | Vulnerabilidad psicológica | N trabajadores con factor individual Bajo+MuyBajo · % total evaluados · % referente país | Diferencia pp vs país semaforizado |

#### Grupo 3 — Prioridades de Intervención

| KPI | Nombre | Contenido |
|-----|--------|-----------|
| KPI-5 | Protocolos urgentes e inmediatos | N trabajadores · % evaluados priorizados en nivel Urgente + Inmediato (niveles_gestion V2) |
| KPI-6 | Vigilancia Epidemiológica | N trabajadores · % evaluados que cumplen criterios de casos sospechosos (fact_gestion_06_vigilancia_resumen) |

**Fuentes parquet:**
- `fact_scores_baremo.parquet` → KPI-1a/b, 2a/b, 3a/b, 4
- `fact_gestion_04_niveles_*.parquet` → KPI-5
- `fact_gestion_06_vigilancia_resumen.parquet` → KPI-6
- `fact_benchmark.parquet` → referentes pp

**Endpoint:** `POST /v3/kpis_globales`

---

### S2 — Demografía y Costos de Ausentismo

**Propósito narrativo:** Revelar el perfil de la población expuesta y cuantificar el impacto económico del ausentismo para justificar la inversión en salud mental.

#### 2A — Variables Demográficas

| Gráfico | Variable | Tipo | Descripción |
|---------|----------|------|-------------|
| G1 | Pirámide poblacional | Barras horizontales bipolares | Grupos edad (10 años) × sexo (H/M) |
| G2 | Antigüedad empresa | Barras verticales | Rangos: <1 · 1-3 · 3-5 · 5-10 · >10 años |
| G3 | Antigüedad cargo | Barras verticales | Rangos: <1 · 1-3 · 3-5 · >5 años |
| G4 | Estado civil | Dona | Soltero · Casado · Unión libre · Divorciado · Viudo |
| G5 | Dependientes económicos | Barras | 0 · 1 · 2 · 3 · 4+ |
| G6 | Distribución por área | Barras horizontales (top 10) | area_departamento × N evaluados |
| G7 | Distribución por cargo | Barras horizontales (top 10) | categoria_cargo × N evaluados |
| G8 | Forma intralaboral | Dona | Forma A (jefaturas) vs Forma B (operativos) |

**Rangos de edad:** <25 · 25-34 · 35-44 · 45-54 · ≥55 años

**Fuentes parquet:**
- `dim_demografia.parquet` → edad_cumplida, sexo, antigüedad_empresa, antigüedad_cargo, estado_civil, dependientes
- `dim_trabajador.parquet` → area_departamento, categoria_cargo, forma_intralaboral

**Endpoint:** `POST /v3/demografia`

---

#### 2B — Costos de Ausentismo y ROI Salud Mental

**Propósito narrativo:** Traducir el riesgo psicosocial a pérdida económica concreta y mostrar el retorno potencial de la inversión en intervención.

##### Fórmula R10 — 6 Pasos

| Paso | Variable | Fórmula | Nota |
|------|----------|---------|------|
| 1 | N trabajadores planta | `dim_trabajador.total_planta` | Universo real |
| 2 | % ausentismo actual | `(total_dias_ausencia / (N_planta × 240)) × 100` | Referente: 3% Colombia |
| 3 | Costo salario anual FTE | `SMLV_mensual × 12` = 2,800,000 × 12 | Mínimo legal; si promedio salarial > SMLV, ajustar |
| 4 | Capacidad productiva anual | `N_planta × Costo_FTE_anual` | Base económica |
| 5 | Pérdida económica anual | `%_ausentismo × Capacidad_productiva` | Empleador + SGSS |
| 6 | Pérdida atribuida empleador | `Pérdida_económica × 0.60` | 60% = empleador |

**Resultado final (Paso 7):**
```
Pérdida_productividad = Pérdida_empleador × 1.40
(+40% presentismo según estudios vigentes)
```

**Estimación psicosocial:**
```
Costo_atribuido_psicosocial = Pérdida_productividad × 0.30
(30% del costo total atribuible a riesgo psicosocial)
```

**Visualización:** Tabla de 7 pasos + gráfico de barras comparativo empresa vs promedio Colombia (3%).

**Indicador de semáforo:**
- % ausentismo > 5%: ROJO
- % ausentismo 3-5%: AMARILLO
- % ausentismo < 3%: VERDE

**Fuentes parquet:**
- `dim_ausentismo.parquet` → total_dias_ausencia, N_trabajadores
- `dim_trabajador.parquet` → total_planta
- `config.yaml` → SMLV_mensual, presentismo_factor, costo_empleador, factor_psicosocial

**Endpoint:** `POST /v3/costos_ausentismo`

---

### S3 — Benchmarking Ejecutivo

**Propósito narrativo:** Posicionar a la empresa respecto al mercado laboral colombiano, evidenciar brechas y prioridades normativas del sector.

#### 3A — Riesgo Intralaboral vs Sector/País

| Columna | Descripción |
|---------|-------------|
| Instrumento | IntraA · IntraB |
| % empresa (A+MA) | % trabajadores en riesgo Alto + Muy Alto |
| % referente | Sector (si disponible en ENCST) o País |
| Diferencia pp | empresa − referente (con signo + / −) |
| Semáforo | pp diferencia: >+10 R · 0 a +10 A · negativo V |

#### 3B — Protocolos de Alta Prioridad vs Sector

| Columna | Descripción |
|---------|-------------|
| N protocolos empresa nivel Urgente | Conteo protocolos en nivel_gestion = Urgente |
| Protocolos prioritarios para el sector | Lista de los top 4 PROT_ID por sector (config.yaml) |
| Coincidencia | Intersección empresa vs sector (binario) |

#### 3C — Top 3 Dimensiones Intralaboral vs Colombia

| Columna | Descripción |
|---------|-------------|
| Dimensión | Nombre dimensión (ej. Demandas de carga mental) |
| % empresa A+MA | % en riesgo alto + muy alto |
| % Colombia (ENCST) | Referente país por dimensión |
| Diferencia pp | Con signo, semaforizada |

**Fuentes parquet:**
- `fact_benchmark.parquet` → comparativas dimensiones/dominios vs ENCST
- `fact_gestion_05_prioridades_protocolos.parquet` → protocolos nivel Urgente
- `config.yaml` → benchmarking sectorial, prioridades por sector

**Endpoint:** `POST /v3/benchmarking`

---

### S4 — Ranking Áreas Críticas Top 5

**Propósito narrativo:** Focalizar la intervención directiva en las unidades organizacionales con mayor carga de riesgo.

**Métricas por área:**
- % trabajadores con riesgo Alto + Muy Alto Intralaboral (promedio A+B)
- N evaluados por área
- Nivel de riesgo predominante (5 categorías)
- Dimensión con mayor diferencia vs Colombia

**Visualización:** Barras horizontales ordenadas de mayor a menor, con colores R11. Top 5 áreas destacadas.

**Regla R8:** Si área < 5 personas → excluir y marcar como "Confidencial" en listado.

**Fuentes parquet:**
- `fact_consolidado.parquet` → scores + area_departamento
- `fact_scores_baremo.parquet` → niveles de riesgo

**Endpoint:** `POST /v3/ranking_areas`

---

### S5 — Alertas y Prioridades: 3 Líneas de Gestión

**Propósito narrativo:** Traducir el análisis técnico en acciones concretas con ficha de protocolo lista para presentar al comité directivo.

**Selección de las 3 líneas principales:**
- Líneas con mayor puntaje de lesividad × % trabajadores afectados
- Priorizadas según sector económico (config.yaml)

**Ficha técnica por línea (formato tarjeta):**

| Campo | Descripción |
|-------|-------------|
| Nombre protocolo | Nombre canónico (dim_protocolos_lineas) |
| Objetivo | Qué busca lograr |
| KPI de seguimiento | Indicador medible |
| Resultado esperado | Meta concreta (% reducción riesgo) |
| N trabajadores a intervenir | Count por protocolo (fact_gestion_07_protocolos_poblacion) |
| Sector económico | Sector de la empresa |
| Dimensiones intralaboral impactadas | Lista dimensiones que interviene |
| Indicadores que impacta | KPIs de V2 afectados |

**Visualización:** 3 tarjetas expandibles en grid 3 columnas. Badge de urgencia (Urgente / Inmediato / Preventivo). Icono por línea de gestión.

**Fuentes parquet:**
- `fact_gestion_05_prioridades_protocolos.parquet`
- `dim_protocolos_lineas.parquet`
- `fact_gestion_07_protocolos_poblacion.parquet`
- `config.yaml` → prioridades_sector

**Endpoint:** `POST /v3/alertas_protocolos`

---

## Endpoints — Resumen

| Endpoint | Método | Sección | Parquets fuente |
|----------|--------|---------|-----------------|
| `/v3/encabezado` | GET | S0 | fact_consolidado · dim_trabajador |
| `/v3/kpis_globales` | POST | S1 | fact_scores_baremo · fact_gestion_04_niveles_ejes · fact_gestion_06_vigilancia_resumen · fact_benchmark |
| `/v3/demografia` | POST | S2A | dim_demografia · dim_trabajador |
| `/v3/costos_ausentismo` | POST | S2B | dim_ausentismo · dim_trabajador · config |
| `/v3/benchmarking` | POST | S3 | fact_benchmark · fact_gestion_05_prioridades_protocolos · config |
| `/v3/ranking_areas` | POST | S4 | fact_consolidado · fact_scores_baremo |
| `/v3/alertas_protocolos` | POST | S5 | fact_gestion_05_prioridades_protocolos · dim_protocolos_lineas · fact_gestion_07_protocolos_poblacion |

---

## Nuevos parquets generados (ETL script 09)

| Archivo | Filas estimadas | Descripción |
|---------|-----------------|-------------|
| `fact_v3_kpis_globales.parquet` | ~18 (6 KPI × 3 niveles) | KPIs globales pre-calculados por empresa |
| `fact_v3_demografia.parquet` | ~200 | Distribuciones demográficas por empresa |
| `fact_v3_costos.parquet` | ~7 (7 pasos) | Cálculo R10 completo por empresa |
| `fact_v3_benchmarking.parquet` | ~15 | Comparativas sector/país ejecutivas |
| `fact_v3_ranking_areas.parquet` | top 5 | Ranking áreas críticas |

---

## Narrativa ejecutiva por sección

```
S1 HOOK: "[N] trabajadores — [X%] en riesgo Alto o Muy Alto Intralaboral"
→ [X] puntos porcentuales por encima del referente [sector/país]

S2 CONFLICT: "El ausentismo actual ([Y%]) supera en [D] pp el promedio nacional"
→ Pérdida estimada de $[Z] anuales atribuida al empleador

S3 INSIGHT: "Las dimensiones [D1, D2, D3] están significativamente por encima de Colombia"
→ Coinciden con [N] de los protocolos prioritarios para el sector [S]

S4 FOCUS: "Las áreas [A1, A2, A3] concentran el [%] de los trabajadores en riesgo crítico"
→ Estas áreas son candidatas prioritarias para intervención inmediata

S5 ACTION: "Se activan [N] protocolos — [N2] en nivel Urgente"
→ Protocolo [P]: intervenir [N] trabajadores. KPI objetivo: [K]
```

---

## UX/UI — Consideraciones

- Layout: 1200px mínimo · columnas flexibles (2–3 por fila)
- S0: Sticky header con empresa + fecha
- S1: Grid 3×2 de tarjetas KPI con semáforo visual
- S2A: Grid de gráficas demográficas en 4 columnas (pirámide ocupa 2)
- S2B: Panel colapsable "Ver cálculo detallado" con tabla paso a paso
- S3: Tabla ejecutiva con badges de semáforo
- S4: Barras horizontales con animación de entrada
- S5: Acordeón de fichas técnicas con badge de urgencia

---

## Archivos generados por este visualizador

| Archivo | Ruta | Tipo |
|---------|------|------|
| Especificación | `docs/v3_gerencial_asis_spec.md` | Markdown |
| ETL | `scripts/09_asis_gerencial.py` | Python |
| Router API | `api/routers/v3_gerencial_asis.py` | Python |
| Auditoría Excel | `output/auditoria_v3_consultel_indecol.xlsx` | Excel |

---

*Generado: 2026-04-01 | MentalPRO v3.0.0 | Resolución 2764/2022*
