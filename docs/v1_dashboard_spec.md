# Visualizador 1: Resultados de Riesgo Psicosocial — Especificacion para Frontend

> Documento de referencia para el equipo de frontend (Next.js/React).
> Describe las secciones, KPIs, fuentes de datos y reglas de negocio del dashboard V1.
> Marco regulatorio: Resolucion 2764/2022 — Ministerio de Trabajo, Colombia.

---

## 1. Arquitectura de datos

El backend Python genera archivos `.parquet` en `data/processed/`. El backend FastAPI expone estos datos como endpoints JSON. El frontend consume los endpoints y renderiza el dashboard.

### Parquets disponibles para consumo API

| Archivo | Filas | Contenido | Columnas clave |
|---------|-------|-----------|---------------|
| `fact_scores_baremo.parquet` | 31,108 | Score + nivel de riesgo por trabajador x dimension/dominio/factor | cedula, empresa, forma_intra, sector_rag, instrumento, nivel_analisis, nombre_nivel, puntaje_bruto, transformacion_max, puntaje_transformado, nivel_riesgo, etiqueta_nivel, tipo_baremo |
| `fact_benchmark.parquet` | 403 | % Alto+MuyAlto empresa vs referente, por nivel | empresa, sector_rag, nivel_analisis, nombre_nivel, forma_intra, n_total, n_alto, pct_empresa, pct_referencia, tipo_referencia, diferencia_pp, semaforo |
| `fact_riesgo_empresa.parquet` | 23 | Riesgo total empresa + alerta reevaluacion | empresa, sector_rag, instrumento, n_trabajadores, puntaje_bruto_empresa, puntaje_transformado, nivel_riesgo_empresa, debe_reevaluar_1anio |
| `fact_frecuencias.parquet` | 13,974 | Distribucion de frecuencia por pregunta | empresa, forma_intra, id_pregunta, opcion_respuesta, n_personas, n_total, pct_empresa |
| `fact_top20_comparables.parquet` | 852 | Preguntas comparables vs ENCST + flag top 20 | empresa, forma_intra, id_pregunta, ref_idx, dimension, n_total, n_alta_presencia, pct_empresa, pct_pais_encst, diferencia_pp, formula, top20_flag |
| `fact_consolidado.parquet` | 31,108 | Scores + demografia + ausentismo (long format) | cedula, empresa, forma_intra, instrumento, nivel_analisis, nombre_nivel, puntaje_transformado, nivel_riesgo, area_departamento, categoria_cargo, modalidad_de_trabajo, sexo, edad_cumplida, antiguedad_empresa, dias_ausencia |
| `dim_trabajador.parquet` | 856 | Planta personal | cedula, nombre_trabajador, empresa, sector_economico, area_departamento, categoria_cargo, tipo_cargo, forma_intra, es_Jefe, modalidad_de_trabajo, nombre_jefe |
| `dim_demografia.parquet` | 856 | Ficha demografica | cedula, fecha_aplicacion, nivel_escolaridad, estrato_economico, sexo, edad_cumplida, antiguedad_empresa, estado_civil, horas_trabajo_diario |

### Valores clave para el Header (S0)

| Dato | Fuente |
|------|--------|
| Empresa | `dim_trabajador.empresa` (filtrar por empresa seleccionada) |
| Sector | `dim_trabajador.sector_economico` |
| # Evaluados | `dim_trabajador.cedula.nunique()` |
| % Cobertura | Calcular: evaluados / planta total (si se tiene dato de planta) |
| Fecha evaluacion | `dim_demografia.fecha_aplicacion` (moda o max) |

---

## 2. Secciones del Dashboard

### S0 — Header

Texto estatico con datos de la empresa.

| Campo | Valor |
|-------|-------|
| Empresa | Nombre de la empresa |
| Sector economico | Sector de la empresa |
| Total evaluados | Conteo de trabajadores unicos |
| % Cobertura | Evaluados / planta total |
| Fecha de evaluacion | Fecha de aplicacion de la bateria |

### S1 — Filtros de segmentacion

Dropdowns que filtran TODAS las secciones del dashboard simultaneamente.

| Filtro | Columna fuente | Tabla |
|--------|---------------|-------|
| Area/Departamento | `area_departamento` | dim_trabajador |
| Categoria de cargo | `categoria_cargo` | dim_trabajador |
| Modalidad de trabajo | `modalidad_de_trabajo` | dim_trabajador |
| Nombre del jefe | `nombre_jefe` | dim_trabajador |
| Edad cumplida | `edad_cumplida` | dim_demografia |
| Antiguedad empresa | `antiguedad_empresa` | dim_demografia |

**Regla R8:** Al aplicar un filtro, si el grupo resultante tiene < 5 personas, mostrar "Confidencial" en lugar de los datos.

### S2 — Tarjetas KPI (5 tarjetas)

#### KPI-1: % Riesgo Alto+MuyAlto Intralaboral

| Fila | Valor | Fuente |
|------|-------|--------|
| Intra A | % trabajadores forma A con nivel_riesgo in (alto, muy_alto) en factor intralaboral | `fact_scores_baremo` donde instrumento=IntraA, nivel_analisis=factor |
| Intra B | % trabajadores forma B con nivel_riesgo in (alto, muy_alto) en factor intralaboral | `fact_scores_baremo` donde instrumento=IntraB, nivel_analisis=factor |
| Sector/Pais | Valor de referencia del sector o promedio pais (39.69%) | `config.yaml:benchmark_sector` |

**Semaforo:** >35% = rojo | 15-34% = amarillo | <15% = verde

#### KPI-2: Trabajadores con Estres Alto+MuyAlto

| Fila | Valor | Fuente |
|------|-------|--------|
| N absoluto | Conteo de trabajadores (A+B) con nivel_riesgo in (alto, muy_alto) en factor estres | `fact_scores_baremo` donde instrumento=Estres, nivel_analisis=factor |
| % evaluados | N / total evaluados * 100 | Calculado |
| % pais | 32.9% | `config.yaml:benchmark_dominio.Estres` |

**Semaforo:** Semaforizar diferencia en puntos porcentuales vs pais

#### KPI-3: Vulnerabilidad psicologica

| Fila | Valor | Fuente |
|------|-------|--------|
| N absoluto | Conteo de trabajadores con nivel_riesgo in (muy_bajo, bajo) en factor Individual | `fact_scores_baremo` donde instrumento in (Afrontamiento, CapPsico), nivel_analisis=factor, nombre_nivel=Individual |
| % evaluados | N / total evaluados * 100 | Calculado |
| % pais | 4.2% | `config.yaml:benchmark_dominio.Individual` |

**Semaforo:** Semaforizar diferencia pp vs pais. Nota: en Individual la escala es de proteccion — muy_bajo y bajo = vulnerabilidad.

#### KPI-4: % Riesgo Alto+MuyAlto Extralaboral

| Fila | Valor | Fuente |
|------|-------|--------|
| Extra A | % trabajadores forma A con nivel alto+muy_alto en factor extralaboral | `fact_scores_baremo` |
| Extra B | % trabajadores forma B con nivel alto+muy_alto en factor extralaboral | `fact_scores_baremo` |
| Pais | 26.3% | `config.yaml:benchmark_dominio.Extralaboral` |

**Semaforo:** >35% = rojo | 15-34% = amarillo | <15% = verde

#### KPI-5: Dimensiones criticas

| Fila | Valor | Fuente |
|------|-------|--------|
| Conteo | Numero de dimensiones donde pct_empresa > pct_referencia | `fact_benchmark` donde nivel_analisis=dimension |

Valor unico, sin semaforo.

### S3 — Resultados globales

**Componente 1: Barras apiladas horizontales**
- Una barra por cada instrumento/factor: IntraA, IntraB, Extralaboral, Estres, Afrontamiento, CapPsicologico
- Cada barra dividida en 5 segmentos de color segun % de trabajadores en cada nivel de riesgo
- Fuente: `fact_scores_baremo` agrupado por instrumento y nivel_riesgo

**Componente 2: Gauge de riesgo empresa**
- Puntaje transformado promedio de la empresa en factor intralaboral (A y B)
- Fuente: `fact_riesgo_empresa`

**Componente 3: Alerta de reevaluacion**
- Si `debe_reevaluar_1anio` = True para IntraA o IntraB -> mostrar alerta
- Texto: "La empresa debe reevaluar al anio siguiente (Res. 2764/2022)"
- Fuente: `fact_riesgo_empresa`

### S4 — Distribucion 0-100

**Boxplot** del puntaje_transformado por instrumento.
- Eje X: puntaje_transformado (0-100)
- Eje Y: instrumento (IntraA, IntraB, Extralaboral, Estres, Afrontamiento, CapPsico)
- Fuente: `fact_scores_baremo` donde nivel_analisis=factor

### S5 — Benchmarking sector

**Barras comparativas** de % Alto+MuyAlto empresa vs sector ENCST.
- Solo factor intralaboral: IntraA e IntraB
- Fuente: `fact_benchmark` donde nivel_analisis=factor, nombre_nivel in (IntraA, IntraB)
- Mostrar: pct_empresa, pct_referencia, diferencia_pp, semaforo

### S6 — Dominios vs Colombia

**Heatmap** de % riesgo Alto+MuyAlto por dominio.
- Filas: dominios intralaboral (Demandas, Control, Liderazgo, Recompensas) + Extralaboral + Estres + Individual
- Columnas: empresa vs referente nacional
- Fuente: `fact_benchmark` donde nivel_analisis=dominio

### S7 — Dimensiones vs Colombia

**Tabla semaforizada** de dimensiones empresa vs ENCST.
- Filas: cada dimension intralaboral que tiene referente ENCST
- Columnas: nombre_nivel, pct_empresa, pct_referencia, diferencia_pp, semaforo
- Fuente: `fact_benchmark` donde nivel_analisis=dimension
- Ordenar por diferencia_pp descendente

### S8 — Top 20 preguntas clave

**Tabla ordenada** de las preguntas con mayor diferencia positiva vs Colombia.
- Filtrar: `top20_flag` = True
- Columnas: id_pregunta, dimension, pct_empresa, pct_pais_encst, diferencia_pp
- Fuente: `fact_top20_comparables`
- Ordenar por diferencia_pp descendente

### S9 — Empresa vs Areas (Intra + Extra + Estres)

**Heatmap** de area x dimension.
- Filas: areas de la empresa (`area_departamento`)
- Columnas: dimensiones intralaboral + extralaboral + estres
- Celdas: % Alto+MuyAlto
- Fuente: `fact_consolidado` agrupado por area_departamento y nombre_nivel
- **R8:** Areas con < 5 personas = "Confidencial"

### S10 — Empresa vs Areas (Individual)

**Heatmap** de area x dimension individual.
- Igual que S9 pero solo para dimensiones del factor Individual (Afrontamiento, CapPsicologico)
- Fuente: `fact_consolidado` filtrado por instrumento in (Afrontamiento, CapPsico)
- **R8:** Areas con < 5 personas = "Confidencial"

---

## 3. Paleta de colores (R11 — inamovible)

### Niveles de riesgo

| Nivel | Color | Hex | Uso |
|-------|-------|-----|-----|
| Sin riesgo | Verde | `#10B981` | Segmentos de barra, celdas heatmap |
| Bajo | Verde claro | `#6EE7B7` | Segmentos de barra, celdas heatmap |
| Medio | Amarillo | `#F59E0B` | Segmentos de barra, celdas heatmap |
| Alto | Naranja | `#F97316` | Segmentos de barra, celdas heatmap |
| Muy alto | Rojo | `#EF4444` | Segmentos de barra, celdas heatmap |

### Niveles de proteccion (factor Individual — escala invertida)

| Nivel | Color | Hex |
|-------|-------|-----|
| Muy bajo | Rojo | `#EF4444` |
| Bajo | Naranja | `#F97316` |
| Medio | Amarillo | `#F59E0B` |
| Alto | Verde claro | `#6EE7B7` |
| Muy alto | Verde | `#10B981` |

### Colores de marca

| Elemento | Color | Hex |
|----------|-------|-----|
| Fondo principal | Navy | `#0A1628` |
| Acento primario | Gold | `#C9952A` |
| Acento secundario | Cyan | `#00C2CB` |
| Texto principal | Blanco | `#FFFFFF` |
| Fondo tarjetas | Gris claro | `#F3F4F6` |

### Fuente

`Inter, Arial, sans-serif`

---

## 4. Semaforo KPI

| Condicion | Color | Interpretacion |
|-----------|-------|---------------|
| % > 35 | Rojo (`#EF4444`) | Critico — requiere intervencion |
| 15% <= % <= 34% | Amarillo (`#F59E0B`) | Moderado — monitorear |
| % < 15 | Verde (`#10B981`) | Controlado — mantener |

Para KPIs de diferencia vs pais, semaforizar la diferencia en puntos porcentuales:
- Positiva > 5 pp = rojo (peor que pais)
- Entre -5 y +5 pp = amarillo (en linea)
- Negativa < -5 pp = verde (mejor que pais)

---

## 5. Reglas de negocio

### R8 — Confidencialidad

Cualquier agrupacion con N < 5 personas debe mostrar "Confidencial" en lugar del dato numerico. Aplica a:
- Todas las celdas de heatmaps (S6, S7, S9, S10)
- Tarjetas KPI cuando se aplican filtros
- Cualquier tabla o grafica segmentada

### Reevaluacion (Res. 2764/2022)

Si el factor intralaboral total de la empresa (forma A o B) esta en nivel Alto o Muy Alto, la empresa debe reevaluar al anio siguiente. Mostrar alerta visual en S3.

### Referente sector

Si el sector economico de la empresa no tiene dato en `benchmark_sector`, usar el promedio general pais (39.69%).

### Instrumentos y formas

- Los trabajadores se clasifican en forma A (jefes/profesionales) o forma B (operativos/auxiliares)
- IntraA y IntraB son instrumentos diferentes con preguntas distintas
- Extralaboral, Estres, Afrontamiento y CapPsicologico aplican igual a A y B
- El dashboard debe permitir ver resultados globales (A+B) y discriminados (solo A, solo B)

---

## 6. Estructura de navegacion sugerida

```
[Header S0]
[Filtros S1]
─────────────────────
[KPI-1] [KPI-2] [KPI-3] [KPI-4] [KPI-5]     ← S2
─────────────────────
[Barras apiladas globales]  [Gauge + Alerta]   ← S3
─────────────────────
[Boxplot distribucion]                          ← S4
─────────────────────
[Benchmarking sector]                           ← S5
─────────────────────
[Heatmap dominios vs Colombia]                  ← S6
─────────────────────
[Tabla dimensiones vs Colombia]                 ← S7
─────────────────────
[Top 20 preguntas]                              ← S8
─────────────────────
[Heatmap areas Intra+Extra+Estres]              ← S9
─────────────────────
[Heatmap areas Individual]                      ← S10
```

Lienzo de desplazamiento vertical. Minimo 3000 x 2000 px. Diseno responsivo.
