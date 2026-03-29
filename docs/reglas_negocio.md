# reglas_negocio.md — Reglas Críticas de Scoring y Clasificación
> Reglas de negocio para el pipeline MentalPRO | Versión 1.5 | Res. 2764/2022

---

## 1. REGLAS DE SCORING BATERÍA (R1-R10)

### R1 — PK triple obligatoria
```
PRIMARY KEY: (cedula, forma_intra, id_pregunta)
NUNCA usar solo 'cedula' en tablas que tienen 'forma_intra'.

Razón: Un trabajador puede evaluar como jefe (forma A) en una empresa
  y como operativo (forma B) en otra. Si solo se usa cedula,
  los registros colisionan y se pierde información.

Tablas afectadas:
  fact_respuestas_clean     → PK: cedula + forma_intra + id_pregunta
  fact_scores_brutos        → PK: cedula + forma_intra + id_pregunta
  fact_scores_baremo        → PK: cedula + forma_intra + nivel_analisis
  fact_gestion_scores       → PK: cedula + forma_intra + eje + linea
```

### R2 — Baremos diferenciados A/B
```
Forma A (IntraA — jefes y coordinadores):
  125 preguntas intralaborales
  Dominio evaluado: Relación colaboradores (NO existe en forma B)
  Transformación máxima: 492 puntos
  Puntos de corte IntraA total:
    Sin riesgo: ≤ 19.7 | Bajo: ≤ 25.8 | Medio: ≤ 31.5 | Alto: ≤ 38.8

Forma B (IntraB — operativos y auxiliares):
  97 preguntas intralaborales
  Dominio 'Relación colaboradores' NO aplica
  Transformación máxima: 388 puntos
  Puntos de corte IntraB total:
    Sin riesgo: ≤ 20.6 | Bajo: ≤ 26.0 | Medio: ≤ 31.2 | Alto: ≤ 38.7

REGLA: NUNCA aplicar el mismo baremo a ambas formas.
REGLA: Analizar forma A y forma B SIEMPRE separados.
```

### R3 — Inversión de ítems (2 niveles)
```
NIVEL 1 — Recodificación escala (V1-Paso2, script 02a):
  Para ítems con escala positiva-negativa invertida:
  Siempre=4→0 | Casi siempre=3→1 | Algunas veces=2→2 | Casi nunca=1→3 | Nunca=0→4
  Listas por instrumento:
    IntraA: 73 ítems (ver pipeline.md V1-Paso2)
    IntraB: 68 ítems
    Extralaboral: 23 ítems

NIVEL 2 — Inversión indicadores gestión (V2-Paso3, script 03):
  Para indicadores que miden RIESGO (más alto = peor):
  score = 1 - score_0a1
  Objetivo: que el eje de gestión sea coherente (mayor = mejor bienestar)
  Indicadores a invertir:
    Autonegación | Evitación cognitiva | Evitación conductual
    Accesibilidad entorno | Apoyo social | Condiciones vivienda
    Alteraciones cognitivas | Desgaste emocional | Pérdida de sentido

ORDEN OBLIGATORIO: Nivel 1 SIEMPRE antes de Nivel 2.
```

### R4 — 5 niveles de riesgo normativos con etiquetas por tipo_baremo
```
Los 5 niveles están definidos en la Res. 2764/2022 del MinTrabajo.
NO se pueden reducir, combinar ni renombrar.

Columna etiqueta_nivel — texto categorico según tipo_baremo:

  tipo "riesgo" (IntraA, IntraB, Extralaboral, Estrés, dominios/dimensiones intra+extra):
    1 = Sin riesgo       color: #10B981
    2 = Riesgo bajo      color: #6EE7B7
    3 = Riesgo medio     color: #F59E0B
    4 = Riesgo alto      color: #F97316
    5 = Riesgo muy alto  color: #EF4444

  tipo "afrontamiento_dim" (dimensiones de Afrontamiento):
    1 = Muy inadecuado   color: #EF4444
    2 = Inadecuado       color: #F97316
    3 = Algo adecuado    color: #F59E0B
    4 = Adecuado         color: #6EE7B7
    5 = Muy adecuado     color: #10B981

  tipo "capitalpsicologico_dim" (dimensiones de Capital Psicológico):
    1 = Muy bajo capital psicológico    color: #EF4444
    2 = Bajo capital psicológico        color: #F97316
    3 = Medio capital psicológico       color: #F59E0B
    4 = Alto capital psicológico        color: #6EE7B7
    5 = Muy alto capital psicológico    color: #10B981

  tipo "individual" (Factor Individual combinado Afrontamiento+CapPsico):
    1 = Muy bajo   color: #EF4444
    2 = Bajo       color: #F97316
    3 = Medio      color: #F59E0B
    4 = Alto       color: #6EE7B7
    5 = Muy alto   color: #10B981

  tipo "proteccion" (dominio Estrategias de Afrontamiento):
    1 = Muy bajo   color: #EF4444
    2 = Bajo       color: #F97316
    3 = Medio      color: #F59E0B
    4 = Alto       color: #6EE7B7
    5 = Muy alto   color: #10B981

% Vulnerabilidad psicológica = (nivel muy_bajo + bajo) / Total × 100
  (aplica solo a tipo "individual" y "proteccion")
```

### R4b — Redondeo de puntaje_transformado a 1 decimal
```
puntaje_transformado = round(puntaje_bruto / transformacion_max × 100, 1)

REGLA: Siempre redondear a 1 decimal antes de aplicar los puntos de corte.
RAZÓN: Un puntaje como 25.85 redondeado a 2 decimales puede clasificarse en un nivel
  diferente que 25.9 redondeado a 1 decimal. La Resolución 2764/2022 usa puntos de corte
  con 1 decimal; el redondeo al mismo nivel garantiza coherencia.

NUNCA usar 2 decimales ni sin redondear en puntaje_transformado.
```

### R5 — Benchmarking: sectorial vs Colombia
```
Benchmarking SECTORIAL (Paso 17 — solo intralaboral total):
  Fuente: III Encuesta Nacional SST 2021
  Métrica: % trabajadores nivel_riesgo >= 4 en factor IntraA / IntraB por empresa
  Comparar empresa vs sector (config.yaml → benchmark_sector)
  PROHIBIDO: NO usar datos sectoriales para dominios o dimensiones

Benchmarking COLOMBIA (Pasos 18-19 — dominios y dimensiones):
  Fuente: II+III ENCST (2013 + 2021 combinadas)
  Métrica: % trabajadores nivel_riesgo >= 4 por dominio / dimensión
  Excepción Vulnerabilidad (Paso 18): % nivel_riesgo <= 2 en factor Individual

  Dominios (nombre_nivel exacto en fact_scores_baremo):
    "Demandas del trabajo"             43.9%
    "Control sobre el trabajo"         16.9%
    "Liderazgo y relaciones sociales"  13.3%
    "Recompensas"                       3.3%
    "Extralaboral" (factor)            26.3%
    "Estres" (factor)                  32.9%
    Vulnerabilidad ("Individual" <=2)   4.2%

  Dimensiones comparables (11 en total — ver pipeline.md Paso 19 para lista completa)
    Carga mental 58.2% | Emocionales 49.4% | Cuantitativas 39.2%
    Claridad de rol: A=20.5% / B=5.8% (diferenciada por forma)

  Semáforo: rojo si diferencia_pp > 0 | verde si <= 0 | insuficiente si n < 5 (R8)
```

### R6 — LEFT JOIN en ausentismo
```
dim_ausentismo contiene ~17 registros (solo trabajadores con eventos).
Si se hace INNER JOIN, se pierden 99%+ de los trabajadores.

REGLA OBLIGATORIA: SIEMPRE LEFT JOIN desde fact hacia dim_ausentismo.
  fact_consolidado LEFT JOIN dim_ausentismo ON cedula
  Registros sin match → pct_ausentismo = 0, dias_ausentismo = 0
```

### R7 — empresa='ASIGNAR' es real
```
ASIGNAR es el nombre de una empresa cliente real.
NO filtrar, excluir, ni tratar como valor nulo o error de datos.

Igualmente: EVENTUAL puede ser nombre de empresa o modalidad de contrato.
Verificar en context antes de filtrar.
```

### R8 — Confidencialidad de datos individuales
```
Regla de confidencialidad (obligatoria, protección datos personales):
  Grupos con < 5 personas → NO mostrar datos individuales
  Reemplazar con: 'No se muestra por confidencialidad'

Aplica en:
  Análisis por área/cargo/departamento con pocos trabajadores
  Reporte individual de líderes con < 5 personas a cargo
  Cualquier cruce demográfico con n < 5

NO aplica en:
  Resultados totales de la empresa
  Promedios agrupados con n ≥ 5
```

### R9 — Media ponderada en gestión
```
score_0a1 en fact_gestion_scores es SIEMPRE media ponderada.
NUNCA usar promedio simple (sum/count) para scores de gestión.

Fórmula:
  score_indicador = Σ(score_item × peso_item) / Σ(peso_item)
  score_linea = Σ(score_indicador × peso_indicador) / Σ(peso_indicador)
  score_eje = Σ(score_linea × peso_linea) / Σ(peso_linea)

Pesos documentados en V2-Paso3 (columnas C, E, H).
Pesos discriminados por forma A y forma B.
```

### R10 — Costo económico con presentismo
```
Fórmula 6 pasos (V4-Paso1):
  1. SMLV_anual = SMLV_mensual × 12
  2. capacidad_productiva = n_trabajadores × SMLV_anual
  3. perdida_ausentismo = (pct_ausentismo / 100) × capacidad_productiva
  4. perdida_empleador = perdida_ausentismo × 0.60
  5. perdida_productividad = perdida_empleador × 1.40   # PRESENTISMO
  6. perdida_psicosocial = perdida_productividad × 0.30

REGLA: El factor 1.40 en paso 5 es OBLIGATORIO. Representa el 40%
  adicional de pérdida productividad por PRESENTISMO (trabajadores
  que asisten pero trabajan por debajo de su capacidad por malestar psicosocial).
NUNCA omitir el × 1.40.

Parámetros ajustables en config.yaml:
  smlv_mensual: 2800000  (actualizar cada año)
  presentismo_factor: 1.40
  costo_empleador_pct: 0.60
  psicosocial_pct: 0.30
```

---

## 2. REGLAS DE DISEÑO (R11-R15)

### R11 — Colores AVANTUM inamovibles
```
Los tokens de color son INAMOVIBLES. No negociar, no personalizar.

Colores normativos (Res. 2764 — riesgo):
  risk_1 = '#10B981'  Sin riesgo
  risk_2 = '#6EE7B7'  Bajo
  risk_3 = '#F59E0B'  Medio
  risk_4 = '#F97316'  Alto
  risk_5 = '#EF4444'  Muy alto

Colores primarios AVANTUM:
  navy   = '#0A1628'  Fondo principal
  gold   = '#C9952A'  Acento, CTAs
  cyan   = '#00C2CB'  IA, tecnología

REGLA: El backend NUNCA se encarga de inyectar colores en gráficas. El backend (Python) mapea
el nivel 1-5 y envía la estructura de datos al Frontend (Next.js). Tailwind CSS implementa la paleta.
```

### R12 — No rutas absolutas
```
NUNCA hardcodear rutas absolutas:
  MAL:  pd.read_excel('C:/Users/juan/Documents/Resultado_mentalPRO.xlsx')
  BIEN: pd.read_excel(config['paths']['raw_fact'])

Cargar config desde config/config.yaml al inicio de cada script.
```

### R13 — Outputs solo en Parquet
```
Todos los outputs intermedios y finales → formato .parquet
  MAL:  df.to_csv('fact_scores.csv')
  BIEN: df.to_parquet('data/processed/fact_scores_baremo.parquet')

Razón: Parquet preserva tipos de datos (fecha, int, float, categoría),
  comprime mejor que CSV, y es 10-100x más rápido para lecturas parciales.
```

### R14 — Validación obligatoria en cada script
```
Cada script debe exponer una función validar_*(df) que retorne:
  (bool, pd.DataFrame)  →  (es_valido, reporte_errores)

Verificar mínimo:
  - No nulos en columnas clave (cedula, forma_intra, id_pregunta)
  - PK sin duplicados
  - Valores en rangos esperados (scores 0-1, niveles 1-5)
  - Conteos de filas razonables (no 0, no outlier extremo)

Llamar validar_*() al final de cada script y loggear resultado.
```

### R15 — Fact tables son inmutables
```
fact_respuestas original (FactRespuestas en Resultado_mentalPRO.xlsx)
→ NUNCA modificar directamente.
→ Crear fact_respuestas_clean como copia validada y transformada.

Todas las tablas derivadas van en data/processed/ como parquets.
La fuente de verdad es siempre Resultado_mentalPRO.xlsx.
```

---

## 3. REGLAS DE ARQUITECTURA (R16-R20)

### R16 — 2 archivos de datos separados
```
Resultado_mentalPRO.xlsx: SOLO tablas de hechos (facts)
  Hoja principal: FactRespuestas (180,617 filas × 10 columnas)

datasets.xlsx: SOLO tablas dimensionales (dims)
  dim_trabajador, dim_pregunta, dim_respuesta
  dim_baremo, dim_demografia, dim_ausentismo
  categorias_analisis

NUNCA mezclar facts y dims en el mismo archivo.
```

### R17 — Sector homologación obligatoria
```
SIEMPRE normalizar sector_economico → sector_rag en 01_etl_star_schema.py.
El SECTOR_MAP contiene 30+ aliases cubriendo todos los sectores ENCST + variantes
de escritura comunes (mayúsculas, tildes, abreviaturas).

Sectores destino válidos (10):
  Agricultura | Manufactura | Servicios | Construcción | Comercio/financiero
  Transporte | Minas y canteras | Administración pública | Educación | Salud

Reglas de mapeo:
  - Alias conocido → sector destino del SECTOR_MAP
  - No mapeado → WARNING en logs + sector_rag = 'No clasificado'
  - 'No clasificado' usa benchmark del promedio general (39.69%)
  - 'Transporte' usa benchmark de 'Servicios' (37.20%) por ausencia en ENCST
  - El pipeline NUNCA rompe por sector desconocido (R18)

OBLIGATORIO: Agregar nuevos aliases al SECTOR_MAP cuando aparezcan sectores
  no reconocidos — NO hardcodear correcciones en otros scripts.
```

### R18 — Escalabilidad del pipeline
```
El pipeline está diseñado para N empresas (no solo una).
Todos los scripts deben funcionar para el universo completo.
Los dashboards filtran por empresa en el frontend (JS dropdown en HTML estático).

NO crear scripts separados por empresa.
NO hardcodear nombres de empresas en los scripts.
```

### R19 — Dashboards Cliente-Servidor Web (Next.js / API)
```
La arquitectura final de visualización confía en un enfoque Dashboard Cliente/Servidor.

El ETL en backend produce Parquets que serán enrutados por servicios API (Ej. FastAPI),
mientras un Frontend en Next.js/React los consume para renderizar interacciones avanzadas.

Reglas del Frontend (Dashboard Web):
  - Canvas fluido 100% responsivo.
  - El frontend no hace cálculos estadísticos pesados, solo rendering e IA reactiva.
  - Filtros: Consumen API y filtran dinámicamente en UI con Recharts/Tremor.
```

### R20 — Documento marco como fuente de verdad
```
docs/Documento marco.md es la ÚNICA fuente de verdad para:
  - Reglas de scoring y baremos
  - Mapeos ítem → dimensión → dominio → factor
  - Fórmulas de cálculo
  - Definiciones de secciones de dashboards

En caso de contradicción entre docs/:
  Documento marco.md > pipeline.md > reglas_negocio.md > agents.md > otros

OBLIGATORIO antes de codificar cualquier paso:
  1. Validar la regla en Documento marco.md
  2. Verificar coherencia con todos los otros docs
  3. Corregir los docs que contradigan el marco ANTES de escribir código
```