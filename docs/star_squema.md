# star_schema.md — Esquema de Datos MentalPRO
> Star schema completo del pipeline | Versión 1.5

---

## 1. ARQUITECTURA DE 2 ARCHIVOS

```
Resultado_mentalPRO.xlsx  →  TABLAS DE HECHOS (facts)
datasets.xlsx             →  TABLAS DIMENSIONALES (dims)
```

---

## 2. FACT TABLES (Resultado_mentalPRO.xlsx)

### fact_respuestas (FactRespuestas — tabla principal)
```
Filas: 180,617
Columnas: 10
PK: cedula + forma_intra + id_pregunta

Columna           Tipo      Descripción
cedula            str/int   Documento de identidad trabajador
nombre            str       Nombre completo
forma_intra       str       'A' (jefes) | 'B' (operativos)
empresa           str       Nombre empresa cliente (incluye ASIGNAR, EVENTUAL)
sector_economico  str       6 valores: Agricultura|Comercio|Construcción|
                              Manufactura|Servicios|Transporte
pct_ausentismo    float     Porcentaje ausentismo del trabajador
dias_ausentismo   int       Días de ausentismo en el período
accidente_laboral bool/int  1 = tuvo accidente | 0 = no tuvo
id_pregunta       int       Identificador del ítem (1-125 IntraA | 1-97 IntraB)
id_respuesta      int/str   Código de respuesta (0-4 Likert | 0-1 dicotómica)
```

---

## 3. DIMENSIONAL TABLES (datasets.xlsx)

### dim_trabajador
```
PK: cedula
Columnas:
  cedula | nombre | empresa | sector_rag | forma_intra
  area_departamento | tipo_cargo | nivel_cargo
```

### dim_pregunta
```
PK: id_pregunta + instrumento
Columnas:
  id_pregunta | instrumento | texto_pregunta | dimension
  dominio | factor | forma_aplicacion | tipo_escala
  es_item_invertido | peso_item | eje_gestion | linea_gestion | indicador_gestion
```

### dim_respuesta
```
PK: id_respuesta + tipo_escala
Columnas:
  id_respuesta | tipo_escala | texto_respuesta | valor_numerico | orden
```

### dim_baremo
```
PK: forma_intra + factor + dimension (o nivel_analisis)
Columnas:
  forma_intra | instrumento | nivel_analisis | factor | dimension
  transformacion_max | corte_sin_riesgo | corte_bajo | corte_medio
  corte_alto | tipo_baremo ('riesgo' | 'proteccion')
```

### dim_demografia
```
PK: cedula
Columnas:
  cedula | sexo | edad | grupo_etario | nivel_escolaridad
  estado_civil | numero_hijos | estrato_vivienda | tipo_contrato
  salario_rango | antiguedad_empresa | antiguedad_cargo
  horas_trabajo_semanal | modalidad_trabajo
```

### dim_ausentismo
```
PK: cedula
Nota: ~17 registros (solo trabajadores con eventos de ausentismo)
SIEMPRE LEFT JOIN desde fact hacia esta tabla
Columnas:
  cedula | empresa | pct_ausentismo | dias_ausentismo
  accidente_laboral | tipo_ausentismo | condicion_salud_reportada
```

### categorias_analisis
```
Tabla de referencia para el modelo de gestión AVANTUM
Columnas:
  eje_modelo | linea_gestion | indicador | prot_id
  nivel_critico_pct | nivel_prioritario_pct | lesividad (Alta/Media/Baja)
```

---

## 4. FACT TABLES PRODUCIDAS POR EL PIPELINE

### fact_respuestas_clean
```
PK: cedula + forma_intra + id_pregunta
Generado por: 01_etl_star_schema.py
= FactRespuestas original + sector_rag homologado + validaciones
Guardado: data/processed/fact_respuestas_clean.parquet
```

### fact_scores_brutos
```
PK: cedula + forma_intra + id_pregunta
Generado por: 02a_scoring_bateria.py
Columnas nuevas:
  valor_numerico    — codificación texto→número
  valor_invertido   — después de inversión nivel 1 (si aplica)
  instrumento       — IntraA | IntraB | Extralaboral | Estres | Afrontamiento | CapPsico
  dimension         — agrupación del ítem
  dominio           — agrupación de dimensiones
Guardado: data/processed/fact_scores_brutos.parquet
```

### fact_scores_baremo
```
PK: cedula + forma_intra + nivel_analisis
Generado por: 02b_baremos.py
Columnas:
  cedula | empresa | forma_intra | sector_rag
  nivel_analisis    — 'dimension' | 'dominio' | 'factor'
  nombre_nivel      — nombre de la dimensión/dominio/factor
  puntaje_bruto     — suma de ítems del nivel
  puntaje_transformado — después de aplicar baremo
  nivel_riesgo      — 1-5 (Res.2764) | nivel_proteccion (1-5 AVANTUM)
  tipo_baremo       — 'riesgo' | 'proteccion'
Guardado: data/processed/fact_scores_baremo.parquet
```

### fact_gestion_scores
```
PK: cedula + forma_intra + eje + linea
Generado por: 03_scoring_gestion.py + 04_categorias_gestion.py
Columnas:
  cedula | empresa | forma_intra | sector_rag
  eje_modelo | linea_gestion | indicador
  score_0a1         — media ponderada (NUNCA promedio simple)
  nivel_gestion     — 1-5 (prorrogable→urgente)
  categoria_gestion — texto categoría
  enfoque_gestion   — texto interpretativo
Guardado: data/processed/fact_gestion_scores.parquet
```

### fact_prioridades
```
PK: empresa + prot_id
Generado por: 05_prioridades_protocolos.py
Columnas:
  empresa | sector_rag | prot_id | linea_gestion
  score_linea | nivel_gestion | lesividad | prioridad_sector
  n_trabajadores_urgente | pct_urgente | activacion_flag
Guardado: data/processed/fact_prioridades.parquet
```

### fact_benchmark
```
PK: empresa + nivel_analisis + forma_intra
Generado por: 06_benchmarking.py
Columnas:
  empresa | sector_rag | nivel_analisis | nombre_nivel | forma_intra
  empresa_pct         — % alto+muy_alto empresa
  pais_pct            — % referencia Colombia (ENCST)
  sector_pct          — % referencia sector (solo intralaboral total)
  diferencia_pais_pp  — empresa_pct - pais_pct
  diferencia_sector_pp — empresa_pct - sector_pct (solo intra total)
  semaforo            — 'verde' | 'amarillo' | 'rojo'
  alerta_reevaluacion — bool (factor intralaboral Alto o Muy alto)
Guardado: data/processed/fact_benchmark.parquet
```

### fact_frecuencias
```
PK: id_pregunta + opcion_respuesta + empresa
Generado por: 07_frecuencias_preguntas.py
Columnas:
  empresa | id_pregunta | dimension | texto_pregunta
  opcion_respuesta | n_personas | pct_empresa
  pct_pais — referencia ENCST (para 39 preguntas clave)
  diferencia_pp — empresa_pct - pais_pct
  top20_flag — bool (Top 20 preguntas más críticas)
Guardado: data/processed/fact_frecuencias.parquet
```

### fact_consolidado
```
PK: cedula
Generado por: 08_consolidado_demografico.py
1 fila por trabajador con TODOS los scores + demografía
= JOIN: fact_scores_baremo × dim_trabajador × dim_demografia × dim_ausentismo (LEFT)
Guardado: data/processed/fact_consolidado.parquet
```

### fact_kpis_gerenciales
```
PK: empresa
Generado por: 08_consolidado_demografico.py
19 KPIs calculados para el Dashboard Gerencial V3
Guardado: data/processed/fact_kpis_gerenciales.parquet
```

### fact_costo_ausentismo
```
PK: empresa
Generado por: 09_costo_ausentismo.py
Columnas para los 6 pasos de la fórmula:
  empresa | n_trabajadores | smlv_mensual | smlv_anual
  capacidad_productiva | pct_ausentismo_promedio
  perdida_ausentismo | perdida_empleador
  perdida_productividad | perdida_psicosocial
Guardado: data/processed/fact_costo_ausentismo.parquet
```

---

## 5. ERD SIMPLIFICADO

```
fact_respuestas_clean
  ├── FK cedula         → dim_trabajador (cedula)
  ├── FK cedula         → dim_demografia (cedula)
  ├── FK cedula         → dim_ausentismo (cedula) [LEFT JOIN]
  ├── FK id_pregunta    → dim_pregunta (id_pregunta)
  └── FK id_respuesta   → dim_respuesta (id_respuesta)

fact_scores_baremo
  ├── FK cedula + nivel_analisis → dim_baremo
  └── FK cedula → dim_trabajador

fact_gestion_scores
  ├── FK cedula → dim_trabajador
  └── FK eje + linea → categorias_analisis

fact_prioridades
  └── FK prot_id → categorias_analisis (prot_id)

fact_consolidado (tabla desnormalizada — 1 fila/trabajador)
  = todos los scores + demografía en una sola tabla
  (diseñada para uso en dashboards, no para análisis tipado)
```

---

## 6. CONVENCIONES DE NOMBRES

```
Prefijo     Significado
fact_       Tabla de hechos (medidas, eventos, transacciones)
dim_        Tabla dimensional (catálogos, maestros, referencia)
stg_        Staging (tablas intermedias temporales)
ref_        Datos de referencia (benchmarks, baremos)

Sufijos:
_clean      Datos validados y homologados
_brutos     Datos con valor numérico pero sin transformar
_baremo     Datos con puntaje transformado y nivel riesgo
_gestion    Datos del modelo de gestión AVANTUM
```