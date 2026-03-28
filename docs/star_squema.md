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

Columna              Tipo    Descripción
cedula               str     Documento de identidad trabajador
nombre               str     Nombre completo
forma_intra          str     'A' (jefes/coordinadores) | 'B' (operativos/auxiliares)
                             SOLO diferencia intraA vs intraB. Los demás instrumentos
                             (extra, estrés, afrontamiento, capitalpsic) usan id_pregunta
                             para identificarse — NO dependen de forma_intra.
empresa              str     Nombre empresa cliente (incluye ASIGNAR=empresa real, R7)
sector_economico     str     6 valores: Agricultura|Comercio|Construcción|
                               Manufactura|Servicios|Transporte
ausentismo_eg_si_no  str     'si'/'no' — ausentismo enfermedad general
ausentismo_al_si_no  str     'si'/'no' — ausentismo accidente laboral
dias_ausencia        int     Días de ausencia en el período
id_pregunta          str     *** IDENTIFICADOR ÚNICO ítem+instrumento ***
                             Formato: "{numero}_{sufijo_instrumento}"
                             Sufijos y rangos:
                               _intra       → 1-125 (A) | 1-98 (B) — Likert + 2 dicot. en A, 1 en B
                               _extra       → 1-31   (ambas formas)
                               _estres      → 1-31   (ambas formas, 3 grupos de pesos)
                               _afrontamiento → 1-12 (ambas formas)
                               _capitalpsic → 1-12   (ambas formas)
                             Ejemplos: "1_intra", "106_intra", "1_extra", "1_estres",
                                       "1_afrontamiento", "1_capitalpsic"
                             Con forma_intra='A'|'B' + sufijo '_intra' → intraA o intraB
id_respuesta         str     *** TEXTO DE RESPUESTA capturado en la evaluación ***
                             Por instrumento:
                               _intra/_extra : "Siempre"|"Casi siempre"|"Algunas veces"|
                                               "Casi nunca"|"Nunca"|"si"|"no" (dicot.)
                               _estres       : "Siempre"|"Casi siempre"|"A veces"|"Nunca"
                               _afrontamiento: "nunca hago eso"|"a veces hago eso"|
                                               "frecuentemente hago eso"|"siempre hago eso"
                               _capitalpsic  : "totalmente en desacuerdo"|"en desacuerdo"|
                                               "de acuerdo"|"totalmente de acuerdo"
```

> **CORRECCIÓN vs versiones anteriores**: id_pregunta NO es int — es str con sufijo de instrumento.
> id_respuesta NO es el código numérico — es el TEXTO de respuesta tal como se capturó.
> Ausentismo: columnas reales son ausentismo_eg_si_no, ausentismo_al_si_no, dias_ausencia
> (NO pct_ausentismo / dias_ausentismo / accidente_laboral como estaba en versiones previas).

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
PK: id_pregunta   (globalmente único — incluye sufijo instrumento)
Columnas:
  id_pregunta      — mismo formato que fact: "1_intra", "1_extra", etc.
  instrumento      — intra | extra | estres | afrontamiento | capitalpsic
  num_item         — número dentro del instrumento (int, 1-125)
  texto_pregunta   — texto completo de la pregunta
  dimension        — agrupación de ítems en dimensión
  dominio          — agrupación de dimensiones en dominio
  factor           — factor del instrumento (intralaboral|extralaboral|estres|individual)
  forma_aplicacion — 'A' | 'B' | 'AB' (si aplica a ambas formas)
  tipo_escala      — 'likert_0_4' | 'dico' | 'estres_g1' | 'estres_g2' | 'estres_g3'
                     | 'afrontamiento' | 'capitalpsic'
  es_item_invertido — bool — True si aplica inversión Nivel 1 (R3)
  peso_item        — float — peso para media ponderada en modelo gestión (R9)
  eje_gestion      — eje del modelo AVANTUM
  linea_gestion    — línea de gestión AVANTUM
  indicador_gestion — indicador de gestión AVANTUM
```

### dim_respuesta
```
NOTA: Con id_respuesta siendo texto libre, esta tabla actúa como catálogo
de normalización de respuestas (limpieza de variantes de escritura).
PK: texto_respuesta_normalizado + tipo_escala
Columnas:
  texto_original   — variante de texto como puede venir en el raw
  texto_normalizado — versión canónica (para matching consistente)
  tipo_escala      — 'likert_0_4' | 'dico' | 'estres_g1|g2|g3' | 'afrontamiento' | 'capitalpsic'
  valor_numerico   — float — valor codificado (Paso 1, R3)
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
  ├── FK cedula         → dim_ausentismo (cedula) [LEFT JOIN — R6]
  ├── FK id_pregunta    → dim_pregunta (id_pregunta)  [JOIN directo, PK única]
  └── id_respuesta      → dim_respuesta (normalización texto) [opcional, para limpieza]

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