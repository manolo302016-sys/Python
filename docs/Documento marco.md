##DOCUMENTO MARCO DEL PROYECTO PYTHON##
## 1.	Modelo de datos- star schema – Limpieza y ETL##

### Entregable 1: calificación baremos batera cada trabajador de cada empresa (base de datos) y visualizador de alto impacto dashboard_v1_riesgo. El proceso de extracción de datos para la posterior transformación, parte de 1 tabla de hechos fact_respuestas que contiene como hechos: respuestas a evaluación de riesgo id_pregunta (string) con id_respuesta (string) y los resultados del ausentismo: ausentismo_eg_si_no (string), ausentismo_al_si_no (string) con dias_ausencia (int). Esta tabla tiene un procedimiento paso a paso de transformación y calificación que permite construir una base de datos de trabajadores calificados en dimensiones, dominios y factores siguiendo los baremos estándar del Ministerio del trabajo Colombia para determinar las magnitudes de la exposición a los factores de riesgo y su benchmarking con Colombia y sectores, lo cual se representa visualmente en el entregable 1: dashboard_v1_riesgo. Este procedimiento de calificación se corre para cada empresa de forma independiente, aunque los resultados puedan consolidarse en una sola base de datos##
## Estos datos se relacionan con tablas dimensionales que permiten filtrar, segmentar y analizar posteriormente los datos. La tabla hechos y las tablas dimensionales, se relacionan con PK (primary key) cedula (string), y se correlaciona FK (features Key) empresa, (string) sector_economico (string), id_respuesta (string) y forma_intr (string)##
##  En las tablas dimensionales se tiene un set de variables que permiten la segmentación de los resultados: 1. tabla planta_personal: empresa, (string) sector_economico (string) y forma_intr (string), area_departamento (string), categoría_cargo (string), tipo_cargo (string), es_jefe (string), tipo_contrato (string), tipo_contratacion (string), departamentodelpais_trabajo(string), ciudad_o_municipio_trabaja(string), modalidad_de_trabajo(string), tipo_teletrabajo_si_aplica(string), nombre_jefe(string). 2. Tabla ficha_demografia: fecha_aplicacion (fecha), nivel_escolaridad (string), estrato_economico(int), tipo_vivienda(string), sexo(string), edad_cumplida(int) antiguedad_empresa_años_cumplidos(float), estado_civil(string), numero_dependientes_economicos(int), horas_trabajo_diario	tipo_salario(float), departamento_pais_residencia(string), municipio_o_ciudad_residencia(string), antiguedad_en_cargo_años_cumplidos(float)##
## Con estas variables, la calificación y relaciones, se genera la 1 base de datos de resultados de riesgo psicosocial de los trabajadores y el visualizador 1: dashboard_v1_riesgo## 
## Antes de pasar a la transformación y calificación, se debe correr un script de limpieza de las tablas que permita revisar por cada empresa las categorías y respuestas, detectar campos vacíos, outliers, mezcla entre minúsculas y mayúsculas, categorías que pueden unificarse entre otras validaciones estándar para asegurar la calidad del dato a procesar##

## Entregable 4: Visualizador de alto impacto de resultados sociodemográficos y de salud de la población dashboard_v4_asis.Antes de pasar a extracción de datos de la tabla: ficha_demografia y ausentismo_12meses, se debe correr un script de limpieza de las tablas que permita revisar por cada empresa las categorías y respuestas, detectar campos vacíos, outliers, mezcla entre minúsculas y mayúsculas, categorías que pueden unificarse entre otras validaciones estándar para asegurar la calidad del dato a procesar.El proceso de extracción de datos en este caso, no aplica transformaciones adicionales, sino que genera directamente, luego de la limpieza, una base de datos para informe descriptivo, diagnóstico, del perfil ASIS de la población evaluada(dashboard_v4_asis).Partiendo de la tabla dimensional ficha_demografia y ausentismo_12 meses, se corre un plan de análisis descriptivo univariado o bivariado: Tabla ficha_demografia: fecha_aplicacion (fecha), nivel_escolaridad (string), estrato_economico(int), tipo_vivienda(string), sexo(string), edad_cumplida(int) antiguedad_empresa_años_cumplidos(float), estado_civil(string), numero_dependientes_economicos(int), horas_trabajo_diario	tipo_salario(float), departamento_pais_residencia(string), municipio_o_ciudad_residencia(string), antiguedad_en_cargo_años_cumplidos(float). + Tabla ausentismo_12meses: diagnostico_CIE (string), dias_ausencia(int), tipo_ausentismo(string)##.

## Entregable 2 y 3: calificación de resultados en el modelo de gestión, priorización y activación de líneas de gestión (base de datos + visualizador 2: dashboard_v2_gestion) y tablero de KPIs para equipos directivos 3. dashboard_v3_gerencial.El proceso de extracción de datos de estos entregables para posterior transformación independiente a la del entregable 1, parte de 1 tabla de hechos fact_respuestas que contiene como hechos: respuestas a evaluación de riesgo id_pregunta (string) con id_respuesta (string) y los resultados del ausentismo: ausentismo_eg_si_no (string), ausentismo_al_si_no (string) con dias_ausencia (int). Esta tabla tiene un procedimiento paso a paso de transformación y calificación que permite construir una base de datos de trabajadores calificados indicadores, líneas de gestión y ejes de gestión con pesos y prioridades para orientar las intervenciones en los protocolos específicos, lo cual se representa visualmente en el entregable 2: dashboard_v2_gestion. Este procedimiento de calificación se corre para cada empresa de forma independiente, aunque los resultados puedan consolidarse en una sola base de datos.  Para estos entregables, se conecta al modelo las variables de la tabla dimensional: categorias_analisis, la cual conecta relaciones y transformaciones que permiten analizar las prioridades de la gestión y hablar el lenguaje del sector económico y la empresa. ##

## Dado que estos entregables, suponen un procesamiento adicional de resultados de cada trabajador evaluado, esta información se convierte en una tabla de hechos adicional que toma los resultados de la tabla hecho fact_respuestas y entrega resultados adicionales por trabajador relacionando variables de fac_respuestas: cedula (PK), id_pregunta (FK), id_respuesta (FK), forma_intra (PK), sector_economico (FK) con los resultados del proceso de transformación que se corre paso a paso con base en modelo y lineamientos Avantum. Los resultados que se obtienen son por indicador(string), linea_gestion(string) y eje_gestion(string).##

## Las variables adicionales de la tabla categorías_analisis funcionan como dimensiones para filtrar y segmentar los resultados: factor(string), dimensión(string), pregunta(string),protocolo_id(string), protocolo_gestion(string)##.

## 2.	Requerimientos de los dashboard entregables ##
## Lineamientos para generar visualizador 1: dashboardriesgo.py##
## debe ser un lienzo que se desplace en vertical de 3000 x 2000 que siga lineamientos UX/UI de alta fidelidad en visualizadores de alto impacto, profesional, confiable, usuarios empresariales y técnicos##

## Estructura de visualizador ##
## Seccion 0 : Head o encabezado#
## Aparece el encabezado del visualizador y datos generales: “Resultados evaluación de factores de riesgo psicosocial” | “Empresa” (** debe indicarse el nombre de la empresa, es 1 visualizador por empresa**)  |“Sector económico”|  “#población evaluada” (** debe indicarse la cantidad de personas evaluadas para la empresa del visualizador**) | “% cobertura” (** debe calcularse cobertura: total de personas evaluadas en la empresa/total personas en la planta_personal**) | “Fecha evaluacion (dd-mm-aaaa”)##
## Seccion 1: Filtros de segmentación##
## Aparecen los filtros para segmentar los datos. (** Estos filtros vienen de las tablas dimensionales planta_personal, ficha_demografia**): “area_departamento”, “categoría_cargo”, “modalidad de trabajo”, “nombre del jefe”, “edad_cumplida”, “antigüedad_empresa”##

## Seccion 2: KPIs de la evaluación: Tarjetas tipo KPI##
## |nombre de la tarjeta|descripción|contenido|indicaciones|##
## 2.1 |“% riesgo A-MA intralaboral A y B” | Tarjeta KPI 1: Tarjeta multi row con 3 datos: total intra A, total intra B, total sector o país|. Indica el resultado del total intralaboral Forma A: % riesgo alto+muy alto y Forma B: %riesgo alto+muy alto. Muestra como referencia el % total intralaboral del sector económico que corresponde a la empresa, si el sector no tiene este referente, traer el resultado de país|La tarjeta muestra el nombre y el %. Tarjeta que permita 3 kpis para verlos de forma comparada, se trae el % con 1 solo decimal, se semaforiza el color de fuente del kpi cuando el valor es >35% “rojo”, entre 15 - 34% “amarillo”, menos de 15% “verde”|##
## 2.2 | “Trabajadores A+B con estrés A+MA” | Tarjeta KPI 2: Tarjeta multi row con 3 datos: total de trabajadores estrés, % del total evaluados y dato % país| Se suma el total de trabajadores tanto forma A como B, con estrés alto y muy alto (A+MA), se representa el % al que equivale este número, del total de evaluados en la empresa y se referencia el % de estrés actual en el país| La tarjeta muestra el nombre y el % y semaforiza la diferencia de puntos porcentuales de la empresa respecto al país|##
## 2.3 | “Vulnerabilidad psicológica” | Tarjeta KPI 3: Tarjeta multi row con 3 datos: total de trabajadores con vulnerabilidad (factor individual bajo y muy bajo), % del total evaluados y dato % país| Se suma el total de trabajadores tanto forma A como B, con vulnerbilidad, se representa el % al que equivale este número, del total de evaluados en la empresa y se referencia el % de vulnerabilidad en el país| La tarjeta muestra el nombre y el % y semaforiza la diferencia de puntos porcentuales de la empresa respecto al país|##
## 2.4 |“% riesgo A-MA extralaboral A y B” | Tarjeta KPI 4: Tarjeta multi row con 3 datos: total extra A, total extra B, total país|. Indica el resultado del total extralaboral Forma A: % riesgo alto+muy alto y Forma B: %riesgo alto+muy alto. Muestra como referencia el % total extralaboral del país|La tarjeta muestra el nombre y el %. Tarjeta que permita 3 kpis para verlos de forma comparada, se trae el % con 1 solo decimal, se semaforiza el color de fuente del kpi cuando el valor es >35% “rojo”, entre 15 - 34% “amarillo”, menos de 15% “verde”|##
## 2.5 |“Dimensiones críticas” | Tarjeta KPI 5: Tarjeta uno solo valor: conteo del total de dimensiones que se encuentran con diferencia % por encima del referente país| La tarjeta contiene el conteo de las dimensiones que se encuentran por encima del referente nacional| La tarjeta muestra el nombre y el conteo |##

## Tablero 2: dashboard_v2_gestion##
## N	Seccion|	Descripcion general|Visualizaciones sugeridas##
## 1	Resultados por eje y linea de gestión|	N y % de trabajadores en los 5 niveles de prioridad de gestion de los ejes y líneas de gestión	|Barras apiladas comparativas entre ejes y lineas Leyenda con interpretacion de los niveles de gestion y enfoques de gestión##
## 2	Resultados por indicadores de gestión	|Poblacion a intervenir, estrategias de acción, kpi y objetivos	|Tabla con semáforo de prioridades, que muestre los 5 primeros protocolos de prioridad legal y tecnica según el sector económico y como está la empresa en cada línea de gestión asociada a los mismos. Asociar a cada línea de gestión priorizada, # de trabajadores en nivel urgente y correctivo, principales áreas a impactar, objetivos estratégicos y kpi sugerido a cada línea de gestion##
## 3	Resultados por indicadores de gestión	|Top 10 de indicadores de alta prioridad según forma a y b que obtiene nivel de prioridad de gestión urgente o correctiva.	|Headmap con top 10 de indicadores de mayor prioridad en la empresa y su comparativo como están estos 10 indicadores de forma discriminada por áreas de trabajo_departamentos##

## Tablero 3: dashboard_v3_gerencial##
## N	Seccion|	Descripcion general|	Visualizaciones sugeridas##
## 1	Ficha técnica general	|Header con empresa, fecha evaluación, #evaluados, cobertura obtenida. 	|Barras apiladas comparativas entre ejes y lineas Leyenda con interpretacion de los niveles de gestion y enfoques de gestión##
## 2	Resultados globales y benchmarking	|% de personas en nivel alto y muy alto intralaboral A y B, comparado con datos de sectores económicos en Colombia. Establecer diferencia (+ o -) en puntos porcentuales y semaforizar	|Tabla semaforizada tipo profesional, muestra frecuencias absolutas y relativas Semaforizar riesgo alto y muy alto >35%, semaforizar si hay diferencia en puntos porcentuales respecto a sector##
## 3	Resultados globales por los 3 ejes de gestión	|N y % de trabajadores en los 5 niveles de prioridad de la gestión	|Tabla que detalla 5 líneas de gestion prioritarias, poblacion, áreas principales, objetivo estrategico y KPIs##
## 4	Ranking áreas críticas top 5	Visual comparativa entre la empresa y las áreas_departamentos de trabajo	Headmap que semaforice los niveles de prioridad en la empresa, y las áreas para el top 5 de las líneas de gestión##
## 5	Referenciar los 5 protocolos claves en el sector económico de la empresa por orden de prioridad.##

## Tablero 4: dashboard_v4_asis##
## N	Seccion	|Descripcion general	|Visualizaciones sugeridas##
## 1	Distribución demográfica	||Sexo, edad, estrato, nivel escolaridad, estado civil, numero de dependientes	Pirámide poblacional sexo y edad (grupos de cada 10 años) según datos de la empresa|Graficos de barras##
## 2	Perfil laboral|Distribucion población por área y cargo|Tiempo de antigüedad en cargo y empresa|Tablas de distribución|Graficos de barras - histogramas##  
## 3	Condiciones de salud y costos morbilidad	|Total de trabajadores con ausentismo por tipo de causa y severidad	|Donut stack: % de ausentismo por tipo de ausentismo    Prevalencia de ausentismo y accidentalidad en tarjetas Promedio días de incapacidad Costos económicos en perdida de productividad estimados por ausentismo 5 principales causas_diagnósticos de ausentismo.
# 3. Pasos para la creacion de los dashboards##
## 1. visualizador 1: dashboard_v1_resultados##
### Paso 1###
## 1.Codificar escalas de texto a numero las respuestas del instrumento se capturan en texto, deben pasarse a codificación numérica
## id_pregunta|    forma_intra|	escala|	codificar_numero##
## 1 a 105|intralaboral_A|	Siempre|4##
## 1 a 105|intralaboral_A|	Casi siempre|	3##
## 1 a 105|intralaboral_A|	Algunas veces|	2##
## 1 a 105|intralaboral_A|	Casi nunca|1##
## 1 a 105|intralaboral_A|nunca|0##
## 106|intralaboral_A|si|1##
## 106|intralaboral_A|no|0##
## 107 a 115|intralaboral_A|Siempre|4##
## 107 a 115|intralaboral_A|Casi siempre|3##
## 107 a 115|intralaboral_A|Algunas veces|2##
## 107 a 115|intralaboral_A|Casi nunca|1##
## 107 a 115|intralaboral_A|nunca|0##
## 116|intralaboral_A|si|1##
## 116|intralaboral_A|no|0##
## 117 a 125|intralaboral_A|Siempre|4##
## 117 a 125|intralaboral_A|Casi siempre|3##
## 117 a 125|intralaboral_A|Algunas veces|2##
## 117 a 125|intralaboral_A|Casi nunca|1##
## 117 a 125|intralaboral_A|nunca|0##
## 1 a 88|intralaboral_B|Siempre|4##
## 1 a 88|intralaboral_B|Casi siempre|3##
## 1 a 88|intralaboral_B|Algunas veces|2##
## 1 a 88|intralaboral_B|Casi nunca|1##
## 1 a 88|intralaboral_B|nunca|0##
## 89|intralaboral_B|si|1##
## 89|intralaboral_B|no|0##
## 90 a 98|intralaboral_B|Siempre|4##
## 90 a 98|intralaboral_B|Casi siempre|3##
## 90 a 98|intralaboral_B|Algunas veces|2##
## 90 a 98|intralaboral_B|Casi nunca|1##
## 90 a 98|intralaboral_B|nunca|0##
## 1 a 31|extralaboral|Siempre|4##
## 1 a 31|extralaboral|Casi siempre|3##
## 1 a 31|extralaboral|Algunas veces|2##
## 1 a 31|extralaboral|Casi nunca|1##
## 1 a 31|extralaboral|nunca|0##
## 1,2,3,9,13,14,15,23,24|estres|Siempre|9##
## 1,2,3,9,13,14,15,23,24|estres|Casi siempre|6##
## 1,2,3,9,13,14,15,23,24|estres|A veces|3##
## 1,2,3,9,13,14,15,23,24|estres|Nunca|0##
## 4,5,6,10,11,16,17,18,19,25,26,27,28|estres|Siempre|6##
## 4,5,6,10,11,16,17,18,19,25,26,27,28|estres|Casi siempre|4##
## 4,5,6,10,11,16,17,18,19,25,26,27,28|estres|A veces|2##
## 4,5,6,10,11,16,17,18,19,25,26,27,28|estres|Nunca|0##
## 7,8,12,20,21,22,29,30,31|estres|Siempre|3##
## 7,8,12,20,21,22,29,30,31|estres|Casi siempre|2##
## 7,8,12,20,21,22,29,30,31|estres|A veces|1##
## 7,8,12,20,21,22,29,30,31|estres|Nunca|0##
## 1 a 12|afrontamiento|nunca hago eso|0##
## 1 a 12|afrontamiento|a veces hago eso|0.5##
## 1 a 12|afrontamiento|frecuentemente hago eso|0.7##
## 1 a 12|afrontamiento|siempre hago eso|1##
## 1 a 12|capital_psicologico|totalmente en desacuerdo|0##
## 1 a 12|capital_psicologico|en desacuerdo|0.5##
## 1 a 12|capital_psicologico|de acuerdo|0.7##
## 1 a 12|capital_psicologico|totalmente de acuerdo|1##

### Paso 2###
## 2. items invertidos_invertir sentido al siguiente conjunto de items, debe ser invertida el sentido de su codificación aplicada en el paso 1
## id_pregunta|	forma_intra|	escala|	invertir_a##
## 4, 5, 6, 9, 12, 14, 32, 34, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99, 100, 101, 102, 103, 104, 105.	|intralaboral_A|	Siempre|	0##
#   4, 5, 6, 9, 12, 14, 32, 34, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99, 100, 101, 102, 103, 104, 105.	|intralaboral_A	|Casi siempre|	1##
#   #4, 5, 6   9, 12, 14, 32, 34, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99, 100, 101, 102, 103, 104, 105.	|intralaboral_A|	Algunas veces|	2##
## 4, 5, 6, 9, 12, 14, 32, 34, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99, 100, 101, 102, 103, 104, 105.	intralaboral_A|	Casi nunca|	3##
#   #4, 5, 6, 9, 12, 14, 32, 34, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99, 100, 101, 102, 103, 104, 105.	intralaboral_A|	nunca|	4## 
## 4, 5, 6, 9, 12, 14, 22, 24, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 98.	intralaboral_B|	Siempre|	0##
## 4, 5, 6, 9, 12, 14, 22, 24, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 1, 4, 5, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 25, 27, 29	extralaboral|	Casi siempre|	1##
## 1, 4, 5, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 25, 27, 29	extralaboral|	Algunas veces|	2##
## 1, 4, 5, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 25, 27, 29	extralaboral|	Casi nunca|	3##
## 1, 4, 5, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 25, 27, 29	extralaboral|	nunca|	4##
## 5,6,7,8	|afrontamiento|	nunca hago eso|	1##
## 5,6,7,8	|afrontamiento|	a veces hago eso|	0.7##
## 5,6,7,8	|afrontamiento|	frecuentemente hago eso|	0.5##
## 5,6,7,8	|afrontamiento|	siempre hago eso	0##


### Paso 3###
## 3. Agrupación intralaboral_A item_a_dimensión los items _intraA deben agruparse en dimensiones, dominio y factor##    
## factor	|dominio	|dimension	|Id_pregunta A##
## Intralaboral_A	|Liderazgo y relaciones sociales en el trabajo|	Características del liderazgo|	63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75##	
## Intralaboral_A|	Liderazgo y relaciones sociales en el trabajo|	Relaciones sociales en el trabajo|	76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89##
## Intralaboral_A	|Liderazgo y relaciones sociales en el trabajo	|Retroalimentación del desempeño	|90, 91, 92, 93, 94##
## Intralaboral_A	|Liderazgo y relaciones sociales en el trabajo	Relación con los colaboradores	| 117, 118, 119, 120, 121, 122, 123, 124,125##
## Intralaboral_A	|Control sobre el trabajo	|Claridad de rol	|53, 54, 55, 56, 57, 58, 59##
## Intralaboral_A	|Control sobre el trabajo	|Capacitación	|60, 61, 62##
## Intralaboral_A	|Control sobre el trabajo	|Participación y manejo del cambio	|48, 49, 50, 51##
## Intralaboral_A	|Control sobre el trabajo	|Oportunidades para el uso y desarrollo de habilidades y conocimientos	|39, 40, 41, 42##
## Intralaboral_A	|Control sobre el trabajo	|Control y autonomía sobre el trabajo	|44, 45, 46##
## Intralaboral_A	|Demandas del trabajo	|Demandas ambientales y de esfuerzo físico	|1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12##
## Intralaboral_A	|Demandas del trabajo	|Demandas emocionales	|107, 108, 109, 110, 111, 112, 113, 114,115##
## Intralaboral_A	|Demandas del trabajo	|Demandas cuantitativas	|13, 14, 15, 32, 43, 47##
## Intralaboral_A	|Demandas del trabajo	|Influencia del trabajo sobre el entorno extralaboral	|35, 36, 37, 38##
## Intralaboral_A	|Demandas del trabajo	|Exigencias de responsabilidad del cargo	|19, 22, 23, 24, 25, 26##
## Intralaboral_A	|Demandas del trabajo	|Demandas de carga mental	|16, 17, 18, 20, 21##
## Intralaboral_A	|Demandas del trabajo	|Consistencia del rol	|27, 28, 29, 30, 52##
## Intralaboral_A	|Demandas del trabajo	|Demandas de la jornada de trabajo	|31, 33, 34##
## Intralaboral_A	|Recompensas	|Recompensas derivadas de la pertenencia a la organización y del trabajo que se realiza	|95, 102, 103, 104, 105##
## Intralaboral_A	|Recompensas	|Reconocimiento y compensación	|96, 97, 98, 99, 100, 101##

### Paso 4##
## 4. Agrupación intralaboral_B item_a_dimensión los items _intraB deben agruparse en dimensiones, dominio y factor##
## factor	|dominio	dimension	|Id_pregunta B##
##  Intralaboral_B|	Liderazgo y relaciones sociales en el trabajo	|Características del liderazgo	|49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61##
##  Intralaboral_B|	Liderazgo y relaciones sociales en el trabajo	|Relaciones sociales en el trabajo	|62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73##
##  Intralaboral_B|	Liderazgo y relaciones sociales en el trabajo	|Retroalimentación del desempeño	|74, 75, 76, 77, 78##
##  Intralaboral_B|	Liderazgo y relaciones sociales en el trabajo	|Relación con los colaboradores	|No aplica##
##  Intralaboral_B|	Control sobre el trabajo	|Claridad de rol	|41, 42, 43, 44, 45##
##  Intralaboral_B|	Control sobre el trabajo	|Capacitación	|46, 47, 48##
##  Intralaboral_B|	Control sobre el trabajo	|Participación y manejo del cambio	|38, 39, 40##
##  Intralaboral_B|	Control sobre el trabajo	|Oportunidades para el uso y desarrollo de habilidades y conocimientos	|29, 30, 31, 32##
##  Intralaboral_B|	Control sobre el trabajo	|Control y autonomía sobre el trabajo	|34, 35, 36##
##  Intralaboral_B|	Demandas del trabajo	|Demandas ambientales y de esfuerzo físico	|1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12##
##  Intralaboral_B|	Demandas del trabajo	|Demandas emocionales	|90, 91, 92, 93, 94, 95, 96, 97,98##
##  Intralaboral_B|	Demandas del trabajo	|Demandas cuantitativas	|13, 14, 15##
##  Intralaboral_B|	Demandas del trabajo	|Influencia del trabajo sobre el entorno extralaboral	|25, 26, 27, 28##
##  Intralaboral_B|	Demandas del trabajo	|Exigencias de responsabilidad del cargo	|No evalúa##
##  Intralaboral_B|	Demandas del trabajo	|Demandas de carga mental	|16, 17, 18, 19, 20##
##  Intralaboral_B|	Demandas del trabajo	|Consistencia del rol	|No evalúa##
##  Intralaboral_B|	Demandas del trabajo	|Demandas de la jornada de trabajo	|21, 22, 23, 24, 33, 37##
##  Intralaboral_B|	Recompensas	|Recompensas derivadas de la pertenencia a la organización y del trabajo que se realiza	|85, 86, 87, 88##
##  Intralaboral_B|	Recompensas	|Reconocimiento y compensación	|79, 80, 81, 82, 83, 84##

## 5. Agrupación extralaboral_ item_a_dimensión lo items _extra se agrupan en dimensiones, las cuales aplican tanto para A como para B##
## Factor	|dimension	|Id_pregunta##
## Extralaboral	|Balance entre la vida laboral y familiar|	14,15,16,17##
## Extralaboral|	Relaciones familiares|	22,25,27##
## Extralaboral|	Comunicación y relaciones interpersonales|	18,19,20,21,23##
## Extralaboral|	Situación económica del grupo familiar|	29,30,31##
## Extralaboral|	Características de la vivienda y de su entorno|	5,6,7,8,9,10,11,12,13##
## Extralaboral|	Influencia del entorno extralaboral sobre el trabajo|	24,26,28##
## Extralaboral|	Desplazamiento vivienda trabajo vivienda|	1,2,3,4##

## paso 6##
## 6. Agrupación estrés_ item_a_dimensión los items _estres se agrupan en 1 solo factor y dimensión, tanto en A como en B##
## Factor	|dimension|	Id_pregunta##
## estres	|estres|	1 al 31##

## paso 7##
## 7. Agrupación afrontamiento_ item_a_dimensión los items _afrontamiento se agrupan en factor y dimensión, tanto en A como en B##
## Factor	|dimension|	Id_pregunta##
## Individual|	afrontamiento_activo_planificacion	|1 a 4##
## Individual|	afrontamiento_pasivo_negacion	|5 a 8##
## Individual|	afrontamiento_activo_busquedasoporte	|9 a 12##

## paso 8##
## 8. Agrupación capitalpsicologico_ item_a_dimensión los items _capitalpsicologico se agrupan en factor y dimensión, tanto en A como en B##
## Factor	|dimension|	Id_pregunta##
## Individual|	Optimismo	|1 a 3##
## Individual|	Esperanza	|4 a 6##
## Individual|	Resiliencia	|7 a 9##
## Individual|	Autoeficacia	|9 a 12##

## paso 9##
## 9. Baremos Bateria Ministerio Colombia— Calificación y tablas de puntos de corte dimensiones intralaboral por forma A y B las dimensiones intralaboral se califican segun sumatoria de respuestas y puntajes de transformación  obteniendo el puntaje bruto, luego se categorizan en 5 niveles de riesgo obteniendo puntaje transformado. Cada persona evaluada obtiene para cada dimensión intralaboral un puntaje bruto y un puntaje transformado en 5 niveles de riesgo##
## forma_intra|	factor	|dominio|	dimension	|formula_calificacion|	Puntaje_transformación	| sin_riesgo_max |	 bajo_max 	| medio_max |	 alto_max 	| muy_alto_max##
## A	intralaboral|	demandas_del_trabajo|	Demandas ambientales y de esfuerzo físico|	(sumatoria de puntajes brutos obtenidos en todas las preguntas demandasambientales A/puntaje de transformacion)*100	|48	|14.6	|22.9	|31.3	39.6|	100##
## A	intralaboral|	demandas_del_trabajo|	Demandas emocionales|	(sumatoria de puntajes brutos obtenidos en todas las preguntas demandasemocionales A/puntaje de transformacion)*100	|36	|16.7	|25	|33.3	47.2|	100##
## A	intralaboral|	demandas_del_trabajo|	Demandas cuantitativas|	(sumatoria de puntajes brutos obtenidos en todas las preguntas demandascuantitativas A/puntaje de transformacion)*100	|24	|25	|33.3	|45.8	54.2|	100##
## A	intralaboral|	demandas_del_trabajo|	Influencia del trabajo sobre el entorno extra|	(sumatoria de puntajes brutos obtenidos en todas las preguntas influenciadeltrabajo A/puntaje de transformacion)*100	|16	|18.8	|31.3	|43.8	50|	100##
## A	intralaboral|	demandas_del_trabajo|	Exigencias de responsabilidad del cargo|	(sumatoria de puntajes brutos obtenidos en todas las preguntas exigenciasderesponsabilidad A/puntaje de transformacion)*100	|24	|37.5	|54.2	|66.7	79.2|	100##
## A	intralaboral|	demandas_del_trabajo|	Demandas de carga mental|	(sumatoria de puntajes brutos obtenidos en todas las preguntas demandascargamental A/puntaje de transformacion)*100	|20	|60	|70	|80	90|	100##
## A	intralaboral|	demandas_del_trabajo|	Consistencia del rol|	(sumatoria de puntajes brutos obtenidos en todas las preguntas consistenciaderol A/puntaje de transformacion)*100	|20	|15	|25	|35	45|	100##
## A	intralaboral|	demandas_del_trabajo|	Demandas de la jornada de trabajo|	(sumatoria de puntajes brutos obtenidos en todas las preguntas demandasdejornada A/puntaje de transformacion)*100	|12	|8.3	|25	|33.3	50|	100##
## A	intralaboral|	control_sobre_el_trabajo|	Claridad de rol|	(sumatoria de puntajes brutos obtenidos en todas las preguntas claridadrolA/puntaje de transformacion)*100	|28	|0.9	|10.7	|21.4	39.3|	100##
## A	intralaboral|	control_sobre_el_trabajo|	Capacitación|	(sumatoria de puntajes brutos obtenidos en todas las preguntas capacitacion A/puntaje de transformacion)*100	|12	|0.9	|16.7	|33.3	50|	100##
## A	intralaboral|	control_sobre_el_trabajo|	Participación y manejo del cambio|	(sumatoria de puntajes brutos obtenidos en todas las preguntas participacionycambio A/puntaje de transformacion)*100	|16	|12.5	|25	|37.5	50|	100##
## A	intralaboral|	control_sobre_el_trabajo|	Oportunidades de desarrollo y uso de habilidades y conocimientos|	(sumatoria de puntajes brutos obtenidos en todas las preguntas oportunidadesparadllo A/puntaje de transformacion)*100	|16	|0.9	|6.3	|18.8	31.3|	100##
## A	intralaboral|	control_sobre_el_trabajo|	Control y autonomía sobre el trabajo|	(sumatoria de puntajes brutos obtenidos en todas las preguntas controlyautonomia A/puntaje de transformacion)*100	|12	|8.3	|25	|41.7	58.3|	100##
## A	intralaboral|	liderazgo_relaciones_sociales|	Características del liderazgo|	(sumatoria de puntajes brutos obtenidos en todas las preguntas caracteristicasliderazgo A/puntaje de transformacion)*100	|52	|3.8	|15.4	|30.8	46.2|	100##
## A	intralaboral|	liderazgo_relaciones_sociales|	Relaciones sociales en el trabajo|	(sumatoria de puntajes brutos obtenidos en todas las preguntas relacionessociales A/puntaje de transformacion)*100	|56	|5.4	|16.1	|25	37.5|	100##
## A	intralaboral|	liderazgo_relaciones_sociales|	Retroalimentación del desempeño|	(sumatoria de puntajes brutos obtenidos en todas las preguntas retroalimentacion A/puntaje de transformacion)*100	|20	|10	|25	|40	55|	100##
## A	intralaboral|	liderazgo_relaciones_sociales|	Relación con los colaboradores (subordinados)|	(sumatoria de puntajes brutos obtenidos en todas las preguntas relacionconcolaboradores A/puntaje de transformacion)*100	|36	|13.9	|25	|33.3	47.2|	100##
## A	intralaboral|	recompensas|	Recompensas derivadas de la pertenencia a la|	(sumatoria de puntajes brutos obtenidos en todas las preguntas recompensas A/puntaje de transformacion)*100	|20	|0.9	|5	|10	20|	100##
## A	intralaboral|	recompensas|	Reconocimiento y compensación|	(sumatoria de puntajes brutos obtenidos en todas las preguntas reconocimiento A/puntaje de transformacion)*100	|24	|4.2	|16.7	|25	37.5|	100##
## B	intralaboral|	demandas_del_trabajo|	Demandas ambientales y de esfuerzo físico|	(sumatoria de puntajes brutos obtenidos en todas las preguntas demandasambientales B/puntaje de transformacion)*100	|48	|22.9	|31.3	|39.6	47.9|	100##
## B	intralaboral|	demandas_del_trabajo|	Demandas emocionales|	(sumatoria de puntajes brutos obtenidos en todas las preguntas demandasemocionales B/puntaje de transformacion)*100	|36	|19.4	|27.8	|38.9	47.2|	100##
## B	intralaboral|	demandas_del_trabajo|	Demandas cuantitativas|	(sumatoria de puntajes brutos obtenidos en todas las preguntas demandascuantitativas B/puntaje de transformacion)*100	|12	|16.7	|33.3	|41.7	50|	100##
## B	intralaboral|	demandas_del_trabajo|	Influencia del trabajo sobre el entorno extra|	(sumatoria de puntajes brutos obtenidos en todas las preguntas influenciadeltrabajo B/puntaje de transformacion)*100	|16	|12.5	|25	|31.3	50|	100##
## B	intralaboral|	demandas_del_trabajo|	Demandas de carga mental|	(sumatoria de puntajes brutos obtenidos en todas las preguntas demandascargamental B/puntaje de transformacion)*100	|20	|50	|65	|75	85|	100##
## B	intralaboral|	demandas_del_trabajo|	Demandas de la jornada de trabajo|	(sumatoria de puntajes brutos obtenidos en todas las preguntas demandasdejornada B/puntaje de transformacion)*100	|24	|25	|37.5	|45.8	58.3|	100##
## B	intralaboral|	control_sobre_el_trabajo|	Claridad de rol|	(sumatoria de puntajes brutos obtenidos en todas las preguntas claridadrol B/puntaje de transformacion)*100	|20	|0.9	5	15	30|	100##
## B	intralaboral|	control_sobre_el_trabajo|	Capacitación|	(sumatoria de puntajes brutos obtenidos en todas las preguntas capacitacion B/puntaje de transformacion)*100	|12	|0.9	16.7	25	50|	100##
## B	intralaboral|	control_sobre_el_trabajo|	Participación y manejo del cambio|	(sumatoria de puntajes brutos obtenidos en todas las preguntas participacionycambio B/puntaje de transformacion)*100	|12	|16.7	33.3	41.7	58.3|	100##
## B	intralaboral|	control_sobre_el_trabajo|	Oportunidades de desarrollo y uso de habilidad|	(sumatoria de puntajes brutos obtenidos en todas las preguntas oportunidadesparadllo B/puntaje de transformacion)*100	|16	|12.5	25	37.5	56.3|	100##
## B	intralaboral|	control_sobre_el_trabajo|	Control y autonomía sobre el trabajo|	(sumatoria de puntajes brutos obtenidos en todas las preguntas controlyautonomia B/puntaje de transformacion)*100	|12	|33.3	50	66.7	75|	100##
## B	intralaboral|	liderazgo_relaciones_sociales|	Características del liderazgo|	(sumatoria de puntajes brutos obtenidos en todas las preguntas caracteristicasliderazgo B/puntaje de transformacion)*100	|52	|3.8	13.5	25	38.5|	100##
## B	intralaboral|	liderazgo_relaciones_sociales|	Relaciones sociales en el trabajo|	(sumatoria de puntajes brutos obtenidos en todas las preguntas relacionessociales B/puntaje de transformacion)*100	|48	|6.3	14.6	27.1	37.5|	100##
## B	intralaboral|	liderazgo_relaciones_sociales|	Retroalimentación del desempeño|	(sumatoria de puntajes brutos obtenidos en todas las preguntas retroalimentacion B/puntaje de transformacion)*100	|20	|5	20	30	50|	100##
## B	intralaboral|	recompensas|	Recompensas derivadas de la pertenencia a la|	(sumatoria de puntajes brutos obtenidos en todas las preguntas recompensas B/puntaje de transformacion)*100	|16	|0.9	6.3	12.5	18.8|	100##
## B	intralaboral|	recompensas|	Reconocimiento y compensación|	(sumatoria de puntajes brutos obtenidos en todas las preguntas reconocimiento B/puntaje de transformacion)*100	|24	|0.9	12.5	25	37.5|	100##

## paso 10##
## 10. Baremos Bateria Ministerio Colombia— Calificación y tablas de puntos de corte dimensiones extralaboral por forma A y B las dimensiones extralaboral se califican segun sumatoria de respuestas y puntajes de transformación  obteniendo el puntaje bruto, luego se categorizan en 5 niveles de riesgo obteniendo puntaje transformado. Cada persona evaluada obtiene para cada dimensión extralaboral un puntaje bruto y un puntaje transformado en 5 niveles de riesgo##
## forma_intra|	factor|	dominio|dimension	|formula_calificacion|	Puntaje_transformación	|sin_riesgo_max| 	 bajo_max |	 medio_max |	 alto_max| 	 muy_alto_max##
## A	|extralaboral|	n/a	|Balance entre la vida laboral y familiar|	(sumatoria de puntajes brutos obtenidos en todas las preguntas balancevidatrabajo A/puntaje de transformacion)*100	|16	|6.3|	25	|37.5|	50|	100##
## A	|extralaboral|	n/a	|Relaciones familiares|	(sumatoria de puntajes brutos obtenidos en todas las preguntas relacionesfamiliares A/puntaje de transformacion)*100	|12	|8.3|	25	|33.3|	50|	100##
## A	|extralaboral|	n/a	|Comunicación y relaciones interpersonales|	(sumatoria de puntajes brutos obtenidos en todas las preguntas comunicacion A/puntaje de transformacion)*100	|20	|0.9|	10	|20|	30|	100##
## A	|extralaboral|	n/a	|Situación económica del grupo familiar|	(sumatoria de puntajes brutos obtenidos en todas las preguntas situacioneconomica A/puntaje de transformacion)*100	|12	|8.3|	25	|33.3|	50|	100##
## A	|extralaboral|	n/a	|Características de la vivienda y de su entorno|	(sumatoria de puntajes brutos obtenidos en todas las preguntas caracteristicasvivivenda A/puntaje de transformacion)*100	|36	|5.6|	11.1|	13.9|	22.2|	100##
## A	|extralaboral|	n/a	|Influencia del entorno extralaboral sobre el trabajo|	(sumatoria de puntajes brutos obtenidos en todas las preguntas influenciadelentornoextra A/puntaje de transformacion)*100	|12	|8.3|	16.7|	25|	41.7|	100##
## A	|extralaboral|	n/a	|Desplazamiento vivienda trabajo vivienda|	(sumatoria de puntajes brutos obtenidos en todas las preguntas desplazamientovivienda A/puntaje de transformacion)*100	|16	|0.9|	12.5|	25|	43.8|	100##
## B	|extralaboral|	n/a	|Balance entre la vida laboral y familiar|	(sumatoria de puntajes brutos obtenidos en todas las preguntas balancevidatrabajo B/puntaje de transformacion)*100	|16	|6.3|	25	|37.5|	50|	100##
## B	|extralaboral|	n/a	|Relaciones familiares|	(sumatoria de puntajes brutos obtenidos en todas las preguntas relacionesfamiliares B/puntaje de transformacion)*100	|12	|8.3|	25	|33.3|	50|	100##
## B	|extralaboral|	n/a	|Comunicación y relaciones interpersonales|	(sumatoria de puntajes brutos obtenidos en todas las preguntas comunicacion B/puntaje de transformacion)*100	|20	|5	|15	|25	|35	100##
## B	|extralaboral|	n/a	|Situación económica del grupo familiar|	(sumatoria de puntajes brutos obtenidos en todas las preguntas situacioneconomica B/puntaje de transformacion)*100	|12	|16.7|	25	|41.7|	50|	100##
## B	|extralaboral|	n/a	|Características de la vivienda y de su entorno|	(sumatoria de puntajes brutos obtenidos en todas las preguntas caracteristicasvivivenda B/puntaje de transformacion)*100	|36	|5.6|	11.1|	16.7|	27.8|	100##
## B	|extralaboral|	n/a	|Influencia del entorno extralaboral sobre el trabajo|	(sumatoria de puntajes brutos obtenidos en todas las preguntas influenciadelentornoextra B/puntaje de transformacion)*100	|12	|0.9|	16.7|	25|	41.7|	100##
## B	|extralaboral|	n/a	|Desplazamiento vivienda trabajo vivienda|	(sumatoria de puntajes brutos obtenidos en todas las preguntas desplazamientovivienda B/puntaje de transformacion)*100	|16	|0.9|	12.5|	25|	43.8|	100##

## paso 11##
## 11. Baremos Avantum— Calificación y tablas de puntos de corte dimensiones individual que aplican tanto para forma A como B las dimensiones individual se califican segun sumatoria de respuestas y puntajes de transformación  obteniendo el puntaje bruto, luego se categorizan en 5 niveles de protección psicológica obteniendo puntaje transformado. Cada persona evaluada obtiene para cada dimensión individual un puntaje bruto y un puntaje transformado en 5 niveles de protección psicológica##

## Afrontamiento## 
## forma_intra|	factor|	dominio	|dimension|	formula_calificacion	|	Puntaje_transformación	|	muyinadecuadaestrategiaafrontamiento_max 	|	inadecuadaestrategiaafrontamiento_max 	|	 algoadecuadaestrategiaafrontamiento_max 	|	adecuadaestrategiaafrontamiento_max 	|	muy adecuadaestrategiaafrontamiento_max ##
## Tanto A como B	|Individual|	Estrategias de Afrontamiento	|Afrontamiento activo_planificación|	(sumatoria de puntajes brutos obtenidos en todas las preguntas afrontamientoactivoplanificacion/puntaje de transformacion)*100	|	4	|	29	|	51	|	69	|	89	|	100##
## Tanto A como B	|Individual|	Estrategias de Afrontamiento	|Afrontamiento evitativo_negación|	(sumatoria de puntajes brutos obtenidos en todas las preguntas afrontamientoevitativonegacion (está invertido)/puntaje de transformacion)*100	|	4	|	29	|	51	|	69	|	89	|	100##
## Tanto A como B	|Individual|	Estrategias de Afrontamiento	|Afrontamiento activo_busquedasoporte|	(sumatoria de puntajes brutos obtenidos en todas las preguntas afrontamientobusquedasoporte/puntaje de transformacion)*100	|	4	|	29	|	51	|	69	|	89	|	100##

## Capitalpsicologico##
## forma_intra|	factor|	dominio|	dimension|	formula_calificacion|	Puntaje_transformación|	muybajocapitalpsicologico_max 	|	 bajocapitalpsicologico_max 	|	 mediocapitalpsicologico_max 	|	 altocapitalpsicologico_max 	|	muyaltocapitalpsicologico_max ##
## Tanto A como B	|Individual|	Capital psicológico|	Optimismo	|(sumatoria de puntajes brutos obtenidos en todas las preguntas optimismo/puntaje de transformacion)*100	|	3	|	29	|	51	|	69	|	89	|	100##
## Tanto A como B	|Individual|	Capital psicológico|	Esperanza	|(sumatoria de puntajes brutos obtenidos en todas las preguntasesperanza/puntaje de transformacion)*100	|	3	|	29	|	51	|	69	|	89	|	100##
## Tanto A como B	|Individual|	Capital psicológico|	Resiliencia	|(sumatoria de puntajes brutos obtenidos en todas las preguntas resiliencia/puntaje de transformacion)*100	|	3	|	29	|	51	|	69	|	89	|	100##
## Tanto A como B	|Individual|	Capital psicológico|	Autoeficacia	|(sumatoria de puntajes brutos obtenidos en todas las preguntas autoeficacia/puntaje de transformacion)*100	|	3	|	29	|	51	|	69	|	89	|	100##


## paso 12##
## 12. Baremos Bateria Ministerio Colombia— Calificación y tablas de puntos de corte dominios intra A y B los dominios intra se califican segun sumatoria de respuestas y puntajes de transformación  obteniendo el puntaje bruto, luego se categorizan en 5 niveles de riesgo obteniendo puntaje transformado. Cada persona evaluada obtiene para cada dominio un puntaje bruto y un puntaje transformado en 5 niveles de riesgo##
## forma_intra|	factor	|dominio	|formula_calificacion|	Puntaje_transformación	| sin_riesgo_max |	 bajo_max |	 medio_max |	 alto_max| 	 muy_alto_max##
## A|	intralaboral	|demandas_del_trabajo|	(sumatoria de puntajes brutos obtenidos en todas las preguntas demandasdeltrabajo A/puntaje de transformacion)*100|	200	|28.5	|	35	|	41.5	|	47.5	|	100##
## A|	intralaboral	|control_sobre_el_trabajo|	(sumatoria de puntajes brutos obtenidos en todas las preguntas controlsobreeltrabajo A/puntaje de transformacion)*100|	84	|10.7	|	19	|	29.8	|	40.5	|	100##
## A|	intralaboral	|liderazgo_relaciones_sociales|	(sumatoria de puntajes brutos obtenidos en todas las preguntas liderazgorelacionesociales A/puntaje de transformacion)*100|	164	|9.1	|	17.7	|	25.6	|	34.8	|	100##
## A|	intralaboral	|recompensas|	(sumatoria de puntajes brutos obtenidos en todas las preguntas recompensas A/puntaje de transformacion)*100|	44	|4.5	|	11.4	|	20.5	|	29.5	|	100##
## B|	intralaboral	|demandas_del_trabajo|	(sumatoria de puntajes brutos obtenidos en todas las preguntas demandasdeltrabajo B/puntaje de transformacion)*100|	156	|26.9	|	33.3	|	37.8	|	44.2	|	100##
## B|	intralaboral	|control_sobre_el_trabajo|	(sumatoria de puntajes brutos obtenidos en todas las preguntas controlsobreeltrabajo B/puntaje de transformacion)*100|	72	|19.4	|	26.4	|	34.7	|	43.1	|	100##
## B|	intralaboral	|liderazgo_relaciones_sociales|	(sumatoria de puntajes brutos obtenidos en todas las preguntas liderazgorelacionesociales B/puntaje de transformacion)*100|	120	|8.3	|	17.5	|	26.7	|	38.3	|	100##
## B|	intralaboral	|recompensas|	(sumatoria de puntajes brutos obtenidos en todas las preguntas recompensas B/puntaje de transformacion)*100|	40	|2.5	|	10	|	17.5	|	27.5	|	100##


## paso 13##
## 13. Baremos Avantum— Calificación y tablas de puntos de corte dominios individual los dominios individual se califican segun sumatoria de respuestas y puntajes de transformación  obteniendo el puntaje bruto, luego se categorizan en 5 niveles de protección psicológica obteniendo puntaje transformado. Cada persona evaluada obtiene para cada dominio un puntaje bruto y un puntaje transformado en 5 niveles de protección psicológica##

## afrontamiento##
## forma_intra|	factor	|dominio	|dimension	|formula_calificacion	|Puntaje_transformación|	 muybajaestrategiaafrontamiento_max |	 bajaestrategiaafrontamiento_max 	| mediaestrategiaafrontamiento_max 	| altaestrategiaafrontamiento_max 	| muyaltaestrategiaafrontamiento_max ##
## n/a	|Individual	|Estrategias de Afrontamiento	|n/a	|((Sumatoria afrontamiento activo planificación * 0.25 + sumatoria afrontamiento activo busqueda soporte * 0.25 + Sumatoria afrontamiento pasivonegacion * 0.50) / 4) * 100	|4	|29	|51	|69	|89	|100##  

## capitalpsicologico##
## forma_intra|	factor	|dominio	|dimension	|formula_calificacion	|Puntaje_transformación|	 muybajocapitalpsicologico_max 	| bajocapitalpsicologico_max 	| mediocapitalpsicologico_max 	| altocapitalpsicologico_max 	| muyaltocapitalpsicologico_max ##
## n/a	|Individual	|Capital psicológico	|n/a	|(sumatoria de puntajes brutos obtenidos en todas las preguntas de capitalpsicologico/puntaje de transformacion)*100	|12	|29	|51	|69	|89	|100##  

## paso 14##
## 14. Baremos Bateria Ministerio Colombia— Tablas de puntos de corte por factor intra, extra tanto A como B los factores intra, extra se califican segun sumatoria de respuestas y puntajes de transformación (puntaje bruto) y se categorizan en 5 niveles de riesgo (puntaje transformado). Cada persona evaluada obtiene para cada factor un puntaje bruto y un puntaje transformado en 5 niveles de riesgo##
## forma_intra|	 factor |	formula_calificacion|	Puntaje_transformación|	 sin_riesgo_max |	 bajo_max 	| medio_max |	 alto_max |	 muy_alto_max##
## A|	intralaboral_A	|(sumatoria de puntajes brutos obtenidos en todas las preguntas A/puntaje de transformacion)|*100|	492|	19.7|	25.8|	31.5|	38.8|	100##
## B|	intralaboral_B	|(sumatoria de puntajes brutos obtenidos en todas las preguntas B/puntaje de transformacion)|*100|	388|	20.6|	26|	31.2|	38.7|	100##
## A|	intralaboral_A+extralaboral	|(sumatoria de puntajes brutos obtenidos en todas las preguntas A+extralaboral/puntaje de transformacion)|*100|	616|	18.8|	24.4|	29.5|	35.4|	100##
## B|	intralaboral_B+extralaboral	|(sumatoria de puntajes brutos obtenidos en todas las preguntas B+extralaboral/puntaje de transformacion)|*100|	512|	19.9|	24.8|	29.5|	35.4|	100##
## A|	extralaboral	|(sumatoria de puntajes brutos obtenidos en todas las preguntas extralaboral A/puntaje de transformacion)|*100|	124|	11.3|	16.9|	22.6|	29|	100##
## B|	extralaboral	|(sumatoria de puntajes brutos obtenidos en todas las preguntas extralaboral B/puntaje de transformacion)|*100|	124|	12.9|	17.7|	24.2|	32.3|	100##

## paso 14.1## 
## 14.1 Baremos Bateria Ministerio Colombia— Tablas de puntos de corte estres. El total del puntaje bruto de estres es el resultado d ela sumatoria de 4 promedios ponderados/puntaje de transformación * 100 y se categorizan en 5 niveles de riesgo (puntaje transformado). Cada persona evaluada obtiene para cada factor un puntaje bruto y un puntaje transformado en 5 niveles de riesgo##

## Promedio ponderado 1| Se obtiene el puntaje promedio de los ítems 1 al 8, (1_estres a 8_estres) y el resultado se multiplica por cuatro (4)##
## Promedio ponderado 2| Se obtiene el puntaje promedio de los ítems 9 al 12 (9_estres a 12_estres), y el resultado se multiplica por tres (3).##
## Promedio ponderado 3| Se obtiene el puntaje promedio de los ítems 13 al 22 (13_estres a 22_estres), y el resultado se multiplica por dos (2).##
## Promedio ponderado 4| Se obtiene el puntaje promedio de los ítems 23 al 31 (23_estres a 31_estres).##

## forma_intra|	 factor |	formula_calificacion|	Puntaje_transformación|	 sin_riesgo_max |	 bajo_max 	| medio_max |	 alto_max |	 muy_alto_max##
## A|	estres	|(sumatoria de los 4 subtotales o promedios ponderados estres A/puntaje de transformacion)|*100|	61.16|	7.8|	12.6|	14.7|	25|	100##
## B|	estres	|(sumatoria de  los 4 subtotales o promedios ponderados estres B/puntaje de transformacion)|*100|	61.16|	6.5|	11.8|	17|	23.4|	100##


## paso 15##
## 15. Baremos Avantum — Tablas de puntos de corte para factor individual el factor individual se obtiene segun sumatoria de respuestas y puntajes de transformación (puntaje bruto) y se categorizan en 5 niveles de protección psicológica (puntaje transformado). Cada persona evaluada obtiene para cada factor un puntaje bruto y un puntaje transformado en 5 niveles de protección psicológica##Calculo % de vulnerabilidad psicológica en la empresa evaluada Establecer la cantidad de personas en puntajes muy bajo y bajo del factor individual sobre el total de evaluados % (vulnerabilidad psicológica)##

## forma_intra| factor |	formula_calificacion|	Puntaje_transformación	| muy bajo_max |	 bajo_max |	 medio_max |	 alto_max |	 muy alto_max ##
## A|	individual	|(sumatoria de puntajes brutos obtenidos en todas las preguntas de afrontamiento y capitalpsicologico/puntaje de transformacion)|*100|	24|	29|	51|	69|	89|	100##
## B|	individual	|(sumatoria de puntajes brutos obtenidos en todas las preguntas de afrontamiento y capitalpsicologico/puntaje de transformacion)|*100|	24|	29|	51|	69|	89|	100##


## paso 16##
## 16. Riesgo total empresa según Res. 2764/2022. Calcular el total de riesgo de la empresa en factor intralaboral A y B Calcular el riesgo total de la empresa. Si alguna forma A o B obtiene nivel de riesgo alto o muy alto, la empresa debe evaluar otra vez al año siguiente##
## forma_intra| factor |formula_calificacion|Puntaje_transformación| sin_riesgo_max | bajo_max | medio_max | alto_max | muy_alto_max##
## A|intralaboral_A|Promedio del puntaje bruto obtenido por todas las personas evaluadas en el total del factor intralaboral forma A|492|19.7|25.8|31.5|38.8|100##
## B|intralaboral_B|Promedio del puntaje bruto obtenido por todas las personas evaluadas en el total del factor intralaboral forma B|388|20.6|26|31.2|38.7|100##

## paso 17##
## 17. Calcular proporciones (%) de personas en nivel de riesgo alto y muy alto en el factor intralaboral A y B, sobre total de personas evaluadas y comparar con país y sector Calcular proporción del total de personas evaluadas que obtuvieron nivel de riesgo alto y muy alto en el factor intralaboral total A y B, sobre el total de personas evaluadas en la empresa. Establecer diferencia (+ o -) en puntos porcentuales y semaforizar##Grafica de colombia y sectores, comparada con grafica de la empresa -- %riesgoalto y muy alto total intralaboral A y %riesgo alto y muy alto total intralaboral B##
## FACTOR INTRALABORAL|RIESGO ALTO Y MUY ALTO|PROM GRAL|Agricultura, ganaderia, caza|Explotacion minas y canteras|Manufactura|servicios|Construcción|Comercio|Administracion publica|Educacion|Salud##
## PROMEDIO GENERAL|39.69%|31.01%|42.92%|44.83%|37.20%|42.21%|36.40%|35.98%|40.44%|40.28%##
## Fuente: III encuesta condiciones de salud y trabajo 2021##


## paso 18##
## 18. Calcular proporciones (%) de personas en nivel de riesgo alto y muy alto en cada dominio intralaboral  A y B, del total extralaboral A y B y el total estrés A y B, todo sobre total de personas evaluadas y comparar resultado de dominios en el país, establecer diferencia porcentual en puntos porcentuales (+ o -) y semaforizar Calcular proporción del total de personas evaluadas que obtuvieron nivel de riesgo alto y muy alto en cada dominio intralaboral total A y B, total extralaboral A y B, total de estrés A y B y total individual sobre el total de personas evaluadas en la empresa## Graficas de colombia en los dominios, comparada con grafica de la empresa -- %riesgoalto y muy alto total dominios intralaboral A y %riesgo alto y muy alto total dominios intralaboral B, %riesgo alto y muy alto extralaboral A, %riesgo alto y muy alto extralaboral B, %riesgo alto y muy alto estrés A, %riesgo alto y muy alto estrés B, % de vulnerbilidad de la empresa (nivel bajo y miuy bajo en factor de individual)##
## Dominio - País|Promedio Nacional riesgo alto y muy alto##
## Demandas|43.9%##
## Estrés|32.9%##
## Extralaboral|26.3%##
## Control|16.9%##
## Liderazgo y relaciones sociales|13.3%##
## Vulnerabilidad|4.2%##
## Recompensas|3.3%##
## Fuente: II y III encuesta condiciones de salud y trabajo 2013 - 2021##


## paso 19##
## 19. Comparar nivel de riesgo alto y muy alto dimensiones con país Tomar las dimensiones intralaboral (% riesgo alto y muy alto) de la empresa, que coinciden con la tabla de abajo y comparar resultado con porcentaje de país, establecer diferencia porcentual en puntos porcentuales (+ o -) y semaforizar## Graficas de colombia en los dominios, comparada con grafica de la empresa -- %riesgoalto y muy alto total dominios intralaboral A y %riesgo alto y muy alto total dominios intralaboral B, %riesgo alto y muy alto extralaboral A, %riesgo alto y muy alto extralaboral B, %riesgo alto y muy alto estrés A, %riesgo alto y muy alto estrés B, % de vulnerbilidad de la empresa (nivel bajo y miuy bajo en factor de individual)##
## Dimension -país|Promedio Nacional riesgo alto y muy alto##
## Demandas de carga mental|58.2%##
## Demandas emocionales|49.4%##
## Demandas cuantitativas|39.2%##
## Caracteristicas de liderazgo|25.9%##
## Control y autonomía sobre el trabajo|22.1%##
## Influencia del trabajo sobre entorno extralaboral|21.5%##
## Claridad del rol|20.5%##
## Oportunidades para el uso y desarrollo de hab y ctos|18.4%##
## Capacitación|11.0%##
## Relaciones sociales en el trabajo|10.1%##
## Supuesto acoso laboral|8.2%##
## Claridad de rol|5.8%##
## Fuente: II y III encuesta condiciones de salud y trabajo 2013 - 2021##

## paso 20##
## 20. Distribución de frecuencias por pregunta y top de preguntas clave Tabla de distribución de frecuencias de respuesta segun las opciones (cantidad de personas que responden la opción de respuesta sobre total de evaluados) de cada pregunta en todos los factores (intra, extra, estrés, individual)## Calculo puntos % de diferencia## Tomar las preguntas de la siguiente tabla, aplicar calificación (%) del total de personas evaluadas en la empresa y comparar con Alta presencia (siempre, casi siempre) % ENCST II-III en el país. Calcular la diferencia % (Puntos porcentuales), ordenar de mayor a menor las preguntas que tiene diferencia porcentual positiva (% empresa- %pais), mostrando el top 20 de las preguntas con diferencia positiva entre empresa y país.##
## id_pregunta A|	id_pregunta B|	Pregunta intralaboral A o B|	formula_calificacion|	Dimension|	Pregunta Pais ENCST II-III|	Alta presencia (siempre, casi siempre) % ENCST II-III##
## 11_afrontamiento|	11_afrontamiento|	A menudo siento que soy capaz de hacer lo que me propongo|	Sumatoria de personas con respuesta en frecuentemente hago eso, siempre hago esto /total de personas evaluadas|	Autoeficacia|	Capaz de tomar decisiones|	95.2%##
## 61_intra|	47_intra|	Recibo capacitación útil para hacer mi trabajo|	Sumatoria de personas con respuesta en siempre y casi siempre /total de personas evaluadas|	Capacitación|	Información y capacitación por parte de la empresa|	87.9%##
## 65_intra|	50_intra|	Mi jefe tiene en cuenta mis puntos de vista y opiniones|	Sumatoria de personas con respuesta en siempre y casi siempre /total de personas evaluadas|	Características del liderazgo|	Mi jefe ejerce control directo y no escucha|	54.4%##
## 75_intra|	61_intra|	Mi jefe me brinda su apoyo cuando lo necesito|	Sumatoria de personas con respuesta en siempre y casi siempre /total de personas evaluadas|	Características del liderazgo|	Obtener ayuda de jefes si la pide|	94.5%##
## 58_intra|	58_intra|	Mi jefe me trata con respeto|	Sumatoria de personas con respuesta en siempre y casi siempre /total de personas evaluadas|	Características del liderazgo|	Acoso por parte de superiores|	14.6%##
## 53_intra|	41_intra|	Me informan con claridad cuáles son mis funciones|	Sumatoria de personas con respuesta en siempre y casi siempre /total de personas evaluadas|	Claridad de rol|	Dispone de información clara para trabajar|	95.8%##
## 59_intra|	45_intra|	Me informan claramente con quien puedo resolver los asuntos de trabajo|	Sumatoria de personas con respuesta en siempre y casi siempre /total de personas evaluadas|	Claridad de rol|	Las responsabilidades no están claramente definidas|	21.8%##
## 55_intra|	43_intra|	Me explican claramente los resultados que debo lograr en mi trabajo|	Sumatoria de personas con respuesta en siempre y casi siempre /total de personas evaluadas|	Claridad de rol|	Lo que se espera del trabajador cambia constantemente|	19.1%##
## 46_intra|	36_intra|	Puedo cambiar el orden de las actividades en mi trabajo|	Sumatoria de personas con respuesta en siempre y casi siempre /total de personas evaluadas|	Control y autonomía sobre el trabajo|	Posibilidad de decidir sobre el orden de las tareas|	81.9%##
## 45_intra|	35_intra|	Puedo decidir la velocidad a la que trabajo|	Sumatoria de personas con respuesta en siempre y casi siempre /total de personas evaluadas|	Control y autonomía sobre el trabajo|	Posibilidad de decidir el ritmo de trabajo|	82.3%##
## 13_intra|	13_intra|	Por la cantidad de trabajo que tengo debo quedarme tiempo adicional|	Sumatoria de personas con respuesta en siempre y casi siempre /total de personas evaluadas|	Demandas cuantitativas|	Tiene mucho trabajo y poco tiempo para realizarlo|	33.3%##
## 14_intra|	14_intra|	Me alcanza el tiempo de trabajo para tener al día mis deberes|	Sumatoria de personas con respuesta en siempre y casi siempre /total de personas evaluadas|	Demandas cuantitativas|	Tengo poco tiempo para tareas asignadas|	16.2%##
## 15_intra|	15_intra|	Por la cantidad de trabajo que tengo debo trabajar sin parar|	Sumatoria de personas con respuesta en siempre y casi siempre /total de personas evaluadas|	Demandas cuantitativas|	Necesidad de trabajar muy rápido|	62.5%##
## 13_intra|	13_intra|	Por la cantidad de trabajo que tengo debo quedarme tiempo adicional|	Sumatoria de personas con respuesta en siempre y casi siempre /total de personas evaluadas|	Demandas cuantitativas|	Necesidad de trabajar con plazos muy estrictos o muy cortos|	51.5%##
## 17_intra|	17_intra|	Mi trabajo me exige estar muy concentrado|	Sumatoria de personas con respuesta en siempre y casi siempre /total de personas evaluadas|	Demandas de carga mental|	Mantener nivel de atención alto|	72.5%##
## 20_intra|	no aplica|	Mi trabajo me exige atender muchos asuntos al mismo tiempo|	Sumatoria de personas con respuesta en siempre y casi siempre /total de personas evaluadas|	Demandas de carga mental|	Atender varias tareas al mismo tiempo|	53.4%##
## 21_intra|	20_intra|	Mi trabajo requiere que me fije en pequeños detalles|	Sumatoria de personas con respuesta en siempre y casi siempre /total de personas evaluadas|	Demandas de carga mental|	Requerimiento de realización de tareas complejas, complicadas o difíciles|	37.3%##
## 113_intra|	n/a|	Para hacer mi trabajo debo mostrar sentimientos distintos a los míos|	Sumatoria de personas con respuesta en siempre y casi siempre /total de personas evaluadas|	Demandas emocionales|	Necesidad de esconder las propias emociones en el puesto de trabajo|	34.4%##
## 6_extra|	6_extra|	En la zona donde vivo se presentan hurtos y mucha delincuencia|	Sumatoria de personas con respuesta en siempre y casi siempre /total de personas evaluadas|	Características de la vivienda y de su entorno|	La zona de trabajo es violenta e insegura|	17.8%##
## 17_extra|	17_extra|	Tengo tiempo para compartir con mi familia o amigos|	Sumatoria de personas con respuesta en siempre y casi siempre /total de personas evaluadas|	Balance entre la vida laboral y familiar|	Logro equilibrio vida personal y trabajo|	90.1%##
## 3_extra|	3_extra|	Paso mucho tiempo viajando de ida y regreso al trabajo|	Sumatoria de personas con respuesta en siempre y casi siempre /total de personas evaluadas|	Desplazamiento vivienda trabajo vivienda|	Tardo una 1 o más de desplazamiento al lugar de trabajo|	46.9%##
## 40_intra|	31_intra|	Mi trabajo me permite aplicar mis conocimientos|	Sumatoria de personas con respuesta en siempre y casi siempre /total de personas evaluadas|	Oportunidades de desarrollo y uso de habilidad|	Oportunidad de hacer aquello que sabía hacer mejor|	91.7%##
## 41_intra|	32_intra|	Mi trabajo me permite aprender nuevas cosas|	Sumatoria de personas con respuesta en siempre y casi siempre /total de personas evaluadas|	Oportunidades de desarrollo y uso de habilidad|	Posibilidad de poner en práctica las propias ideas en el trabajo|	87.5%##
## n/a|	29_intra|	Mi trabajo me permite hacer cosas nuevas|	Sumatoria de personas con respuesta en siempre y casi siempre /total de personas evaluadas|	Oportunidades para el uso y desarrollo de hab y ctos|	Trabajo no es monótono o repetitivo|	66.8%##
## 2_afrontamiento|	2_afrontamiento|	Estoy convencido de que me esperan cosas buenas en la vida|	Sumatoria de personas con respuesta en frecuentemente hago eso, siempre hago esto /total de personas evaluadas|	Optimismo|	Capaz de disfrutar de la vida diaria|	95.5%##
## 103_intra|	86_intra|	El trabajo que hago me hace sentir bien|	Sumatoria de personas con respuesta en siempre y casi siempre /total de personas evaluadas|	Recompensas derivadas de la pertenencia|	Se siente satisfecho en su ocupación|	94.7%##
## 87_intra|	71_intra|	Mis compañeros de trabajo me ayudan cuando tengo dificultades|	Sumatoria de personas con respuesta en siempre y casi siempre /total de personas evaluadas|	Relaciones sociales en el trabajo|	Obtener ayuda de compañeros si la pide|	96.2%##
## 88_intra|	72_intra|	En mi trabajo las personas nos apoyamos unos a otros|	Sumatoria de personas con respuesta en siempre y casi siempre /total de personas evaluadas|	Relaciones sociales en el trabajo|	Relaciones con otros son positivas|	96.6%##
## 80_intra|	66_intra|	En mi grupo de trabajo algunas personas me maltratan|	Sumatoria de personas con respuesta en siempre y casi siempre /total de personas evaluadas|	Relaciones sociales en el trabajo|	Acoso por parte de compañeros de trabajo|	4.9%##
## 118_intra|	n/a|	Tengo colaboradores que tienen comportamientos irrespetuosos|	Sumatoria de personas con respuesta en siempre y casi siempre /total de personas evaluadas|	Relacion con colaboradores|	Acoso por parte de subordinados a directivos|	5.1%##
## 8_afrontamiento|	8_afrontamiento|	Puedo recuperarme rápidamente después de experimentar dificultades|	Sumatoria de personas con respuesta en frecuentemente hago eso, siempre hago esto /total de personas evaluadas|	Resiliencia|	Capaz de enfrentar sus problemas|	96.9%##
## 21_estres|	21_estres|	Dificultad para tomar decisiones|	Sumatoria de personas con respuesta en siempre, casi siempre/total de personas evaluadas|	Respuestas cognitivas de estrés|	Dificultad para tomar decisiones|	2.9%##
## 13_estres|	13_estres|	Sentimiento de sobrecarga de trabajo|	Sumatoria de personas con respuesta en siempre, casi siempre/total de personas evaluadas|	Respuestas comportamentales de estrés|	Se ha sentido bajo tensión|	39.4%##
## 25_estres|	25_estres|	Sentimiento de angustia, preocupación o tristeza|	Sumatoria de personas con respuesta en siempre, casi siempre/total de personas evaluadas|	Respuestas emocionales de estrés|	Se ha sentido triste o deprimido|	40.2%##
## 12_estres|	12_estres|	Sensación de aislamiento y desinterés|	Sumatoria de personas con respuesta en siempre, casi siempre/total de personas evaluadas|	Respuestas emocionales de estrés|	Me siento solo y desconectado de otras personas|	33.9%##
## 31_estres|	31_estres|	Sensación de no poder manejar los problemas de la vida|	Sumatoria de personas con respuesta en siempre, casi siempre/total de personas evaluadas|	Respuestas emocionales de estrés|	Siente que no puede solucionar sus problemas|	8.6%##
## 27_estres|	27_estres|	Sentimiento de que no vale nada o no sirve para nada|	Sumatoria de personas con respuesta en siempre, casi siempre/total de personas evaluadas|	Respuestas emocionales de estrés|	Ha perdido confianza en sí mismo|	2.7%##
## 5_estres|	5_estres|	Trastornos del sueño como somnolencia durante el día o desvelo en la noche|	Sumatoria de personas con respuesta en siempre, casi siempre/total de personas evaluadas|	Respuestas físicas de estrés|	Ha perdido el sueño por preocupaciones|	30.1%##
## 1_estres|	1_estres|	Dolores en el cuello y espalda o tensión muscular|	Sumatoria de personas con respuesta en siempre, casi siempre/total de personas evaluadas|	Respuestas físicas de estrés|	Mi salud física ha empeorado|	31.8%##
## Fuente: II y III encuesta condiciones de salud y trabajo 2013 - 2021##

## paso 21##
## 21. Revisar calculos de los 20 pasos anteriores y verificar que los resultados sean correctos##
## paso 22##
## 22. Segun plan de analisis del visualizador 1, estimar lo que se requiere para el visualizador 1, y ejecutarlo##



2. visualizador 2: dashboard_v2_gestion
3. visualizador 3: dashboard_v3_gerencial
4. visualizador 4: dashboard_v4_asis

##pasos para la creacion de los dashboards##
1. 