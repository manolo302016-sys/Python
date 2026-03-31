## DOCUMENTO MARCO DEL PROYECTO PYTHON — ÍNDICE MAESTRO ##

> Este documento es el índice maestro del proyecto MentalPRO.
> Cada visualizador tiene su propio documento con las especificaciones completas.
> Las secciones 1 y 2 son transversales a todos los visualizadores.

---

## 1. Modelo de datos — Star Schema – ETL ##

### Tabla de Hechos Principal: fact_respuestas
- **Hechos:** `id_pregunta` (string), `id_respuesta` (string), `ausentismo_eg_si_no` (string), `ausentismo_al_si_no` (string), `dias_ausencia` (int)
- **PK:** `cedula` (string)
- **FK:** `empresa`, `sector_economico`, `id_respuesta`, `forma_intra`

### Tablas Dimensionales
**planta_personal:** empresa, sector_economico, forma_intra, area_departamento, categoría_cargo, tipo_cargo, es_jefe, tipo_contrato, tipo_contratacion, departamentodelpais_trabajo, ciudad_o_municipio_trabaja, modalidad_de_trabajo, tipo_teletrabajo_si_aplica, nombre_jefe

**ficha_demografia:** fecha_aplicacion, nivel_escolaridad, estrato_economico, tipo_vivienda, sexo, edad_cumplida, antiguedad_empresa_años_cumplidos, estado_civil, numero_dependientes_economicos, horas_trabajo_diario, tipo_salario, departamento_pais_residencia, municipio_o_ciudad_residencia, antiguedad_en_cargo_años_cumplidos

**categorias_analisis (V2):** factor, dimensión, pregunta, protocolo_id, protocolo_gestion, indicador, linea_gestion, eje_gestion

**ausentismo_12meses (V4):** diagnostico_CIE, dias_ausencia, tipo_ausentismo

### Regla de confidencialidad R8
Cualquier agrupación con N < 5 personas se enmascara como "Confidencial" en todos los endpoints de la API y en todas las visualizaciones.

### Arquitectura general
El pipeline Python genera archivos `.parquet` en `data/processed/`. El backend FastAPI (`api/`) expone estos datos como endpoints JSON. El frontend Next.js (`frontend/`) consume los endpoints y renderiza los dashboards. El diseño visual, lienzo responsivo y componentes interactivos están delegados al Frontend.

---

## 2. Índice de Visualizadores ##

| # | Visualizador | Sub-documento | Router API | Estado |
|---|---|---|---|---|
| 1 | Resultados Riesgo Psicosocial | [v1_riesgo_psicosocial.md](./v1_riesgo_psicosocial.md) | `api/routers/v1_riesgo.py` | ✅ Backend completado |
| 2 | Gestión Salud Mental | [v2_gestion_saludmental.md](./v2_gestion_saludmental.md) | `api/routers/v2_gestion.py` | 🟡 ETL en especificación |
| 3 | Resumen Gerencial y ROI | *(versión futura del documento)* | `api/routers/v3_gerencial.py` | ⬜ Pendiente |
| 4 | Variables Demográficas y Ausentismo | *(versión futura del documento)* | `api/routers/v4_asis.py` | ⬜ Pendiente |

---

## 2. Requerimientos visuales transversales ##

- Lienzo desplazable verticalmente: **3000 × 2000 px**
- Lineamientos UX/UI de alta fidelidad — profesional, confiable, usuarios empresariales y técnicos
- Cada visualizador pertenece a **una empresa** (no consolida varias)
- Los filtros de segmentación usan: `area_departamento`, `categoría_cargo`, `modalidad_de_trabajo`, `nombre_jefe`, `edad_cumplida`, `antigüedad_empresa`
- Aplicar R8 en todas las celdas, gráficas y tablas

---

## 3. Benchmarking nacional transversal (ENCST II-III) ##

| Dominio / Factor | % Riesgo Alto+MA País |
|---|---|
| Demandas | 43.9% |
| Estrés | 32.9% |
| Extralaboral | 26.3% |
| Control | 16.9% |
| Liderazgo y relaciones sociales | 13.3% |
| Vulnerabilidad psicológica | 4.2% |
| Recompensas | 3.3% |
| Factor intralaboral promedio general | 39.69% |

*Fuente: II y III Encuesta Nacional de Condiciones de Salud y Trabajo 2013-2021*

---

## Notas Arquitectónicas ##

> El diseño visual, lienzo responsivo, componentes interactivos y despliegue final (Visualizadores 1, 2, 3 y 4) están delegados al Frontend implementando Next.js/React. El pipeline de datos en Python culmina con la disponibilidad de las bases analíticas que aseguran fidelidad con las reglas establecidas en este documento.

> Para añadir nuevos dashboards (V3, V4), crear el sub-documento `.md` correspondiente, desarrollar los scripts ETL y registrar el router en `api/main.py` siguiendo el patrón de `v1_riesgo.py`.