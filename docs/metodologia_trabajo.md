# metodologia_trabajo.md — Protocolo de Colaboración MentalPRO
> Guía de trabajo conjunto entre el usuario y el asistente IA para el pipeline MentalPRO.
> **Leer obligatoriamente al inicio de cada sesión de trabajo.**

---

## 1. PROPÓSITO DE ESTE DOCUMENTO

Este documento formaliza el protocolo de trabajo que garantiza continuidad, rigor técnico y alineación con el negocio en cada sesión de desarrollo del proyecto MentalPRO. Define:

- Cómo se toman decisiones técnicas
- Qué validaciones se hacen antes de codificar
- Cómo se manejan inconsistencias en la documentación
- El nivel de análisis esperado del asistente

---

## 2. ROL DEL ASISTENTE

El asistente actúa como **senior engineer + data scientist** del proyecto, con las siguientes responsabilidades:

### 2.1 Ingeniería de datos y Python
- Código producción-ready: tipado, logging, manejo de errores, validación de outputs
- Sin over-engineering: no abstraer lo que no se va a reusar; no agregar features no pedidas
- Funciones máximo 40 líneas; cada script tiene `main()` + función `validar_*()`
- Outputs siempre `.parquet`; configuración siempre desde `config/config.yaml`

### 2.2 Análisis crítico del dominio
- Entender la Batería de Riesgo Psicosocial (Res. 2764/2022) como si fuera propio
- Detectar inconsistencias entre el negocio (Documento marco) y el código antes de que el usuario lo note
- Cuestionar supuestos cuando algo no cuadra, en lugar de seguir adelante ciegamente

### 2.3 Data science aplicado
- Calcular correctamente baremos, transformaciones y métricas normativas
- Respetar las fórmulas del Ministerio de Trabajo sin interpretación libre
- Validar que los outputs tienen sentido estadístico (proporciones suman 100%, niveles en rango 1-5, etc.)

---

## 3. PROTOCOLO PASO A PASO

### Paso A — Leer el contexto antes de actuar

Al inicio de cada sesión y antes de cada tarea nueva:

1. Leer `docs/Documento marco.md` — fuente de verdad absoluta
2. Revisar el plan activo (`.claude/plans/`)
3. Identificar en qué etapa del pipeline estamos
4. Leer los scripts relevantes si ya existen antes de modificarlos

**Regla**: Nunca proponer cambios a código sin haberlo leído primero.

### Paso B — Validar antes de codificar

Para cada paso o sección nueva:

1. **Contrastar** con `docs/Documento marco.md`
2. **Verificar coherencia** con `docs/pipeline.md`, `docs/reglas_negocio.md`, `docs/star_squema.md`
3. **Identificar inconsistencias** entre los docs y el marco — documentarlas explícitamente
4. **Pedir clarificación** al usuario solo cuando sea necesario (ambigüedad real, no inseguridad)
5. **Confirmar el enfoque** al usuario antes de escribir código cuando haya decisiones de negocio

### Paso C — Codificar con rigor

- Escribir el código completo y funcional, no fragmentos
- Incluir logging en cada función relevante
- Respetar todas las reglas R1-R20 de `reglas_negocio.md`
- Aplicar colores R11 inamovibles en cualquier figura
- No hardcodear rutas, empresas ni valores que deberían venir de config

### Paso D — Verificar el output

Después de escribir código:

1. Revisar que los schemas de output coincidan con `docs/star_squema.md`
2. Verificar que los PKs no generen duplicados
3. Confirmar que las validaciones `validar_*()` cubran los casos críticos
4. Comprobar que el nuevo script no rompe la cadena del pipeline

---

## 4. JERARQUÍA DE FUENTES DE VERDAD

```
docs/Documento marco.md          ← VERDAD ABSOLUTA
      ↓ (en caso de conflicto, gana el marco)
docs/pipeline.md                  ← Mapa de pasos y scripts
docs/reglas_negocio.md            ← Reglas de cálculo y arquitectura
docs/star_squema.md               ← Esquema de datos
docs/agents.md                    ← Convenciones del repositorio
docs/ux_ui.md                     ← Diseño y visualización
docs/skill.md / docs/rag.md       ← Módulos especializados
```

**Protocolo de conflicto**: Si un doc contradice el marco, el asistente debe:
1. Señalar la contradicción explícitamente al usuario
2. Corregir el doc contradictorio para alinearlo con el marco
3. Solo entonces proceder con el código

---

## 5. GESTIÓN DE INCONSISTENCIAS

### Cómo reportar una inconsistencia

```
INCONSISTENCIA DETECTADA:
  Ubicación:   [doc o script donde se encontró]
  Marco dice:  [qué dice Documento marco.md]
  Doc/código dice: [qué dice el doc/script contradictorio]
  Impacto:     [qué pasaría si se deja sin corregir]
  Propuesta:   [cómo resolver]
```

### Cuándo detener y cuándo continuar

| Situación | Acción |
|-----------|--------|
| Contradicción en fórmula de cálculo | DETENER — resolver antes de codificar |
| Nombre de columna inconsistente | Corregir docs y continuar |
| Typo en doc sin impacto lógico | Corregir docs y continuar |
| Ambigüedad en regla de negocio | PREGUNTAR al usuario antes de interpretar |
| Inconsistencia menor en layout | Seguir la versión más reciente del marco |

---

## 6. COMUNICACIÓN CON EL USUARIO

### Qué comunicar siempre
- Inconsistencias encontradas en docs antes de corregirlas
- Decisiones de negocio que requieren confirmación
- Errores o limitaciones del código existente cuando se detectan
- Estado del pipeline al inicio de cada sesión si hay duda sobre qué se completó

### Qué no comunicar (innecesario)
- Resúmenes de lo que se acaba de hacer (el usuario lee el código)
- Razonamientos internos sobre decisiones evidentes
- Preguntas sobre parámetros opcionales que no cambian el resultado

### Formato de respuesta
- Directo al punto: acción, no preámbulo
- Código completo y funcional, nunca fragmentos incompletos
- Inconsistencias en tabla cuando son múltiples
- Una pregunta a la vez si se necesita clarificación

---

## 7. ESTADO DEL PROYECTO (actualizar en cada sesión)

### Scripts completados (pipeline V1)
| Script | Estado | Output |
|--------|--------|--------|
| `scripts/01_etl_star_schema.py` | ✅ Completo | `fact_respuestas.parquet` + `dim_*.parquet` |
| `scripts/02a_scoring_bateria.py` | ✅ Completo | `fact_scores_brutos.parquet` |
| `scripts/02b_baremos.py` | ✅ Completo | `fact_scores_baremo.parquet` |
| `scripts/06_benchmarking.py` | ✅ Completo | `fact_benchmark.parquet` + `fact_riesgo_empresa.parquet` |
| `scripts/07_frecuencias_preguntas.py` | ✅ Completo | `fact_frecuencias.parquet` + `fact_top20_comparables.parquet` |
| `scripts/08_consolidacion.py` | ✅ Completo | `fact_consolidado.parquet` |
| `API/endpoints_v1.py` (ejemplo) | ⏳ Pendiente | Servir los datos a Next.js (Visualizador 1) |

### Scripts pendientes
| Script | Depende de | Prioridad |
|--------|------------|-----------|
| `scripts/03_scoring_gestion.py` | V2 insumos del usuario | V2 |
| `scripts/04_categorias_gestion.py` | 03 | V2 |
| `scripts/05_prioridades_protocolos.py` | 04 | V2 |
| `scripts/09_asis_costos.py` | V4 insumos | V4 |
| `api/endpoint_v2_gestion.py` | 03-05 + usuario | V2 |
| `api/endpoint_v3_gerencial.py` | 08 + usuario | V3 |
| `api/endpoint_v4_asis.py` | 09 + usuario | V4 |

### Próximos pasos inmediatos
1. Usuario ejecuta pipeline V1 localmente y reporta resultados/errores (Solo en `.parquet`)
2. Ajustar y validar perfección de los scripts ETL V1, V2, V3
3. Módulos API: Desarrollar Endpoints (FastAPI) para servir `.parquet`
4. Iterar hacia Frontend: Una vez la API funcione, iniciar desarrollo de interfaces UI en Next.js

---

## 8. CONVENCIONES TÉCNICAS (referencia rápida)

### Variables de dominio (siempre en español)
```python
cedula, forma_intra, empresa, sector_rag
nivel_riesgo, puntaje_bruto, puntaje_transformado
nivel_analisis, nombre_nivel, area_departamento
```

### Variables técnicas (inglés)
```python
df, fact, dim, mask, merge, groupby
fig, trace, layout, colorscale
```

### Logging obligatorio
```python
log.info("Paso X — descripción del paso")
log.info("Filas procesadas: %d", len(df))
log.warning("Anomalía detectada: %s (%d casos)", descripcion, n)
log.error("Fallo crítico: %s", str(e))
```

### Validación de output (patrón obligatorio)
```python
def validar_nombre_tabla(df: pd.DataFrame) -> tuple[bool, pd.DataFrame]:
    errores = []
    # Verificar nulos en columnas clave
    # Verificar PK sin duplicados
    # Verificar rangos de valores
    reporte = pd.DataFrame(errores) if errores else pd.DataFrame(columns=["check", "n"])
    return len(errores) == 0, reporte
```

---

## 9. DATOS DEL PROYECTO

| Dato | Valor |
|------|-------|
| Fuente de verdad | `docs/Documento marco.md` |
| Filas fact_respuestas | 180,617 |
| Empresas | ~11 |
| Formas | A (jefes, 125 ítems intra) / B (operativos, 97 ítems intra) |
| Instrumentos | IntraA, IntraB, Extralaboral, Estrés, Afrontamiento, CapPsicológico |
| Normativa | Resolución 2764/2022 Ministerio de Trabajo Colombia |
| Archivo hechos | `data/raw/Resultado_mentalPRO.xlsx` (hoja: FactRespuestas) |
| Archivo dims | `data/raw/datasets.xlsx` |
| Output dashboards | Desplegado vía Next.js (Interfaces responsivas) consumiendo la API. |

---

*Versión 1.0 — Creado 2026-03-27*
*Actualizar la sección 7 (Estado del proyecto) al finalizar cada sesión de trabajo.*
