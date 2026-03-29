# MentalPRO — Instrucciones de desarrollo para Claude

## Skills activos para este proyecto

Cuando trabajes en cualquier archivo de `Dashboards/`, activa estos skills:
- `/kpi-dashboard-design` — diseño de métricas, layout, visualizaciones
- `/data-storytelling` — narrativa ejecutiva, estructura de insights, presentación a stakeholders

## Stack técnico (Frontend Web Moderno / Opción 3)

- **Backend (ETL y API):** Python 3.11+ / pandas / pyarrow / FastAPI
- **Frontend (Visualizadores):** Next.js / React / TailwindCSS / Recharts / Tremor
- Config Backend: `config/config.yaml`
- El ETL termina generando archivos `.parquet` en `data/processed/`, los cuales serán consumidos por el Frontend vía FastAPI. Ya no se generan HTMLs estáticos.

## Paleta R11 — inamovible

```python
COLORES_RIESGO = {1: "#10B981", 2: "#6EE7B7", 3: "#F59E0B", 4: "#F97316", 5: "#EF4444"}
NAVY  = "#0A1628"
GOLD  = "#C9952A"
CYAN  = "#00C2CB"
WHITE = "#FFFFFF"
GRAY  = "#F3F4F6"
FONT  = "Inter, Arial, sans-serif"
```

## Reglas de negocio obligatorias

- **Regla R8 Confidencialidad**: grupos < 5 personas → el backend debe enviar alerta de "Confidencial" en lugar del dato para que el frontend no lo dibuje.
- **Lienzo**: Diseño responsivo y dinámico (delegado a los componentes de Next.js).
- **Umbrales semáforo KPI**: >35% = rojo | 15–34% = amarillo | <15% = verde (Lógica gestionada en el backend para enviar el token correcto al front).
- **Referente sector**: si el sector no tiene dato, usar referente país (ENCST).
- **Dashboard Cliente/Servidor**: El backend surte múltiples empresas, el frontend filtra.

---

## Visualizador 1: Riesgo Psicosocial (Migración a Web)

**Archivos fuente** (parquet en `data/processed/`): resultados baremos, benchmarking, frecuencias preguntas, consolidación demográfica.

### Secciones

| # | Nombre | Contenido clave |
|---|--------|-----------------|
| S0 | Header | Empresa · Sector · #evaluados · %cobertura · Fecha evaluación |
| S1 | Filtros | area_departamento · categoria_cargo · modalidad_trabajo · nombre_jefe · edad_cumplida · antigüedad_empresa |
| S2 | KPIs | Ver tarjetas KPI abajo |
| S3 | Resultados globales | Barras apiladas 5 niveles + gauge + alerta reevaluación |
| S4 | Distribución 0–100 | Boxplot scores transformados por instrumento |
| S5 | Benchmarking sector | % Alto+MuyAlto IntraA y B vs sector ENCST |
| S6 | Dominios vs Colombia | Heatmap % riesgo dominios empresa vs referencia |
| S7 | Dimensiones vs Colombia | Tabla semaforizada empresa vs ENCST |
| S8 | Preguntas clave | Top 20 preguntas con mayor diferencia vs Colombia |
| S9 | Empresa vs Áreas (Intra+Extra+Estrés) | Heatmap área × dimensión |
| S10 | Empresa vs Áreas (Individual) | Heatmap área × dimensión individual |

### Tarjetas KPI (S2)

| ID | Nombre | Estructura | Semáforo |
|----|--------|------------|----------|
| KPI-1 | % Riesgo A-MA Intralaboral | 3 filas: Intra A · Intra B · Sector/País | >35% rojo · 15–34% amarillo · <15% verde |
| KPI-2 | Trabajadores A+B con Estrés A+MA | 3 filas: N · % total evaluados · % país | Semaforizar diferencia pp vs país |
| KPI-3 | Vulnerabilidad psicológica | 3 filas: N · % total evaluados · % país | Semaforizar diferencia pp vs país |
| KPI-4 | % Riesgo A-MA Extralaboral | 3 filas: Extra A · Extra B · País | >35% rojo · 15–34% amarillo · <15% verde |
| KPI-5 | Dimensiones críticas | Conteo dimensiones por encima del referente nacional | Valor único |

---

## Visualizadores 2, 3 y 4 (Pendientes de Endpoint)

- **Visualizador 2 (Gestión):** Resultados por eje/línea, indicadores y Top 10 indicadores × áreas.
- **Visualizador 3 (Gerencial):** Ficha técnica, resultados globales + benchmarking, 3 ejes de gestión, Ranking áreas top 5 y protocolos prioritarios.
- **Visualizador 4 (ASIS):** Distribución demográfica, perfil laboral, salud/costos ausentismo.

*(Los scripts en Python correspondientes a v2, v3 y v4 funcionarán exclusivamente como tuberías hacia los endpoints de FastAPI).*

---

## Marco regulatorio

Resolución 2764/2022 — Ministerio de Trabajo Colombia
Referentes nacionales: II y III Encuesta Nacional de Condiciones de Salud y Trabajo (ENCST 2013–2021)
