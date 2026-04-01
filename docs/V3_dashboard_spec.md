# Visualizador 3 — Especificación Frontend
## MentalPRO · Dashboard Gerencial + ASIS · v3.0.0

**Stack:** Next.js · React · TailwindCSS · Recharts · Tremor  
**API base:** `/v3`  
**Audiencia:** Alta dirección · Gerencias · Talento Humano estratégico  
**Marco legal:** Resolución 2764/2022 — Ministerio de Trabajo Colombia

---

## 1. Tokens de diseño (R11 — inamovibles)

```ts
// lib/tokens.ts
export const RISK_COLORS = {
  1: "#10B981",  // Sin riesgo — verde
  2: "#6EE7B7",  // Bajo      — verde claro
  3: "#F59E0B",  // Medio     — amarillo
  4: "#F97316",  // Alto      — naranja
  5: "#EF4444",  // Muy alto  — rojo
} as const

export const SEMAFORO_COLOR = {
  verde:    "#10B981",
  amarillo: "#F59E0B",
  rojo:     "#EF4444",
} as const

export const PALETTE = {
  navy:  "#0A1628",
  gold:  "#C9952A",
  cyan:  "#00C2CB",
  white: "#FFFFFF",
  gray:  "#F3F4F6",
} as const

export const FONT = "Inter, Arial, sans-serif"
```

**Regla semáforo KPI:**
```ts
export function semaforoKPI(pct: number): string {
  if (pct > 35) return SEMAFORO_COLOR.rojo
  if (pct >= 15) return SEMAFORO_COLOR.amarillo
  return SEMAFORO_COLOR.verde
}
```

**Regla R8 — Confidencialidad:**
```ts
export const CONFIDENCIAL_LABEL = "Confidencial"
export const CONFIDENCIAL_MIN_N = 5
// Si el backend envía confidencial: true → renderizar CONFIDENCIAL_LABEL, nunca el dato.
```

---

## 2. Interfaces TypeScript

```ts
// types/v3.ts

// ── S0 Encabezado ──────────────────────────────────────────────────────────
export interface Encabezado {
  empresa: string
  sector: string
  n_evaluados: number
  n_planta: number
  pct_cobertura: number
  fecha_evaluacion: string
  formas_disponibles: ("A" | "B")[]
  instrumento: string
}

// ── S1 KPIs Globales ───────────────────────────────────────────────────────
export interface NivelRiesgo {
  nivel_label: string       // "Sin riesgo" | "Riesgo bajo" | "Riesgo medio" | "Riesgo alto" | "Riesgo muy alto"
  n_nivel: number
  pct_nivel: number
}

export interface KPIGrupo {
  kpi_grupo: string         // "Intralaboral A" | "Intralaboral B" | "Extralaboral A" | ...
  niveles: NivelRiesgo[]    // 5 elementos (nivel_num 1..5)
  pct_alto_muy_alto: number
  pct_referente: number | null
  tipo_referente: "sector" | "pais"
  diferencia_pp: number | null
  semaforo: string          // hex color
}

export interface KpisGlobalesResponse {
  empresa: string
  kpis: KPIGrupo[]
  insights: Insight[]
}

// ── S2A Demografía ─────────────────────────────────────────────────────────
export interface DemoRow {
  categoria: string
  sexo?: string | null      // solo para piramide_poblacional
  n: number | null          // null = confidencial
  pct: number | null
}

export interface DemografiaResponse {
  empresa: string
  graficas: {
    piramide_poblacional?: DemoRow[]
    antiguedad_empresa?: DemoRow[]
    antiguedad_cargo?: DemoRow[]
    estado_civil?: DemoRow[]
    dependientes?: DemoRow[]
    area?: DemoRow[]
    cargo?: DemoRow[]
    forma_intra?: DemoRow[]
  }
  nota_r8: string
}

// ── S2B Costos Ausentismo ──────────────────────────────────────────────────
export interface PasoCalculo {
  paso: number
  nombre_paso: string
  valor: number
  unidad: string
  nota: string
}

export interface MetaAusentismo {
  pct_ausentismo: number
  diferencia_pp_vs_pais: number
  referente_pais_pct: number
  semaforo_ausentismo: string
  n_planta: number
  total_dias_ausencia: number
  dias_cap_instalada: number
}

export interface CostosAusentismoResponse {
  empresa: string
  pasos_calculo: PasoCalculo[]
  meta_ausentismo: MetaAusentismo
  costo_atribuible_psicosocial: number | null
  inversion_sst_estimada: number
  roi_estimado_pct: number | null
  nota_roi: string
  fuente: string
}

// ── S3 Benchmarking ────────────────────────────────────────────────────────
export interface BenchRow {
  tipo: "riesgo_intralaboral" | "protocolos_urgentes" | "dimension_critica"
  instrumento?: string
  pct_empresa_ama?: number
  pct_referente?: number
  tipo_referente?: "sector" | "pais"
  diferencia_pp?: number
  semaforo?: string
  descripcion?: string
}

export interface BenchmarkingResponse {
  empresa: string
  sector: string
  riesgo_intralaboral_vs_referente: BenchRow[]
  protocolos_urgentes_vs_sector: BenchRow[]
  top3_dimensiones_vs_colombia: BenchRow[]
  insights: Insight[]
  fuente_referente: string
}

// ── S4 Ranking Áreas ───────────────────────────────────────────────────────
export interface AreaCritica {
  ranking: number
  area_departamento: string
  n_evaluados: number
  pct_ama: number
  nivel_predominante: string
  dimension_critica: string
  semaforo: string
}

export interface RankingAreasResponse {
  empresa: string
  top5_areas_criticas: AreaCritica[]
  areas_confidenciales: string[]
  nota: string
  nota_r8: string
}

// ── S5 Alertas Protocolos ──────────────────────────────────────────────────
export interface FichaProtocolo {
  protocolo_id: string
  nombre_protocolo: string
  nivel_urgencia: string
  badge_color: string
  objetivo: string
  kpi_seguimiento: string
  resultado_esperado: string
  n_trabajadores_intervenir: number
  sector_economico: string
  dimensiones_intralaboral_impactadas: string[]
  indicadores_que_impacta: string[]
  es_prioritario_sector: boolean
}

export interface AlertasProtocolosResponse {
  empresa: string
  sector: string
  lineas_gestion_top3: FichaProtocolo[]
  protocolos_prioritarios_sector: string[]
  nota: string
  marco_legal: string
}

// ── Shared ─────────────────────────────────────────────────────────────────
export interface Insight {
  kpi?: string
  tipo: "alerta" | "advertencia"
  severity?: "crítica" | "alta" | "media"
  mensaje: string
}
```

---

## 3. Cliente API

```ts
// lib/api/v3.ts
const BASE = process.env.NEXT_PUBLIC_API_URL ?? "http://localhost:8000"

const headers = { "Content-Type": "application/json" }

export async function getEncabezado(empresa: string, fecha?: string): Promise<Encabezado> {
  const params = new URLSearchParams({ empresa })
  if (fecha) params.set("fecha_evaluacion", fecha)
  const res = await fetch(`${BASE}/v3/encabezado?${params}`)
  if (!res.ok) throw new Error(await res.text())
  return res.json()
}

type FiltrosBase = {
  empresa: string
  fecha_evaluacion?: string
  area_departamento?: string
  categoria_cargo?: string
  forma_intra?: "A" | "B"
}

async function postV3<T>(endpoint: string, filtros: FiltrosBase): Promise<T> {
  const res = await fetch(`${BASE}/v3/${endpoint}`, {
    method: "POST",
    headers,
    body: JSON.stringify(filtros),
  })
  if (!res.ok) throw new Error(await res.text())
  return res.json()
}

export const getKpisGlobales   = (f: FiltrosBase) => postV3<KpisGlobalesResponse>("kpis_globales", f)
export const getDemografia     = (f: FiltrosBase) => postV3<DemografiaResponse>("demografia", f)
export const getCostosAusentismo = (f: FiltrosBase) => postV3<CostosAusentismoResponse>("costos_ausentismo", f)
export const getBenchmarking   = (f: FiltrosBase) => postV3<BenchmarkingResponse>("benchmarking", f)
export const getRankingAreas   = (f: FiltrosBase) => postV3<RankingAreasResponse>("ranking_areas", f)
export const getAlertasProtocolos = (f: FiltrosBase) => postV3<AlertasProtocolosResponse>("alertas_protocolos", f)
```

---

## 4. Estructura de página

```
app/
  visualizador3/
    page.tsx              ← Página raíz (SSR filtro empresa desde URL)
    components/
      V3Header.tsx         ← S0 sticky header
      V3KpisGlobales.tsx   ← S1 grid de tarjetas
      V3KpiCard.tsx        ← Tarjeta individual con barras 5 niveles
      V3Demografia.tsx     ← S2A grid de gráficas
      V3Piramide.tsx       ← Pirámide poblacional bipolar
      V3CostosAusentismo.tsx ← S2B tabla pasos + gauge ausentismo
      V3Benchmarking.tsx   ← S3 tabla ejecutiva
      V3RankingAreas.tsx   ← S4 barras horizontales
      V3AlertasProtocolos.tsx ← S5 fichas acordeón
      V3InsightBanner.tsx  ← Banner narrativo con insights automáticos
```

---

## 5. Especificación de componentes

### S0 — V3Header

**Componente:** Tremor `Card` sticky · fondo `NAVY` · texto blanco

```tsx
// Muestra: Empresa | Sector | N evaluados / N planta (% cobertura) | Fecha
// Badge instrumento: "Forma A" | "Forma B" | "A + B"
// Indicador cobertura: badge verde si ≥80%, amarillo si 60-79%, rojo si <60%
```

**JSON de entrada:** `Encabezado`  
**Recharts/Tremor:** Sin gráfica, solo layout.

---

### S1 — V3KpisGlobales + V3KpiCard

**Narrativa (hook):** Antes del grid, banner `V3InsightBanner` con el insight de mayor severidad.

**Layout:** Grid `2 × 3` responsive → `3 col desktop / 2 col tablet / 1 col mobile`

**Por tarjeta `V3KpiCard`:**

```
┌─────────────────────────────────────────────────┐
│ [Badge semáforo hex]  Intralaboral A    Forma A  │
├─────────────────────────────────────────────────┤
│ Barra apilada 5 colores R11 (ancho ∝ pct_nivel) │
│  ██ Sin  ██ Bajo ██ Medio ██ Alto ██ MuyAlto     │
│  48%    12%    22%    10%    8%                  │
├─────────────────────────────────────────────────┤
│ Alto+MuyAlto: 18%  Referente sector: 37.2%      │
│ Diferencia: −19.2 pp  ▼ Por debajo del referente│
└─────────────────────────────────────────────────┘
```

**Recharts:** `BarChart` horizontal con `stackId="a"` (5 `Bar` con `fill` de `RISK_COLORS`).

```tsx
// Transformar niveles[] a formato Recharts:
const chartData = [{ name: kpi_grupo, ...Object.fromEntries(niveles.map(n => [n.nivel_label, n.pct_nivel])) }]
```

**Semáforo:** Badge con `semaforo` (hex) del backend. No recalcular en frontend.

---

### S2A — V3Demografia

**Narrativa:** Sub-título: `"Perfil de [N] trabajadores evaluados en [empresa]"`.  
R8: Si `n === null` o `confidencial === true` → mostrar `"Confidencial"` en celda (nunca 0).

**Grid:** `2 col desktop / 1 col mobile`. Pirámide ocupa `col-span-2`.

| Gráfico | Variable parquet | Componente Recharts |
|---------|-----------------|---------------------|
| Pirámide poblacional | `piramide_poblacional` | `BarChart` horizontal bipolar (hombres ← → mujeres) |
| Antigüedad empresa | `antiguedad_empresa` | `BarChart` vertical |
| Antigüedad cargo | `antiguedad_cargo` | `BarChart` vertical |
| Estado civil | `estado_civil` | Tremor `DonutChart` |
| Dependientes económicos | `dependientes` | `BarChart` vertical |
| Distribución área | `area` | `BarChart` horizontal (top 10) |
| Distribución cargo | `cargo` | `BarChart` horizontal (top 10) |
| Forma intralaboral | `forma_intra` | Tremor `DonutChart` |

**Pirámide bipolar:**
```tsx
// Mujeres: valores positivos → right bars (fill CYAN)
// Hombres: valores negativos → left bars (fill GOLD)
// Eje X: CustomXAxisTick → abs(value)
```

---

### S2B — V3CostosAusentismo

**Narrativa (conflict hook):**
> "El ausentismo actual de [X%] [supera / está bajo] en [D pp] el promedio nacional del 3%."  
> "Pérdida estimada atribuible al riesgo psicosocial: **$[Z COP]** anuales."

**Layout:** 2 columnas — tabla pasos (izq) · panel resumen ROI (der)

**Tabla pasos:**
```
Paso | Nombre                          | Valor          | Unidad
  1  | N trabajadores (planta)         | 269            | trabajadores
  2  | % ausentismo actual             | 4.8%           | %   [semáforo]
  3  | Costo FTE anual (SMLV)          | $33,600,000    | COP/trabajador
  4  | Capacidad productiva anual      | $9,038,400,000 | COP
  5  | Pérdida económica anual         | $433,843,200   | COP
  6  | Pérdida atribuida empleador 60% | $260,305,920   | COP
  7  | +40% presentismo                | $364,428,288   | COP
  8  | Costo psicosocial (30%)         | $109,328,486   | COP  ← destacar
```

**Panel resumen ROI:**
- Tremor `Metric` con `costo_atribuible_psicosocial`
- `ProgressBar` ausentismo vs referente 3%
- ROI estimado si `roi_estimado_pct` disponible: badge verde si positivo

**Recharts:** `BarChart` comparativo empresa vs referente 3% para ausentismo.

---

### S3 — V3Benchmarking

**Narrativa (insight hook):** Usar `insights` del response. Mostrar máximo 2 banners (alerta > advertencia).

**Layout:** 3 sub-secciones separadas con `Divider`

**3A — Tabla riesgo intralaboral vs referente:**
```
Instrumento | % Empresa A+MA | % Referente (sector/país) | Diferencia pp | Semáforo
IntraA      |     18.0%      |        37.2% (sector)     |   −19.2 pp    |  Verde
IntraB      |     44.4%      |        37.2% (sector)     |   +7.2 pp     |  Amarillo
```
- Badge `tipo_referente`: "Sector" | "País"  
- `diferencia_pp`: color del texto según `semaforo`

**3B — Protocolos urgentes vs sector:** Tremor `List` con checkmarks.

**3C — Top 3 dimensiones vs Colombia:** Tremor `Table` con `semaforo` en celda diferencia.

---

### S4 — V3RankingAreas

**Narrativa (focus hook):**
> "Las áreas [A1, A2] concentran el mayor porcentaje de trabajadores en riesgo crítico."

**Componente:** `BarChart` horizontal Recharts, ordenado por `pct_ama` descendente.

```tsx
// Barra → color = row.semaforo (hex directo del backend)
// Label derecho = `${pct_ama}% A+MA`
// Tooltip: nivel_predominante + dimension_critica
// R8: areas_confidenciales → chip "Confidencial" al final de la lista
```

**Tabla complementaria:**
```
# | Área                   | N  | % A+MA | Nivel         | Dimensión crítica
1 | 300121 - UT IC INTEGRAL| 269| 48.7%  | Muy alto      | Demandas emocionales
2 | 300125 - PREPAGO        | 71 | 36.6%  | Sin riesgo    | Exigencias responsabilidad
```

**Animación:** `animationBegin={0}` `animationDuration={800}` en Recharts.

---

### S5 — V3AlertasProtocolos

**Narrativa (action hook):**
> "Se activan [N] líneas de gestión — [N urgentes] en nivel Urgente."  
> "Protocolo [P]: intervenir [N] trabajadores. KPI objetivo: [K]"

**Layout:** 3 tarjetas expandibles en grid `3 col desktop / 1 col mobile`

**Por tarjeta `FichaProtocolo`:**
```
┌──────────────────────────────────────────────────────────┐
│ [Badge urgencia: color badge_color]  Nombre protocolo    │
│ Sector: [sector_economico]  ★ Prioritario sector (si es) │
├──────────────────────────────────────────────────────────┤
│ Objetivo: ...                                            │
│                                                          │
│ Trabajadores a intervenir: [N] personas                  │
│ ────────────────────────────────────────────             │
│ KPI de seguimiento: ...                                  │
│ Resultado esperado: ...                                  │
├──────────────────────────────────────────────────────────┤
│ [▼ Ver dimensiones e indicadores]  ← Acordeón           │
│   Dimensiones: chip, chip, chip                          │
│   Indicadores: chip, chip                                │
└──────────────────────────────────────────────────────────┘
```

**Badge urgencia:** Usar `badge_color` hex del backend directamente.  
`"Urgente"` → `#EF4444` | `"Correctiva"` → `#F97316` | `"Mejora selectiva"` → `#F59E0B` | Preventivo → `#10B981`

---

## 6. Página principal `page.tsx`

```tsx
// app/visualizador3/page.tsx
"use client"
import { useSearchParams } from "next/navigation"
import { useEffect, useState } from "react"
import * as api from "@/lib/api/v3"

export default function Visualizador3Page() {
  const params = useSearchParams()
  const empresa = params.get("empresa") ?? ""
  
  const filtros = { empresa }

  const [encabezado, setEncabezado] = useState<Encabezado | null>(null)
  const [kpis, setKpis]             = useState<KpisGlobalesResponse | null>(null)
  const [demo, setDemo]             = useState<DemografiaResponse | null>(null)
  const [costos, setCostos]         = useState<CostosAusentismoResponse | null>(null)
  const [bench, setBench]           = useState<BenchmarkingResponse | null>(null)
  const [ranking, setRanking]       = useState<RankingAreasResponse | null>(null)
  const [alertas, setAlertas]       = useState<AlertasProtocolosResponse | null>(null)

  useEffect(() => {
    if (!empresa) return
    // Cargar en paralelo — S0 primero (bloquea header), resto en paralelo
    api.getEncabezado(empresa).then(setEncabezado)
    Promise.all([
      api.getKpisGlobales(filtros).then(setKpis),
      api.getDemografia(filtros).then(setDemo),
      api.getCostosAusentismo(filtros).then(setCostos),
      api.getBenchmarking(filtros).then(setBench),
      api.getRankingAreas(filtros).then(setRanking),
      api.getAlertasProtocolos(filtros).then(setAlertas),
    ])
  }, [empresa])

  return (
    <main className="min-h-screen bg-[#0A1628] text-white font-[Inter,Arial,sans-serif]">
      {encabezado && <V3Header data={encabezado} />}
      <div className="max-w-[1400px] mx-auto px-6 py-8 space-y-12">
        {kpis    && <V3KpisGlobales data={kpis} />}
        {demo    && <V3Demografia data={demo} />}
        {costos  && <V3CostosAusentismo data={costos} />}
        {bench   && <V3Benchmarking data={bench} />}
        {ranking && <V3RankingAreas data={ranking} />}
        {alertas && <V3AlertasProtocolos data={alertas} />}
      </div>
    </main>
  )
}
```

---

## 7. Narrativa ejecutiva por sección (data-storytelling)

### Marco: Situación → Conflicto → Resolución

```
S0  CONTEXTO
    "[N evaluados] trabajadores de [empresa] ([sector]) — cobertura [X%]"
    Fecha: [fecha_evaluacion]

S1  SITUACIÓN ACTUAL (Hook)
    "[X%] de trabajadores presentan riesgo Alto o Muy Alto en Intralaboral [A/B]"
    → [±D pp] vs referente [sector/país] (ENCST 2021)
    → Semaforizar diferencia según umbrales narrativos:
       >+15 pp → "Significativamente superior al promedio nacional"
       +5..+15 pp → "Por encima del referente, requiere atención"
       ±5 pp → "En línea con el promedio nacional"
       <-5 pp → "Por debajo del referente nacional"

S2  CONFLICTO (Cuantificación)
    "El ausentismo actual ([Y%]) [supera / está bajo] en [D pp] el promedio nacional"
    → "Pérdida económica estimada: $[Z] anuales atribuida al empleador"
    → "El [30%] de esa pérdida ($[W]) es atribuible al riesgo psicosocial"

S3  POSICIONAMIENTO
    "[N] de [M] instrumentos superan el referente [sector/país]"
    → "Dimensiones más alejadas del referente nacional: [D1, D2, D3]"
    → "[N protocolos] coinciden con los priorizados para el sector [S]"

S4  FOCO (Áreas críticas)
    "Las áreas [A1, A2] concentran el mayor riesgo ([X% A+MA])"
    → Candidatas prioritarias para intervención inmediata

S5  ACCIÓN
    "Se activan [N] protocolos — [N urgentes] en nivel Urgente"
    → "Protocolo [P]: intervenir [N] trabajadores · KPI: [K]"
    → "Resultado esperado: reducción ≥ 10 pp en próxima evaluación"
```

### Umbrales narrativos (Res. 2764/2022)

| Diferencia pp vs referente | Texto narrativo |
|---------------------------|-----------------|
| > +15 pp | "Significativamente superior al promedio nacional" |
| +5 a +15 pp | "Por encima del referente, requiere atención" |
| ±5 pp | "En línea con el promedio nacional" |
| < −5 pp | "Por debajo del referente nacional" |

---

## 8. Respuestas JSON de referencia

### GET /v3/encabezado?empresa=CONSULTEL

```json
{
  "empresa": "CONSULTEL",
  "sector": "Servicios",
  "n_evaluados": 269,
  "n_planta": 269,
  "pct_cobertura": 100.0,
  "fecha_evaluacion": "2024-01-15",
  "formas_disponibles": ["A", "B"],
  "instrumento": "Batería MinTrabajo Col. Res. 2764/2022"
}
```

### POST /v3/kpis_globales  `{ "empresa": "CONSULTEL" }`

```json
{
  "empresa": "CONSULTEL",
  "kpis": [
    {
      "kpi_grupo": "Intralaboral A",
      "niveles": [
        { "nivel_label": "Sin riesgo",     "n_nivel": 24, "pct_nivel": 48.0 },
        { "nivel_label": "Riesgo bajo",    "n_nivel": 6,  "pct_nivel": 12.0 },
        { "nivel_label": "Riesgo medio",   "n_nivel": 11, "pct_nivel": 22.0 },
        { "nivel_label": "Riesgo alto",    "n_nivel": 5,  "pct_nivel": 10.0 },
        { "nivel_label": "Riesgo muy alto","n_nivel": 4,  "pct_nivel": 8.0  }
      ],
      "pct_alto_muy_alto": 18.0,
      "pct_referente": 37.2,
      "tipo_referente": "sector",
      "diferencia_pp": -19.2,
      "semaforo": "#F59E0B"
    }
  ],
  "insights": [
    {
      "kpi": "Intralaboral B",
      "tipo": "advertencia",
      "severity": "media",
      "mensaje": "44.4% en riesgo Alto+Muy Alto en Intralaboral B. Requiere monitoreo preventivo."
    }
  ]
}
```

### POST /v3/ranking_areas  `{ "empresa": "CONSULTEL" }`

```json
{
  "empresa": "CONSULTEL",
  "top5_areas_criticas": [
    {
      "ranking": 1,
      "area_departamento": "300121 - UT IC INTEGRAL",
      "n_evaluados": 269,
      "pct_ama": 48.7,
      "nivel_predominante": "Riesgo muy alto",
      "dimension_critica": "Demandas emocionales",
      "semaforo": "#EF4444"
    }
  ],
  "areas_confidenciales": [],
  "nota": "Ordenado de mayor a menor % riesgo Alto+Muy Alto Intralaboral (A+B promedio).",
  "nota_r8": "0 área(s) excluida(s) por confidencialidad (n < 5)."
}
```

---

## 9. Parquets V3 — Esquemas reales

### fact_v3_kpis_globales.parquet (245 filas)

| Columna | Tipo | Valores ejemplo |
|---------|------|-----------------|
| empresa | str | "CONSULTEL" |
| kpi_grupo | str | "Intralaboral A", "Intralaboral B", "Extralaboral A"... |
| instrumento | str | "IntraA", "IntraB", "Extra", "Estres" |
| forma | str | "A", "B" |
| nivel_num | int | 1–5 |
| nivel_label | str | "Sin riesgo", "Riesgo bajo", "Riesgo medio", "Riesgo alto", "Riesgo muy alto" |
| n_total | int | total evaluados |
| n_nivel | int | N en ese nivel |
| pct_nivel | float | % en ese nivel |
| pct_alto_muy_alto | float | % niveles 4+5 |
| pct_referente | float | % del referente (sector o país) |
| tipo_referente | str | "sector" \| "pais" |
| diferencia_pp | float | empresa − referente |
| semaforo | str | hex color |

### fact_v3_demografia.parquet (464 filas)

| Columna | Tipo | Valores ejemplo |
|---------|------|-----------------|
| empresa | str | "CONSULTEL" |
| variable | str | "piramide_poblacional", "antiguedad_empresa", "antiguedad_cargo", "estado_civil", "dependientes", "area", "cargo", "forma_intra" |
| categoria | str | etiqueta de categoría |
| sexo | str \| null | "Hombre" / "Mujer" (solo piramide_poblacional) |
| n | int | conteo |
| pct | float | porcentaje |
| confidencial | bool | R8: true si n < 5 |

### fact_v3_costos.parquet (96 filas — ~8 pasos × N empresas)

| Columna | Tipo |
|---------|------|
| empresa | str |
| paso | int (1–8) |
| nombre_paso | str |
| valor | float |
| unidad | str |
| nota | str |
| n_planta | int |
| total_dias_ausencia | int |
| dias_cap_instalada | int |
| pct_ausentismo | float |
| diferencia_pp_vs_pais | float |
| semaforo_ausentismo | str (hex) |

### fact_v3_benchmarking.parquet (66 filas)

| Columna | Tipo |
|---------|------|
| empresa | str |
| sector | str |
| tipo | str ("riesgo_intralaboral", "protocolos_urgentes", "dimension_critica") |
| instrumento | str |
| pct_empresa_ama | float |
| pct_referente | float |
| tipo_referente | str |
| diferencia_pp | float |
| semaforo | str (hex) |
| descripcion | str |

### fact_v3_ranking_areas.parquet (44 filas)

| Columna | Tipo |
|---------|------|
| empresa | str |
| area_departamento | str |
| n_evaluados | int |
| confidencial | bool |
| pct_ama | float |
| nivel_predominante | str |
| dimension_critica | str |
| semaforo | str (hex) |
| ranking | float |

---

## 10. Checklist de implementación frontend

- [ ] Instalar dependencias: `recharts tremor @tremor/ui`
- [ ] Crear `lib/tokens.ts` con paleta R11
- [ ] Crear `types/v3.ts` con todas las interfaces
- [ ] Crear `lib/api/v3.ts` con cliente
- [ ] Implementar `V3Header` (sticky, S0)
- [ ] Implementar `V3KpisGlobales` + `V3KpiCard` (barras 5 colores)
- [ ] Implementar `V3Piramide` (bipolar hombres/mujeres)
- [ ] Implementar `V3Demografia` (grid 8 gráficas)
- [ ] Implementar `V3CostosAusentismo` (tabla pasos + panel ROI)
- [ ] Implementar `V3Benchmarking` (tabla + insights)
- [ ] Implementar `V3RankingAreas` (barras horizontales animadas)
- [ ] Implementar `V3AlertasProtocolos` (fichas acordeón)
- [ ] Aplicar R8 en todos los componentes: `null` → "Confidencial"
- [ ] Validar paleta R11 en todos los semáforos
- [ ] Responsive: mobile-first, mínimo 320px

---

## 11. Comando para levantar la API

```bash
# Backend
uvicorn api.main:app --reload
# → http://localhost:8000/docs  (Swagger UI)
# → GET /v3/encabezado?empresa=CONSULTEL
# → POST /v3/kpis_globales  body: {"empresa":"CONSULTEL"}
```

---

*Generado: 2026-04-01 | MentalPRO v3.0.0 | Resolución 2764/2022*  
*Skills aplicados: kpi-dashboard-design · data-storytelling*
