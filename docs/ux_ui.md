# ux_ui.md — Design System AVANTUM (MentalPRO Web)
> Reglas de diseño obligatorias para la plataforma Frontend MentalPRO | Versión 3.0
> **La arquitectura visual está basada en Next.js, React, Tailwind CSS y componentes compatibles (Recharts/Tremor).**

---

## REGLAS CRÍTICAS — NUNCA VIOLAR

| Código | Regla |
|--------|-------|
| RC-01 | El esquema de color y variables CSS deben registrarse estrictamente en `tailwind.config.ts`. |
| RC-02 | El renderizado de gráficas recae sobre Chart.js, Recharts, o Tremor, no sobre Plotly nativo de Python. |
| RC-03 | Los 5 colores risk_1…risk_5 son normativos (Res. 2764). NUNCA cambiarlos. Su código HEX exacto debe respetarse en las interfaces. |
| RC-04 | El Backend nunca envía colores, solo datos numéricos y etiquetas de riesgo. El Frontend mapea la etiqueta al color. |
| RC-05 | Los Tooltips son obligatorios en toda gráfica y deben mantener un fondo `navy` (`#0A1628`) y texto `white` (`#FFFFFF`). |
| RC-06 | Nunca omitir etiquetas en ejes X e Y para accesibilidad. |
| RC-07 | Interacción rápida: los componentes del dashboard deben soportar filtrado en cliente sin repintados completos lentos. |
| RC-08 | Diseño 100% responsivo (Grid adaptativo, no canvas fijos). El diseño pasa de `grid-cols-1` en móviles a `grid-cols-4` en pantallas ultra-anchas. |
| RC-09 | **Confidencialidad (R8):** Si el endpoint devuelve `"Confidencial"`, el UI debe blurear el dato o mostrar un componente de alerta (shield icon) sin exponer el número. |

---

## 1. PALETA AVANTUM — TOKENS (Tailwind config)

Estos colores deben ser los `theme.extend.colors` oficiales del proyecto Next.js:

```typescript
// tailwind.config.ts
const avantumColors = {
  // Primarios
  navy:      '#0A1628',    // Fondo principal, headers
  navy_m:    '#0E2244',    // Fondos secundarios, sidebars
  navy_l:    '#1A3A6B',    // Bordes énfasis
  gold:      '#C9952A',    // CTAs, highlights, valor
  gold_l:    '#E8B84B',    // Hover states
  gold_s:    '#F5D78E',    // Tags, chips
  cyan:      '#00C2CB',    // IA, tendencias
  cyan_l:    '#5CE8EE',    // Elementos secundarios
  teal:      '#00D4AA',    // Métricas crecimiento
  // Soporte
  white:     '#FFFFFF',
  g100:      '#F4F5F7',
  g200:      '#E2E5EA',
  g400:      '#9AA3B0',
  g600:      '#5A6478',
  g800:      '#2D3445',
  // Sistema / Estado
  ok:        '#10B981',
  warn:      '#F59E0B',
  err:       '#EF4444',
  orange:    '#F97316',
  ok_light:  '#6EE7B7',
  
  // NORMATIVOS — 5 niveles riesgo Res. 2764 — INAMOVIBLES
  risk_1:    '#10B981',    // Sin riesgo / Muy bajo
  risk_2:    '#6EE7B7',    // Bajo
  risk_3:    '#F59E0B',    // Medio
  risk_4:    '#F97316',    // Alto
  risk_5:    '#EF4444',    // Muy alto
}
```

La escala categórica dinámica para gráficas multidimensionales es:
`['#00C2CB', '#C9952A', '#10B981', '#1A3A6B', '#F59E0B', '#EF4444', '#F5D78E', '#5CE8EE']`

---

## 2. TIPOGRAFÍA

```css
/* fonts.css o integracion next/font */
--font-display: 'Montserrat', sans-serif;
--font-body: 'Inter', sans-serif;
--font-mono: 'JetBrains Mono', monospace;
```

Escala de fuente en UI:
- KPI Valores: `text-4xl` o `text-5xl` (Montserrat)
- KPI Etiquetas: `text-xs uppercase tracking-wide opacity-50` (Inter)
- Headers Gráficas: `text-lg font-semibold`
- Tablas y Tooltips: `text-sm`

---

## 3. COMPONENTES GLOBALES (Guía de Tailwind)

*   **Fondo de la App:** `bg-navy text-white min-h-screen font-body`
*   **Sidebar Navigation:** `bg-navy_m/95 border-r border-white/10 w-64 fixed h-full`
*   **Main Content Area:** `ml-64 p-6 md:p-8 flex-1`
*   **Tarjetas KPI:** `bg-navy_m/60 border border-white/10 rounded-xl p-5 hover:bg-navy_m transition-colors`
*   **Tarjetas de Gráficos:** `bg-navy_m/50 border border-white/5 rounded-xl p-5`
*   **Celdas Risk N (Backgrounds para Heatmaps/Tablas):**
    *   Risk 1: `bg-ok/15 text-ok`
    *   Risk 2: `bg-ok_light/12 text-ok_light`
    *   Risk 3: `bg-warn/15 text-warn`
    *   Risk 4: `bg-orange/15 text-orange`
    *   Risk 5: `bg-err/15 text-err`

---

## 4. CATÁLOGO DE GRÁFICAS POR CASO DE USO (Recharts / Tremor)

| Variable | Tipo de Componente | Nota |
|---------|---------|------|
| 1 categoría ≤6 valores | PieChart / DonutChart | Colores de la escala categórica |
| 1 categoría >6 valores | BarChart horizontal | Ordenar por frecuencia desc. |
| Niveles riesgo 1-5 | BarChart apilada 100% | Colores normativos OBLIGATORIOS exactos |
| Flujo eje→línea | Sankey Diagram | Idealmente mediante Nivo Sankey o D3.js |
| Cross-tabulation | Heatmap | Componente grid de Tailwind con colores risk |
| Benchmarking | Bullet Chart / Progress | Mostrar meta verticalmente |
| Radar (múltiples vars) | RadarChart | Opacidad de fill: 0.2, stroke width: 2 |

---
*Fin Documento Marco Frontend*