"""
api/utils/storytelling.py
=========================
Motor de Insights y Storytelling Condicional para MentalPRO.

Genera recomendaciones textuales basadas en reglas de negocio que el
Backend inyecta en cada respuesta JSON. El Frontend solo necesita
renderizar el array `insights[]` que recibe.

Tipos de insight:
  - "alerta"    → Situación de riesgo que requiere acción inmediata
  - "advertencia" → Indicador por encima de umbrales moderados
  - "positivo"  → Resultado favorable respecto a referentes
  - "informativo" → Contexto técnico o normativo relevante

Severidades: "critica", "alta", "media", "baja"
"""

import logging
from typing import Any

log = logging.getLogger(__name__)

# ══════════════════════════════════════════════════════════════════════════════
# Umbrales de negocio (coherente con config.yaml y reglas_negocio.md)
# ══════════════════════════════════════════════════════════════════════════════

UMBRAL_ROJO = 35.0          # % A+MA que activa alerta roja
UMBRAL_AMARILLO = 15.0      # % A+MA que activa advertencia
UMBRAL_ALTA_PRESENCIA = 40.0  # % alta presencia en preguntas
UMBRAL_VULNERABILIDAD = 20.0  # % vulnerabilidad que preocupa
UMBRAL_DIFERENCIA_PP = 5.0    # Diferencia en pp respecto a país para alertar


def _insight(tipo: str, texto: str, severidad: str, seccion: str = "", metrica: str = "") -> dict:
    """Constructor base de un insight."""
    return {
        "tipo": tipo,
        "severidad": severidad,
        "texto": texto,
        "seccion": seccion,
        "metrica": metrica,
    }


# ══════════════════════════════════════════════════════════════════════════════
# INSIGHTS PARA SECCIÓN 2: KPIs
# ══════════════════════════════════════════════════════════════════════════════

def generar_insights_kpis(kpis: dict) -> list[dict]:
    """Analiza los 5 KPIs y genera insights contextuales."""
    insights = []

    # ── KPI 2.1: Intralaboral A y B ──
    kpi_intra = kpis.get("kpi_2_1_intralaboral", {})
    for forma_key, forma_label in [("forma_a", "Forma A"), ("forma_b", "Forma B")]:
        data = kpi_intra.get(forma_key, {})
        pct = data.get("pct")
        if isinstance(pct, (int, float)):
            ref = kpi_intra.get("referente", {}).get("pct", 0)
            diff = round(pct - ref, 1)
            if pct > UMBRAL_ROJO:
                insights.append(_insight(
                    "alerta",
                    f"El riesgo intralaboral {forma_label} ({pct}%) supera el umbral crítico "
                    f"del {UMBRAL_ROJO}%. Se recomienda activar protocolos de intervención "
                    f"prioritaria según Res. 2764/2022.",
                    "critica", "S2", "intralaboral"
                ))
            if diff > UMBRAL_DIFERENCIA_PP:
                insights.append(_insight(
                    "advertencia",
                    f"El riesgo intralaboral {forma_label} está {diff} puntos porcentuales "
                    f"por encima del referente ({ref}%). Considere focalizar la intervención "
                    f"en los dominios con mayor contribución.",
                    "alta", "S2", "intralaboral_vs_ref"
                ))
            elif diff < -UMBRAL_DIFERENCIA_PP:
                insights.append(_insight(
                    "positivo",
                    f"El riesgo intralaboral {forma_label} ({pct}%) se encuentra {abs(diff)} "
                    f"puntos por debajo del referente ({ref}%). Resultado favorable.",
                    "baja", "S2", "intralaboral_vs_ref"
                ))

    # ── KPI 2.2: Estrés ──
    kpi_estres = kpis.get("kpi_2_2_estres", {})
    pct_estres = kpi_estres.get("pct_empresa")
    diff_estres = kpi_estres.get("diferencia_pp")
    if isinstance(pct_estres, (int, float)):
        if pct_estres > UMBRAL_ROJO:
            insights.append(_insight(
                "alerta",
                f"El {pct_estres}% de los trabajadores presenta estrés alto o muy alto. "
                f"Este nivel exige intervención inmediata y re-evaluación en máximo 1 año "
                f"según lineamientos del Ministerio de Trabajo.",
                "critica", "S2", "estres"
            ))
        if isinstance(diff_estres, (int, float)) and diff_estres > UMBRAL_DIFERENCIA_PP:
            insights.append(_insight(
                "advertencia",
                f"El estrés en la empresa supera al promedio nacional en {diff_estres} "
                f"puntos porcentuales. Se sugiere revisar las demandas cuantitativas "
                f"y la jornada de trabajo como posibles factores contribuyentes.",
                "alta", "S2", "estres_vs_pais"
            ))

    # ── KPI 2.3: Vulnerabilidad Psicológica ──
    kpi_vuln = kpis.get("kpi_2_3_vulnerabilidad", {})
    pct_vuln = kpi_vuln.get("pct_empresa")
    if isinstance(pct_vuln, (int, float)):
        if pct_vuln > UMBRAL_VULNERABILIDAD:
            insights.append(_insight(
                "alerta",
                f"El {pct_vuln}% de los trabajadores presenta vulnerabilidad psicológica "
                f"(factor individual bajo o muy bajo). Esto indica baja capacidad de "
                f"afrontamiento ante el estrés. Se recomienda implementar programas de "
                f"fortalecimiento de habilidades socioemocionales.",
                "alta", "S2", "vulnerabilidad"
            ))
        elif pct_vuln < 10:
            insights.append(_insight(
                "positivo",
                f"Solo el {pct_vuln}% presenta vulnerabilidad psicológica. La mayoría "
                f"de los trabajadores cuenta con recursos personales de protección "
                f"adecuados frente al riesgo psicosocial.",
                "baja", "S2", "vulnerabilidad"
            ))

    # ── KPI 2.5: Dimensiones críticas ──
    kpi_dims = kpis.get("kpi_2_5_dimensiones_criticas", {})
    n_criticas = kpi_dims.get("conteo", 0)
    dims_lista = kpi_dims.get("dimensiones", [])
    if n_criticas > 5:
        top3 = ", ".join([d["dimension"] for d in dims_lista[:3]])
        insights.append(_insight(
            "alerta",
            f"Se identificaron {n_criticas} dimensiones con resultados por encima "
            f"del referente nacional. Las más críticas son: {top3}. "
            f"Es prioritario diseñar un plan de intervención multidimensional.",
            "critica", "S2", "dimensiones_criticas"
        ))
    elif n_criticas > 0:
        insights.append(_insight(
            "informativo",
            f"{n_criticas} dimensión(es) de riesgo se encuentra(n) por encima "
            f"del parámetro nacional. Consulte la tabla comparativa en la Sección 5 "
            f"para identificar los focos de intervención.",
            "media", "S2", "dimensiones_criticas"
        ))

    return insights


# ══════════════════════════════════════════════════════════════════════════════
# INSIGHTS PARA SECCIÓN 3: GRÁFICAS
# ══════════════════════════════════════════════════════════════════════════════

def generar_insights_dominios(dominios: list[dict]) -> list[dict]:
    """Analiza barras de dominios intralaboral A vs B."""
    insights = []
    for dom in dominios:
        for forma in ["A", "B"]:
            pct = dom.get(f"pct_{forma}")
            if isinstance(pct, (int, float)) and pct > UMBRAL_ROJO:
                insights.append(_insight(
                    "alerta",
                    f"El dominio '{dom['dominio']}' en Forma {forma} alcanza {pct}% "
                    f"de riesgo alto y muy alto, superando el umbral de {UMBRAL_ROJO}%. "
                    f"Revise las dimensiones que lo componen para priorizar la acción.",
                    "alta", "S3.1", f"dominio_{forma}"
                ))

    # Comparar A vs B
    for dom in dominios:
        pct_a = dom.get("pct_A")
        pct_b = dom.get("pct_B")
        if isinstance(pct_a, (int, float)) and isinstance(pct_b, (int, float)):
            diff = abs(pct_a - pct_b)
            if diff > 15:
                mayor = "A (jefaturas)" if pct_a > pct_b else "B (operativos)"
                insights.append(_insight(
                    "informativo",
                    f"En '{dom['dominio']}' existe una brecha de {diff:.1f} pp entre "
                    f"Forma A y B. La Forma {mayor} presenta mayor exposición. "
                    f"Considere estrategias diferenciadas por tipo de cargo.",
                    "media", "S3.1", "brecha_a_b"
                ))

    return insights


def generar_insights_proteccion(distribucion: list[dict]) -> list[dict]:
    """Analiza la distribución de protección psicológica (dona)."""
    insights = []
    pct_vulnerables = sum(
        d["pct"] for d in distribucion
        if d.get("nivel_num", 0) <= 2
    )
    pct_protegidos = sum(
        d["pct"] for d in distribucion
        if d.get("nivel_num", 0) >= 4
    )

    if pct_vulnerables > 40:
        insights.append(_insight(
            "alerta",
            f"El {pct_vulnerables:.1f}% de la población tiene niveles bajos o muy bajos "
            f"de protección psicológica. Esto implica alta susceptibilidad ante "
            f"factores de riesgo psicosocial. Se recomienda implementar programas de "
            f"desarrollo de capital psicológico y resiliencia organizacional.",
            "critica", "S3.3", "proteccion"
        ))
    elif pct_protegidos > 60:
        insights.append(_insight(
            "positivo",
            f"El {pct_protegidos:.1f}% de los trabajadores presenta niveles altos o "
            f"muy altos de protección psicológica. Este es un factor protector "
            f"significativo para la organización.",
            "baja", "S3.3", "proteccion"
        ))

    return insights


# ══════════════════════════════════════════════════════════════════════════════
# INSIGHTS PARA SECCIÓN 4: HEATMAP Y TREEMAP
# ══════════════════════════════════════════════════════════════════════════════

def generar_insights_heatmap(areas_data: list[dict], dominios: list[str]) -> list[dict]:
    """Detecta áreas con concentración crítica de riesgos en múltiples dominios."""
    insights = []
    for area in areas_data:
        n_dominios_criticos = 0
        for dom in dominios:
            dom_key = dom.lower().replace(" ", "_")[:15]
            pct = area.get(f"pct_{dom_key}")
            if isinstance(pct, (int, float)) and pct > UMBRAL_ROJO:
                n_dominios_criticos += 1

        if n_dominios_criticos >= 3:
            insights.append(_insight(
                "alerta",
                f"El área '{area['area']}' presenta riesgo crítico (>{UMBRAL_ROJO}%) "
                f"en {n_dominios_criticos} de 4 dominios intralaborales. "
                f"Esta área requiere intervención integral e inmediata.",
                "critica", "S4.1", "area_multicritica"
            ))
        elif n_dominios_criticos >= 2:
            insights.append(_insight(
                "advertencia",
                f"El área '{area['area']}' supera el umbral crítico en "
                f"{n_dominios_criticos} dominios. Se recomienda priorizar "
                f"intervenciones focalizadas en esta área.",
                "alta", "S4.1", "area_riesgo"
            ))

    return insights


def generar_insights_treemap(cargos: list[dict]) -> list[dict]:
    """Identifica cargos con mayor concentración de riesgo."""
    insights = []
    cargos_criticos = [
        c for c in cargos
        if isinstance(c.get("pct"), (int, float)) and c["pct"] > UMBRAL_ROJO
    ]

    if len(cargos_criticos) > 3:
        nombres = ", ".join([c["cargo"] for c in cargos_criticos[:3]])
        insights.append(_insight(
            "advertencia",
            f"{len(cargos_criticos)} cargos superan el {UMBRAL_ROJO}% de riesgo "
            f"intralaboral alto y muy alto. Los más afectados: {nombres}. "
            f"Considere intervenciones específicas por rol.",
            "alta", "S4.2", "cargos_criticos"
        ))

    return insights


# ══════════════════════════════════════════════════════════════════════════════
# INSIGHTS PARA SECCIÓN 5: TABLAS DIMENSIONES
# ══════════════════════════════════════════════════════════════════════════════

def generar_insights_tabla_dimensiones(dimensiones: list[dict], forma: str) -> list[dict]:
    """Analiza la tabla de dimensiones vs país (S5.1)."""
    insights = []
    criticas = [d for d in dimensiones if d.get("is_critical")]

    if criticas:
        top = sorted(criticas, key=lambda x: x.get("diferencia_pp", 0)
                     if isinstance(x.get("diferencia_pp"), (int, float)) else 0, reverse=True)
        if len(top) >= 1 and isinstance(top[0].get("diferencia_pp"), (int, float)):
            insights.append(_insight(
                "informativo",
                f"En Forma {forma}, la dimensión con mayor brecha frente al país es "
                f"'{top[0]['dimension']}' (+{top[0]['diferencia_pp']} pp). "
                f"Total de dimensiones por encima del referente: {len(criticas)}.",
                "media", "S5.1", "dimension_brecha"
            ))

    return insights


def generar_insights_res2764(filas: list[dict]) -> list[dict]:
    """Analiza el resultado global Res. 2764 (S5.3)."""
    insights = []
    for fila in filas:
        nivel = fila.get("nivel_riesgo", "")
        forma = fila.get("forma", "")
        if fila.get("flag_alert"):
            insights.append(_insight(
                "alerta",
                f"⚠️ Res. 2764/2022: El resultado global de riesgo intralaboral "
                f"Forma {forma} clasifica en nivel '{nivel}'. Esto activa la "
                f"obligación legal de re-evaluación en máximo 1 año e "
                f"intervención inmediata según la normativa colombiana.",
                "critica", "S5.3", "res2764"
            ))

    return insights


# ══════════════════════════════════════════════════════════════════════════════
# INSIGHTS PARA SECCIÓN 6: FRECUENCIAS
# ══════════════════════════════════════════════════════════════════════════════

def generar_insights_frecuencias(preguntas: list[dict]) -> list[dict]:
    """Analiza las preguntas con mayor diferencia vs país (S6.1)."""
    insights = []
    criticas = [p for p in preguntas if p.get("is_critical")]

    if len(criticas) > 10:
        insights.append(_insight(
            "advertencia",
            f"{len(criticas)} preguntas tienen mayor alta presencia que el promedio "
            f"nacional. Esto sugiere patrones sistemáticos de exposición a riesgo. "
            f"Revise las primeras 5 preguntas para identificar factores comunes.",
            "alta", "S6.1", "preguntas_criticas"
        ))

    # Identificar la pregunta más preocupante
    if criticas:
        peor = max(criticas, key=lambda x: x.get("diferencia_pp", 0))
        if peor.get("diferencia_pp", 0) > 20:
            insights.append(_insight(
                "alerta",
                f"La pregunta con mayor brecha frente al país (+{peor['diferencia_pp']} pp): "
                f"'{str(peor.get('pregunta', ''))[:80]}...'. "
                f"Este indicador requiere atención prioritaria.",
                "critica", "S6.1", "pregunta_peor"
            ))

    return insights


def generar_insights_alta_presencia(preguntas: list[dict], forma: str) -> list[dict]:
    """Analiza todas las preguntas de alta presencia (S6.2)."""
    insights = []
    criticas_40 = [p for p in preguntas if p.get("is_critical")]

    if len(criticas_40) > 15:
        insights.append(_insight(
            "advertencia",
            f"En Forma {forma}, {len(criticas_40)} preguntas superan el 40% de "
            f"alta presencia. Esto indica una exposición generalizada a factores "
            f"de riesgo en múltiples dimensiones.",
            "alta", "S6.2", "alta_presencia_masiva"
        ))
    elif len(criticas_40) == 0:
        insights.append(_insight(
            "positivo",
            f"En Forma {forma}, ninguna pregunta supera el 40% de alta presencia. "
            f"Los niveles de exposición son moderados en esta población.",
            "baja", "S6.2", "alta_presencia_ok"
        ))

    return insights
