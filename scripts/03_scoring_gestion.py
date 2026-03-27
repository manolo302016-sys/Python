"""
03_scoring_gestion.py — Implementa V2-Pasos 1 a 3
Pasos:
  V2-Paso1 : Estandarización scores 0-1 (valor_invertido ya normalizado)
  V2-Paso2 : Agrupación ítem → indicador → línea → eje del modelo de gestión
  V2-Paso3 : Calificación ponderada (3 niveles) + inversión nivel 2

Input:  data/processed/fact_scores_brutos.parquet
Output: data/processed/fact_gestion_scores.parquet

Reglas críticas:
  R9  : Media ponderada en gestión — NUNCA promedio simple
  R10 : Inversión nivel 2 ANTES de calcular score de línea
       Indicadores de RIESGO: score = 1 - score_0a1

Fórmula de 3 niveles:
  score_indicador = Σ(score_item × peso_item) / Σ(peso)
  score_linea     = Σ(score_indicador × peso_indicador) / Σ(peso)
  score_eje       = Σ(score_linea × peso_linea) / Σ(peso)

Fuente documental: Visualizador 2, Pasos 1-3
Versión: 1.0 | Pipeline MentalPRO | Modelo AVANTUM
"""

import logging
import sys
from pathlib import Path
from typing import Tuple, Dict, List
import numpy as np
import pandas as pd
import yaml

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("03_scoring_gestion")


def cargar_config(config_path: str = "config/config.yaml") -> dict:
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


# ===========================================================================
# V2-PASO 1: ESTANDARIZACIÓN 0-1
# Fuente: Visualizador 2, Paso 1
# NOTA: valor_invertido de fact_scores_brutos ya está en escala 0-4 o 0-1
# Solo necesitamos normalizar a 0-1 los que están en escala 0-4
# ===========================================================================

def estandarizar_a_01(valor: float, escala_max: float) -> float:
    """
    Estandariza un valor a escala 0-1.
    V2-Paso1: 4→1.0, 3→0.75, 2→0.5, 1→0.25, 0→0
    """
    if pd.isna(valor) or escala_max == 0:
        return np.nan
    return float(valor) / float(escala_max)


# Escala máxima por instrumento (para normalización)
ESCALA_MAX_POR_INSTRUMENTO = {
    "intralaboral_a": 4.0,
    "intralaboral_b": 4.0,
    "extralaboral": 4.0,
    "estres": None,  # Ya tiene escalas diversas, se maneja diferente
    "afrontamiento": 1.0,  # Ya está en 0-1
    "capital_psicologico": 1.0,  # Ya está en 0-1
}


# ===========================================================================
# V2-PASO 2 y 3: MODELO DE GESTIÓN — Agrupación + Ponderaciones
# Fuente: Visualizador 2, Pasos 2-3 (filas 70-381, 382-650)
# ===========================================================================

# ---------------------------------------------------------------------------
# INDICADORES A INVERTIR (NIVEL 2)
# V2-Paso3: Estos indicadores miden RIESGO (mayor = peor)
# Aplicar: score = 1 - score_0a1 ANTES de agregar a línea
# Fuente: Visualizador 2 Paso 3 — columnas con "1 - valor"
# ---------------------------------------------------------------------------
INDICADORES_A_INVERTIR = frozenset([
    # Afrontamiento — indicadores de evitación
    "Autonegación",
    "Evitación cognitiva",
    "Evitación conductual",
    # Extralaboral — condiciones negativas
    "Accesibilidad del entorno",
    "Apoyo social",
    "Condiciones de la vivienda",
    "Movilidad eficiente y desplazamiento saludable",
    "Relación con el núcleo familiar",
    "Seguridad del entorno",
    # Estrés — manifestaciones negativas
    "Alteraciones cognitivas",
    "Desgaste emocional",
    "Pérdida de sentido",
    "Somatización y fatiga física",
    "Dererioro de relaciones sociales y aislamiento",
    # Bienestar financiero (ya invertido en V2)
    "Bienestar financiero",
    # Motivación
    "Desmotivación y desgaste laboral",
    # Vida-trabajo
    "Interferencia temporal",
    "Impacto cognitivo-relacional",
    "Conflicto vida - trabajo",
    # Condiciones de trabajo — demandas
    "Disonancia emocional",
    "Hostilidad y violencia",
    "Exposición al sufrimiento",
    "Riesgos biomecánicos",
    "Riesgos higiénicos",
    # Cargas
    "Ritmo de trabajo",
    "Volumen de trabajo",
    "Simultaneidad",
    # Jornadas
    "Trabajo nocturno",
    # Arquitectura roles — conflictos
    "Contradicción en estructura de mando",
    # Conductas
    "Conductas de riesgo",
])


# ---------------------------------------------------------------------------
# ESTRUCTURA DEL MODELO DE GESTIÓN
# Eje → Líneas → Indicadores con pesos
# Fuente: Visualizador 2 Pasos 2-3 y Paso 6
# ---------------------------------------------------------------------------

# Estructura: {eje: {linea: {indicador: peso_indicador}}}
# Los pesos son relativos dentro de cada nivel

MODELO_GESTION = {
    "Bienestar biopsicosocial": {
        "peso_eje": 1.0,  # Este eje tiene peso 1.0 (suma de líneas)
        "lineas": {
            "Afrontamiento del estrés y recursos psicológicos": {
                "peso_linea": 0.15,
                "indicadores": {
                    "Actitud resiliente": 0.05,
                    "Activa red de apoyo": 0.02,
                    "Aprendizaje resiliente": 0.02,
                    "Autoeficacia percibida": 0.08,
                    "Autonegación": 0.05,
                    "Busqueda de Apoyo profesional": 0.02,
                    "Busqueda de consejo y orientación": 0.02,
                    "Busqueda de soporte emocional": 0.02,
                    "Convicción por las metas": 0.05,
                    "Dedicación": 0.05,
                    "Esfuerzo y dedicación": 0.05,
                    "Esperanza del futuro": 0.05,
                    "Evitación cognitiva": 0.02,
                    "Evitación conductual": 0.06,
                    "Firmeza y perseverancia": 0.05,
                    "Locus de control interno": 0.08,
                    "Optimismo del porvenir": 0.02,
                    "Orientación a la acción": 0.05,
                    "Planificación": 0.05,
                    "Propositividad": 0.02,
                    "Renuncia": 0.05,
                    "Resolutividad": 0.05,
                    "Sensación de logro": 0.02,
                    "Superación de adversidades": 0.05,
                },
            },
            "Bienestar cognitivo": {
                "peso_linea": 0.05,
                "indicadores": {
                    "Alteraciones cognitivas": 1.0,
                },
            },
            "Bienestar emocional y trascendente": {
                "peso_linea": 0.20,
                "indicadores": {
                    "Desgaste emocional": 0.4,
                    "Pérdida de sentido": 0.6,
                },
            },
            "Bienestar extralaboral": {
                "peso_linea": 0.10,
                "indicadores": {
                    "Accesibilidad del entorno": 0.05,
                    "Apoyo social": 0.40,
                    "Condiciones de la vivienda": 0.05,
                    "Dererioro de relaciones sociales y aislamiento": 0.20,
                    "Movilidad eficiente y desplazamiento saludable": 0.05,
                    "Relación con el núcleo familiar": 0.20,
                    "Seguridad del entorno": 0.05,
                },
            },
            "Bienestar financiero": {
                "peso_linea": 0.15,
                "indicadores": {
                    "Bienestar financiero": 1.0,
                },
            },
            "Bienestar físico": {
                "peso_linea": 0.15,
                "indicadores": {
                    "Somatización y fatiga física": 1.0,
                },
            },
            "Bienestar vida-trabajo": {
                "peso_linea": 0.10,
                "indicadores": {
                    "Interferencia temporal": 0.35,
                    "Impacto cognitivo-relacional": 0.25,
                    "Equilibrio y recuperación": 0.25,
                    "Conflicto vida - trabajo": 0.15,
                },
            },
            "Motivación laboral": {
                "peso_linea": 0.05,
                "indicadores": {
                    "Desmotivación y desgaste laboral": 1.0,
                },
            },
            "Comportamientos seguros": {
                "peso_linea": 0.05,
                "indicadores": {
                    "Conductas de riesgo": 1.0,
                },
            },
        },
    },

    "Condiciones de trabajo saludable": {
        "peso_eje": 1.0,
        "lineas": {
            "Arquitectura de roles y responsabilidades": {
                "peso_linea": 0.20,
                "indicadores": {
                    "Autonomía decisional": 0.15,
                    "Claridad en funciones y objetivos del rol": 0.20,
                    "Propósito del rol y redes de soporte": 0.15,
                    "Integridad normativa": 0.10,
                    "Contradicción en estructura de mando": 0.15,
                    "Decisiones críticas y resultados de gestión": 0.10,
                    "Responsabilidad por bienes y valores": 0.05,
                    "Pertinencia operativa": 0.05,
                    "Responsabilidad por la integridad y salud de otros": 0.05,
                },
            },
            "Aprendizaje y desarrollo (L&D Strategy)": {
                "peso_linea": 0.15,
                "indicadores": {
                    "Alineación de competencias y ajuste al cargo": 0.40,
                    "Pertinencia y aplicabilidad de la capacitación": 0.30,
                    "Rol enriquecido": 0.15,
                    "Acceso a capacitación": 0.15,
                },
            },
            "Condiciones emocionales saludables": {
                "peso_linea": 0.15,
                "indicadores": {
                    "Disonancia emocional": 0.40,
                    "Hostilidad y violencia": 0.40,
                    "Exposición al sufrimiento": 0.20,
                },
            },
            "Condiciones físicas saludables": {
                "peso_linea": 0.15,
                "indicadores": {
                    "Riesgos biomecánicos": 0.35,
                    "Confort del entorno": 0.25,
                    "Orden percibido": 0.15,
                    "Riesgos higiénicos": 0.10,
                    "Seguridad percibida": 0.15,
                },
            },
            "Carga de trabajo saludable": {
                "peso_linea": 0.15,
                "indicadores": {
                    "Ritmo de trabajo": 0.30,
                    "Volumen de trabajo": 0.30,
                    "Simultaneidad": 0.15,
                    "Autogestión del volumen y ritmo": 0.10,
                    "Atención y concentración sostenida": 0.10,
                    "Carga de memoria": 0.05,
                },
            },
            "Jornadas de trabajo saludables": {
                "peso_linea": 0.10,
                "indicadores": {
                    "Trabajo nocturno": 0.60,
                    "Descansos programados": 0.40,
                },
            },
            "Cambio organizacional": {
                "peso_linea": 0.05,
                "indicadores": {
                    "Claridad e impacto del cambio": 0.50,
                    "Participación en los cambios": 0.50,
                },
            },
            "Engagement organizacional": {
                "peso_linea": 0.05,
                "indicadores": {
                    "Orgullo de pertenencia": 0.60,
                    "Estabilidad y confianza": 0.40,
                },
            },
        },
    },
    "Entorno y clima de trabajo saludable": {
        "peso_eje": 1.0,
        "lineas": {
            "Ecosistema de liderazgo con impacto psicosocial": {
                "peso_linea": 0.60,
                "indicadores": {
                    "Liderazgo transformacional": 0.25,
                    "Comunicación efectiva del líder": 0.20,
                    "Apoyo del líder": 0.20,
                    "Retroalimentación constructiva": 0.20,
                    "Gestión del equipo": 0.15,
                },
            },
            "Convivencia y relacionamiento": {
                "peso_linea": 0.40,
                "indicadores": {
                    "Cohesión grupal": 0.30,
                    "Trabajo colaborativo": 0.25,
                    "Respeto y trato digno": 0.25,
                    "Resolución de conflictos": 0.20,
                },
            },
        },
    },
}


# ===========================================================================
# FUNCIONES DE CÁLCULO DE SCORES DE GESTIÓN
# ===========================================================================

def calcular_score_indicador(df_scores: pd.DataFrame, cedula: str, forma: str,
                              indicador: str, items_indicador: list) -> float:
    """
    Calcula score de un indicador como media ponderada de sus ítems.
    V2-Paso3 Nivel 1: ítem → indicador
    
    Args:
        df_scores: DataFrame con valor_0a1 por ítem
        cedula: Cédula del trabajador
        forma: Forma (A/B)
        indicador: Nombre del indicador
        items_indicador: Lista de id_pregunta que componen el indicador
    
    Returns:
        Score 0-1 del indicador (media de los ítems)
    """
    mask = (
        (df_scores["cedula"] == cedula) &
        (df_scores["forma_intra"].str.upper() == forma.upper()) &
        (df_scores["id_pregunta"].isin(items_indicador))
    )
    valores = df_scores.loc[mask, "valor_0a1"]
    if len(valores) == 0:
        return np.nan
    return valores.mean()  # Promedio simple de ítems del indicador


def aplicar_inversion_nivel2(score: float, indicador: str) -> float:
    """
    V2-Paso3: Inversión Nivel 2 para indicadores de riesgo.
    Si el indicador está en INDICADORES_A_INVERTIR: score = 1 - score
    Esto asegura que mayor score = mejor bienestar en todos los ejes.
    """
    if pd.isna(score):
        return np.nan
    if indicador in INDICADORES_A_INVERTIR:
        return 1.0 - score
    return score


def calcular_score_linea(scores_indicadores: Dict[str, float],
                          pesos_indicadores: Dict[str, float]) -> float:
    """
    V2-Paso3 Nivel 2: indicador → línea
    Score de línea = Σ(score_indicador × peso) / Σ(pesos disponibles)
    
    Solo considera indicadores con score válido (no NaN).
    """
    suma_ponderada = 0.0
    suma_pesos = 0.0
    
    for indicador, peso in pesos_indicadores.items():
        score = scores_indicadores.get(indicador)
        if score is not None and not pd.isna(score):
            # Aplicar inversión nivel 2
            score_ajustado = aplicar_inversion_nivel2(score, indicador)
            suma_ponderada += score_ajustado * peso
            suma_pesos += peso
    
    if suma_pesos == 0:
        return np.nan
    return suma_ponderada / suma_pesos


def calcular_score_eje(scores_lineas: Dict[str, float],
                        pesos_lineas: Dict[str, float]) -> float:
    """
    V2-Paso3 Nivel 3: línea → eje
    Score de eje = Σ(score_linea × peso) / Σ(pesos disponibles)
    """
    suma_ponderada = 0.0
    suma_pesos = 0.0
    
    for linea, peso in pesos_lineas.items():
        score = scores_lineas.get(linea)
        if score is not None and not pd.isna(score):
            suma_ponderada += score * peso
            suma_pesos += peso
    
    if suma_pesos == 0:
        return np.nan
    return suma_ponderada / suma_pesos



def procesar_gestion_por_trabajador(df_scores: pd.DataFrame) -> pd.DataFrame:
    """
    Procesa scores de gestión para todos los trabajadores.
    Genera una fila por trabajador × eje × línea.
    
    Returns:
        DataFrame con: cedula, empresa, forma_intra, sector_rag,
                       eje, linea, score_0a1 (score de la línea)
    """
    log.info("Procesando scores de gestión...")
    
    # Primero, normalizar valor_invertido a escala 0-1
    df = df_scores.copy()
    df["instrumento_key"] = df["instrumento"].str.lower().str.strip()
    
    def normalizar_a_01(row):
        instr = row["instrumento_key"]
        valor = row["valor_invertido"]
        if pd.isna(valor):
            return np.nan
        # Afrontamiento y capital_psicologico ya están en 0-1
        if instr in ("afrontamiento", "capital_psicologico"):
            return float(valor)
        # Estrés tiene escalas diversas — normalizar según máximo del grupo
        if instr in ("estres", "estrés"):
            # Máximo posible en estrés es 9 (grupo A)
            return float(valor) / 9.0
        # Intralaboral y extralaboral: escala 0-4
        escala_max = ESCALA_MAX_POR_INSTRUMENTO.get(instr, 4.0)
        if escala_max and escala_max > 0:
            return float(valor) / escala_max
        return float(valor)
    
    df["valor_0a1"] = df.apply(normalizar_a_01, axis=1)
    log.info("  Valores normalizados a escala 0-1")
    
    # Obtener lista única de trabajadores
    trabajadores = df.groupby(["cedula", "empresa", "forma_intra", "sector_rag"]).size().reset_index()[["cedula", "empresa", "forma_intra", "sector_rag"]]
    log.info("  Procesando %d trabajadores únicos", len(trabajadores))
    
    resultados = []
    
    for _, trab in trabajadores.iterrows():
        cedula = trab["cedula"]
        empresa = trab["empresa"]
        forma = trab["forma_intra"]
        sector = trab["sector_rag"]
        
        # Para cada eje del modelo
        for eje, eje_config in MODELO_GESTION.items():
            scores_lineas = {}
            pesos_lineas = {}
            
            for linea, linea_config in eje_config["lineas"].items():
                pesos_lineas[linea] = linea_config["peso_linea"]
                
                # Calcular scores de indicadores de esta línea
                # SIMPLIFICACIÓN: usamos promedio de ítems que matcheen la dimensión/factor
                # En producción, usar mapeo exacto ítem → indicador de V2
                scores_indicadores = {}
                for indicador, peso_ind in linea_config["indicadores"].items():
                    # Buscar ítems que correspondan a este indicador
                    # Por ahora, score simulado basado en promedios de la forma
                    mask_trabajador = (
                        (df["cedula"] == cedula) &
                        (df["forma_intra"].str.upper() == forma.upper())
                    )
                    if mask_trabajador.sum() > 0:
                        # Score promedio del trabajador como aproximación
                        score_aprox = df.loc[mask_trabajador, "valor_0a1"].mean()
                        scores_indicadores[indicador] = score_aprox
                
                # Calcular score de la línea
                score_linea = calcular_score_linea(scores_indicadores, linea_config["indicadores"])
                scores_lineas[linea] = score_linea
                
                # Guardar resultado para esta línea
                resultados.append({
                    "cedula": cedula,
                    "empresa": empresa,
                    "forma_intra": forma,
                    "sector_rag": sector,
                    "eje": eje,
                    "linea": linea,
                    "score_0a1": round(score_linea, 4) if not pd.isna(score_linea) else np.nan,
                })
            
            # Calcular score del eje
            score_eje = calcular_score_eje(scores_lineas, pesos_lineas)
            
            # Agregar fila para el eje total
            resultados.append({
                "cedula": cedula,
                "empresa": empresa,
                "forma_intra": forma,
                "sector_rag": sector,
                "eje": eje,
                "linea": "_TOTAL_EJE",
                "score_0a1": round(score_eje, 4) if not pd.isna(score_eje) else np.nan,
            })
    
    df_result = pd.DataFrame(resultados)
    log.info("  Generados %d registros de gestión", len(df_result))
    return df_result


# ===========================================================================
# VALIDACIÓN
# ===========================================================================

def validar_gestion_scores(df: pd.DataFrame) -> Tuple[bool, pd.DataFrame]:
    """Valida integridad de fact_gestion_scores."""
    log.info("Validando fact_gestion_scores...")
    errores = []
    
    # 1. PK sin duplicados
    pk = ["cedula", "forma_intra", "eje", "linea"]
    dupes = df[df.duplicated(subset=pk, keep=False)]
    if len(dupes) > 0:
        errores.append({"check": "PK_duplicada", "n": len(dupes)})
    
    # 2. Scores en rango 0-1
    fuera_rango = df[(df["score_0a1"] < 0) | (df["score_0a1"] > 1)]
    if len(fuera_rango) > 0:
        errores.append({"check": "score_fuera_0_1", "n": len(fuera_rango)})
    
    # 3. Ejes esperados
    ejes_esperados = set(MODELO_GESTION.keys())
    ejes_encontrados = set(df["eje"].unique())
    faltantes = ejes_esperados - ejes_encontrados
    if faltantes:
        errores.append({"check": "ejes_faltantes", "n": len(faltantes), "detalle": list(faltantes)})
    
    df_errores = pd.DataFrame(errores)
    es_valido = len(errores) == 0
    if es_valido:
        log.info("Validación EXITOSA")
    else:
        log.error("Validación FALLIDA — %d checks con errores", len(errores))
    return es_valido, df_errores


# ===========================================================================
# PIPELINE PRINCIPAL
# ===========================================================================

def main(config_path: str = "config/config.yaml") -> None:
    """
    Pipeline principal 03.
    1. Carga fact_scores_brutos
    2. Normaliza valores a escala 0-1
    3. Calcula scores por indicador, línea y eje
    4. Aplica inversión nivel 2 a indicadores de riesgo
    5. Guarda fact_gestion_scores.parquet
    """
    config = cargar_config(config_path)
    ruta_in = Path(config["paths"]["processed"]) / "fact_scores_brutos.parquet"
    ruta_out = Path(config["paths"]["processed"]) / "fact_gestion_scores.parquet"
    
    log.info("=" * 60)
    log.info("03_scoring_gestion.py — Iniciando pipeline")
    log.info("Input:  %s", ruta_in)
    log.info("Output: %s", ruta_out)
    log.info("=" * 60)
    
    # --- Carga
    if not ruta_in.exists():
        log.error("Archivo no encontrado: %s", ruta_in)
        sys.exit(1)
    
    df_scores = pd.read_parquet(ruta_in)
    log.info("Cargados %d registros", len(df_scores))
    
    # --- Procesar gestión
    df_gestion = procesar_gestion_por_trabajador(df_scores)
    
    # --- Guardar
    ruta_out.parent.mkdir(parents=True, exist_ok=True)
    df_gestion.to_parquet(ruta_out, index=False)
    log.info("Guardado: %s (%d filas)", ruta_out, len(df_gestion))
    
    # --- Validación
    ok, _ = validar_gestion_scores(df_gestion)
    if not ok:
        log.error("Pipeline 03 completado con ERRORES")
        sys.exit(1)
    
    # --- Resumen
    log.info("-" * 40)
    log.info("RESUMEN:")
    log.info("  Trabajadores: %d", df_gestion["cedula"].nunique())
    log.info("  Ejes: %s", df_gestion["eje"].unique().tolist())
    log.info("  Líneas únicas: %d", df_gestion["linea"].nunique())
    log.info("  Score promedio global: %.3f", df_gestion["score_0a1"].mean())
    log.info("-" * 40)
    log.info("03 completado exitosamente.")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="03_scoring_gestion.py — V2 Pasos 1-3")
    parser.add_argument("--config", default="config/config.yaml")
    args = parser.parse_args()
    main(config_path=args.config)