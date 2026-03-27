"""
02a_scoring_bateria.py — Implementa V1-Pasos 1 a 8
Pasos: V1-Paso1 Codificación | V1-Paso2 Inversión Nivel 1 | V1-Pasos 3-8 Agrupaciones
Input:  data/processed/fact_respuestas_clean.parquet
Output: data/processed/fact_scores_brutos.parquet
Reglas: R1 PK triple | R2 Baremos A/B separados | R5 5 niveles normativos
Fuente: Visualizador 1, Pasos 1-8 | Res. 2764/2022 MinTrabajo Colombia
"""

import logging
import sys
from pathlib import Path
from typing import Tuple
import numpy as np
import pandas as pd
import yaml

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("02a_scoring_bateria")


def cargar_config(config_path: str = "config/config.yaml") -> dict:
    """Carga config.yaml. Nunca hardcodear rutas (Regla R3)."""
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


# ===========================================================================
# V1-PASO 1: TABLAS DE CODIFICACIÓN TEXTO → NÚMERO
# Fuente: Visualizador 1, Paso 1 (filas 8-65)
# ===========================================================================

LIKERT_STD = {"siempre":4, "casi siempre":3, "algunas veces":2, "casi nunca":1, "nunca":0}
DICOTOMICA  = {"si":1, "sí":1, "no":0}

# Estrés: 3 subgrupos con escalas diferenciadas
ESTRES_GRUPO_A = {
    "items":  frozenset({1,2,3,9,13,14,15,23,24}),
    "escala": {"siempre":9, "casi siempre":6, "a veces":3, "nunca":0},
}
ESTRES_GRUPO_B = {
    "items":  frozenset({4,5,6,10,11,16,17,18,19,25,26,27,28}),
    "escala": {"siempre":6, "casi siempre":4, "a veces":2, "nunca":0},
}
ESTRES_GRUPO_C = {
    "items":  frozenset({7,8,12,20,21,22,29,30,31}),
    "escala": {"siempre":3, "casi siempre":2, "a veces":1, "nunca":0},
}

# Afrontamiento: ítems 1-4 dirección normal, ítems 5-8 ya invertidos desde Paso 1
AFRONTAMIENTO_NORMAL = {
    "nunca hago eso":0, "a veces hago eso":0.5,
    "frecuentemente hago eso":0.7, "siempre hago eso":1,
}
AFRONTAMIENTO_INVERTIDO = {
    "nunca hago eso":1, "a veces hago eso":0.7,
    "frecuentemente hago eso":0.5, "siempre hago eso":0,
}

CAP_PSICOLOGICO = {
    "totalmente en desacuerdo":0, "en desacuerdo":0.5,
    "de acuerdo":0.7, "totalmente de acuerdo":1,
}


def codificar_valor(instrumento: str, id_preg: int, respuesta: str) -> float:
    """
    V1-Paso1: Convierte respuesta en texto a valor numérico.
    Aplica escala correcta según instrumento e id_pregunta.
    Returns float o np.nan si no se puede codificar.
    """
    instr = instrumento.lower().strip()
    resp  = respuesta.lower().strip()

    if instr in ("estres", "estrés"):
        for grp in (ESTRES_GRUPO_A, ESTRES_GRUPO_B, ESTRES_GRUPO_C):
            if id_preg in grp["items"]:
                return float(grp["escala"].get(resp, np.nan))
        return np.nan

    if instr == "afrontamiento":
        tabla = AFRONTAMIENTO_INVERTIDO if id_preg in {5,6,7,8} else AFRONTAMIENTO_NORMAL
        return float(tabla.get(resp, np.nan))

    if instr == "capital_psicologico":
        return float(CAP_PSICOLOGICO.get(resp, np.nan))

    if instr in ("intralaboral_a", "intralaboral_b"):
        dicotomica = (
            (instr == "intralaboral_a" and id_preg in {106, 116}) or
            (instr == "intralaboral_b" and id_preg == 89)
        )
        return float(DICOTOMICA.get(resp, np.nan) if dicotomica else LIKERT_STD.get(resp, np.nan))

    if instr == "extralaboral":
        return float(LIKERT_STD.get(resp, np.nan))

    log.warning("Instrumento no reconocido: %r | pregunta %d", instrumento, id_preg)
    return np.nan


# ===========================================================================
# V1-PASO 2: INVERSIÓN DE ÍTEMS NIVEL 1
# Fuente: Visualizador 1, Paso 2 (filas 71-90)
# Regla: valor_invertido = 4 - valor_numerico (escala 0-4)
# NOTA: afrontamiento 5-8 ya está invertido en Paso 1
# ===========================================================================

# IntraA — 73 ítems (V1 Paso 2, lista exacta del visualizador)
ITEMS_INVERTIDOS_INTRA_A = frozenset([
    4,5,6,9,12,14,32,34,39,40,41,42,43,44,45,46,47,48,49,50,
    51,53,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,
    71,72,73,74,75,76,77,78,79,81,82,83,84,85,86,87,88,89,90,
    91,92,93,94,95,96,97,98,99,100,101,102,103,104,105,
])

# IntraB — 68 ítems (V1 Paso 2, lista exacta del visualizador)
ITEMS_INVERTIDOS_INTRA_B = frozenset([
    4,5,6,9,12,14,22,24,29,30,31,32,33,34,35,36,37,38,39,40,
    41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,
    60,61,62,63,64,65,67,68,69,70,71,72,73,74,75,76,77,78,79,
    80,81,82,83,84,85,86,87,88,98,
])

# Extralaboral — 23 ítems (V1 Paso 2)
ITEMS_INVERTIDOS_EXTRA = frozenset([
    1,4,5,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,25,27,29,
])


def invertir_nivel1(instrumento: str, id_preg: int, valor: float) -> float:
    """
    V1-Paso2: Inversión Nivel 1 — 4 - valor para ítems en listas.
    Solo IntraA, IntraB y Extralaboral (escala 0-4).
    Estrés, afrontamiento y capital psicológico no se invierten aquí.
    """
    if pd.isna(valor):
        return np.nan
    instr = instrumento.lower().strip()
    if instr == "intralaboral_a" and id_preg in ITEMS_INVERTIDOS_INTRA_A:
        return 4.0 - float(valor)
    if instr == "intralaboral_b" and id_preg in ITEMS_INVERTIDOS_INTRA_B:
        return 4.0 - float(valor)
    if instr == "extralaboral" and id_preg in ITEMS_INVERTIDOS_EXTRA:
        return 4.0 - float(valor)
    return float(valor)


# ===========================================================================
# V1-PASOS 3-8: TABLAS DE AGRUPACIÓN ÍTEM → DIMENSIÓN / DOMINIO / FACTOR
# ===========================================================================

# --- V1-Paso3: IntraA (fuente: Visualizador 1 Paso 3, filas 96-115)
AGRUPACION_INTRA_A = [
    {"factor":"Intralaboral_A","dominio":"Liderazgo y relaciones sociales en el trabajo",
     "dimension":"Características del liderazgo",
     "items":[63,64,65,66,67,68,69,70,71,72,73,74,75]},
    {"factor":"Intralaboral_A","dominio":"Liderazgo y relaciones sociales en el trabajo",
     "dimension":"Relaciones sociales en el trabajo",
     "items":[76,77,78,79,80,81,82,83,84,85,86,87,88,89]},
    {"factor":"Intralaboral_A","dominio":"Liderazgo y relaciones sociales en el trabajo",
     "dimension":"Retroalimentación del desempeño",
     "items":[90,91,92,93,94]},
    {"factor":"Intralaboral_A","dominio":"Liderazgo y relaciones sociales en el trabajo",
     "dimension":"Relación con los colaboradores",
     "items":[117,118,119,120,121,122,123,124,125]},
    {"factor":"Intralaboral_A","dominio":"Control sobre el trabajo",
     "dimension":"Claridad de rol","items":[53,54,55,56,57,58,59]},
    {"factor":"Intralaboral_A","dominio":"Control sobre el trabajo",
     "dimension":"Capacitación","items":[60,61,62]},
    {"factor":"Intralaboral_A","dominio":"Control sobre el trabajo",
     "dimension":"Participación y manejo del cambio","items":[48,49,50,51]},
    {"factor":"Intralaboral_A","dominio":"Control sobre el trabajo",
     "dimension":"Oportunidades para el uso y desarrollo de habilidades y conocimientos",
     "items":[39,40,41,42]},
    {"factor":"Intralaboral_A","dominio":"Control sobre el trabajo",
     "dimension":"Control y autonomía sobre el trabajo","items":[44,45,46]},
    {"factor":"Intralaboral_A","dominio":"Demandas del trabajo",
     "dimension":"Demandas ambientales y de esfuerzo físico",
     "items":[1,2,3,4,5,6,7,8,9,10,11,12]},
    {"factor":"Intralaboral_A","dominio":"Demandas del trabajo",
     "dimension":"Demandas emocionales","items":[107,108,109,110,111,112,113,114,115]},
    {"factor":"Intralaboral_A","dominio":"Demandas del trabajo",
     "dimension":"Demandas cuantitativas","items":[13,14,15,32,43,47]},
    {"factor":"Intralaboral_A","dominio":"Demandas del trabajo",
     "dimension":"Influencia del trabajo sobre el entorno extralaboral","items":[35,36,37,38]},
    {"factor":"Intralaboral_A","dominio":"Demandas del trabajo",
     "dimension":"Exigencias de responsabilidad del cargo","items":[19,22,23,24,25,26]},
    {"factor":"Intralaboral_A","dominio":"Demandas del trabajo",
     "dimension":"Demandas de carga mental","items":[16,17,18,20,21]},
    {"factor":"Intralaboral_A","dominio":"Demandas del trabajo",
     "dimension":"Consistencia del rol","items":[27,28,29,30,52]},
    {"factor":"Intralaboral_A","dominio":"Demandas del trabajo",
     "dimension":"Demandas de la jornada de trabajo","items":[31,33,34]},
    {"factor":"Intralaboral_A","dominio":"Recompensas",
     "dimension":"Recompensas derivadas de la pertenencia a la organización y del trabajo",
     "items":[95,102,103,104,105]},
    {"factor":"Intralaboral_A","dominio":"Recompensas",
     "dimension":"Reconocimiento y compensación","items":[96,97,98,99,100,101]},
]

# --- V1-Paso4: IntraB (fuente: Visualizador 1 Paso 4, filas 118-136)
AGRUPACION_INTRA_B = [
    {"factor":"Intralaboral_B","dominio":"Liderazgo y relaciones sociales en el trabajo",
     "dimension":"Características del liderazgo",
     "items":[49,50,51,52,53,54,55,56,57,58,59,60,61]},
    {"factor":"Intralaboral_B","dominio":"Liderazgo y relaciones sociales en el trabajo",
     "dimension":"Relaciones sociales en el trabajo",
     "items":[62,63,64,65,66,67,68,69,70,71,72,73]},
    {"factor":"Intralaboral_B","dominio":"Liderazgo y relaciones sociales en el trabajo",
     "dimension":"Retroalimentación del desempeño","items":[74,75,76,77,78]},
    # "Relación con los colaboradores" NO APLICA para forma B
    {"factor":"Intralaboral_B","dominio":"Control sobre el trabajo",
     "dimension":"Claridad de rol","items":[41,42,43,44,45]},
    {"factor":"Intralaboral_B","dominio":"Control sobre el trabajo",
     "dimension":"Capacitación","items":[46,47,48]},
    {"factor":"Intralaboral_B","dominio":"Control sobre el trabajo",
     "dimension":"Participación y manejo del cambio","items":[38,39,40]},
    {"factor":"Intralaboral_B","dominio":"Control sobre el trabajo",
     "dimension":"Oportunidades para el uso y desarrollo de habilidades y conocimientos",
     "items":[29,30,31,32]},
    {"factor":"Intralaboral_B","dominio":"Control sobre el trabajo",
     "dimension":"Control y autonomía sobre el trabajo","items":[34,35,36]},
    {"factor":"Intralaboral_B","dominio":"Demandas del trabajo",
     "dimension":"Demandas ambientales y de esfuerzo físico",
     "items":[1,2,3,4,5,6,7,8,9,10,11,12]},
    {"factor":"Intralaboral_B","dominio":"Demandas del trabajo",
     "dimension":"Demandas emocionales","items":[90,91,92,93,94,95,96,97,98]},
    {"factor":"Intralaboral_B","dominio":"Demandas del trabajo",
     "dimension":"Demandas cuantitativas","items":[13,14,15]},
    {"factor":"Intralaboral_B","dominio":"Demandas del trabajo",
     "dimension":"Influencia del trabajo sobre el entorno extralaboral","items":[25,26,27,28]},
    # "Exigencias de responsabilidad del cargo" NO EVALÚA en forma B
    # "Consistencia del rol" NO EVALÚA en forma B
    {"factor":"Intralaboral_B","dominio":"Demandas del trabajo",
     "dimension":"Demandas de carga mental","items":[16,17,18,19,20]},
    {"factor":"Intralaboral_B","dominio":"Demandas del trabajo",
     "dimension":"Demandas de la jornada de trabajo","items":[21,22,23,24,33,37]},
    {"factor":"Intralaboral_B","dominio":"Recompensas",
     "dimension":"Recompensas derivadas de la pertenencia a la organización y del trabajo",
     "items":[85,86,87,88]},
    {"factor":"Intralaboral_B","dominio":"Recompensas",
     "dimension":"Reconocimiento y compensación","items":[79,80,81,82,83,84]},
]

# --- V1-Paso5: Extralaboral (fuente: Visualizador 1 Paso 5)
AGRUPACION_EXTRA = [
    {"factor":"Extralaboral","dominio":"Extralaboral",
     "dimension":"Balance entre la vida laboral y familiar","items":[14,15,16,17]},
    {"factor":"Extralaboral","dominio":"Extralaboral",
     "dimension":"Relaciones familiares","items":[22,25,27]},
    {"factor":"Extralaboral","dominio":"Extralaboral",
     "dimension":"Comunicación y relaciones interpersonales","items":[18,19,20,21,23]},
    {"factor":"Extralaboral","dominio":"Extralaboral",
     "dimension":"Situación económica del grupo familiar","items":[29,30,31]},
    {"factor":"Extralaboral","dominio":"Extralaboral",
     "dimension":"Características de la vivienda y de su entorno","items":[5,6,7,8,9,10,11,12,13]},
    {"factor":"Extralaboral","dominio":"Extralaboral",
     "dimension":"Influencia del entorno extralaboral sobre el trabajo","items":[24,26,28]},
    {"factor":"Extralaboral","dominio":"Extralaboral",
     "dimension":"Desplazamiento vivienda trabajo vivienda","items":[1,2,3,4]},
]

# --- V1-Paso6: Estrés (fuente: Visualizador 1 Paso 6)
AGRUPACION_ESTRES = [
    {"factor":"Estres","dominio":"Estres","dimension":"Estres",
     "items":list(range(1,32))},  # ítems 1 al 31
]

# --- V1-Paso7: Afrontamiento (fuente: Visualizador 1 Paso 7)
AGRUPACION_AFRONTAMIENTO = [
    {"factor":"Individual","dominio":"Afrontamiento",
     "dimension":"afrontamiento_activo_planificacion","items":[1,2,3,4]},
    {"factor":"Individual","dominio":"Afrontamiento",
     "dimension":"afrontamiento_pasivo_negacion","items":[5,6,7,8]},
    {"factor":"Individual","dominio":"Afrontamiento",
     "dimension":"afrontamiento_activo_busquedasoporte","items":[9,10,11,12]},
]

# --- V1-Paso8: Capital Psicológico (fuente: Visualizador 1 Paso 8)
AGRUPACION_CAP_PSICOLOGICO = [
    {"factor":"Individual","dominio":"Capital_Psicologico",
     "dimension":"Optimismo","items":[1,2,3]},
    {"factor":"Individual","dominio":"Capital_Psicologico",
     "dimension":"Esperanza","items":[4,5,6]},
    {"factor":"Individual","dominio":"Capital_Psicologico",
     "dimension":"Resiliencia","items":[7,8,9]},
    {"factor":"Individual","dominio":"Capital_Psicologico",
     "dimension":"Autoeficacia","items":[9,10,11,12]},
]


# ===========================================================================
# FUNCIONES DE PROCESAMIENTO PRINCIPAL
# ===========================================================================

# Mapa instrumento → tabla de agrupación
AGRUPACIONES_POR_INSTRUMENTO = {
    "intralaboral_a":     AGRUPACION_INTRA_A,
    "intralaboral_b":     AGRUPACION_INTRA_B,
    "extralaboral":       AGRUPACION_EXTRA,
    "estres":             AGRUPACION_ESTRES,
    "estrés":             AGRUPACION_ESTRES,
    "afrontamiento":      AGRUPACION_AFRONTAMIENTO,
    "capital_psicologico":AGRUPACION_CAP_PSICOLOGICO,
}


def construir_lookup_agrupacion() -> pd.DataFrame:
    """
    Construye tabla lookup: (instrumento, id_pregunta) → (factor, dominio, dimension).
    Usada para hacer JOIN con fact_scores_brutos.
    """
    registros = []
    for instrumento, tabla in AGRUPACIONES_POR_INSTRUMENTO.items():
        for grupo in tabla:
            for item in grupo["items"]:
                registros.append({
                    "instrumento": instrumento,
                    "id_pregunta": item,
                    "factor":     grupo["factor"],
                    "dominio":    grupo["dominio"],
                    "dimension":  grupo["dimension"],
                })
    return pd.DataFrame(registros)


def aplicar_codificacion(df: pd.DataFrame) -> pd.DataFrame:
    """
    V1-Paso1: Aplica codificación texto → número a toda la tabla.
    
    Requiere columnas: instrumento, id_pregunta, id_respuesta_texto
    Agrega columna: valor_numerico
    """
    log.info("V1-Paso1: Codificando %d respuestas...", len(df))
    df = df.copy()
    df["id_respuesta_texto"] = df["id_respuesta_texto"].astype(str).str.lower().str.strip()

    df["valor_numerico"] = df.apply(
        lambda r: codificar_valor(
            str(r["instrumento"]),
            int(r["id_pregunta"]),
            str(r["id_respuesta_texto"])
        ),
        axis=1
    )

    nulos = df["valor_numerico"].isna().sum()
    if nulos > 0:
        log.warning("V1-Paso1: %d respuestas no pudieron codificarse (NaN)", nulos)
    log.info("V1-Paso1: Codificación completada. NaN: %d / %d", nulos, len(df))
    return df


def aplicar_inversion_nivel1(df: pd.DataFrame) -> pd.DataFrame:
    """
    V1-Paso2: Aplica inversión Nivel 1 a todos los ítems correspondientes.
    Regla: SIEMPRE antes de las agrupaciones.
    
    Agrega columna: valor_invertido
    """
    log.info("V1-Paso2: Aplicando inversión Nivel 1...")
    df = df.copy()
    df["valor_invertido"] = df.apply(
        lambda r: invertir_nivel1(
            str(r["instrumento"]),
            int(r["id_pregunta"]),
            r["valor_numerico"]
        ),
        axis=1
    )
    # Auditoría: contar ítems invertidos por instrumento
    invertidos = df[df["valor_invertido"] != df["valor_numerico"]]
    log.info("V1-Paso2: %d ítems invertidos (de %d total)", len(invertidos), len(df))
    return df


def aplicar_agrupaciones(df: pd.DataFrame) -> pd.DataFrame:
    """
    V1-Pasos 3-8: Agrega columnas factor, dominio, dimension mediante JOIN
    con la tabla lookup de agrupaciones.
    
    Ítems dicotómicos (106, 116 IntraA; 89 IntraB) se incluyen SIN agrupación
    (son preguntas especiales sin dimensión asignada en Res. 2764).
    """
    log.info("V1-Pasos 3-8: Aplicando agrupaciones ítem → dimensión...")
    df = df.copy()
    lookup = construir_lookup_agrupacion()
    lookup["instrumento"] = lookup["instrumento"].str.lower().str.strip()

    df["instrumento_key"] = df["instrumento"].str.lower().str.strip()
    df_merged = df.merge(
        lookup,
        left_on=["instrumento_key", "id_pregunta"],
        right_on=["instrumento", "id_pregunta"],
        how="left",
        suffixes=("", "_lookup")
    )

    # Ítems sin agrupación (dicotómicos, ítems especiales)
    sin_grupo = df_merged["factor"].isna()
    if sin_grupo.sum() > 0:
        log.warning(
            "V1-Pasos3-8: %d ítems sin grupo asignado — revisar ítems: %s",
            sin_grupo.sum(),
            df_merged[sin_grupo]["id_pregunta"].unique().tolist()[:20]
        )

    df_merged.drop(columns=["instrumento_key", "instrumento_lookup"], errors="ignore", inplace=True)
    log.info("V1-Pasos 3-8: Agrupación completada. Filas resultado: %d", len(df_merged))
    return df_merged


# ===========================================================================
# VALIDACIÓN DE INTEGRIDAD
# ===========================================================================

def validar_scores_brutos(df: pd.DataFrame) -> Tuple[bool, pd.DataFrame]:
    """
    Valida integridad de fact_scores_brutos.
    Returns: (es_valido, df_errores)
    """
    log.info("Validando fact_scores_brutos...")
    errores = []

    # 1. PK sin duplicados (R1)
    pk = ["cedula", "forma_intra", "id_pregunta"]
    dupes = df[df.duplicated(subset=pk, keep=False)]
    if len(dupes) > 0:
        errores.append({
            "check": "PK_duplicada",
            "n": len(dupes),
            "detalle": f"Primeras PKs duplicadas: {dupes[pk].head(3).to_dict(orient='records')}",
        })

    # 2. Valores numéricos en rango esperado
    intra_mask = df["instrumento"].str.lower().str.startswith("intralaboral")
    extra_mask = df["instrumento"].str.lower() == "extralaboral"
    rango_mask = intra_mask | extra_mask
    fuera_rango = df[rango_mask & ((df["valor_invertido"] < 0) | (df["valor_invertido"] > 4))]
    if len(fuera_rango) > 0:
        errores.append({
            "check": "valor_fuera_rango_0_4",
            "n": len(fuera_rango),
            "detalle": f"Instrumentos afectados: {fuera_rango['instrumento'].unique().tolist()}",
        })

    # 3. Formas válidas (R2)
    formas = df["forma_intra"].str.upper().unique().tolist()
    formas_invalidas = [f for f in formas if f not in ("A", "B")]
    if formas_invalidas:
        errores.append({
            "check": "forma_invalida",
            "n": len(formas_invalidas),
            "detalle": f"Formas no reconocidas: {formas_invalidas}",
        })

    # 4. Verificar que empresa ASIGNAR esté presente (R8)
    if "empresa" in df.columns and "ASIGNAR" not in df["empresa"].values:
        log.warning("ASIGNAR no encontrada — puede ser normal si no está en este corte")

    # 5. NaN en valor_invertido
    nans = df["valor_invertido"].isna().sum()
    if nans > 0:
        errores.append({
            "check": "valor_invertido_nan",
            "n": nans,
            "detalle": "Respuestas que no pudieron codificarse",
        })

    df_errores = pd.DataFrame(errores)
    es_valido = len(errores) == 0
    if es_valido:
        log.info("Validación EXITOSA — fact_scores_brutos integro")
    else:
        log.error("Validación FALLIDA — %d checks con errores", len(errores))
        log.error(df_errores.to_string())
    return es_valido, df_errores


# ===========================================================================
# PIPELINE PRINCIPAL
# ===========================================================================

def main(config_path: str = "config/config.yaml") -> None:
    """
    Pipeline principal 02a.
    1. Carga fact_respuestas_clean
    2. V1-Paso1: Codificación texto → número
    3. V1-Paso2: Inversión Nivel 1
    4. V1-Pasos 3-8: Agrupaciones ítem → dimensión
    5. Guarda fact_scores_brutos.parquet
    6. Valida y reporta
    """
    config    = cargar_config(config_path)
    ruta_in   = Path(config["paths"]["processed"]) / "fact_respuestas_clean.parquet"
    ruta_out  = Path(config["paths"]["processed"]) / "fact_scores_brutos.parquet"

    log.info("=" * 60)
    log.info("02a_scoring_bateria.py — Iniciando pipeline")
    log.info("Input:  %s", ruta_in)
    log.info("Output: %s", ruta_out)
    log.info("=" * 60)

    # --- Carga
    if not ruta_in.exists():
        log.error("Archivo no encontrado: %s", ruta_in)
        log.error("Ejecutar primero: 01_etl_star_schema.py")
        sys.exit(1)

    df = pd.read_parquet(ruta_in)
    log.info("Cargados %d registros × %d columnas", len(df), df.shape[1])
    log.info("Formas disponibles: %s", df["forma_intra"].str.upper().unique().tolist())
    log.info("Empresas: %s", df["empresa"].unique().tolist())

    # Verificar columna instrumento (generada en ETL)
    if "instrumento" not in df.columns:
        log.error("Columna 'instrumento' no encontrada. Verificar 01_etl_star_schema.py")
        sys.exit(1)

    # --- V1-Paso1: Codificación
    df = aplicar_codificacion(df)

    # --- V1-Paso2: Inversión Nivel 1
    df = aplicar_inversion_nivel1(df)

    # --- V1-Pasos 3-8: Agrupaciones
    df = aplicar_agrupaciones(df)

    # --- Seleccionar columnas finales
    cols_finales = [
        "cedula", "empresa", "forma_intra", "sector_rag",
        "instrumento", "id_pregunta", "id_respuesta_texto",
        "valor_numerico", "valor_invertido",
        "factor", "dominio", "dimension",
    ]
    cols_disponibles = [c for c in cols_finales if c in df.columns]
    df_out = df[cols_disponibles].copy()

    # --- Guardar
    ruta_out.parent.mkdir(parents=True, exist_ok=True)
    df_out.to_parquet(ruta_out, index=False)
    log.info("Guardado: %s (%d filas × %d cols)", ruta_out, len(df_out), df_out.shape[1])

    # --- Validación final
    ok, df_err = validar_scores_brutos(df_out)
    if not ok:
        log.error("Pipeline 02a completado con ERRORES — revisar validación")
        sys.exit(1)

    # --- Resumen estadístico
    log.info("-" * 40)
    log.info("RESUMEN FACT_SCORES_BRUTOS:")
    log.info("  Total registros  : %d", len(df_out))
    log.info("  Trabajadores únicos: %d", df_out["cedula"].nunique())
    log.info("  Instrumentos     : %s", df_out["instrumento"].str.lower().unique().tolist())
    log.info("  Dimensiones únicas: %d", df_out["dimension"].nunique())
    log.info("  Dominios únicos  : %d", df_out["dominio"].nunique())
    log.info("  NaN en valor_invertido: %d", df_out["valor_invertido"].isna().sum())
    log.info("-" * 40)
    log.info("02a completado exitosamente.")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="02a_scoring_bateria.py — V1 Pasos 1-8")
    parser.add_argument("--config", default="config/config.yaml",
                        help="Ruta al archivo config.yaml")
    args = parser.parse_args()
    main(config_path=args.config)