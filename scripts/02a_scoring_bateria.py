"""
02a_scoring_bateria.py
======================
Pasos 1-8 del pipeline V1 — Codificación y agrupaciones.

PASO 1  Codificar id_respuesta (texto) → valor_numerico (float)
PASO 2  Invertir ítems (Nivel 1, R3) — pendiente de lista de ítems
PASO 3  Etiquetar instrumento, dimensión, dominio, factor desde dim_pregunta
        (aplica para intraA, intraB, extra, estrés, afrontamiento, capitalpsic)
Pasos 4-8 se resuelven en el mismo paso 3 vía JOIN con dim_pregunta.

Output: data/processed/fact_scores_brutos.parquet

Reglas aplicadas:
  R1  — PK: cedula + forma_intra + id_pregunta
  R3  — Inversión nivel 1 (solo para ítems con es_item_invertido=True)
  R13 — Output en Parquet
  R14 — Función validar_scores_brutos()
  R15 — fact_respuestas_clean es inmutable
"""

import logging
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import yaml

# ── Rutas ──────────────────────────────────────────────────────────────────
ROOT = Path(__file__).resolve().parents[1]
CONFIG_PATH = ROOT / "config" / "config.yaml"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════════════════════
# PASO 1 — Tablas de codificación texto → número
# Fuente: Documento marco, sección "Pasos para la creación de los dashboards"
# ══════════════════════════════════════════════════════════════════════════════

# Escala Likert estándar 0-4 (intralaboral A/B + extralaboral)
LIKERT_0_4: dict[str, float] = {
    "siempre":       4.0,
    "casi siempre":  3.0,
    "algunas veces": 2.0,
    "casi nunca":    1.0,
    "nunca":         0.0,
}

# Escala dicotómica (ítems 106 y 116 de intraA; ítem 89 de intraB)
DICO: dict[str, float] = {
    "si":  1.0,
    "no":  0.0,
    "sí":  1.0,   # variante con tilde
}

# Estrés — Grupo 1: ítems 1,2,3,9,13,14,15,23,24  → pesos 9/6/3/0
ESTRES_G1_ITEMS = {1, 2, 3, 9, 13, 14, 15, 23, 24}
ESTRES_G1: dict[str, float] = {
    "siempre":      9.0,
    "casi siempre": 6.0,
    "a veces":      3.0,
    "nunca":        0.0,
}

# Estrés — Grupo 2: ítems 4,5,6,10,11,16,17,18,19,25,26,27,28  → pesos 6/4/2/0
ESTRES_G2_ITEMS = {4, 5, 6, 10, 11, 16, 17, 18, 19, 25, 26, 27, 28}
ESTRES_G2: dict[str, float] = {
    "siempre":      6.0,
    "casi siempre": 4.0,
    "a veces":      2.0,
    "nunca":        0.0,
}

# Estrés — Grupo 3: ítems 7,8,12,20,21,22,29,30,31  → pesos 3/2/1/0
ESTRES_G3_ITEMS = {7, 8, 12, 20, 21, 22, 29, 30, 31}
ESTRES_G3: dict[str, float] = {
    "siempre":      3.0,
    "casi siempre": 2.0,
    "a veces":      1.0,
    "nunca":        0.0,
}

# Afrontamiento: ítems 1-12  → escala 0 / 0.5 / 0.7 / 1.0
AFRONTAMIENTO: dict[str, float] = {
    "nunca hago eso":          0.0,
    "a veces hago eso":        0.5,
    "frecuentemente hago eso": 0.7,
    "siempre hago eso":        1.0,
}

# Capital psicológico: ítems 1-12  → escala 0 / 0.5 / 0.7 / 1.0
CAPITAL_PSIC: dict[str, float] = {
    "totalmente en desacuerdo": 0.0,
    "en desacuerdo":            0.5,
    "de acuerdo":               0.7,
    "totalmente de acuerdo":    1.0,
}

# Ítems dicotómicos dentro de intralaboral (números de ítem, no id_pregunta completo)
DICO_INTRA_A = {106, 116}
DICO_INTRA_B = {89}


# ══════════════════════════════════════════════════════════════════════════════
# Funciones auxiliares
# ══════════════════════════════════════════════════════════════════════════════

def parsear_id_pregunta(id_pregunta: str) -> tuple[int, str]:
    """
    Extrae (num_item, sufijo_instrumento) de id_pregunta.

    Ejemplos:
      "1_intra"        → (1, "intra")
      "106_intra"      → (106, "intra")
      "1_extra"        → (1, "extra")
      "1_estres"       → (1, "estres")
      "1_afrontamiento"→ (1, "afrontamiento")
      "1_capitalpsic"  → (1, "capitalpsic")
    """
    partes = str(id_pregunta).split("_", 1)
    if len(partes) != 2:
        raise ValueError(f"id_pregunta con formato inesperado: '{id_pregunta}'")
    num = int(partes[0])
    sufijo = partes[1].lower().strip()
    return num, sufijo


def codificar_respuesta(id_pregunta: str, forma_intra: str, id_respuesta: str) -> float:
    """
    PASO 1 — Codifica el texto de respuesta a valor numérico.

    Parámetros
    ----------
    id_pregunta  : str   — e.g. "1_intra", "1_extra", "1_estres"
    forma_intra  : str   — 'A' o 'B'
    id_respuesta : str   — texto libre: "Siempre", "si", "A veces", etc.

    Retorna
    -------
    float  — valor codificado, o np.nan si no se puede mapear
    """
    try:
        num_item, sufijo = parsear_id_pregunta(id_pregunta)
    except ValueError:
        return np.nan

    resp = str(id_respuesta).strip().lower()

    # ── Intralaboral A o B ──────────────────────────────────────────────────
    if sufijo == "intra":
        es_dico = (
            (forma_intra == "A" and num_item in DICO_INTRA_A) or
            (forma_intra == "B" and num_item in DICO_INTRA_B)
        )
        if es_dico:
            val = DICO.get(resp)
        else:
            val = LIKERT_0_4.get(resp)

    # ── Extralaboral (aplica A y B) ─────────────────────────────────────────
    elif sufijo == "extra":
        val = LIKERT_0_4.get(resp)

    # ── Estrés — 3 grupos de pesos (aplica A y B) ──────────────────────────
    elif sufijo == "estres":
        if num_item in ESTRES_G1_ITEMS:
            val = ESTRES_G1.get(resp)
        elif num_item in ESTRES_G2_ITEMS:
            val = ESTRES_G2.get(resp)
        elif num_item in ESTRES_G3_ITEMS:
            val = ESTRES_G3.get(resp)
        else:
            log.warning("Ítem estrés fuera de rango: num=%d", num_item)
            val = np.nan

    # ── Afrontamiento (aplica A y B) ────────────────────────────────────────
    elif sufijo == "afrontamiento":
        val = AFRONTAMIENTO.get(resp)

    # ── Capital psicológico (aplica A y B) ──────────────────────────────────
    elif sufijo == "capitalpsicologico":
        val = CAPITAL_PSIC.get(resp)

    else:
        log.warning("Sufijo de instrumento desconocido: '%s' en id_pregunta='%s'", sufijo, id_pregunta)
        val = np.nan

    if val is None:
        log.debug(
            "Respuesta no mapeada | id_pregunta=%s forma=%s respuesta='%s'",
            id_pregunta, forma_intra, id_respuesta,
        )
        return np.nan

    return float(val)


# ══════════════════════════════════════════════════════════════════════════════
# PASO 2 — Inversión de ítems Nivel 1 (R3)
# Fuente: Documento marco, Paso 2 + pipeline.md V1-Paso2
#
# Regla: Siempre=4→0 | Casi siempre=3→1 | Algunas veces=2→2 | Casi nunca=1→3 | Nunca=0→4
# Fórmula Likert: valor_invertido = 4 - valor_numerico
#
# Afrontamiento ítems 5-8: inversión propia con escala distinta (no 4-x)
# Estrés y CapitalPsicológico: NO tienen ítems invertidos en Nivel 1
# ══════════════════════════════════════════════════════════════════════════════

# IntraA — 73 ítems invertidos
# Fuente: Documento marco Paso 2
INVERTIDOS_INTRA_A: set[int] = {
    4, 5, 6, 9, 12, 14, 32, 34,
    39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51,
    53, 54, 55, 56, 57, 58, 59, 60, 61, 62,
    63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75,
    76, 77, 78, 79,
    81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94,
    95, 96, 97, 98, 99, 100, 101, 102, 103, 104, 105,
}  # Total: 73 ítems ✓

# IntraB — 68 ítems invertidos
INVERTIDOS_INTRA_B: set[int] = {
    4, 5, 6, 9, 12, 14, 22, 24,
    29, 30, 31, 32, 33, 34, 35, 36, 37, 38,
    39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52,
    53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65,
    67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80,
    81, 82, 83, 84, 85, 86, 87, 88,
    98,
}  # Total: 68 ítems ✓

# Extralaboral — 23 ítems invertidos (aplica a forma A y B)
INVERTIDOS_EXTRA: set[int] = {
    1, 4, 5, 7, 8, 9, 10, 11, 12, 13, 14, 15,
    16, 17, 18, 19, 20, 21, 22, 23, 25, 27, 29,
}  # Total: 23 ítems ✓

# Afrontamiento ítems 5,6,7,8 — inversión con escala propia (no 4-x)
# Escala invertida: nunca=1, a_veces=0.7, frecuentemente=0.5, siempre=0
INVERTIDOS_AFRONTAMIENTO: set[int] = {5, 6, 7, 8}

AFRONTAMIENTO_INVERTIDO: dict[str, float] = {
    "nunca hago eso":          1.0,
    "a veces hago eso":        0.7,
    "frecuentemente hago eso": 0.5,
    "siempre hago eso":        0.0,
}


def es_item_invertido(num_item: int, sufijo: str, forma_intra: str) -> bool:
    """Determina si un ítem debe invertirse en Nivel 1 (R3)."""
    if sufijo == "intra":
        if forma_intra == "A":
            return num_item in INVERTIDOS_INTRA_A
        elif forma_intra == "B":
            return num_item in INVERTIDOS_INTRA_B
    elif sufijo == "extra":
        return num_item in INVERTIDOS_EXTRA
    elif sufijo == "afrontamiento":
        return num_item in INVERTIDOS_AFRONTAMIENTO
    # estrés y capitalpsic: sin inversión Nivel 1
    return False


def invertir_valor(
    valor_numerico: float,
    num_item: int,
    sufijo: str,
    id_respuesta_original: str,
) -> float:
    """
    PASO 2 — Aplica inversión Nivel 1 según instrumento e ítem.

    Likert 0-4 (intra, extra):  valor_invertido = 4 - valor_numerico
    Afrontamiento 5-8:          escala invertida propia (0→1, 0.5→0.7, 0.7→0.5, 1→0)
    """
    if sufijo == "afrontamiento" and num_item in INVERTIDOS_AFRONTAMIENTO:
        resp = str(id_respuesta_original).strip().lower()
        return AFRONTAMIENTO_INVERTIDO.get(resp, valor_numerico)
    else:
        # Likert 0-4 e Extralaboral
        return 4.0 - valor_numerico


# ══════════════════════════════════════════════════════════════════════════════
# Ejecución principal
# ══════════════════════════════════════════════════════════════════════════════

def cargar_config() -> dict:
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def aplicar_paso1_vectorizado(df: pd.DataFrame) -> pd.Series:
    """
    Aplica codificar_respuesta() a todo el DataFrame de forma vectorizada.
    Retorna Serie con el valor_numerico por fila.
    """
    return df.apply(
        lambda row: codificar_respuesta(
            row["id_pregunta"], row["forma_intra"], row["id_respuesta"]
        ),
        axis=1,
    )


def aplicar_paso2(df: pd.DataFrame) -> pd.DataFrame:
    """
    PASO 2 — Inversión de ítems Nivel 1 (R3).

    Usa los conjuntos hardcodeados INVERTIDOS_INTRA_A/B, INVERTIDOS_EXTRA,
    INVERTIDOS_AFRONTAMIENTO — no depende de dim_pregunta.es_item_invertido.

    Reglas:
      IntraA (73 ítems) | IntraB (68 ítems) | Extra (23 ítems): valor_invertido = 4 - valor
      Afrontamiento ítems 5-8: escala propia invertida
      Estrés + CapitalPsic: sin inversión (valor_invertido = valor_numerico)
    """
    df = df.copy()
    df["valor_invertido"] = df["valor_numerico"].copy()

    # Calcular máscara de ítems a invertir usando es_item_invertido()
    def _necesita_inversion(row) -> bool:
        try:
            num, sufijo = parsear_id_pregunta(row["id_pregunta"])
        except ValueError:
            return False
        return es_item_invertido(num, sufijo, row["forma_intra"])

    mask_invertir = df.apply(_necesita_inversion, axis=1)

    # Aplicar invertir_valor() fila a fila donde corresponda
    df.loc[mask_invertir, "valor_invertido"] = df.loc[mask_invertir].apply(
        lambda row: invertir_valor(
            row["valor_numerico"],
            parsear_id_pregunta(row["id_pregunta"])[0],  # num_item
            parsear_id_pregunta(row["id_pregunta"])[1],  # sufijo
            row["id_respuesta"],
        ),
        axis=1,
    )

    invertidos = int(mask_invertir.sum())
    log.info("Paso 2 — Ítems invertidos: %d de %d total", invertidos, len(df))
    return df


def aplicar_pasos3_a_8(df: pd.DataFrame, dim_pregunta: pd.DataFrame) -> pd.DataFrame:
    """
    PASOS 3-8 — Agrupaciones: asignar instrumento, dimensión, dominio, factor
    desde dim_pregunta mediante JOIN en id_pregunta.

    Pasos cubiertos:
      3 — Agrupación intralaboral_A: item → dimensión → dominio → factor
      4 — Agrupación intralaboral_B: item → dimensión → dominio → factor
      5 — Agrupación extralaboral: item → dimensión
      6 — Agrupación estrés: item → factor/dimensión
      7 — Agrupación afrontamiento: item → factor/dimensión
      8 — Agrupación capitalpsicologico: item → factor/dimensión
    """
    cols_dim = ["id_pregunta", "forma_intra", "instrumento", "dimension", "dominio", "factor"]
    # Solo añadir columnas que no estén ya en df
    cols_nuevas = [c for c in cols_dim if c not in df.columns]

    if not cols_nuevas:
        log.info("Pasos 3-8 — columnas de agrupación ya presentes, se omite JOIN")
        return df

    df = df.merge(
        dim_pregunta[cols_dim].drop_duplicates(["id_pregunta", "forma_intra"]),
        on=["id_pregunta", "forma_intra"],
        how="left",
    )

    sin_dimension = df["dimension"].isna().sum()
    if sin_dimension > 0:
        log.warning("Pasos 3-8 — %d ítems sin dimensión asignada (revisar dim_pregunta)", sin_dimension)

    log.info("Pasos 3-8 — Instrumentos presentes: %s", df["instrumento"].unique().tolist())
    return df


def validar_scores_brutos(df: pd.DataFrame) -> tuple[bool, pd.DataFrame]:
    """
    R14 — Validación obligatoria de fact_scores_brutos.
    Retorna (es_valido, reporte_errores).
    """
    errores = []

    # PK sin duplicados (R1)
    dup = df.duplicated(subset=["cedula", "forma_intra", "id_pregunta"]).sum()
    if dup > 0:
        errores.append({"check": "pk_duplicada", "n": int(dup)})

    # Sin nulos en columnas clave
    for col in ["cedula", "forma_intra", "id_pregunta", "valor_numerico"]:
        n = df[col].isna().sum()
        if n > 0:
            errores.append({"check": f"nulos_{col}", "n": int(n)})

    # valor_invertido no debe tener nulos
    n_inv_null = df["valor_invertido"].isna().sum()
    if n_inv_null > 0:
        errores.append({"check": "nulos_valor_invertido", "n": int(n_inv_null)})

    # valor_numerico: verificar que no hay respuestas no mapeadas (NaN) excesivas
    pct_nan = df["valor_numerico"].isna().mean() * 100
    if pct_nan > 5.0:
        errores.append({"check": "pct_nan_alto_valor_numerico", "n": round(pct_nan, 2)})

    # Rango: intra y extra deben estar en [0, 4], estrés en [0, 9]
    intra_extra = df[df["id_pregunta"].str.endswith(("_intra", "_extra"))]
    fuera_rango = intra_extra[
        (intra_extra["valor_invertido"] < 0) | (intra_extra["valor_invertido"] > 4)
    ].shape[0]
    if fuera_rango > 0:
        errores.append({"check": "fuera_rango_likert_0_4", "n": fuera_rango})

    reporte = pd.DataFrame(errores) if errores else pd.DataFrame(columns=["check", "n"])
    return len(errores) == 0, reporte


# ══════════════════════════════════════════════════════════════════════════════
# Construcción interna de dim_pregunta (no existe como hoja Excel)
# ══════════════════════════════════════════════════════════════════════════════

def _max_valor_item(id_pregunta: str, forma_intra: str) -> float:
    """Retorna el máximo puntaje posible por ítem según instrumento y grupo."""
    try:
        num, sufijo = parsear_id_pregunta(id_pregunta)
    except ValueError:
        return 4.0
    if sufijo == "intra":
        es_dico = (
            (forma_intra == "A" and num in DICO_INTRA_A) or
            (forma_intra == "B" and num in DICO_INTRA_B)
        )
        return 1.0 if es_dico else 4.0
    elif sufijo == "extra":
        return 4.0
    elif sufijo == "estres":
        if num in ESTRES_G1_ITEMS:
            return 9.0
        elif num in ESTRES_G2_ITEMS:
            return 6.0
        elif num in ESTRES_G3_ITEMS:
            return 3.0
    elif sufijo in ("afrontamiento", "capitalpsicologico"):
        return 1.0
    return 4.0


def _construir_dim_pregunta(fact: pd.DataFrame, cat: pd.DataFrame) -> pd.DataFrame:
    """
    Construye dim_pregunta desde categorias_analisis.

    NOTA IMPORTANTE: En la hoja categorias_analisis del Excel, los sufijos
    de id_pregunta para 'afrontamiento' y 'capitalpsicologico' están INVERTIDOS
    respecto a fact_respuestas. Esta función corrige ese swap:

      fact: sufijo 'afrontamiento'      = instrumento de coping ("nunca hago eso")
      fact: sufijo 'capitalpsicologico' = capital psicológico ("de acuerdo")

      categorias: id con 'afrontamiento'      → dims CapPsic (Autoeficacia, etc.)
      categorias: id con 'capitalpsicologico' → dims Afrontamiento (activo_, etc.)

    Derivaciones:
      instrumento   ← sufijo en fact + forma_intra
      dimension     ← categorias_analisis (con swap)
      dominio       ← mapa dimension→dominio hardcodeado (batería Ministerio)
      factor        ← categorias_analisis
    """
    # ── Swap de sufijos entre fact y categorias ─────────────────────────────
    SWAP = {
        "afrontamiento":     "capitalpsicologico",
        "capitalpsicologico": "afrontamiento",
    }

    # ── Instrumento según sufijo ─────────────────────────────────────────────
    INSTRUMENTO_MAP: dict[str, str | dict] = {
        "extra":             "Extralaboral",
        "estres":            "Estres",
        "afrontamiento":     "Afrontamiento",
        "capitalpsicologico": "CapPsico",
    }  # "intra" se resuelve con forma_intra

    # ── Dominio intralaboral (Ministerio, Res. 2764/2022) ────────────────────
    DOMINIO_INTRA: dict[str, str] = {
        # Demandas del trabajo
        "Demandas cuantitativas":                      "Demandas del trabajo",
        "Demandas de carga mental":                    "Demandas del trabajo",
        "Demandas emocionales":                        "Demandas del trabajo",
        "Exigencias de responsabilidad del cargo":     "Demandas del trabajo",
        "Demandas ambientales y de esfuerzo f\u00edsico": "Demandas del trabajo",
        "Consistencia del rol":                        "Demandas del trabajo",
        "Demandas de la jornada de trabajo":           "Demandas del trabajo",
        "Influencia del trabajo sobre el entorno extra": "Demandas del trabajo",
        # Control sobre el trabajo
        "Control y autonom\u00eda sobre el trabajo":  "Control sobre el trabajo",
        "Oportunidades de desarrollo y uso de habilidad": "Control sobre el trabajo",
        "Participaci\u00f3n y manejo del cambio":     "Control sobre el trabajo",
        "Claridad de rol":                             "Control sobre el trabajo",
        "Capacitaci\u00f3n":                          "Control sobre el trabajo",
        # Liderazgo y relaciones sociales
        "Caracter\u00edsticas del liderazgo":         "Liderazgo y relaciones sociales",
        "Relaciones sociales en el trabajo":           "Liderazgo y relaciones sociales",
        "Retroalimentaci\u00f3n del desempe\u00f1o": "Liderazgo y relaciones sociales",
        "Relaci\u00f3n con los colaboradores (subordinados)": "Liderazgo y relaciones sociales",
        # Recompensas
        "Reconocimiento y compensaci\u00f3n":         "Recompensas",
        "Recompensas derivadas de la pertenencia a la": "Recompensas",
    }

    # ── Preparar categorias: calcular sufijo real en fact ───────────────────
    cat = cat[["id_pregunta", "forma_intra", "dimension", "factor"]].copy()
    cat["num"]       = cat["id_pregunta"].str.split("_", n=1).str[0]
    cat["suf_cat"]   = cat["id_pregunta"].str.split("_", n=1).str[1]
    # sufijo que tendrá en fact_respuestas (aplicar swap)
    cat["suf_fact"]  = cat["suf_cat"].map(lambda s: SWAP.get(s, s))
    cat["id_fact"]   = cat["num"] + "_" + cat["suf_fact"]

    # ── IDs únicos de fact ───────────────────────────────────────────────────
    fact_ids = (
        fact[["id_pregunta", "forma_intra"]]
        .drop_duplicates()
        .copy()
    )
    fact_ids["sufijo"] = fact_ids["id_pregunta"].str.split("_", n=1).str[1]

    # ── JOIN intra: específico por forma ─────────────────────────────────────
    intra_cat = cat[cat["suf_cat"] == "intra"][
        ["id_fact", "forma_intra", "dimension", "factor"]
    ]
    intra_fact = fact_ids[fact_ids["sufijo"] == "intra"]
    intra_merged = intra_fact.merge(
        intra_cat.rename(columns={"id_fact": "id_pregunta", "forma_intra": "forma_cat"}),
        left_on=["id_pregunta", "forma_intra"],
        right_on=["id_pregunta", "forma_cat"],
        how="left",
    ).drop(columns="forma_cat", errors="ignore")

    # ── JOIN otros instrumentos: sin restricción de forma ───────────────────
    other_cat = (
        cat[cat["suf_cat"] != "intra"]
        .drop_duplicates("id_fact")[["id_fact", "dimension", "factor"]]
    )
    other_fact = fact_ids[fact_ids["sufijo"] != "intra"]
    other_merged = other_fact.merge(
        other_cat.rename(columns={"id_fact": "id_pregunta"}),
        on="id_pregunta",
        how="left",
    )

    result = pd.concat([intra_merged, other_merged], ignore_index=True)

    # ── Instrumento ──────────────────────────────────────────────────────────
    def _instrumento(row) -> str:
        s = row["sufijo"]
        if s == "intra":
            return "IntraA" if row["forma_intra"] == "A" else "IntraB"
        return INSTRUMENTO_MAP.get(s, s)

    result["instrumento"] = result.apply(_instrumento, axis=1)

    # ── Dominio ──────────────────────────────────────────────────────────────
    def _dominio(row) -> str:
        s = row["sufijo"]
        if s == "intra":
            return DOMINIO_INTRA.get(str(row.get("dimension", "")), "Sin dominio")
        elif s == "extra":
            return "Extralaboral"
        elif s == "estres":
            return "Estr\u00e9s"
        elif s == "afrontamiento":
            return "Afrontamiento"
        elif s == "capitalpsicologico":
            return "Vulnerabilidad"
        return None

    result["dominio"] = result.apply(_dominio, axis=1)

    dim_pq = result[
        ["id_pregunta", "forma_intra", "instrumento", "dimension", "dominio", "factor"]
    ].drop_duplicates(["id_pregunta", "forma_intra"])

    sin_dim = dim_pq["dimension"].isna().sum()
    if sin_dim > 0:
        log.warning("dim_pregunta — %d ítems sin dimensión asignada", sin_dim)

    log.info(
        "dim_pregunta construida: %d ítems — instrumentos: %s",
        len(dim_pq), sorted(dim_pq["instrumento"].unique().tolist()),
    )
    return dim_pq


def main():
    log.info("=" * 60)
    log.info("SCRIPT 02a — Scoring Batería (Pasos 1-8)")
    log.info("=" * 60)

    cfg = cargar_config()
    proc = ROOT / cfg["paths"]["processed"]

    # Cargar insumos
    fact = pd.read_parquet(proc / "fact_respuestas_clean.parquet")
    categorias = pd.read_parquet(proc / "categorias_analisis.parquet")
    log.info("fact_respuestas_clean: %d filas", len(fact))

    # Construir dim_pregunta internamente (no existe como hoja Excel)
    dim_pregunta = _construir_dim_pregunta(fact, categorias)

    # ── PASO 1: codificación texto → número ────────────────────────────────
    log.info("Paso 1 — Codificando respuestas texto → número...")
    fact = fact.copy()
    fact["valor_numerico"] = aplicar_paso1_vectorizado(fact)
    nan_count = fact["valor_numerico"].isna().sum()
    log.info(
        "Paso 1 completado — %d valores codificados, %d no mapeados (%.1f%%)",
        len(fact) - nan_count, nan_count, nan_count / len(fact) * 100,
    )

    # Reporte de respuestas no mapeadas (para debugging)
    if nan_count > 0:
        no_mapeadas = fact[fact["valor_numerico"].isna()][
            ["id_pregunta", "forma_intra", "id_respuesta"]
        ].value_counts().head(20)
        log.warning("Top respuestas no mapeadas:\n%s", no_mapeadas.to_string())

    # Descartar filas con valor_numerico nulo:
    # - Ítems fuera de la forma del trabajador (e.g. ítems 98-125 para IntraB → "No aplica")
    # - Respuestas no reconocidas en las escalas de codificación
    n_antes = len(fact)
    fact = fact.dropna(subset=["valor_numerico"])
    n_dropped = n_antes - len(fact)
    if n_dropped > 0:
        log.warning(
            "Descartadas %d filas con valor_numerico nulo (ítems no aplicables a esta forma, %.1f%%)",
            n_dropped, 100 * n_dropped / n_antes,
        )

    # Agregar max_item_score (necesario en 02b para calcular transformacion_max)
    fact["max_item_score"] = fact.apply(
        lambda r: _max_valor_item(r["id_pregunta"], r["forma_intra"]), axis=1
    )

    # ── PASO 2: inversión de ítems (requiere es_item_invertido en dim_pregunta) ──
    log.info("Paso 2 — Invirtiendo ítems marcados como es_item_invertido=True...")
    fact = aplicar_paso2(fact)

    # ── PASOS 3-8: agrupaciones → instrumento, dimensión, dominio, factor ──
    log.info("Pasos 3-8 — Asignando instrumento, dimensión, dominio, factor...")
    fact = aplicar_pasos3_a_8(fact, dim_pregunta)

    # ── Validación (R14) ────────────────────────────────────────────────────
    es_valido, reporte = validar_scores_brutos(fact)
    if not es_valido:
        log.error("Validación FALLIDA:\n%s", reporte.to_string())
        sys.exit(1)
    log.info("Validación OK")

    # ── Guardar (R13) ───────────────────────────────────────────────────────
    out = proc / "fact_scores_brutos.parquet"
    fact.to_parquet(out, index=False)
    log.info("Guardado: %s (%d filas × %d columnas)", out, *fact.shape)

    log.info("=" * 60)
    log.info("Pasos 1-8 completados → fact_scores_brutos.parquet")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
