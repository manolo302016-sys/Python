# ════════════════════════════════════════════════════════════════════════════════
# 01_ETL_STAR_SCHEMA.PY — ETL y Construcción del Star Schema
# Pipeline MentalPRO — Análisis de Riesgo Psicosocial (Res. 2764/2022)
# ════════════════════════════════════════════════════════════════════════════════

# ══════════════════════════════════════════════════════════════════════════════
# CONFIGURACIÓN E IMPORTS
# ══════════════════════════════════════════════════════════════════════════════

import pandas as pd
import numpy as np
from pathlib import Path
from typing import Dict, Tuple, List
import logging
from datetime import datetime

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# ══════════════════════════════════════════════════════════════════════════════
# RUTAS DE ARCHIVOS
# ══════════════════════════════════════════════════════════════════════════════

# Archivos de entrada (2-file architecture confirmada)
PATH_FACT = Path('Resultado_mentalPRO.xlsx')      # Fact tables
PATH_DIM  = Path('datasets.xlsx')                  # Dimensional tables

# Archivo de salida
PATH_OUTPUT = Path('star_schema_clean.parquet')

# ══════════════════════════════════════════════════════════════════════════════
# CONSTANTES Y MAPEOS
# ══════════════════════════════════════════════════════════════════════════════

# Columnas esperadas en FactRespuestas
FACT_COLUMNS = [
    'cedula', 'nombre_trabajador', 'forma_intra', 'empresa', 'sector_economico',
    'ausentismo_eg_si_no', 'ausentismo_al_si_no', 'n_dias_totales_ausentismo',
    'id_pregunta', 'id_respuesta'
]

# Homologación sector_economico → sector_rag (V2 Paso 7)
# Confirmado: 6 sectores activos en data, 10 en RAG
SECTOR_MAP = {
    'Agricultura': 'Agricultura',
    'Comercio': 'Comercio/financiero',   # ← único que cambia
    'Construcción': 'Construcción',
    'Manufactura': 'Manufactura',
    'Servicios': 'Servicios',
    'Transporte': 'Transporte',
    # Sectores en RAG pero no presentes aún en data:
    # 'Salud', 'Educación', 'Administración pública', 'Minas/energía'
}

# Sectores válidos según RAG (para validación)
SECTORES_RAG_VALIDOS = [
    'Agricultura', 'Comercio/financiero', 'Construcción', 'Educación',
    'Manufactura', 'Minas/energía', 'Administración pública',
    'Salud', 'Servicios', 'Transporte'
]

# Formas válidas del instrumento
FORMAS_VALIDAS = ['A', 'B']

# ══════════════════════════════════════════════════════════════════════════════
# TABLA DE CODIFICACIÓN TEXTO → NÚMERO (V1 Paso 1)
# ══════════════════════════════════════════════════════════════════════════════

# Escala Likert estándar (intralaboral, extralaboral)
CODIFICACION_LIKERT = {
    'Siempre': 4,
    'Casi siempre': 3,
    'Algunas veces': 2,
    'A veces': 2,          # variante
    'Casi nunca': 1,
    'Nunca': 0,
}

# Escala dicotómica (sí/no)
CODIFICACION_DICOTOMICA = {
    'Sí': 1, 'Si': 1, 'SI': 1, 'sí': 1, 'si': 1,
    'No': 0, 'NO': 0, 'no': 0,
}

# Escala afrontamiento (ítems 5-8 capital psicológico) — escala propia
CODIFICACION_AFRONTAMIENTO = {
    'Siempre hago eso': 0,
    'siempre hago eso': 0,
    'Frecuentemente hago eso': 0.5,
    'frecuentemente hago eso': 0.5,
    'A veces hago eso': 0.7,
    'a veces hago eso': 0.7,
    'Nunca hago eso': 1,
    'nunca hago eso': 1,
}

# ══════════════════════════════════════════════════════════════════════════════
# FUNCIONES DE CARGA
# ══════════════════════════════════════════════════════════════════════════════

def cargar_fact_respuestas(path: Path) -> pd.DataFrame:
    '''
    Carga FactRespuestas desde Excel.
    Valida columnas esperadas y tipos de datos.
    '''
    logger.info(f'Cargando FactRespuestas desde {path}...')
    
    df = pd.read_excel(path, sheet_name='FactRespuestas')
    logger.info(f'  Filas cargadas: {len(df):,}')
    
    # Validar columnas
    cols_faltantes = set(FACT_COLUMNS) - set(df.columns)
    if cols_faltantes:
        raise ValueError(f'Columnas faltantes en FactRespuestas: {cols_faltantes}')
    
    # Seleccionar solo columnas esperadas
    df = df[FACT_COLUMNS].copy()
    
    # Normalizar tipos
    df['cedula'] = df['cedula'].astype(str).str.strip()
    df['nombre_trabajador'] = df['nombre_trabajador'].astype(str).str.strip()
    df['forma_intra'] = df['forma_intra'].astype(str).str.strip().str.upper()
    df['empresa'] = df['empresa'].astype(str).str.strip()
    df['sector_economico'] = df['sector_economico'].astype(str).str.strip()
    df['id_pregunta'] = df['id_pregunta'].astype(str).str.strip().str.lower()
    df['id_respuesta'] = df['id_respuesta'].astype(str).str.strip()
    df['n_dias_totales_ausentismo'] = pd.to_numeric(df['n_dias_totales_ausentismo'], errors='coerce').fillna(0).astype(int)
    
    return df


def cargar_dimensiones(path: Path) -> Dict[str, pd.DataFrame]:
    '''
    Carga tablas dimensionales desde datasets.xlsx.
    Retorna diccionario {nombre_tabla: DataFrame}.
    '''
    logger.info(f'Cargando dimensiones desde {path}...')
    
    dims = {}
    tablas_esperadas = [
        'dim_trabajador', 'dim_pregunta', 'dim_respuesta', 'dim_baremo',
        'dim_demografia', 'dim_ausentismo', 'categorias_analisis'
    ]
    
    xl = pd.ExcelFile(path)
    hojas_disponibles = xl.sheet_names
    logger.info(f'  Hojas disponibles: {hojas_disponibles}')
    
    for tabla in tablas_esperadas:
        if tabla in hojas_disponibles:
            dims[tabla] = pd.read_excel(xl, sheet_name=tabla)
            logger.info(f'  ✓ {tabla}: {len(dims[tabla]):,} filas')
        else:
            logger.warning(f'  ⚠ {tabla} no encontrada en {path}')
    
    return dims


# ══════════════════════════════════════════════════════════════════════════════
# FUNCIONES DE VALIDACIÓN
# ══════════════════════════════════════════════════════════════════════════════

def validar_pk_triple(df: pd.DataFrame) -> Tuple[bool, pd.DataFrame]:
    '''
    Valida que la PK triple (cedula + forma_intra + id_pregunta) sea única.
    Retorna (es_valida, df_duplicados).
    '''
    logger.info('Validando PK triple (cedula + forma_intra + id_pregunta)...')
    
    pk_cols = ['cedula', 'forma_intra', 'id_pregunta']
    duplicados = df[df.duplicated(subset=pk_cols, keep=False)]
    
    if len(duplicados) > 0:
        n_dups = len(duplicados)
        n_grupos = duplicados.groupby(pk_cols).ngroups
        logger.error(f'  ✗ {n_dups:,} filas duplicadas en {n_grupos:,} grupos')
        return False, duplicados
    else:
        logger.info(f'  ✓ PK triple válida. {len(df):,} filas únicas.')
        return True, pd.DataFrame()


def validar_formas(df: pd.DataFrame) -> pd.DataFrame:
    '''
    Valida que forma_intra solo contenga valores válidos (A, B).
    Retorna filas con formas inválidas.
    '''
    logger.info('Validando formas del instrumento...')
    
    formas_encontradas = df['forma_intra'].unique()
    formas_invalidas = set(formas_encontradas) - set(FORMAS_VALIDAS)
    
    if formas_invalidas:
        logger.error(f'  ✗ Formas inválidas encontradas: {formas_invalidas}')
        return df[df['forma_intra'].isin(formas_invalidas)]
    else:
        logger.info(f'  ✓ Formas válidas: {list(formas_encontradas)}')
        return pd.DataFrame()


def validar_sectores(df: pd.DataFrame) -> Tuple[pd.DataFrame, List[str]]:
    '''
    Valida sectores y aplica homologación.
    Retorna (df_con_sector_rag, sectores_no_mapeados).
    '''
    logger.info('Validando y homologando sectores económicos...')
    
    sectores_encontrados = df['sector_economico'].unique()
    logger.info(f'  Sectores únicos en data: {list(sectores_encontrados)}')
    
    # Aplicar homologación
    df = df.copy()
    df['sector_rag'] = df['sector_economico'].map(SECTOR_MAP)
    
    # Detectar sectores no mapeados
    no_mapeados = df[df['sector_rag'].isna()]['sector_economico'].unique().tolist()
    
    if no_mapeados:
        logger.warning(f'  ⚠ Sectores sin mapeo RAG: {no_mapeados}')
        # Mantener sector original si no hay mapeo
        df['sector_rag'] = df['sector_rag'].fillna(df['sector_economico'])
    else:
        logger.info(f'  ✓ Todos los sectores mapeados correctamente')
    
    # Validar contra sectores RAG válidos
    sectores_rag_encontrados = df['sector_rag'].unique()
    invalidos = set(sectores_rag_encontrados) - set(SECTORES_RAG_VALIDOS)
    if invalidos:
        logger.warning(f'  ⚠ Sectores RAG no estándar: {invalidos}')
    
    return df, no_mapeados


# ══════════════════════════════════════════════════════════════════════════════
# FUNCIONES DE TRANSFORMACIÓN
# ══════════════════════════════════════════════════════════════════════════════

def determinar_tipo_escala(id_pregunta: str) -> str:
    '''
    Determina el tipo de escala según el id_pregunta.
    '''
    if '_intra' in id_pregunta or '_extra' in id_pregunta or '_estres' in id_pregunta:
        # Ítems 106 y 116 de IntraA son dicotómicos (sí/no)
        num = id_pregunta.split('_')[0]
        if num in ['106', '116']:
            return 'dicotomica'
        return 'likert'
    elif '_afrontamiento' in id_pregunta:
        # Ítems 5-8 de afrontamiento tienen escala propia
        num = id_pregunta.split('_')[0]
        if num in ['5', '6', '7', '8']:
            return 'afrontamiento_especial'
        return 'afrontamiento'
    elif '_capitalpsicologico' in id_pregunta:
        return 'likert'
    else:
        return 'likert'  # default


def codificar_respuesta(id_respuesta: str, tipo_escala: str) -> float:
    '''
    Convierte respuesta texto a código numérico según tipo de escala.
    V1 Paso 1 — Codificación texto → número.
    '''
    respuesta = str(id_respuesta).strip()
    
    if tipo_escala == 'likert':
        return CODIFICACION_LIKERT.get(respuesta, np.nan)
    elif tipo_escala == 'dicotomica':
        return CODIFICACION_DICOTOMICA.get(respuesta, np.nan)
    elif tipo_escala in ['afrontamiento', 'afrontamiento_especial']:
        # Primero intentar escala afrontamiento especial
        val = CODIFICACION_AFRONTAMIENTO.get(respuesta)
        if val is not None:
            return val
        # Fallback a Likert estándar
        return CODIFICACION_LIKERT.get(respuesta, np.nan)
    else:
        return CODIFICACION_LIKERT.get(respuesta, np.nan)


def aplicar_codificacion(df: pd.DataFrame) -> pd.DataFrame:
    '''
    Aplica codificación numérica a todas las respuestas.
    '''
    logger.info('Aplicando codificación texto → número (V1 Paso 1)...')
    
    df = df.copy()
    
    # Determinar tipo de escala para cada pregunta
    df['tipo_escala'] = df['id_pregunta'].apply(determinar_tipo_escala)
    
    # Aplicar codificación
    df['score_bruto'] = df.apply(
        lambda row: codificar_respuesta(row['id_respuesta'], row['tipo_escala']),
        axis=1
    )
    
    # Verificar NaN
    n_nan = df['score_bruto'].isna().sum()
    if n_nan > 0:
        logger.warning(f'  ⚠ {n_nan:,} respuestas no codificadas (NaN)')
        respuestas_problema = df[df['score_bruto'].isna()]['id_respuesta'].unique()[:10]
        logger.warning(f'    Ejemplos: {list(respuestas_problema)}')
    else:
        logger.info(f'  ✓ Todas las respuestas codificadas correctamente')
    
    return df


# ══════════════════════════════════════════════════════════════════════════════
# FUNCIÓN PRINCIPAL DE ETL
# ══════════════════════════════════════════════════════════════════════════════

def ejecutar_etl() -> Tuple[pd.DataFrame, Dict[str, pd.DataFrame], dict]:
    '''
    Ejecuta el pipeline ETL completo.
    Retorna (fact_respuestas_clean, dimensiones, reporte).
    '''
    logger.info('═' * 60)
    logger.info('INICIANDO ETL STAR SCHEMA — MentalPRO')
    logger.info('═' * 60)
    
    reporte = {
        'inicio': datetime.now().isoformat(),
        'errores': [],
        'advertencias': [],
        'metricas': {}
    }
    
    # ─────────────────────────────────────────────────────────────────────
    # PASO 1: Cargar datos
    # ─────────────────────────────────────────────────────────────────────
    logger.info('\n[PASO 1] Cargando datos...')
    
    df = cargar_fact_respuestas(PATH_FACT)
    dims = cargar_dimensiones(PATH_DIM)
    
    reporte['metricas']['filas_originales'] = len(df)
    reporte['metricas']['dimensiones_cargadas'] = list(dims.keys())
    
    # ─────────────────────────────────────────────────────────────────────
    # PASO 2: Validaciones
    # ─────────────────────────────────────────────────────────────────────
    logger.info('\n[PASO 2] Ejecutando validaciones...')
    
    # 2.1 Validar PK triple
    pk_valida, df_dups = validar_pk_triple(df)
    if not pk_valida:
        reporte['errores'].append(f'PK duplicada: {len(df_dups)} filas')
        # Eliminar duplicados manteniendo primera ocurrencia
        df = df.drop_duplicates(subset=['cedula', 'forma_intra', 'id_pregunta'], keep='first')
        logger.info(f'  → Duplicados eliminados. Filas restantes: {len(df):,}')
    
    # 2.2 Validar formas
    df_formas_inv = validar_formas(df)
    if len(df_formas_inv) > 0:
        reporte['errores'].append(f'Formas inválidas: {len(df_formas_inv)} filas')
        df = df[df['forma_intra'].isin(FORMAS_VALIDAS)]
    
    # 2.3 Validar y homologar sectores
    df, sectores_no_mapeados = validar_sectores(df)
    if sectores_no_mapeados:
        reporte['advertencias'].append(f'Sectores sin mapeo: {sectores_no_mapeados}')
    
    # ─────────────────────────────────────────────────────────────────────
    # PASO 3: Transformaciones
    # ─────────────────────────────────────────────────────────────────────
    logger.info('\n[PASO 3] Aplicando transformaciones...')
    
    # 3.1 Codificar respuestas
    df = aplicar_codificacion(df)
    
    # 3.2 Extraer componentes de id_pregunta
    df['numero_pregunta'] = df['id_pregunta'].str.extract(r'^(\d+)').astype(float)
    df['instrumento'] = df['id_pregunta'].str.extract(r'_(\w+)$')[0]
    
    # ─────────────────────────────────────────────────────────────────────
    # PASO 4: Métricas finales
    # ─────────────────────────────────────────────────────────────────────
    logger.info('\n[PASO 4] Calculando métricas finales...')
    
    reporte['metricas'].update({
        'filas_finales': len(df),
        'n_trabajadores': df['cedula'].nunique(),
        'n_empresas': df['empresa'].nunique(),
        'n_sectores': df['sector_economico'].nunique(),
        'n_preguntas': df['id_pregunta'].nunique(),
        'empresas': df['empresa'].unique().tolist(),
        'sectores_originales': df['sector_economico'].unique().tolist(),
        'sectores_rag': df['sector_rag'].unique().tolist(),
        'formas': df['forma_intra'].unique().tolist(),
        'instrumentos': df['instrumento'].unique().tolist(),
        'pct_nan_score': round(df['score_bruto'].isna().mean() * 100, 2)
    })
    
    # Log métricas
    logger.info(f"  • Trabajadores únicos: {reporte['metricas']['n_trabajadores']:,}")
    logger.info(f"  • Empresas: {reporte['metricas']['n_empresas']}")
    logger.info(f"  • Sectores: {reporte['metricas']['n_sectores']}")
    logger.info(f"  • Preguntas únicas: {reporte['metricas']['n_preguntas']}")
    logger.info(f"  • % NaN en scores: {reporte['metricas']['pct_nan_score']}%")
    
    reporte['fin'] = datetime.now().isoformat()
    
    logger.info('\n' + '═' * 60)
    logger.info('ETL COMPLETADO EXITOSAMENTE')
    logger.info('═' * 60)
    
    return df, dims, reporte


# ══════════════════════════════════════════════════════════════════════════════
# GUARDAR RESULTADOS
# ══════════════════════════════════════════════════════════════════════════════

def guardar_resultados(df: pd.DataFrame, dims: Dict[str, pd.DataFrame], reporte: dict):
    '''
    Guarda fact_respuestas_clean y dimensiones en formato parquet.
    '''
    logger.info('\nGuardando resultados...')
    
    # Guardar fact table principal
    df.to_parquet(PATH_OUTPUT, index=False)
    logger.info(f'  ✓ {PATH_OUTPUT} guardado ({len(df):,} filas)')
    
    # Guardar dimensiones
    for nombre, dim_df in dims.items():
        path_dim = Path(f'{nombre}.parquet')
        dim_df.to_parquet(path_dim, index=False)
        logger.info(f'  ✓ {path_dim} guardado ({len(dim_df):,} filas)')
    
    # Guardar reporte JSON
    import json
    with open('etl_reporte.json', 'w', encoding='utf-8') as f:
        json.dump(reporte, f, indent=2, ensure_ascii=False, default=str)
    logger.info('  ✓ etl_reporte.json guardado')


# ══════════════════════════════════════════════════════════════════════════════
# EJECUCIÓN
# ══════════════════════════════════════════════════════════════════════════════

if __name__ == '__main__':
    # Ejecutar ETL
    fact_respuestas_clean, dimensiones, reporte = ejecutar_etl()
    
    # Guardar resultados
    guardar_resultados(fact_respuestas_clean, dimensiones, reporte)
    
    # Mostrar resumen
    print('\n' + '═' * 60)
    print('RESUMEN ETL')
    print('═' * 60)
    print(f"Filas procesadas: {reporte['metricas']['filas_finales']:,}")
    print(f"Trabajadores: {reporte['metricas']['n_trabajadores']:,}")
    print(f"Empresas: {reporte['metricas']['empresas']}")
    print(f"Sectores RAG: {reporte['metricas']['sectores_rag']}")
    print(f"Errores: {len(reporte['errores'])}")
    print(f"Advertencias: {len(reporte['advertencias'])}")