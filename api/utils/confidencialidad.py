import logging
import pandas as pd

log = logging.getLogger(__name__)

def aplicar_regla_r8(df: pd.DataFrame, conteo_col: str = "n_personas", umbral: int = 5, columnas_ocultar: list[str] = None) -> pd.DataFrame:
    """
    Aplica protección de confidencialidad R8 (N < 5) a un DataFrame agrupado.
    
    Args:
        df: DataFrame pre-agrupado que será enviado al frontend.
        conteo_col: Nombre de la columna que indica la cantidad de personas (N).
        umbral: Número mínimo de personas requeridas para revelar información.
        columnas_ocultar: Lista de columnas (scores, porcentajes) que deben blurearse. 
                          Si es None, se enmascaran todas las numéricas por defecto.
                          
    Returns:
        DataFrame con los valores enmascarados como "Confidencial" donde N < umbral.
    """
    if df.empty or conteo_col not in df.columns:
        return df

    out = df.copy()
    
    # Identificar filas que rompen la regla R8
    mask_r8 = out[conteo_col] < umbral
    
    filas_afectadas = mask_r8.sum()
    if filas_afectadas > 0:
        log.info(f"Regla R8 Activa: {filas_afectadas} filas enmascaradas por confidencialidad (N<{umbral}).")
        
        # Si no se proveen columnas explícitas, ocultar las métricas de score
        if columnas_ocultar is None:
            # Ocultar cualquier columna que parezca un score, porcentaje o kpi
            columnas_ocultar = [c for c in out.columns if c != conteo_col and 'score' in c.lower() or 'pct' in c.lower() or 'porcentaje' in c.lower()]
            
        # Aplicar el string "Confidencial" a las columnas afectadas en las filas que rompen la regla
        for col in columnas_ocultar:
            if col in out.columns:
                # Cambiar el dtype a object para permitir strings si era numérica
                out[col] = out[col].astype(object)
                out.loc[mask_r8, col] = "Confidencial"
                
    return out
