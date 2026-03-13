import pandas as pd
import logging

logger = logging.getLogger(__name__)

class Cleaner:
    @staticmethod
    def drop_duplicates(df: pd.DataFrame, subset=None) -> pd.DataFrame:
        initial_len = len(df)
        df_clean = df.drop_duplicates(subset=subset)
        dropped = initial_len - len(df_clean)
        logger.info(f"Dropped {dropped} duplicate rows.")
        return df_clean

    @staticmethod
    def handle_missing_values(df: pd.DataFrame, strategy='drop', fill_value=None) -> pd.DataFrame:
        """
        Handle missing values in the DataFrame.
        
        strategy: 'drop', 'fill', 'mean', 'median', 'mode'
        """
        df_clean = df.copy()
        
        if strategy == 'drop':
            df_clean = df_clean.dropna()
        elif strategy == 'fill':
            if fill_value is not None:
                df_clean = df_clean.fillna(fill_value)
        elif strategy in ['mean', 'median']:
            numeric_cols = df_clean.select_dtypes(include=['number']).columns
            for col in numeric_cols:
                if strategy == 'mean':
                    df_clean[col] = df_clean[col].fillna(df_clean[col].mean())
                elif strategy == 'median':
                    df_clean[col] = df_clean[col].fillna(df_clean[col].median())
        elif strategy == 'mode':
            categorical_cols = df_clean.select_dtypes(include=['object', 'category']).columns
            for col in categorical_cols:
                if not df_clean[col].mode().empty:
                    df_clean[col] = df_clean[col].fillna(df_clean[col].mode()[0])
                    
        nulls_handled = df.isnull().sum().sum() - df_clean.isnull().sum().sum()
        logger.info(f"Handled {nulls_handled} missing values using strategy: {strategy}.")
        return df_clean
