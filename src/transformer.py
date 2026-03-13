import pandas as pd
import logging

logger = logging.getLogger(__name__)

class Transformer:
    @staticmethod
    def categorize_variable(df: pd.DataFrame, column: str, bins: list, labels: list) -> pd.DataFrame:
        """Categorize a continuous variable into discrete bins."""
        df_trans = df.copy()
        if column in df_trans.columns:
            df_trans[f'{column}_cat'] = pd.cut(df_trans[column], bins=bins, labels=labels)
            logger.info(f"Categorized column {column} into {len(labels)} bins.")
        else:
            logger.warning(f"Column {column} not found for categorization.")
        return df_trans

    @staticmethod
    def one_hot_encode(df: pd.DataFrame, columns: list) -> pd.DataFrame:
        """One-hot encode specified categorical columns."""
        df_trans = df.copy()
        for col in columns:
            if col in df_trans.columns:
                dummies = pd.get_dummies(df_trans[col], prefix=col, drop_first=True)
                df_trans = pd.concat([df_trans, dummies], axis=1)
                df_trans.drop(col, axis=1, inplace=True)
                logger.info(f"One-hot encoded column {col}.")
            else:
                logger.warning(f"Column {col} not found for encoding.")
        return df_trans
