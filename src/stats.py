import pandas as pd
import logging

logger = logging.getLogger(__name__)

class StatsAnalyzer:
    @staticmethod
    def describe_data(df: pd.DataFrame) -> pd.DataFrame:
        """Generate comprehensive descriptive statistics for numeric columns."""
        logger.info("Generating descriptive statistics.")
        return df.describe(include='all')

    @staticmethod
    def correlation_matrix(df: pd.DataFrame, method='pearson') -> pd.DataFrame:
        """Calculate the correlation matrix for numeric columns."""
        numeric_df = df.select_dtypes(include=['number'])
        logger.info(f"Calculating {method} correlation matrix on {len(numeric_df.columns)} numeric columns.")
        return numeric_df.corr(method=method)
