import pandas as pd
from sqlalchemy import create_engine
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataLoader:
    def __init__(self, db_uri: str = None):
        """
        Initialize the DataLoader.

        Args:
            db_uri (str, optional): Connection string. Defaults to None.
        """
        self.db_uri = db_uri
        self.engine = None
        if self.db_uri:
            try:
                self.engine = create_engine(self.db_uri)
                logger.info("Database engine instantiated successfully.")
            except Exception as e:
                logger.error(f"Could not connect to database: {e}")

    def load_from_sql(self, query: str) -> pd.DataFrame:
        """
        Execute a SQL query and return a pandas DataFrame.
        """
        if not self.engine:
            raise ValueError("Database engine is not configured.")
        try:
            df = pd.read_sql(query, con=self.engine)
            logger.info(f"Loaded {len(df)} rows from query.")
            return df
        except Exception as e:
            logger.error(f"Error executing query: {e}")
            raise

    def load_from_csv(self, file_path: str) -> pd.DataFrame:
        """
        Load flat files based on the file_path
        """
        try:
            df = pd.read_csv(file_path)
            logger.info(f"Loaded {len(df)} rows from {file_path}.")
            return df
        except Exception as e:
            logger.error(f"Error loading CSV {file_path}: {e}")
            raise
