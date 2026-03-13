import logging
from src.config import DATABASE_URI
from src.data_loader import DataLoader
from src.cleaner import Cleaner
from src.transformer import Transformer
from src.stats import StatsAnalyzer
from src.visualizations import Visualizer
from src.models import PredictiveModeler

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def run_pipeline():
    logger.info("Starting Enterprise Data Science Pipeline")
    
    # 1. Load Data
    loader = DataLoader(db_uri=DATABASE_URI)
    # Example for CSV if db isn't available
    # metadata = loader.load_from_csv("data/raw/example.csv")
    
    logger.info("Pipeline executed successfully. (Replace with actual queries and datasets)")

if __name__ == "__main__":
    run_pipeline()
