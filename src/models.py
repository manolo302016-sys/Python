import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor
from sklearn.metrics import accuracy_score, classification_report, mean_squared_error, r2_score
import logging

logger = logging.getLogger(__name__)

class PredictiveModeler:
    def __init__(self, target_type='classification'):
        """
        Initialize the modeler.
        target_type: 'classification' or 'regression'
        """
        self.target_type = target_type
        if self.target_type == 'classification':
            self.model = RandomForestClassifier(n_estimators=100, random_state=42)
        elif self.target_type == 'regression':
            self.model = RandomForestRegressor(n_estimators=100, random_state=42)
        else:
            raise ValueError("target_type must be either 'classification' or 'regression'")
            
    def train_evaluate(self, X: pd.DataFrame, y: pd.Series, test_size=0.2):
        """Train the model and return evaluation metrics."""
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=test_size, random_state=42)
        
        logger.info(f"Training {self.target_type} model on {len(X_train)} samples.")
        self.model.fit(X_train, y_train)
        
        predictions = self.model.predict(X_test)
        
        if self.target_type == 'classification':
            acc = accuracy_score(y_test, predictions)
            report = classification_report(y_test, predictions)
            logger.info(f"Classification Evaluation - Accuracy: {acc:.4f}")
            return {'accuracy': acc, 'report': report}
        elif self.target_type == 'regression':
            mse = mean_squared_error(y_test, predictions)
            r2 = r2_score(y_test, predictions)
            logger.info(f"Regression Evaluation - MSE: {mse:.4f}, R2: {r2:.4f}")
            return {'mse': mse, 'r2': r2}
