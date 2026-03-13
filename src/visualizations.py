import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import plotly.express as px
import plotly.graph_objects as go
import logging

logger = logging.getLogger(__name__)

class Visualizer:
    @staticmethod
    def set_theme():
        """Apply a sleek, modern enterprise theme to seaborn and matplotlib."""
        sns.set_theme(style="whitegrid", palette="muted")
        plt.rcParams['figure.figsize'] = (12, 6)
        logger.info("Enterprise visualization theme applied.")

    @staticmethod
    def interactive_scatter(df: pd.DataFrame, x: str, y: str, color=None, title="Scatter Plot") -> go.Figure:
        """Generate an interactive Plotly scatter plot."""
        fig = px.scatter(df, x=x, y=y, color=color, title=title, template='plotly_white')
        logger.info(f"Generated interactive scatter plot: {title}")
        return fig

    @staticmethod
    def correlation_heatmap(df_corr: pd.DataFrame, title="Correlation Heatmap"):
        """Plot a seaborn heatmap for a correlation matrix."""
        plt.figure(figsize=(10, 8))
        sns.heatmap(df_corr, annot=True, cmap='coolwarm', fmt=".2f", linewidths=.5)
        plt.title(title)
        plt.tight_layout()
        logger.info("Generated correlation heatmap.")
        # plt.show() # Uncomment to show directly during testing, better for notebooks to control display.

    @staticmethod
    def interactive_bar(df: pd.DataFrame, x: str, y: str, title="Bar Chart") -> go.Figure:
        """Generate an interactive Plotly bar chart."""
        fig = px.bar(df, x=x, y=y, title=title, template='plotly_white')
        logger.info(f"Generated interactive bar chart: {title}")
        return fig
