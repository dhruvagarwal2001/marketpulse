import pandas as pd
import numpy as np
from dataclasses import dataclass

@dataclass
class Signal:
    action: str  # "BUY", "SELL", "HOLD"
    confidence: float  # 0.0 to 1.0
    reason: str

class TechnicalAnalyst:
    """
    Automated Intelligence for the Co-Pilot.
    Analyzes price history to generate signals.
    """
    
    def analyze(self, history: pd.DataFrame) -> Signal:
        """
        Analyzes a DataFrame with 'price' column.
        Simple Strategy: 
        - RSI < 30 -> BUY (Oversold)
        - RSI > 70 -> SELL (Overbought)
        - Trend following otherwise.
        """
        if len(history) < 15:
             # Not enough data
             return Signal("HOLD", 0.0, "Insufficient Data")

        prices = history['price']
        # Squeeze if it's a DataFrame to get a Series
        if isinstance(prices, pd.DataFrame):
            prices = prices.squeeze()
            
        rsi = self._calculate_rsi(prices)
        current_rsi = float(rsi.iloc[-1])
        
        # Determine Trend (Simple Moving Average 10 vs 50) - simplified for MVP using slope
        short_term_slope = float(prices.iloc[-5:].pct_change().mean())
        
        if current_rsi < 30:
            return Signal("BUY", 0.85, f"Oversold (RSI: {current_rsi:.1f})")
        elif current_rsi > 70:
            return Signal("SELL", 0.85, f"Overbought (RSI: {current_rsi:.1f})")
        elif short_term_slope > 0.01:
            return Signal("BUY", 0.60, "Strong Uptrend")
        elif short_term_slope < -0.01:
            return Signal("SELL", 0.60, "Strong Downtrend")
        else:
            return Signal("HOLD", 0.50, "Market choppy")

    def _calculate_rsi(self, series: pd.Series, period: int = 14) -> pd.Series:
        delta = series.diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
        
        rs = gain / loss
        return 100 - (100 / (1 + rs))
