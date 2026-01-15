import pandas as pd
import asyncio
import sqlite3
import random
import time
from dataclasses import dataclass
from typing import Optional, List, Dict, Any

import logging
import yfinance as yf # [NEW]

# Configure logging
logger = logging.getLogger(__name__)

@dataclass
class MarketEvent:
    event_type: str  # "PRICE_UPDATE", "RAW_NEWS", "FUNDAMENTALS"
    symbol: str
    data: Dict[str, Any]
    timestamp: float

class MarketStream:
    """
    Real-Time Market Data Stream using Yahoo Finance (yfinance).
    """
    def __init__(self):
        self.running = False
        self._subscribers = []
        # Diverse Universe: Tech, Crypto, Forex, Indices
        self.universe = ["NVDA", "TSLA", "AAPL", "BTC-USD", "ETH-USD", "EURUSD=X", "^GSPC"]
        self._cache = {} # To track changes

    def subscribe(self, callback):
        self._subscribers.append(callback)

    async def start(self):
        self.running = True
        logger.info("MarketStream: Connected to Real Markets (Yahoo Finance)")
        
        while self.running:
            # Poll one symbol at a time to avoid rate limits
            for symbol in self.universe:
                if not self.running: break
                
                await self._poll_symbol(symbol)
                await asyncio.sleep(2.0) # 2s delay between symbols

    def stop(self):
        self.running = False
        logger.info("MarketStream: Disconnected")

    def get_history(self, symbol: str) -> pd.DataFrame:
        """Fetches real 1-day history (1m intervals) for charts."""
        try:
            # Fetch last 5 hours of 1m data for granular charts
            df = yf.download(symbol, period="1d", interval="5m", progress=False, auto_adjust=True)
            if df.empty:
                return pd.DataFrame()
            return df.rename(columns={"Close": "price"}) # Normalize for Analysis
        except Exception as e:
            logger.error(f"Failed to fetch history for {symbol}: {e}")
            return pd.DataFrame()

    async def _poll_symbol(self, symbol: str):
        """Fetches live data for a single symbol."""
        try:
            logger.info(f"Polling {symbol}...") # Verbose debug
            
            # Run blocking yfinance call in a separate thread to keep UI responsive
            ticker = await asyncio.to_thread(yf.Ticker, symbol)
            
            # 1. Get Fast Info (Price)
            price = ticker.fast_info.last_price
            logger.info(f"{symbol} Price: {price}")
            
            # Detect Price Change
            last_price = self._cache.get(symbol, price)
            self._cache[symbol] = price
            
            # 2. Get News
            news_list = await asyncio.to_thread(lambda: ticker.news)
            logger.info(f"{symbol} News Count: {len(news_list) if news_list else 0}")
            
            if news_list:
                latest = news_list[0]
                # logger.info(f"RAW NEWS OBJ: {latest}") 
                
                # Check for nested 'content' (common in recent yfinance)
                content = latest.get('content', latest) 
                
                headline = content.get('title') or content.get('headline', 'No Headline')
                
                # Parse
                headline = latest.get('title', 'No Headline')
                publisher = latest.get('publisher', 'Unknown')
                url = latest.get('link', '')
                
                # Safe Extraction for Nested Objects
                if 'content' in latest and isinstance(latest['content'], dict):
                    headline = latest['content'].get('title', headline)
                    publisher = latest['content'].get('publisher', publisher)
                    url = latest['content'].get('canonicalUrl', {}).get('url', url)

                logger.info(f"Parsed News: {headline}")

                if headline == "No Headline":
                     return # Skip invalid news
                
                # Extract Summary if available (Fallback to headline if missing)
                summary = latest.get('summary', '') 
                if not summary and 'content' in latest and isinstance(latest['content'], dict):
                    summary = latest['content'].get('summary', '')

                # Emit RAW_NEWS
                event = MarketEvent(
                    event_type="RAW_NEWS",
                    symbol=symbol,
                    data={
                        "source": publisher,
                        "headline": headline,
                        "summary": summary,
                        "sentiment": "NEUTRAL", 
                        "url": url
                    },
                    timestamp=time.time()
                )
                self._emit(event)
                
            # 3. Simulate Fundamentals
            if random.random() > 0.95: 
                 event = MarketEvent(
                    event_type="FUNDAMENTALS",
                    symbol=symbol,
                    data={
                         "revenue_growth": random.uniform(-0.10, 0.30),
                         "net_margin": random.uniform(0.05, 0.25),
                         "debt_to_equity": random.uniform(0.5, 3.0),
                         "guidance": random.choice(["RAISED", "LOWERED", "MAINTAINED"])
                    },
                    timestamp=time.time()
                )
                 self._emit(event)

        except Exception as e:
            logger.error(f"Error polling {symbol}: {e}")

    def _emit(self, event):
        for callback in self._subscribers:
            if asyncio.iscoroutinefunction(callback):
                asyncio.create_task(callback(event))
            else:
                callback(event)

class LocalBrain:
    """
    Local SQLite database wrapper for storing user preferences and strategies.
    Privacy-first: All data stays local.
    """
    def __init__(self, db_path: str = "brain.db"):
        self.db_path = db_path
        self._init_db()

    def _init_db(self):
        """Initialize the database schema."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Strategies table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS strategies (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                parameters JSON
            )
        ''')
        
        # User settings table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT
            )
        ''')
        
        conn.commit()
        conn.close()

    def add_strategy(self, name: str, params: Dict[str, Any]):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        import json
        cursor.execute('INSERT INTO strategies (name, parameters) VALUES (?, ?)', (name, json.dumps(params)))
        conn.commit()
        conn.close()
        logger.info(f"LocalBrain: Strategy '{name}' saved.")

    def get_setting(self, key: str) -> Optional[str]:
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute('SELECT value FROM settings WHERE key = ?', (key,))
        result = cursor.fetchone()
        conn.close()
        return result[0] if result else None
