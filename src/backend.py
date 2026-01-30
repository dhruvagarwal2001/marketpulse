import pandas as pd
import asyncio
import sqlite3
import random
import time
from dataclasses import dataclass
from typing import Optional, List, Dict, Any

import logging
import yfinance as yf
import requests
import os
from dotenv import load_dotenv

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
    def __init__(self, db=None):
        self.running = False
        self._subscribers = []
        self.db = db
        # Active Polling List (Start with popular ones)
        self.monitoring_universe = ["NVDA", "TSLA", "AAPL", "BTC-USD", "ETH-USD", "EURUSD=X", "^GSPC"]
        # Full Available Universe (for UI Search)
        self.full_universe = []
        self._cache = {} # To track changes
        
        # Alpha Vantage Integration
        load_dotenv()
        av_key = os.getenv("ALPHA_VANTAGE_API_KEY")
        self.av_client = AlphaVantageClient(av_key) if av_key else None

    def subscribe(self, callback):
        self._subscribers.append(callback)

    async def start(self):
        self.running = True
        logger.info("MarketStream: Connected to Real Markets")
        
        # [NEW] Sync Universe from DB/remote
        # This will populate self.full_universe (thousands) and self.monitoring_universe (subset)
        await self._sync_universe()
        
        # Ensure we monitor at least the default set if nothing came back (fail-safe)
        if not self.monitoring_universe:
             self.monitoring_universe = ["NVDA", "TSLA", "AAPL", "BTC-USD", "ETH-USD", "EURUSD=X", "^GSPC"]

        logger.info(f"MarketStream: DB contains {len(self.full_universe)} tickers. Monitoring {len(self.monitoring_universe)}.")
        
        while self.running:
            # Poll one symbol at a time for Yahoo (Price + News)
            # Only poll the ACTIVE monitoring set, not the whole DB!
            for symbol in self.monitoring_universe:
                if not self.running: break
                await self._poll_symbol(symbol)
                await asyncio.sleep(2.0)
            
            # Poll Alpha Vantage every cycle (or more frequently if needed)
            if self.av_client:
                await self._poll_alpha_vantage()

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

    async def _poll_alpha_vantage(self):
        """Fetches news from Alpha Vantage for the entire universe."""
        if not self.av_client:
            return
            
        # Optimization: Alpha Vantage NEWS_SENTIMENT can only take 50 items max.
        # We can't poll news for 5000 items. 
        # Strategy: Poll for the *monitoring_universe* ONLY.
        
        # Split into chunks of 50
        batch_size = 50
        for i in range(0, len(self.monitoring_universe), batch_size):
            chunk = self.monitoring_universe[i:i + batch_size]
            
            logger.info(f"Polling Alpha Vantage for chunk {i}: {chunk[:3]}...")
            news_feed = await self.av_client.fetch_news(chunk)
            
            for item in news_feed:
                # ... (rest of processing loop)
                # Extract relevant info
                headline = item.get("title", "No Headline")
                summary = item.get("summary", "")
                url = item.get("url", "")
                source = item.get("source", "Alpha Vantage")
                
                # Find which of our universe tickers this applies to
                # (Ticker sentiment contains the relevant tickers)
                ticker_sentiments = item.get("ticker_sentiment", [])
                for ts in ticker_sentiments:
                    av_symbol = ts.get("ticker")
                    
                    # Try to find a match in our universe
                    matched_symbol = None
                    for u_symbol in self.monitoring_universe: # Match against monitored
                        if av_symbol == u_symbol:
                            matched_symbol = u_symbol
                            break
                        # Check mapped versions
                        if "-" in u_symbol and av_symbol == u_symbol.split("-")[0]:
                            matched_symbol = u_symbol
                            break
                        if u_symbol == "EURUSD=X" and av_symbol in ["EURUSD", "EUR/USD"]:
                            matched_symbol = u_symbol
                            break
                        if u_symbol == "^GSPC" and av_symbol in ["SPY", "GSPC", "S&P 500"]:
                            matched_symbol = u_symbol
                            break
                    
                    if matched_symbol:
                        # Map sentiment labels to our format if needed
                        av_sentiment = ts.get("ticker_sentiment_label", "Neutral")
                        
                        event = MarketEvent(
                            event_type="RAW_NEWS",
                            symbol=matched_symbol,
                            data={
                                "source": f"{source} (via AlphaVantage)",
                                "headline": headline,
                                "summary": summary,
                                "sentiment": av_sentiment.upper(),
                                "url": url
                            },
                            timestamp=time.time()
                        )
                        self._emit(event)
            
            # Rate limit protection between chunks?
            # Free tier is 5 req/min. 
            # If we have 1 chunk we are fine. If we have 10 chunks we die.
            # Assume monitoring universe is small (<50) for now.
            break # FORCE ONLY ONE CHUNK for safety on Free Tier

    async def _sync_universe(self):
        """Loads FULL universe from DB. Updates from Alpha Vantage daily."""
        if not self.db: return

        # 1. Load Everything from DB (Fast)
        stored_tickers = self.db.get_tickers()
        if stored_tickers:
            self.full_universe = stored_tickers
            # If we have a lot, don't monitor them all by default or we die.
            # Keep monitoring_universe as is (defaults) plus maybe some logic later.
        
        # 2. Check Daily Update (Heavy Fetch)
        last_update = float(self.db.get_setting("last_universe_update") or 0)
        
        # If DB is empty, forced update
        force_update = not stored_tickers
        
        if force_update or (time.time() - last_update > 86400): # 24 hours
             logger.info("MarketStream: Performing Message Universe Sync (LISTING_STATUS)...")
             if self.av_client:
                 try:
                     # FETCH ALL US LISTINGS
                     all_tickers = await self.av_client.fetch_listing_status()
                     if all_tickers:
                        logger.info(f"Fetched {len(all_tickers)} tickers. Saving to DB...")
                        self.db.add_tickers(all_tickers)
                        self.full_universe = self.db.get_tickers() # Reload
                        self.db.set_setting("last_universe_update", str(time.time()))
                        
                        # EMIT EVENT TO REFRESH UI
                        self._emit(MarketEvent("UNIVERSE_UPDATE", "SYSTEM", self.full_universe, time.time()))
                 except Exception as e:
                     logger.error(f"Failed to update universe: {e}")

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
        
        # Tickers table [NEW]
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS tickers (
                symbol TEXT PRIMARY KEY
            )
        ''')
        
        conn.commit()
        conn.close()

    def add_tickers(self, tickers: List[str]):
        if not tickers: return
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        for t in tickers:
            cursor.execute('INSERT OR IGNORE INTO tickers (symbol) VALUES (?)', (t,))
        conn.commit()
        conn.close()

    def get_tickers(self) -> List[str]:
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute('SELECT symbol FROM tickers')
        rows = cursor.fetchall()
        conn.close()
        return [r[0] for r in rows]
        
    def set_setting(self, key: str, value: str):
         conn = sqlite3.connect(self.db_path)
         cursor = conn.cursor()
         cursor.execute('INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)', (key, value))
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

class AlphaVantageClient:
    """
    Alpha Vantage API Client for News & Sentiment.
    """
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://www.alphavantage.co/query"

    async def fetch_news(self, tickers: List[str]) -> List[Dict]:
        """Fetches latest news sentiment for a list of tickers."""
        if not self.api_key:
            return []
            
        # Map symbols for Alpha Vantage
        av_tickers = []
        for t in tickers:
            if "-" in t and not t.startswith("^"): # Crypto e.g. BTC-USD
                av_tickers.append(t.split("-")[0])
            elif t == "EURUSD=X":
                av_tickers.append("EURUSD")
            elif t.startswith("^"): # Indices
                if t == "^GSPC": av_tickers.append("SPY")
                elif t == "^IXIC": av_tickers.append("QQQ")
            else:
                av_tickers.append(t)

        symbols_str = ",".join(av_tickers)
        params = {
            "function": "NEWS_SENTIMENT",
            "tickers": symbols_str,
            "apikey": self.api_key,
            "sort": "LATEST",
            "limit": 10
        }
        
        try:
            # Perform blocking request in a thread
            response = await asyncio.to_thread(requests.get, self.base_url, params=params)
            data = response.json()
            
            if "feed" not in data:
                logger.warning(f"Alpha Vantage: No news feed found. Response: {data}")
                return []
                
            return data["feed"]
        except Exception as e:
            logger.error(f"Alpha Vantage API error: {e}")
            return []

    async def fetch_listing_status(self) -> List[str]:
        """Fetches ALL active US Listings (CSV). Heavy."""
        if not self.api_key: return []
        
        params = {
            "function": "LISTING_STATUS",
            "state": "active",
            "apikey": self.api_key
        }
        
        try:
            logger.info("Fetching LISTING_STATUS (Full Market) from Alpha Vantage...")
            # Use pandas to read directly from CSV URL if possible, or request text then pandas
            
            # 1. Fetch Text
            response = await asyncio.to_thread(requests.get, self.base_url, params=params)
            csv_text = response.text
            
            if "Error" in csv_text or "Information" in csv_text: # API Error/Limit message
                 logger.warning(f"Listing Status Limit/Error: {csv_text[:100]}")
                 return []
            
            # 2. Parse CSV
            # Using io.StringIO to make it file-like for pandas
            import io
            df = pd.read_csv(io.StringIO(csv_text))
            
            # 3. Extract Symbols
            if 'symbol' in df.columns:
                symbols = df['symbol'].dropna().unique().tolist()
                return symbols
            
            return []
        except Exception as e:
            logger.error(f"Alpha Vantage Listing Fetch Error: {e}")
            return []
