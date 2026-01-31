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
        
        # Deduplication Cache (URL or Tuple of (Symbol, Title))
        self._news_dedup = set()
        
        # Alpha Vantage Integration
        load_dotenv()
        av_key = os.getenv("ALPHA_VANTAGE_API_KEY")
        self.av_client = AlphaVantageClient(av_key) if av_key else None

    async def track_symbol(self, symbol: str):
        """Adds a symbol to the active monitoring loop and polls it immediately."""
        if symbol not in self.monitoring_universe:
            logger.info(f"MarketStream: Tracking new symbol {symbol}")
            self.monitoring_universe.append(symbol)
            # Immediate Poll to give user instant feedback
            if self.running:
                # Run as task to not block the caller
                asyncio.create_task(self._poll_symbol(symbol))
        else:
             logger.info(f"MarketStream: Already tracking {symbol}")

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
        
        # Run Polling Loops in Parallel
        # 1. Yahoo Finance Loop (Active List: Price + Specific News)
        # 2. Alpha Vantage Loop (Global Firehose: All News)
        await asyncio.gather(
            self._run_yahoo_loop(),
            self._run_av_loop()
        )

    async def _run_yahoo_loop(self):
        """Polls active monitoring list using Yahoo Finance."""
        while self.running:
            if not self.monitoring_universe:
                 await asyncio.sleep(1)
                 continue
                 
            # Poll all active symbols rapidly
            tasks = []
            for symbol in self.monitoring_universe:
                if not self.running: break
                tasks.append(self._poll_symbol(symbol))
            
            # Run symbol polls concurrently (parallel network requests)
            if tasks:
                await asyncio.gather(*tasks)
            
            await asyncio.sleep(30) # Wait before next detailed scan of monitoring list

    async def _run_av_loop(self):
        """Polls Global News Stream using Alpha Vantage."""
        while self.running:
            if self.av_client:
                 await self._poll_alpha_vantage()
            
            # Polling delay for AV (Global feed updates frequently but we have loop limits)
            # Alpha Vantage Free Tier limit is 25 calls per day? No, user has pro or we handle it.
            # Assuming we want frequent updates. 
            await asyncio.sleep(60) # Poll global feed every minute

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
            
            # 2. Get News - Re-enabled for Hybrid Coverage (Yahoo + Alpha Vantage)
            # logger.info(f"Polling Yahoo for {symbol}...")
            news_list = await asyncio.to_thread(lambda: ticker.news)
            
            if news_list:
                logger.debug(f"Yahoo found {len(news_list)} items for {symbol}")
                for latest in news_list[:2]: # Check top 2 recent news items, not just 1
                    # Check for nested 'content' (common in recent yfinance)
                    # Correct Parsing Logic for both flat and nested yfinance objects
                    content = latest.get('content', latest) 
                    
                    headline = content.get('title') or content.get('headline', 'No Headline')
                    publisher = content.get('publisher', 'Unknown')
                    url = content.get('canonicalUrl', {}).get('url', '') if isinstance(content.get('canonicalUrl'), dict) else content.get('link', '')
                    
                    if headline == "No Headline":
                         continue
                    
                    # --- DEDUPLICATION (Hybrid) ---
                    dedup_key = url if url else f"{headline}|{publisher}"
                     
                    # Check DB
                    # if self.db and self.db.is_news_seen(dedup_key):
                    #      # logger.info(f"Skipping duplicate Yahoo news: {headline}")
                    #      continue 
                    if dedup_key in self._news_dedup:
                         continue
                         
                    # Mark Seen
                    self._news_dedup.add(dedup_key)
                    if self.db: self.db.mark_news_seen(dedup_key)
                    
                    logger.info(f"New Yahoo News for {symbol}: {headline}")

                    # Extract Summary if available (Fallback to headline if missing)
                    summary = latest.get('summary', '') 
                    if not summary and 'content' in latest and isinstance(latest['content'], dict):
                        summary = latest['content'].get('summary', '')

                    # Emit RAW_NEWS
                    event = MarketEvent(
                        event_type="RAW_NEWS",
                        symbol=symbol,
                        data={
                            "source": f"{publisher} (via Yahoo)",
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
        """Fetches GLOBAL news from Alpha Vantage (All Markets)."""
        if not self.av_client:
            return
            
        # [MODIFIED] GLOBAL FIREHOSE STRATEGY
        # Instead of asking for specific tickers (which limits us to ~50),
        # we ask for "Latest News" generally. This returns news for ALL tickers.
        # We then filter this stream against our "full_universe" to authorize the alert.
        
        logger.info("Polling Alpha Vantage Global Feed...")
        
        # Pass None to get general market news
        news_feed = await self.av_client.fetch_news(tickers=None) 
        
        if not news_feed:
             return

        for item in news_feed:
            # Alpha Vantage provides news for multiple tickers in one article
            
            headline = item.get("title", "No Headline")
            summary = item.get("summary", "")
            url = item.get("url", "")
            source = item.get("source", "Alpha Vantage")
            
            # --- DEDUPLICATION ---
            # Use URL if available, else Headline + Source
            dedup_key = url if url else f"{headline}|{source}"
            
            # 1. Check Memory Cache first (Fast)
            if dedup_key in self._news_dedup:
                continue 
            
            # 2. Check Persistent DB (Robust)
            if self.db and self.db.is_news_seen(dedup_key):
                self._news_dedup.add(dedup_key) # Sync memory
                continue
                
            # New Item! Mark as seen in both
            self._news_dedup.add(dedup_key)
            if self.db:
                self.db.mark_news_seen(dedup_key)
            
            # Maintenance: Keep set size manageable (last 2000 items)
            if len(self._news_dedup) > 2000:
                self._news_dedup.pop()

            # Ticker Sentiment contains the list of stocks mentioned
            ticker_sentiments = item.get("ticker_sentiment", [])
            
            for ts in ticker_sentiments:
                av_symbol = ts.get("ticker")
                
                # CHECK VALIDITY: Is this a known US Stock?
                # We check against full_universe (populated from LISTING_STATUS)
                # If full_universe is empty (startup), we allow it tentatively (or strict check?)
                # Let's be permissive if DB is empty, but strict if populated.
                
                is_valid = False
                if self.full_universe:
                    if av_symbol in self.full_universe:
                        is_valid = True
                else:
                    # Fallback if DB not ready: allow standard looking tickers
                    if av_symbol and av_symbol.isalpha(): 
                        is_valid = True
                        
                if is_valid:
                    av_sentiment = ts.get("ticker_sentiment_label", "Neutral")
                    
                    event = MarketEvent(
                        event_type="RAW_NEWS",
                        symbol=av_symbol, # Use the AV symbol directly as it matches our DB
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
             # 1. Try SEC (Official, Free, Comprehensive)
             try:
                 logger.info("MarketStream: Fetching official ticker list from SEC...")
                 sec_tickers = await self._fetch_sec_tickers()
                 if sec_tickers:
                     logger.info(f"Fetched {len(sec_tickers)} tickers from SEC. Saving...")
                     self.db.add_tickers(sec_tickers)
                     self.full_universe = self.db.get_tickers()
                     self.db.set_setting("last_universe_update", str(time.time()))
                     
                     # EMIT EVENT TO REFRESH UI
                     self._emit(MarketEvent("UNIVERSE_UPDATE", "SYSTEM", self.full_universe, time.time()))
                     return # Success!
             except Exception as e:
                 logger.error(f"SEC Fetch failed: {e}")

             # 2. Fallback to Alpha Vantage (Backup)
             if self.av_client:
                 try:
                     # FETCH ALL US LISTINGS
                     all_tickers = await self.av_client.fetch_listing_status()
                     if all_tickers:
                        logger.info(f"Fetched {len(all_tickers)} tickers from AV. Saving...")
                        self.db.add_tickers(all_tickers)
                        self.full_universe = self.db.get_tickers() # Reload
                        self.db.set_setting("last_universe_update", str(time.time()))
                        
                        # EMIT EVENT TO REFRESH UI
                        self._emit(MarketEvent("UNIVERSE_UPDATE", "SYSTEM", self.full_universe, time.time()))
                 except Exception as e:
                     logger.error(f"Failed to update universe: {e}")

    async def _fetch_sec_tickers(self) -> List[str]:
        """Fetches all US public companies directly from the SEC."""
        url = "https://www.sec.gov/files/company_tickers.json"
        headers = {
            "User-Agent": "TradingCopilot/1.0 (contact@example.com)",
            "Accept-Encoding": "gzip, deflate",
            "Host": "www.sec.gov"
        }
        
        try:
             # Run in thread to avoid blocking loop
             response = await asyncio.to_thread(requests.get, url, headers=headers)
             response.raise_for_status()
             data = response.json()
             
             # SEC format is Dict[str, Dict] -> {"0": {"ticker": "AAPL", ...}}
             tickers = []
             for key in data:
                 entry = data[key]
                 if "ticker" in entry:
                     tickers.append(entry["ticker"].upper())
            
             return tickers
        except Exception as e:
             logger.error(f"SEC Download Error: {e}")
             return []

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
        
        # News Deduplication Table [NEW]
        # Stores hash of URL or Headline to prevent repeats across restarts
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS seen_news (
                id TEXT PRIMARY KEY,
                timestamp REAL
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

    def is_news_seen(self, news_id: str) -> bool:
        """Checks if news_id exists in DB."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute('SELECT 1 FROM seen_news WHERE id = ?', (news_id,))
        result = cursor.fetchone()
        conn.close()
        return result is not None

    def mark_news_seen(self, news_id: str):
        """Marks news_id as seen. Auto-cleans old entries."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        now = time.time()
        cursor.execute('INSERT OR IGNORE INTO seen_news (id, timestamp) VALUES (?, ?)', (news_id, now))
        
        # Cleanup old news (older than 24h) roughly 10% of the time to save performance
        if random.random() > 0.9:
             cursor.execute('DELETE FROM seen_news WHERE timestamp < ?', (now - 86400,))
        
        conn.commit()
        conn.close()

class AlphaVantageClient:
    """
    Alpha Vantage API Client for News & Sentiment.
    """
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://www.alphavantage.co/query"

    async def fetch_news(self, tickers: List[str] = None) -> List[Dict]:
        """Fetches latest news. If tickers is None, fetches GLOBAL market news."""
        if not self.api_key:
            return []
            
        params = {
            "function": "NEWS_SENTIMENT",
            "apikey": self.api_key,
            "sort": "LATEST",
            "limit": 50
        }
        
        if tickers:
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
            
            params["tickers"] = ",".join(av_tickers)
        
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
