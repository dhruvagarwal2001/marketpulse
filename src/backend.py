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
import json
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
        self.monitoring_universe = ["NVDA", "TSLA", "AAPL", "BTC-USD", "ETH-USD"]
        # [NEW] Priority Polling (High-Frequency)
        self.priority_universe = []
        # Full Available Universe (for UI Search)
        self.full_universe = []
        self._cache = {} # To track changes
        
        # Deduplication Cache (URL or Tuple of (Symbol, Title))
        self._news_dedup = set()
        
        # Alpha Vantage Integration
        load_dotenv()
        av_key = os.getenv("ALPHA_VANTAGE_API_KEY")
        self.av_client = AlphaVantageClient(av_key) if av_key else None

    async def track_symbol(self, symbol: str) -> bool:
        """Adds a symbol to the active monitoring loop and polls it immediately. Returns True if valid."""
        symbol = symbol.upper().strip()
        if not symbol: return False

        # 1. Check if already known valid
        is_known = symbol in self.full_universe
        
        # 2. If not known, try to verify with yfinance
        if not is_known:
             is_valid = await self.validate_ticker(symbol)
             if not is_valid:
                  logger.warning(f"MarketStream: {symbol} is not a valid ticker.")
                  return False
             # Add to full universe for future quick checks
             self.full_universe.append(symbol)
             if self.db: self.db.add_tickers([symbol])

        if symbol not in self.monitoring_universe:
            logger.info(f"MarketStream: Tracking new symbol {symbol}")
            self.monitoring_universe.append(symbol)
            # Persist to DB
            if self.db:
                self.db.set_setting("monitoring_universe", json.dumps(self.monitoring_universe))
            # Immediate Poll to give user instant feedback
            if self.running:
                # Run as task to not block the caller
                asyncio.create_task(self._poll_symbol(symbol))
        else:
             logger.info(f"MarketStream: Already tracking {symbol}")
        
        return True

    async def mark_priority(self, symbol: str) -> bool:
        """Adds symbol to priority list (limit 5)."""
        symbol = symbol.upper().strip()
        if symbol not in self.monitoring_universe:
            return False
        
        if symbol in self.priority_universe:
            return True
            
        if len(self.priority_universe) >= 5:
            return False
            
        self.priority_universe.append(symbol)
        if self.db:
            self.db.set_setting("priority_universe", json.dumps(self.priority_universe))
        logger.info(f"MarketStream: {symbol} marked as PRIORITY.")
        return True

    async def unmark_priority(self, symbol: str):
        """Removes symbol from priority list."""
        symbol = symbol.upper().strip()
        if symbol in self.priority_universe:
            self.priority_universe.remove(symbol)
            if self.db:
                self.db.set_setting("priority_universe", json.dumps(self.priority_universe))
            logger.info(f"MarketStream: {symbol} removed from priority.")

    async def remove_symbol(self, symbol: str):
        """Removes a symbol from monitoring."""
        symbol = symbol.upper().strip()
        if symbol in self.monitoring_universe:
            self.monitoring_universe.remove(symbol)
            if self.db:
                self.db.set_setting("monitoring_universe", json.dumps(self.monitoring_universe))
            logger.info(f"MarketStream: Stopped tracking {symbol}")

    async def validate_ticker(self, symbol: str) -> bool:
        """Robust ticker verification."""
        try:
            # Method 1: Try a tiny history download (Most reliable way to check if it actually has data)
            df = await asyncio.to_thread(yf.download, symbol, period="1d", interval="1m", progress=False)
            if not df.empty:
                return True
            
            # Method 2: Fallback to Ticker object attributes
            ticker = await asyncio.to_thread(yf.Ticker, symbol)
            # Try to see if it has ANY history at all (max period)
            info = await asyncio.to_thread(lambda: ticker.history(period="1d"))
            if not info.empty:
                return True
            
            # Method 3: SEC/Full Universe list fallback (if we already synced it)
            if symbol in self.full_universe:
                return True
                
            return False
        except Exception as e:
            logger.warning(f"Validation failed for {symbol}: {e}")
            return False

    def subscribe(self, callback):
        self._subscribers.append(callback)

    async def start(self):
        self.running = True
        logger.info("MarketStream: Connected to Real Markets")
        
        # [NEW] Sync Universe from DB/remote
        # This will populate self.full_universe (thousands) and self.monitoring_universe (subset)
        await self._sync_universe()
        
        # [MODIFIED] Load monitoring universe from DB if it exists
        stored_monitored = self.db.get_setting("monitoring_universe")
        if stored_monitored:
            try:
                self.monitoring_universe = json.loads(stored_monitored)
            except: pass
        
        # Load priority universe from DB
        stored_priority = self.db.get_setting("priority_universe")
        if stored_priority:
            try:
                self.priority_universe = json.loads(stored_priority)
            except: pass

        logger.info(f"MarketStream: DB contains {len(self.full_universe)} tickers. Monitoring {len(self.monitoring_universe)}. Priority {len(self.priority_universe)}.")
        
        # Run Polling Loops in Parallel
        # 1. Priority Loop (RAPID: Every 10s)
        # 2. Standard Loop (NORMAL: Every 45s)
        # 3. Alpha Vantage Loop (GLOBAL: Every 60s)
        await asyncio.gather(
            self._run_priority_loop(),
            self._run_yahoo_loop(),
            self._run_av_loop()
        )

    async def _run_priority_loop(self):
        """High-frequency polling for priority tickers."""
        while self.running:
            if not self.priority_universe:
                await asyncio.sleep(2)
                continue
            
            logger.debug(f"Priority Scan: {self.priority_universe}")
            tasks = [self._poll_symbol(s, is_priority=True) for s in self.priority_universe]
            await asyncio.gather(*tasks)
            
            await asyncio.sleep(10) # 10s frequency for priority news

    async def _run_yahoo_loop(self):
        """Polls active monitoring list (excluding priority) at normal pace."""
        while self.running:
            # Only poll symbols NOT in priority
            standard_list = [s for s in self.monitoring_universe if s not in self.priority_universe]
            
            if not standard_list:
                 await asyncio.sleep(5)
                 continue
                 
            # Rapid poll of standard list but with longer sleep between cycles
            tasks = [self._poll_symbol(s) for s in standard_list]
            await asyncio.gather(*tasks)
            
            await asyncio.sleep(45) # 45s wait for standard

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
        """Fetches history with local caching and incremental updates."""
        try:
            # 1. Load from Cache (Last 2 months)
            two_months_ago = (pd.Timestamp.now() - pd.Timedelta(days=60)).isoformat()
            cached_df = self.db.get_price_history(symbol, start_time=two_months_ago)
            
            # 2. Check if we need more
            last_ts = self.db.get_last_price_timestamp(symbol)
            needs_update = True
            if last_ts and (pd.Timestamp.now(tz=last_ts.tz) - last_ts < pd.Timedelta(minutes=30)):
                needs_update = False
            
            if needs_update:
                logger.info(f"Fetching incremental history for {symbol}...")
                # If we have data, fetch from last_ts, else fetch 60d
                period = "6d" if last_ts else "60d"
                interval = "5m"
                
                # Fetch fresh from Yahoo
                new_df = yf.download(symbol, period=period, interval=interval, progress=False, auto_adjust=True)
                if not new_df.empty:
                    new_df = new_df.rename(columns={"Close": "price"})
                    # Store in DB
                    self.db.store_prices(symbol, new_df)
                    # Refresh Cache
                    cached_df = self.db.get_price_history(symbol, start_time=two_months_ago)
            
            return cached_df
        except Exception as e:
            logger.error(f"Failed to fetch history for {symbol}: {e}")
            return pd.DataFrame()

    async def _poll_symbol(self, symbol: str, is_priority: bool = False):
        """Fetches live data for a single symbol."""
        try:
            # logger.info(f"Polling {symbol}...") # Verbose debug
            
            # Run blocking yfinance call in a separate thread
            ticker = await asyncio.to_thread(yf.Ticker, symbol)
            
            # 1. Get Fast Info (Price)
            price = None
            try:
                price = ticker.fast_info.last_price
            except Exception as e:
                # Silently fallback for multi-tasking stability
                pass
            
            if price is not None:
                self._cache[symbol] = price
            
            # 2. Get News
            news_list = await asyncio.to_thread(lambda: ticker.news)
            
            if news_list:
                # Priority: check more news items, Standard: check top 2
                limit = 5 if is_priority else 2
                for latest in news_list[:limit]:
                    content = latest.get('content', latest) 
                    headline = content.get('title') or content.get('headline', 'No Headline')
                    publisher = content.get('publisher', 'Unknown')
                    url = content.get('canonicalUrl', {}).get('url', '') if isinstance(content.get('canonicalUrl'), dict) else content.get('link', '')
                    
                    if headline == "No Headline" or not url: continue
                    
                    dedup_key = url
                    if dedup_key in self._news_dedup: continue
                    if self.db and self.db.is_news_seen(dedup_key): continue
                         
                    self._news_dedup.add(dedup_key)
                    if self.db: self.db.mark_news_seen(dedup_key)
                    
                    logger.info(f"{'🚨 PRIORITY' if is_priority else 'New'} News for {symbol}: {headline}")

                    summary = latest.get('summary', '') 
                    if not summary and 'content' in latest and isinstance(latest['content'], dict):
                        summary = latest['content'].get('summary', '')

                    event = MarketEvent(
                        event_type="RAW_NEWS",
                        symbol=symbol,
                        data={
                            "source": f"{publisher} (via Yahoo)",
                            "headline": headline,
                            "summary": summary,
                            "sentiment": "NEUTRAL", 
                            "url": url,
                            "is_priority": is_priority
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
        
        try:
            # Pass None to get general market news
            news_feed = await self.av_client.fetch_news(tickers=None) 
        except Exception as e:
            logger.error(f"Alpha Vantage Polling error: {e}")
            return
        
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
        
        # Tickers table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS tickers (
                symbol TEXT PRIMARY KEY
            )
        ''')
        
        # News Deduplication Table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS seen_news (
                id TEXT PRIMARY KEY,
                timestamp REAL
            )
        ''')

        # [NEW] Price History Table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS price_history (
                symbol TEXT,
                timestamp DATETIME,
                price REAL,
                PRIMARY KEY (symbol, timestamp)
            )
        ''')

        # [NEW] Analysis Cache Table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS analysis_cache (
                content_hash TEXT PRIMARY KEY,
                agent_response JSON,
                timestamp REAL
            )
        ''')
        
        conn.commit()
        conn.close()

    def store_prices(self, symbol: str, df: pd.DataFrame):
        """Stores price history in batches."""
        if df.empty: return
        conn = sqlite3.connect(self.db_path)
        try:
            # Flatten columns if multi-index (yfinance sometimes does this)
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)
            
            # Prepare data
            data = []
            for ts, row in df.iterrows():
                data.append((symbol, ts.isoformat(), float(row['price'])))
            
            cursor = conn.cursor()
            cursor.executemany('INSERT OR REPLACE INTO price_history (symbol, timestamp, price) VALUES (?, ?, ?)', data)
            conn.commit()
        except Exception as e:
            logger.error(f"DB Error storing prices for {symbol}: {e}")
        finally:
            conn.close()

    def get_price_history(self, symbol: str, start_time: Optional[str] = None) -> pd.DataFrame:
        """Retrieves price history from DB."""
        conn = sqlite3.connect(self.db_path)
        try:
            query = 'SELECT timestamp, price FROM price_history WHERE symbol = ?'
            params = [symbol]
            if start_time:
                query += ' AND timestamp >= ?'
                params.append(start_time)
            query += ' ORDER BY timestamp ASC'
            
            df = pd.read_sql_query(query, conn, index_col='timestamp', params=params)
            if not df.empty:
                df.index = pd.to_datetime(df.index)
            return df
        except Exception as e:
            logger.error(f"DB Error fetching prices for {symbol}: {e}")
            return pd.DataFrame()
        finally:
            conn.close()

    def get_last_price_timestamp(self, symbol: str) -> Optional[pd.Timestamp]:
        """Gets the most recent timestamp we have for a symbol."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute('SELECT MAX(timestamp) FROM price_history WHERE symbol = ?', (symbol,))
        row = cursor.fetchone()
        conn.close()
        return pd.Timestamp(row[0]) if row and row[0] else None

    def get_analysis_cache(self, content_hash: str) -> Optional[Dict]:
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute('SELECT agent_response FROM analysis_cache WHERE content_hash = ?', (content_hash,))
        row = cursor.fetchone()
        conn.close()
        if row:
            import json
            return json.loads(row[0])
        return None

    def store_analysis_cache(self, content_hash: str, response: Dict):
        conn = sqlite3.connect(self.db_path)
        import json
        cursor = conn.cursor()
        cursor.execute('INSERT OR REPLACE INTO analysis_cache (content_hash, agent_response, timestamp) VALUES (?, ?, ?)',
                     (content_hash, json.dumps(response), time.time()))
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
