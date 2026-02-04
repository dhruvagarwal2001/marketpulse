from PyQt6.QtCore import QObject, pyqtSignal, QTimer
import asyncio
from collections import deque
from .backend import MarketStream
from .analysis import TechnicalAnalyst
from .intelligence import NewsAggregator, FundamentalAnalyst, FundamentalData
from .agent import TraderAgent
import logging
import re
import os
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

class Controller(QObject):
    """
    Business Logic Layer.
    Orchestrates Data -> Intelligence -> Presentation (UI).
    """
    
    # Signal to update the UI
    show_alert = pyqtSignal(str, str, str, object, list, str, str, str)
    # Signal to update Queue Counter in UI
    queue_updated = pyqtSignal(int)

    def __init__(self, market_stream, ui, db):
        super().__init__()
        self.market_stream = market_stream
        self.ui = ui
        self.db = db
        
        # Intelligence
        self.tech_analyst = TechnicalAnalyst()
        # [MODIFIED] Threshold 1: We trust the Backend Global Deduplication to filter repeats.
        # This ensures the FIRST verified source (Yahoo OR Alpha Vantage) triggers the Agent immediately.
        self.news_aggregator = NewsAggregator(consensus_threshold=1) 
        self.fund_analyst = FundamentalAnalyst()
        # [NEW] The Wolf with REAL EYES
        load_dotenv()
        api_key = os.getenv("GEMINI_API_KEY")
        self.agent = TraderAgent(api_key=api_key)
        
        # Flow Control
        self.alert_queue = deque()
        self.mode = "AUTO" # "AUTO" or "MANUAL"
        self.current_filter = "ALL"
        
        self.timer = QTimer()
        self.timer.setInterval(15000) # 15 Seconds
        self.timer.timeout.connect(self._on_timer_tick)
        self.timer.start()
        
        # Connect UI signals
        self.ui.action_triggered.connect(self.handle_user_action)
        self.ui.mode_toggled.connect(self.set_mode)
        self.ui.next_clicked.connect(self.force_next_alert)
        self.ui.filter_changed.connect(self.update_filter)
        self.ui.tickers_requested.connect(self.handle_bulk_tickers)
        self.ui.remove_requested.connect(self.handle_remove_ticker)
        
        # Connect internal signal to UI slot
        self.show_alert.connect(self.ui.expand)
        self.queue_updated.connect(self.ui.update_queue_count)

        # Init UI Data
        # Dropdown should show what we are monitoring
        self.ui.set_universe(self.market_stream.monitoring_universe)

        # Subscribe
        self.market_stream.subscribe(self.process_market_event)

    def update_filter(self, filter_text):
        """Updates the current active filter and requests data if needed."""
        self.current_filter = filter_text.upper() # Ensure uppercase
        
        # [NEW] Dynamic Tuning: If user selects a specific ticker, start monitoring it immediately
        if self.current_filter and self.current_filter != "ALL":
             # We need to schedule the async method from this sync slot
             # Check if we have a running loop
             try:
                 loop = asyncio.get_running_loop()
                 loop.create_task(self.market_stream.track_symbol(self.current_filter))
             except RuntimeError:
                 # Fallback if no loop (unlikely in qasync app)
                 logger.warning("No running event loop to schedule track_symbol")

        self.ui.update_queue_count(len(self.alert_queue)) # Just to refresh
        if self.mode == "AUTO":
             self._try_show_next()

    def set_mode(self, mode: str):
        """Switches between AUTO and MANUAL."""
        self.mode = mode
        logger.info(f"Switched to {mode} mode")
        if mode == "AUTO":
             self.timer.start()
             self._try_show_next() # Trigger immediately if stuff is waiting
        else:
             self.timer.stop()

    def handle_bulk_tickers(self, tickers):
        """Processes a list of tickers, validates them, and updates UI."""
        if not tickers:
            self._refresh_ui_watchlist()
            return
        loop = asyncio.get_event_loop()
        loop.create_task(self._process_tickers_async(tickers))

    async def _process_tickers_async(self, tickers):
        # Deduplicate and clean
        tickers = list(dict.fromkeys([t.upper().strip() for t in tickers if t.strip()]))
        
        # Parallel validation
        tasks = [self.market_stream.track_symbol(t) for t in tickers]
        results = await asyncio.gather(*tasks)
        
        valid_tickers = []
        invalid_tickers = []
        
        for t, success in zip(tickers, results):
            if success:
                valid_tickers.append(t)
            else:
                invalid_tickers.append(t)
        
        # Update UI with results
        if invalid_tickers:
            msg = f"Added {len(valid_tickers)}/{len(tickers)} tickers. ERROR: {', '.join(invalid_tickers)} not found."
            self.ui.update_ticker_status(msg, is_error=True)
        else:
            msg = f"Successfully added all {len(valid_tickers)} tickers to watchlist."
            self.ui.update_ticker_status(msg, is_error=False)
        
        self._refresh_ui_watchlist()

    def _refresh_ui_watchlist(self):
        """Calculates news counts and refreshes UI components."""
        # Calculate news counts in queue per ticker
        counts = {}
        for item in self.alert_queue:
            symbol = item['symbol']
            counts[symbol] = counts.get(symbol, 0) + 1
        
        # Update UI
        self.ui.update_manager_watchlist(self.market_stream.monitoring_universe, counts)

    def handle_remove_ticker(self, symbol):
        """Removes a ticker from monitoring."""
        loop = asyncio.get_event_loop()
        loop.create_task(self._remove_ticker_async(symbol))

    async def _remove_ticker_async(self, symbol):
        await self.market_stream.remove_symbol(symbol)
        self._refresh_ui_watchlist()

    def force_next_alert(self):
        """User clicked 'Next'."""
        self._try_show_next(force=True)

    def _on_timer_tick(self):
        """Called every 15s in AUTO mode."""
        
        # 1. Flush Stale News (Singles that didn't get corroboration)
        flushed_news = self.news_aggregator.flush(timeout=10) # 10s wait window
        if flushed_news:
            logger.info(f"Flushing {len(flushed_news)} single-source news items...")
            for verified_news in flushed_news:
                self._analyze_and_queue(verified_news, url=None) # URL lost in agg unless we track it better, fine for now.

        # 2. Show Alert
        if self.mode == "AUTO":
            self._try_show_next()

    def _try_show_next(self, force=False):
        """Pops the next alert if available and limits it."""
        if not self.alert_queue:
            return

        # Find the first item that matches the filter
        matched_item = None
        for item in self.alert_queue:
            if self._matches_filter(item['symbol']):
                matched_item = item
                break
        
        if not matched_item:
            return # Nothing matches current filter, wait.

        # Remove the specific item we found
        self.alert_queue.remove(matched_item)
        self.queue_updated.emit(len(self.alert_queue))
        
        # Emit to UI
        # Unpack the payload tuple from the wrapper
        self.show_alert.emit(*matched_item['payload'])
        
        # [NEW] Refresh news counts in manager if it's open
        self._refresh_ui_watchlist()
        
        # Reset Timer to ensure user gets full 15s to read
        if self.mode == "AUTO":
            self.timer.start(15000)

    async def process_market_event(self, event):
        """Ingests raw events, verifies them, and enqueues them."""
        
        # THROTTLE: If queue is full (3+), ignore new events to save resources
        if len(self.alert_queue) >= 5: # Increased buffer for filtering
            return
        
        alert_payload = None

        if event.event_type == "UNIVERSE_UPDATE":
             logger.info(f"Received Universe Update: {len(event.data)} tickers")
             # We keep the dropdown showing the monitored ones, but full_universe is updated in background
             return

        if event.event_type == "RAW_NEWS":
            verified_news = self.news_aggregator.process(
                source=event.data.get('source', 'Unknown'),
                symbol=event.symbol,
                headline=event.data['headline'],
                sentiment=event.data.get('sentiment', 'NEUTRAL'),
                summary=event.data.get('summary')
            )
            
            if verified_news:
                # Use common handler
                # Note: URL might be specific to the last event, but for aggregation we can just pass this one.
                self._analyze_and_queue(verified_news, url=event.data.get('url'))

        elif event.event_type == "FUNDAMENTALS":
            data = FundamentalData(**event.data)
            analysis = self.fund_analyst.analyze(data)
            
            history = self.market_stream.get_history(event.symbol)
            tech_signal = self.tech_analyst.analyze(history)
            verdict = f"{tech_signal.action} ({int(tech_signal.confidence*100)}%)"
            
            alert_payload = (
                f"📊 {event.symbol} EARNINGS",
                "Earnings PDF Parsed. Key Metrics below.",
                verdict,
                history,
                ["SEC Filings"],
                analysis['summary'], # Fix: Dictionary access
                None,
                "NORMAL" # Fundamentals treated as NORMAL for now, or calculate based on score
            )

            # ENQUEUE Fundamentals
            if alert_payload:
                item = {'symbol': event.symbol, 'payload': alert_payload}
                self.alert_queue.append(item)
                self.queue_updated.emit(len(self.alert_queue))

    def _analyze_and_queue(self, verified_news, url=None):
        """Common logic to analyze verified news (from event or flush) and enqueue it."""
        
        # Get Context
        history = self.market_stream.get_history(verified_news.symbol)
        
        # Check if history is valid/meaningful
        has_history = history is not None and not history.empty
        
        if has_history:
            tech_signal = self.tech_analyst.analyze(history)
            verdict = f"{tech_signal.action} ({int(tech_signal.confidence*100)}%)"
        else:
            verdict = "NEWS ONLY"
            history = None # Explicitly set to None to trigger Text-Only Mode in UI
        
        # [NEW] AGENT ANALYSIS CACHE CHECK
        import hashlib
        content_hash = hashlib.md5(f"{verified_news.symbol}|{verified_news.headline}".encode()).hexdigest()
        cached_analysis = self.db.get_analysis_cache(content_hash)
        
        if cached_analysis:
            logger.info(f"Using cached analysis for {verified_news.symbol}")
            # Map back to AgentResponse-like object or just use as dict
            from dataclasses import asdict
            agent_response = type('obj', (object,), cached_analysis)
        else:
            # Ask the Wolf to interpret the news
            agent_response = self.agent.analyze(
                symbol=verified_news.symbol,
                headline=verified_news.headline,
                summary=verified_news.summary,
                all_summaries=verified_news.all_summaries
            )
            # Cache it
            from dataclasses import asdict
            self.db.store_analysis_cache(content_hash, asdict(agent_response))
        
        # Construct Rich Description from Agent's perspective
        description = f"{agent_response.summary}<br><br>"
        
        # Format the reasoning with HTML colors using Regex for robustness
        reasoning_html = agent_response.reasoning.replace("\n", "<br>")
        
        # Highlight TRADER (Cyan)
        reasoning_html = re.sub(
            r"(⚡\s*TRADER(?:\s*\(.*?\))?:)", 
            r"<br><font color='#00F0FF'><b>\1</b></font>", 
            reasoning_html, 
            flags=re.IGNORECASE
        )
        
        # Highlight INVESTOR (Gold)
        reasoning_html = re.sub(
            r"(💎\s*INVESTOR(?:\s*\(.*?\))?:)", 
            r"<br><br><font color='#D4AF37'><b>\1</b></font>", 
            reasoning_html, 
            flags=re.IGNORECASE
        )
        
        description += f"<b>ANALYSIS LOADED:</b><br>{reasoning_html}"

        # Update Verdict with Agent's take
        verdict = f"{agent_response.action} ({int(agent_response.confidence*100)}%)"

        alert_payload = (
            str(agent_response.headline), # Use Agent's re-written slogan
            str(description),
            str(verdict),
            history,
            verified_news.sources,
            "", # Fundamentals (Empty String instead of None)
            url or "", # URL
            str(verified_news.impact) # Keep original impact flag
        )
        
        # ENQUEUE
        # Wrap in dict for filtering
        item = {'symbol': verified_news.symbol, 'payload': alert_payload}
        self.alert_queue.append(item)
        self.queue_updated.emit(len(self.alert_queue))
        
        # [NEW] Refresh news counts in manager if it's open
        self._refresh_ui_watchlist()

        # Urgent Trigger for first item
        if len(self.alert_queue) == 1 and self.mode == "AUTO" and not self.timer.isActive():
                self._try_show_next()
                self.timer.start(15000)


    def _matches_filter(self, symbol):
        if not self.current_filter or self.current_filter == "ALL":
            return True
        return symbol == self.current_filter

    def handle_user_action(self, action):
        logger.info(f"Action: {action}")
