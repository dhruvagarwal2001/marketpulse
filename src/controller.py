from PyQt6.QtCore import QObject, pyqtSignal, QTimer
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
        self.news_aggregator = NewsAggregator(consensus_threshold=1) 
        self.fund_analyst = FundamentalAnalyst()
        # [NEW] The Wolf with REAL EYES
        load_dotenv()
        api_key = os.getenv("GEMINI_API_KEY")
        self.agent = TraderAgent(api_key=api_key)
        
        # Flow Control
        self.alert_queue = deque()
        self.mode = "AUTO" # "AUTO" or "MANUAL"
        
        self.timer = QTimer()
        self.timer.setInterval(15000) # 15 Seconds
        self.timer.timeout.connect(self._on_timer_tick)
        self.timer.start()
        
        # Connect UI signals
        self.ui.action_triggered.connect(self.handle_user_action)
        self.ui.mode_toggled.connect(self.set_mode)
        self.ui.next_clicked.connect(self.force_next_alert)
        
        # Connect internal signal to UI slot
        self.show_alert.connect(self.ui.expand)
        self.queue_updated.connect(self.ui.update_queue_count)

        # Subscribe
        self.market_stream.subscribe(self.process_market_event)

    def set_mode(self, mode: str):
        """Switches between AUTO and MANUAL."""
        self.mode = mode
        logger.info(f"Switched to {mode} mode")
        if mode == "AUTO":
             self.timer.start()
             self._try_show_next() # Trigger immediately if stuff is waiting
        else:
             self.timer.stop()

    def force_next_alert(self):
        """User clicked 'Next'."""
        self._try_show_next(force=True)

    def _on_timer_tick(self):
        """Called every 15s in AUTO mode."""
        if self.mode == "AUTO":
            self._try_show_next()

    def _try_show_next(self, force=False):
        """Pops the next alert if available and emits it."""
        if not self.alert_queue:
            return

        # Prepare payload
        alert_payload = self.alert_queue.popleft() # Renamed to avoid confusion
        self.queue_updated.emit(len(self.alert_queue))
        
        # Emit to UI
        self.show_alert.emit(*alert_payload)
        
        # Reset Timer to ensure user gets full 15s to read
        if self.mode == "AUTO":
            self.timer.start(15000)

    async def process_market_event(self, event):
        """Ingests raw events, verifies them, and enqueues them."""
        
        # THROTTLE: If queue is full (3+), ignore new events to save resources
        if len(self.alert_queue) >= 3:
            return
        
        alert_payload = None

        if event.event_type == "RAW_NEWS":
            verified_news = self.news_aggregator.process(
                source=event.data.get('source', 'Unknown'),
                symbol=event.symbol,
                headline=event.data['headline'],
                sentiment=event.data.get('sentiment', 'NEUTRAL'),
                summary=event.data.get('summary')
            )
            
            if verified_news:
                # Get Context
                history = self.market_stream.get_history(event.symbol)
                
                # Check if history is valid/meaningful
                has_history = history is not None and not history.empty
                
                if has_history:
                    tech_signal = self.tech_analyst.analyze(history)
                    verdict = f"{tech_signal.action} ({int(tech_signal.confidence*100)}%)"
                else:
                    verdict = "NEWS ONLY"
                    history = None # Explicitly set to None to trigger Text-Only Mode in UI
                
                # [NEW] AGENT ANALYSIS
                # Ask the Wolf to interpret the news
                agent_response = self.agent.analyze(
                    symbol=event.symbol,
                    headline=verified_news.headline,
                    summary=verified_news.summary
                )
                
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
                    event.data.get('url') or "", # URL (Empty String instead of None)
                    str(verified_news.impact) # Keep original impact flag
                )

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
            
        # ENQUEUE
        # ENQUEUE
        if alert_payload:
            self.alert_queue.append(alert_payload)
            self.queue_updated.emit(len(self.alert_queue))
            
            # If queue was empty and we are in Auto, show immediately? 
            # Design Choice: Wait for timer to keep rhythm.
            # EXCEPTION: If the user is staring at a blank screen (first load), trigger once.
            if len(self.alert_queue) == 1 and self.mode == "AUTO" and not self.timer.isActive():
                 self._try_show_next()
                 self.timer.start(15000)

    def handle_user_action(self, action):
        logger.info(f"Action: {action}")
