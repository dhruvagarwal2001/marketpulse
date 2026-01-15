from dataclasses import dataclass
from typing import List, Dict, Optional
import time

@dataclass
class VerifiedNews:
    symbol: str
    headline: str
    sources: List[str]
    sentiment: str
    timestamp: float
    summary: Optional[str] = None

class NewsAggregator:
    """
    "The Truth Engine".
    Buffers raw news feeds and only emits if consensus is reached.
    """
    def __init__(self, consensus_threshold: int = 2):
        self.threshold = consensus_threshold
        self._buffer: Dict[str, List[Dict]] = {} # Key: Symbol, Value: List of news items
        self.ttl = 30  # Seconds to wait for corroboration

    def process(self, source: str, symbol: str, headline: str, sentiment: str, summary: str = None) -> Optional[VerifiedNews]:
        """
        Ingests a raw news item. Returns VerifiedNews if consensus is reached, else None.
        """
        now = time.time()
        
        # Initialize buffer for symbol
        if symbol not in self._buffer:
            self._buffer[symbol] = []
        
        # Clean buffer of old news
        self._buffer[symbol] = [n for n in self._buffer[symbol] if now - n['time'] < self.ttl]
        
        # Add new item
        # Check if this source already reported (deduplication)
        if any(n['source'] == source for n in self._buffer[symbol]):
             return None # Ignore duplicate from same source

        self._buffer[symbol].append({
            'source': source,
            'headline': headline,
            'sentiment': sentiment,
            'summary': summary,
            'time': now
        })
        
        # Check Consensus
        count = len(self._buffer[symbol])
        if count >= self.threshold:
            # Consensus Reached!
            sources = [n['source'] for n in self._buffer[symbol]]
            # Prefer the longest summary available
            summaries = [n['summary'] for n in self._buffer[symbol] if n['summary']]
            best_summary = max(summaries, key=len) if summaries else None
            
            # Clear buffer to avoid re-triggering for the same event immediately
            self._buffer[symbol] = [] 
            
            return VerifiedNews(
                symbol=symbol,
                headline=headline, # Use the latest headline
                sources=sources,
                sentiment=sentiment,
                timestamp=now,
                summary=best_summary
            )
        
        return None

@dataclass
class FundamentalData:
    revenue_growth: float # %
    net_margin: float # %
    debt_to_equity: float
    guidance: str # "RAISED", "LOWERED", "MAINTAINED"

class FundamentalAnalyst:
    """
    Simulates parsing Earnings PDFs and analyzing fundamentals.
    """
    
    def analyze(self, data: FundamentalData) -> Dict[str, any]:
        """
        Returns a score (0-100) and a summary.
        """
        score = 50
        reasons = []
        
        # Growth
        if data.revenue_growth > 0.10:
            score += 20
            reasons.append("High Growth")
        elif data.revenue_growth < 0:
            score -= 20
            reasons.append("Revenue Shrinking")
            
        # Profitability
        if data.net_margin > 0.15:
            score += 15
            reasons.append("High Margins")
        
        # Health
        if data.debt_to_equity > 2.0:
            score -= 15
            reasons.append("High Debt")
            
        # Guidance (Critical)
        if data.guidance == "RAISED":
            score += 25
            reasons.append("Guidance Raised")
        elif data.guidance == "LOWERED":
            score -= 30
            reasons.append("Guidance Lowered !!")
            
        return {
            "score": max(0, min(100, score)),
            "summary": ", ".join(reasons)
        }
