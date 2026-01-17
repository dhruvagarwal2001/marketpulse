import random
from dataclasses import dataclass
from typing import Optional, Dict
import threading
import time
import json
import re

import google.generativeai as genai
from google.api_core import retry

@dataclass
class AgentResponse:
    headline: str
    summary: str
    action: str # BUY, SELL, HOLD, URGENT SELL, AGGRESSIVE BUY
    confidence: float # 0.0 to 1.0
    reasoning: str

class TraderAgent:
    """
    The 'Wolf of Wall Street' AI Agent.
    Analyses news with a ruthlessly profit-focused persona.
    """
    def __init__(self, api_key: str = None):
        self.api_key = api_key
        self.model = None
        if self.api_key:
            try:
                genai.configure(api_key=self.api_key)
                self.model = genai.GenerativeModel('gemini-2.0-flash-lite',
                    generation_config={"response_mime_type": "application/json"})
            except Exception as e:
                print(f"Failed to init Gemini: {e}")

        # Persona settings
        self.role = "Hedge Fund Manager"
        self.style = "Aggressive, Cynical, Profit-Driven"

    def analyze(self, symbol: str, headline: str, summary: str, market_data=None) -> AgentResponse:
        """
        Main entry point. Uses LLM if available, else High-Fidelity Simulation.
        """
        # 1. Use Real Intelligence if Key is Present
        if self.model:
            try:
                return self._query_llm(symbol, headline, summary)
            except Exception as e:
                print(f"LLM Error: {e}. Falling back to Simulation.")
                # Fallthrough to simulation

        # 2. Simulation Logic (State of the Art Mocking)
        
        # Detect Sentiment from simple keywords for the mock
        text = (headline + " " + (summary or "")).upper()
        
        action = "HOLD"
        confidence = 0.5
        new_headline = headline
        # Enforce max 10 words constraint (approx) for consistency
        words = headline.split()
        if len(words) > 8:
            new_headline = " ".join(words[:8]) + "..."
            
        new_summary = summary or "Market data indicates significant movement. Review details below."
        reasoning = "Unclear signal. \n\nTRADER (1 Day): Wait for volume confirmation. \nINVESTOR (2+ Yrs): No thesis change."
        
        # Bullish Keywords
        if any(x in text for x in ["ACQUISITION", "SURGE", "RECORD", "BEATS", "GROWTH", "AI", "PARTNERSHIP"]):
            action = "AGGRESSIVE BUY"
            confidence = random.uniform(0.85, 0.99)
            new_headline = f"🚀 {symbol}: {new_headline}"
            
            reasoning = (
                "Bullish Catalyst Confirmed.\n\n"
                "⚡ TRADER (1 Day): MOMENTUM PLAY. Heavy institutional inflows detected. Target intra-day highs.\n"
                "💎 INVESTOR (2+ Yrs): ACCUMULATE. This solidifies the long-term growth thesis and expanding TAM."
            )
            
            new_summary = (
                f"MAJOR MOVER: {headline}\n\n"
                f"Why it matters: The stock is surging on positive news ({summary or 'Strong growth signals'}). "
                "This typically attracts more buyers and drives the price up."
            )

        # Bearish Keywords
        elif any(x in text for x in ["BANKRUPTCY", "CRASH", "HALT", "LOWERED", "MISSES", "LAWSUIT", "FRAUD"]):
            action = "URGENT SELL"
            confidence = random.uniform(0.90, 1.0)
            new_headline = f"🩸 {symbol}: {new_headline}"
            
            reasoning = (
                "Structural Damage Detected.\n\n"
                "⚡ TRADER (1 Day): SHORT/EXIT. Momentum is broken. Expect panic selling to continue.\n"
                "💎 INVESTOR (2+ Yrs): RE-EVALUATE. Fundamentals are deteriorating. Reduce exposure to preserve capital."
            )
            
            new_summary = (
                f"WARNING: {headline}\n\n"
                f"Why it matters: Negative developments ({summary or 'Risk factors detected'}) are shaking investor confidence. "
                "The stock could see further downside."
            )

        return AgentResponse(
            headline=new_headline,
            summary=new_summary,
            action=action,
            confidence=confidence,
            reasoning=reasoning
        )

    def _query_llm(self, symbol: str, headline: str, summary: str) -> AgentResponse:
        """Call Gemini API."""
        
        system_prompt = (
            "You are an Elite Financial Intelligence Agent (The Wolf). "
            "You have the instincts of a ruthless Day Trader and the wisdom of a Warren Buffett-level Investor.\n"
            "Your goal is to maximize profit. You do not hedge your words.\n"
            "Response MUST be a valid JSON object."
        )
        
        user_prompt = (
            f"{system_prompt}\n\n"
            f"Analyze this news for ticker {symbol}.\n"
            f"Headline: {headline}\n"
            f"Summary: {summary}\n\n"
            "Provide a JSON response with these keys:\n"
            "- 'headline': A simple, engaging title for a layman. Max 10 words. Contextual and clear.\n"
            "- 'summary': A contextual explanation for a layman (explain 'why it matters'). Max 150 words. Do NOT be technical.\n"
            "- 'action': One of [AGGRESSIVE BUY, BUY, HOLD, SELL, URGENT SELL].\n"
            "- 'confidence': A float between 0.0 and 1.0.\n"
            "- 'reasoning': A dual-perspective analysis. YOU MUST USE THE EXACT FORMAT BELOW (including emojis):\n"
            "   '⚡ TRADER (1 Day): [Your 1-day actionable view]'\n"
            "   '💎 INVESTOR (2+ Yrs): [Your long-term thesis view]'"
        )

        try:
            response = self.model.generate_content(user_prompt)
            # Clean up the response text in case it contains markdown formatting
            text = response.text.strip()
            if text.startswith("```json"):
                text = text[7:-3].strip()
            elif text.startswith("```"):
                text = text[3:-3].strip()
                
            data = json.loads(text)
            
            return AgentResponse(
                headline=data.get('headline', headline),
                summary=data.get('summary', summary),
                action=data.get('action', 'HOLD').upper(),
                confidence=float(data.get('confidence', 0.5)),
                reasoning=data.get('reasoning', "Analysis unavailable.")
            )
        except Exception as e:
            print(f"Gemini Generation Error: {e}")
            raise e
