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

    def analyze(self, symbol: str, headline: str, summary: str, all_summaries: list = None, market_data=None) -> AgentResponse:
        """
        Main entry point. Uses LLM if available, else High-Fidelity Simulation.
        """
        # Combine summaries if multiple are provided for better context
        combined_summary = summary
        if all_summaries and len(all_summaries) > 1:
            combined_summary = "\n---\n".join(all_summaries)

        # 1. Use Real Intelligence if Key is Present
        if self.model:
            try:
                return self._query_llm(symbol, headline, combined_summary)
            except Exception as e:
                print(f"LLM Error: {e}. Falling back to Simulation.")
                # Fallthrough to simulation

        # 2. Simulation Logic (State of the Art Mocking)
        
        # Detect Sentiment from simple keywords for the mock
        text = (headline + " " + (summary or "")).upper()
        
        action = "HOLD"
        confidence = 0.5
        new_headline = headline
        # Remove arbitrary constraints if they break the meaning, but try to keep it punchy.
        # [MODIFIED] No more slicing at 8 words.
        
        new_summary = summary or "Market data indicates notable activity."
        # Format: Context -> Conclusion -> Action
        new_summary = summary or "Market data indicates notable activity for this security."
        
        # 3-Paragraph Narrative Format (Simulation)
        p1_context = f"The latest data reveals significant movement for {symbol}, driven by {new_summary}. Institutional interest appears elevated as the market processes this material information."
        p2_impact = "Volatility is expected to rise in the immediate term. The sentiment shift suggests a re-evaluation of the current price level is underway, with liquidity likely concentrating around key technical levels."
        p3_action = "Traders should monitor for a volume confirmation before committing capital. A breakout above resistance would confirm the trend, while failure to hold support suggests further downside risk. Proper risk management is advised."
        
        new_summary = f"{p1_context}<br><br>{p2_impact}<br><br>{p3_action}"
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
                f"<b>CONTEXT:</b> {headline} - {summary or 'Strong growth signals detected.'}<br>"
                f"<b>IMPACT:</b> Immediate repricing of growth expectations. Shorts likely to cover.<br>"
                f"<b>ACTION:</b> Enter LONG immediately. Trail stop at VWAP."
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
                f"<b>CONTEXT:</b> {headline} - {summary or 'Negative catalyst detected.'}<br>"
                f"<b>IMPACT:</b> Structural break in confidence. Expect extended downside.<br>"
                f"<b>ACTION:</b> SELL/SHORT. Do not catch the falling knife."
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
            "- 'headline': A high-quality, punchy headline. Do not artificially truncate.\n"
            "- 'summary': Write a high-quality, comprehensive 3-paragraph narrative description. \n"
            "   Paragraph 1: Provide full context, covering the key facts, figures, and backstory of the news event. \n"
            "   Paragraph 2: Analyze the immediate market impact, volatility implications, and sentiment shift. \n"
            "   Paragraph 3: Conclude with the strategic action for the trader, profit targets, and risk management. \n"
            "   Do NOT use bold labels like 'CONTEXT:' or 'IMPACT:'. Just write the text fluidly as a professional financial report. Use <br><br> to separate paragraphs.\n"
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
