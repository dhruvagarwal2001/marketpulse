import sys
import asyncio
import signal
from PyQt6.QtWidgets import QApplication
from qasync import QEventLoop

from src.backend import MarketStream, LocalBrain
from src.ui import SmartDock
from src.controller import Controller

def main():
    # 1. Setup Environment
    app = QApplication(sys.argv)
    loop = QEventLoop(app)
    asyncio.set_event_loop(loop)

    # 2. Initialize Components
    
    # Backend
    brain = LocalBrain() # Ensures DB exists
    market = MarketStream(brain)
    
    # UI
    dock = SmartDock()
    dock.show() # Shows the initial "Pulse" state
    
    # Controller (The Bridge)
    controller = Controller(market, dock, brain)

    # 3. Define Shutdown Logic
    def close_app():
        market.stop()
        loop.stop()
        print("Application closed.")

    app.aboutToQuit.connect(close_app)

    # 4. Start Everything
    with loop:
        # Schedule the market stream to start immediately
        asyncio.ensure_future(market.start())
        
        # Run the Qt Event Loop via qasync
        loop.run_forever()

if __name__ == "__main__":
    # Handle Ctrl+C gracefully
    signal.signal(signal.SIGINT, signal.SIG_DFL)
    main()
