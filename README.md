# Trading Co-Pilot (Project Onyx)

A high-performance, aesthetically "world-class" trading assistant built with Python and PyQt6.

![Status](https://img.shields.io/badge/Status-Active-brightgreen)
![Python](https://img.shields.io/badge/Python-3.9%2B-blue)
![License](https://img.shields.io/badge/License-MIT-purple)

## 🌟 Key Features

### 1. The "Onyx" Design System
-   **Glassmorphism v2**: Deep black (`#080808`) backgrounds with translucent layers.
-   **Neon Accents**: Cyber Cyan (`#00F0FF`) and Metallic Gold (`#D4AF37`) highlights.
-   **Zero Lag**: Custom-built "Staggered Animation Engine" ensures 60 FPS performance even during window resizing.

### 2. Adaptive Intelligence
-   **Context-Aware UI**: The interface breathes.
    -   *Standard Mode*: Shows Chart + Summary.
    -   *Reading Mode*: If no price history is relevant, the chart vanishes and the text area expands to fill the screen.
-   **Smart Summaries**: Aggregates news from Yahoo Finance, extracting full summaries instead of just headlines.

### 3. Cyber-Scanner
-   **Radar Loader**: A custom-painted hardware-accelerated widget that "scans" for market data with a glowing laser beam.
-   **Lazy Rendering**: Charts are plotted in the background (450ms delay) to prevent UI freezing during animations.

### 4. Zen Mode (Auto-Pilot)
-   **Rhythm Control**: Enforces a strict 15-second interval between alerts to prevent information overload.
-   **Queueing**: Bursts of market data are silently queued and presented one by one.

## 🛠️ Tech Stack
-   **Language**: Python 3.10+
-   **UI Framework**: PyQt6 (Custom Widgets, Property Animations)
-   **Charting**: PyQtGraph (High-performance rendering)
-   **Data**: yFinance (Real-time polling)
-   **Concurrency**: QAsync (Asyncio integration)

## 🚀 Getting Started

### Prerequisites
-   Python 3.9 or higher.

### Installation
1.  Clone the repository:
    ```bash
    git clone https://github.com/your-username/trading-copilot.git
    cd trading-copilot
    ```

2.  Install dependencies:
    ```bash
    pip install -r requirements.txt
    ```

### Running the App
Double-click `run.bat` or execute:
```bash
python main.py
```

## 🎮 Controls
-   **Drag**: Click anywhere to move the floating dock.
-   **Click**: Collapse/Expand the dock.
-   **Manual Mode**: Toggle to `MANUAL` to browse alerts at your own pace.
-   **Next**: Click the dock (or the Pulse dot) to advance the alert queue.

---
*Built with ❤️ by Antigravity*
