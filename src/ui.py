from PyQt6.QtWidgets import (QWidget, QVBoxLayout, QHBoxLayout, QLabel, 
                             QPushButton, QApplication, QFrame, QGraphicsOpacityEffect, QSizePolicy, QScrollArea, QSizeGrip, QComboBox, QLineEdit, QTextEdit, QDialog)
from PyQt6.QtCore import Qt, pyqtSignal, QSize, QPropertyAnimation, QEasingCurve, QRect, QPoint, QSequentialAnimationGroup, QTimer
from PyQt6.QtGui import QColor, QFont, QCursor, QPainter, QPen, QBrush, QLinearGradient, QIcon
import pyqtgraph as pg
import pyqtgraph as pg

class RadarLoader(QWidget):
    """
    High-Performance 'Cyber-Scanner' Animation.
    """
    def __init__(self, parent=None):
        super().__init__(parent)
        self.scan_x = 0
        self.timer = QTimer(self)
        self.timer.timeout.connect(self._animate)
        self.timer.setInterval(16) # ~60 FPS
        self.setMinimumHeight(150)
        
        # Force Opaque Rendering
        self.setAttribute(Qt.WidgetAttribute.WA_OpaquePaintEvent)
        self.setAutoFillBackground(True)
        p = self.palette()
        p.setColor(self.backgroundRole(), QColor("#080808"))
        self.setPalette(p)
        
        self.hide()

    def start(self):
        self.scan_x = 0
        self.show()
        self.timer.start()

    def stop(self):
        self.timer.stop()
        self.hide()

    def _animate(self):
        self.scan_x += 12 # Speed of scan
        if self.scan_x > self.width() + 50:
             self.scan_x = -50
        self.update()

    def paintEvent(self, event):
        painter = QPainter(self)
        painter.setRenderHint(QPainter.RenderHint.Antialiasing)
        
        # 0. Fill Background (Fix Transparency Glitch)
        painter.fillRect(self.rect(), QColor("#080808"))
        
        # 1. Draw Tech Grid
        pen = QPen(QColor(0, 240, 255, 20)) # Cyan very low opacity
        pen.setWidth(1)
        painter.setPen(pen)
        
        step_x = 40
        step_y = 40
        
        # Vertical
        for x in range(0, self.width(), step_x):
            painter.drawLine(x, 0, x, self.height())
            
        # Horizontal
        for y in range(0, self.height(), step_y):
            painter.drawLine(0, y, self.width(), y)
            
        # 2. Draw Scan Beam (Gradient)
        # Gradient from Right (Bright) to Left (Transparent)
        grad = QLinearGradient(self.scan_x, 0, self.scan_x - 100, 0)
        grad.setColorAt(0.0, QColor(0, 240, 255, 0))
        grad.setColorAt(0.8, QColor(0, 240, 255, 150)) # Bright Leading Edge
        grad.setColorAt(1.0, QColor(0, 240, 255, 0))
        
        painter.fillRect(0, 0, self.width(), self.height(), grad)
        
        # 3. Draw "Scanning" Text overlay
        painter.setPen(QColor(0, 240, 255, 200))
        painter.setFont(QFont("Consolas", 10, QFont.Weight.Bold))
        painter.drawText(self.rect(), Qt.AlignmentFlag.AlignCenter, "SEARCHING MARKET DATA...")

class TickerManager(QDialog):
    """
    Premium Overlay for adding/validating tickers.
    Now shows the current watchlist.
    """
    tickers_added = pyqtSignal(list) 
    remove_requested = pyqtSignal(str)
    toggle_priority = pyqtSignal(str)

    def __init__(self, parent=None, current_watchlist=None):
        super().__init__(parent)
        self.setWindowFlags(Qt.WindowType.FramelessWindowHint | Qt.WindowType.WindowStaysOnTopHint | Qt.WindowType.Dialog)
        self.setAttribute(Qt.WidgetAttribute.WA_TranslucentBackground)
        self.watchlist = current_watchlist or []
        self.priority_list = []
        self._init_ui()
        self.setFixedSize(400, 600)
        
    def _init_ui(self):
        layout = QVBoxLayout()
        layout.setContentsMargins(10, 10, 10, 10)
        self.setLayout(layout)
        
        container = QFrame()
        container.setObjectName("ManagerContainer")
        container.setStyleSheet("""
            #ManagerContainer {
                background-color: #0F0F12;
                border: 1px solid #333344;
                border-radius: 16px;
            }
            QLabel { color: #AAAAAA; font-size: 11px; font-weight: bold; }
            QTextEdit {
                background-color: #080808;
                border: 1px solid #222222;
                border-radius: 8px;
                color: #00F0FF;
                font-family: 'Consolas', monospace;
                padding: 10px;
                max-height: 80px;
            }
            QPushButton#AddBtn {
                background: qlineargradient(x1:0, y1:0, x2:1, y2:0, stop:0 #00F0FF, stop:1 #0099FF);
                color: #000000;
                border: none;
                padding: 10px;
                border-radius: 8px;
                font-weight: 800;
            }
            QPushButton#CloseBtn {
                background: transparent;
                color: #555566;
                font-size: 18px;
                font-weight: bold;
            }
            #WatchlistArea {
                background-color: #080808;
                border: 1px solid #222222;
                border-radius: 8px;
            }
            .TickerRow {
                background-color: #15151A;
                border: 1px solid #222222;
                border-radius: 4px;
                padding: 5px;
            }
            .RemoveBtn {
                color: #FF3333;
                background: transparent;
                font-weight: bold;
                border: none;
            }
        """)
        
        c_layout = QVBoxLayout()
        container.setLayout(c_layout)
        
        # Header
        h_layout = QHBoxLayout()
        title = QLabel("WATCHLIST MANAGER")
        title.setStyleSheet("color: #FFFFFF; font-size: 14px; letter-spacing: 1px;")
        close_btn = QPushButton("×")
        close_btn.setObjectName("CloseBtn")
        close_btn.setFixedSize(30, 30)
        close_btn.clicked.connect(self.close)
        h_layout.addWidget(title)
        h_layout.addStretch()
        h_layout.addWidget(close_btn)
        c_layout.addLayout(h_layout)
        
        # 1. Watchlist Section
        c_layout.addWidget(QLabel("CURRENTLY MONITORING:"))
        self.scroll = QScrollArea()
        self.scroll.setObjectName("WatchlistArea")
        self.scroll.setWidgetResizable(True)
        self.scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)
        
        self.watchlist_container = QWidget()
        self.watchlist_layout = QVBoxLayout(self.watchlist_container)
        self.watchlist_layout.setAlignment(Qt.AlignmentFlag.AlignTop)
        self.scroll.setWidget(self.watchlist_container)
        c_layout.addWidget(self.scroll)
        
        self.refresh_watchlist(self.watchlist)
        
        # 2. Add Section
        c_layout.addSpacing(10)
        c_layout.addWidget(QLabel("ADD NEW TICKERS:"))
        self.input_area = QTextEdit()
        self.input_area.setPlaceholderText("NVDA, TSLA, BTC-USD")
        c_layout.addWidget(self.input_area)
        
        self.status_label = QLabel("")
        self.status_label.setWordWrap(True)
        self.status_label.setStyleSheet("color: #00FF99;")
        c_layout.addWidget(self.status_label)
        
        self.add_btn = QPushButton("VERIFY & TRACK")
        self.add_btn.setObjectName("AddBtn")
        self.add_btn.setCursor(QCursor(Qt.CursorShape.PointingHandCursor))
        self.add_btn.clicked.connect(self._on_add_clicked)
        c_layout.addWidget(self.add_btn)
        
        layout.addWidget(container)

    def refresh_watchlist(self, tickers, alert_counts=None, priority_list=None):
        self.watchlist = sorted(tickers)
        alert_counts = alert_counts or {}
        self.priority_list = priority_list or []
        # Clear layout
        while self.watchlist_layout.count():
            item = self.watchlist_layout.takeAt(0)
            if item.widget():
                item.widget().deleteLater()
        
        for t in self.watchlist:
            row = QFrame()
            is_prio = t in self.priority_universe if hasattr(self, 'priority_universe') else False # Fallback
            is_prio = t in self.priority_list
            
            row.setStyleSheet(f"""
                background-color: {'#1A1A22' if is_prio else '#121217'}; 
                border-radius: 4px; 
                border: 1px solid {'#D4AF37' if is_prio else '#222222'};
            """)
            row_layout = QHBoxLayout(row)
            row_layout.setContentsMargins(10, 5, 10, 5)
            
            # [NEW] Priority Star Toggle
            prio_btn = QPushButton("★" if is_prio else "☆")
            prio_btn.setFixedSize(24, 24)
            prio_btn.setCursor(QCursor(Qt.CursorShape.PointingHandCursor))
            prio_btn.setStyleSheet(f"color: {'#D4AF37' if is_prio else '#555566'}; background: transparent; border: none; font-size: 16px;")
            prio_btn.setToolTip("Toggle High-Frequency Polling (RAPID)")
            prio_btn.clicked.connect(lambda checked, s=t: self.toggle_priority.emit(s))
            row_layout.addWidget(prio_btn)

            label = QLabel(t)
            label.setStyleSheet(f"color: {'#FFFFFF' if is_prio else '#00F0FF'}; font-family: 'Consolas'; font-size: 13px; font-weight: bold;")
            
            # News Status Badge
            count = alert_counts.get(t, 0)
            if count > 0:
                badge = QLabel(f"{count} NEWS")
                badge.setStyleSheet("""
                    background-color: rgba(0, 240, 255, 0.1);
                    color: #00F0FF;
                    border: 1px solid #00F0FF;
                    border-radius: 4px;
                    padding: 2px 6px;
                    font-size: 9px;
                    font-weight: 900;
                """)
                row_layout.addWidget(badge)
            
            row_layout.addSpacing(10)
            row_layout.addWidget(label)
            row_layout.addStretch()
            
            rem_btn = QPushButton("REMOVE")
            rem_btn.setObjectName("RemoveBtn")
            rem_btn.setStyleSheet("color: #FF3333; font-weight: bold; border: none; font-size: 10px;")
            rem_btn.setCursor(QCursor(Qt.CursorShape.PointingHandCursor))
            rem_btn.clicked.connect(lambda checked, symbol=t: self.remove_requested.emit(symbol))
            
            row_layout.addWidget(label)
            row_layout.addStretch()
            row_layout.addWidget(rem_btn)
            self.watchlist_layout.addWidget(row)

    def _on_add_clicked(self):
        text = self.input_area.toPlainText()
        import re
        parsed = re.split(r'[,\n\s]+', text)
        tickers = [t.strip().upper() for t in parsed if t.strip()]
        
        if not tickers:
            self.status_label.setText("Please enter at least one ticker.")
            self.status_label.setStyleSheet("color: #FF3333;")
            return
            
        self.status_label.setText("Verifying tickers...")
        self.status_label.setStyleSheet("color: #D4AF37;")
        self.tickers_added.emit(tickers)
        self.input_area.clear()

    def update_status(self, message, is_error=False):
        self.status_label.setText(message)
        self.status_label.setStyleSheet("color: #FF3333;" if is_error else "color: #00FF99;")

    def mousePressEvent(self, event):
        if event.button() == Qt.MouseButton.LeftButton:
            self.old_pos = event.globalPosition().toPoint()

    def mouseMoveEvent(self, event):
        if hasattr(self, 'old_pos') and self.old_pos:
            delta = event.globalPosition().toPoint() - self.old_pos
            self.move(self.x() + delta.x(), self.y() + delta.y())
            self.old_pos = event.globalPosition().toPoint()

    def mouseReleaseEvent(self, event):
        self.old_pos = None

class SmartDock(QWidget):
    """
    The 'Onyx' Trading Interface.
    Focused on Zero Lag and Premium Aesthetics.
    """
    
    # Signal to notify controller
    action_triggered = pyqtSignal(str)
    mode_toggled = pyqtSignal(str) # "AUTO" or "MANUAL"
    next_clicked = pyqtSignal()
    filter_changed = pyqtSignal(str) # "ALL" or "NVDA", etc.
    tickers_requested = pyqtSignal(list) # For manager
    remove_requested = pyqtSignal(str)
    toggle_priority = pyqtSignal(str)

    def __init__(self):
        super().__init__()
        
        self.setWindowTitle("Trading Co-Pilot")
        
        # Frameless & Always on Top
        self.setWindowFlags(Qt.WindowType.FramelessWindowHint | Qt.WindowType.WindowStaysOnTopHint | Qt.WindowType.Tool)
        self.setAttribute(Qt.WidgetAttribute.WA_TranslucentBackground)
        
        # Dimensions 
        self.pulse_size = QSize(80, 80) 
        self.action_size = QSize(400, 560) # Taller for Control Bar
        
        # UI Setup
        self._init_ui()
        self._setup_style()
        self.resize(self.pulse_size)

        # --- ANIMATION STATE ---
        self._active_anim = None
        self._pending_history = None
        
        # Dragging Logic variables
        self.old_pos = None

    def _init_ui(self):
        self.layout = QVBoxLayout()
        self.layout.setContentsMargins(5, 5, 5, 5) # Small margin for anti-aliasing
        self.setLayout(self.layout)
        
        # --- Main Container ---
        self.container = QFrame()
        self.container.setObjectName("Container")
        self.layout.addWidget(self.container)
        
        # Opacity Effect (Attached to Container for global fade)
        # Opacity Effect (Attached to Container for global fade)
        self._fade_eff = QGraphicsOpacityEffect(self.container)
        self._fade_eff.setOpacity(1.0)
        self.container.setGraphicsEffect(self._fade_eff)
        self.container.setAutoFillBackground(True) # Force solid paint
        
        self.container_layout = QVBoxLayout()
        self.container_layout.setContentsMargins(25, 25, 25, 25)
        self.container_layout.setSpacing(15)
        self.container.setLayout(self.container_layout)
        
        # --- 1. PULSE STATE ---
        self.pulse_label = QLabel("●") 
        self.pulse_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.pulse_label.setObjectName("PulseLabel")
        self.container_layout.addWidget(self.pulse_label)
        
        # --- 2. ACTION STATE (Hidden) ---
        self.action_frame = QFrame()
        self.action_frame.setVisible(False)
        self.action_layout = QVBoxLayout()
        self.action_layout.setSpacing(10)
        self.action_layout.setContentsMargins(0, 0, 0, 0)
        self.action_frame.setLayout(self.action_layout)
        
        # Header (Title)
        self.header_layout = QVBoxLayout()
        self.title_label = QLabel("TITLE")
        self.title_label.setObjectName("TitleLabel")
        self.title_label.setWordWrap(True)
        self.header_layout.addWidget(self.title_label)
        self.action_layout.addLayout(self.header_layout)
        
        # Badges (Verdict | Source)
        self.badge_layout = QHBoxLayout()
        self.badge_layout.setSpacing(10)
        
        self.verdict_label = QLabel("VERDICT")
        self.verdict_label.setObjectName("VerdictLabel")
        self.verdict_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.badge_layout.addWidget(self.verdict_label)
        
        self.source_label = QLabel("SOURCE")
        self.source_label.setObjectName("SourceLabel")
        self.badge_layout.addWidget(self.source_label)
        
        self.badge_layout.addStretch()
        self.action_layout.addLayout(self.badge_layout)
        
        # Description
        self.desc_label = QLabel("Description goes here.")
        self.desc_label.setWordWrap(True)
        self.desc_label.setObjectName("DescLabel")
        self.desc_label.setTextFormat(Qt.TextFormat.RichText)
        self.desc_label.setOpenExternalLinks(True)
        self.action_layout.addWidget(self.desc_label)
        
        # Fundamentals
        self.fund_label = QLabel("")
        self.fund_label.setObjectName("FundLabel")
        self.fund_label.hide()
        self.action_layout.addWidget(self.fund_label)

        # Chart Loader [NEW: RADAR ANIMATION]
        self.loader = RadarLoader()
        self.action_layout.addWidget(self.loader)

        # Chart (Optimized)
        date_axis = pg.DateAxisItem(orientation='bottom')
        self.chart = pg.PlotWidget(axisItems={'bottom': date_axis})
        self.chart.setBackground('#080808')
        self.chart.showGrid(x=False, y=True, alpha=0.1) 
        self.chart.getAxis('left').setPen('#444455') 
        self.chart.getAxis('bottom').setPen('#444455')
        self.chart.setMinimumHeight(150) # Ensure it has space
        self.chart.hide() 
        self.action_layout.addWidget(self.chart)
        
        # Buttons logic follows...
        
        # --- RESIZING GRIP ---
        self.sizegrip = QSizeGrip(self)
        self.sizegrip.setStyleSheet("background: transparent; width: 20px; height: 20px;")
        
        # Min/Max Size
        self.setMinimumSize(350, 200)
        self.setMinimumSize(350, 200)
        self.setMaximumSize(900, 2000) # Increased max height significantly


        
        # Buttons
        self.btn_layout = QHBoxLayout()
        self.btn_layout.setSpacing(10)
        
        # Gold Primary
        self.action_btn = QPushButton("EXECUTE")
        self.action_btn.setObjectName("PrimaryBtn")
        self.action_btn.setCursor(QCursor(Qt.CursorShape.PointingHandCursor))
        self.action_btn.clicked.connect(self._on_action_clicked)

        # Secondary
        self.link_btn = QPushButton("READ")
        self.link_btn.setObjectName("SecondaryBtn")
        self.link_btn.setCursor(QCursor(Qt.CursorShape.PointingHandCursor))
        self.link_btn.clicked.connect(self._open_link)
        self.link_btn.hide()
        self.current_url = None

        # Dismiss
        self.dismiss_btn = QPushButton("×")
        self.dismiss_btn.setObjectName("CloseBtn")
        self.dismiss_btn.setFixedSize(30, 30)
        self.dismiss_btn.setCursor(QCursor(Qt.CursorShape.PointingHandCursor))
        self.dismiss_btn.clicked.connect(self.collapse)
        
        self.btn_layout.addWidget(self.action_btn, 4) # 40% width
        self.btn_layout.addWidget(self.link_btn, 2)   # 20% width
        self.btn_layout.addWidget(self.dismiss_btn, 0)
        self.action_layout.addLayout(self.btn_layout)
        
        # Line Divider
        line = QFrame()
        line.setFrameShape(QFrame.Shape.HLine)
        line.setObjectName("Divider")
        self.action_layout.addWidget(line)
        
        # --- CONTROL BAR (Phase 7) ---
        self.control_layout = QHBoxLayout()
        
        self.mode_btn = QPushButton("AUTO")
        self.mode_btn.setObjectName("ModeBtn")
        self.mode_btn.setCheckable(True)
        self.mode_btn.setChecked(True)
        self.mode_btn.setCursor(QCursor(Qt.CursorShape.PointingHandCursor))
        self.mode_btn.toggled.connect(self._toggle_mode)
        
        self.next_btn = QPushButton("NEXT >")
        self.next_btn.setObjectName("NextBtn")
        self.next_btn.setCursor(QCursor(Qt.CursorShape.PointingHandCursor))
        self.next_btn.clicked.connect(self.next_clicked.emit)
        self.next_btn.setEnabled(False) 
        self.next_btn.setStyleSheet("color: #555566;") # Disabled look initially
        
        self.control_layout.addWidget(self.mode_btn)
        
        self.manage_btn = QPushButton("+")
        self.manage_btn.setObjectName("ModeBtn") # Use same style
        self.manage_btn.setToolTip("Manage Watchlist")
        self.manage_btn.setFixedSize(30, 26)
        self.manage_btn.clicked.connect(self._show_ticker_manager)
        self.control_layout.addWidget(self.manage_btn)
        
        # [NEW] SEARCH / FILTER
        self.search_box = QComboBox()
        self.search_box.setObjectName("SearchBox")
        self.search_box.setEditable(True)
        self.search_box.setInsertPolicy(QComboBox.InsertPolicy.NoInsert)
        self.search_box.setPlaceholderText("FILTER TICKER...")
        self.search_box.setMinimumWidth(120)
        # Initialize with Common Tickers so it's not empty while fetching
        self.search_box.addItems(["ALL", "NVDA", "TSLA", "AAPL", "AMD", "MSFT", "GOOGL", "AMZN", "META", "SPY", "QQQ"]) 
        
        # Style the line edit inside
        self.search_box.lineEdit().setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.search_box.currentTextChanged.connect(self._on_search_changed)
        
        self.control_layout.addSpacing(10)
        self.control_layout.addWidget(self.search_box)
        
        self.control_layout.addStretch()
        self.control_layout.addWidget(self.next_btn)
        
        # [NEW] Debounce Timer for Search
        self.filter_timer = QTimer()
        self.filter_timer.setSingleShot(True)
        self.filter_timer.setInterval(800) # 800ms debounce
        self.filter_timer.timeout.connect(self._emit_filter_changed)
        self.pending_filter_text = ""
        
        self.action_layout.addLayout(self.control_layout)
        
        self.container_layout.addWidget(self.action_frame)
        
        # Ticker Manager Overlay (Lazy Init)
        self.manager = None
    
    def _show_ticker_manager(self):
        # Calculate alert counts per ticker
        # We need access to the alert_queue. Since SmartDock doesn't own it, 
        # we'll emit a signal to the controller to provide this data, or just refresh if the controller calls it.
        # For now, let's assume the controller will handle the refresh with data.
        self.tickers_requested.emit([]) # Trigger a refresh from controller
        
        if not self.manager:
            self.manager = TickerManager(self)
            self.manager.tickers_added.connect(self.tickers_requested.emit)
            self.manager.remove_requested.connect(self.remove_requested.emit)
            self.manager.toggle_priority.connect(self.toggle_priority.emit)
        
        # Position it near the dock
        self.manager.move(self.x(), self.y() - self.manager.height() - 10)
        self.manager.show()

    def update_manager_watchlist(self, tickers, alert_counts=None, priority_list=None):
        """Called by controller to sync data to manager."""
        if self.manager:
            self.manager.refresh_watchlist(tickers, alert_counts, priority_list)
        # Also update the main search box universe
        self.set_universe(tickers)

    def update_ticker_status(self, message, is_error=False):
        if self.manager:
            self.manager.update_status(message, is_error)
    
    def _emit_filter_changed(self):
        self.filter_changed.emit(self.pending_filter_text.upper())

    def resizeEvent(self, event):
        """Handle resizing logic."""
        rect = self.rect()
        self.sizegrip.move(rect.right() - self.sizegrip.width(), rect.bottom() - self.sizegrip.height())
        super().resizeEvent(event)

    def update_queue_count(self, count):
        """Updates the Next button text based on queue size."""
        if count > 0:
            self.next_btn.setText(f"NEXT ({count}) >")
            self.next_btn.setEnabled(True)
            self.next_btn.setStyleSheet("color: #FFFFFF;") # Enabled look
        else:
            self.next_btn.setText("NEXT >")
            self.next_btn.setEnabled(False)
            self.next_btn.setStyleSheet("color: #555566;")

    def _toggle_mode(self, checked):
        if checked:
            self.mode_btn.setText("AUTO")
            self.mode_toggled.emit("AUTO")
            self.mode_btn.setStyleSheet("#ModeBtn { color: #00FF99; border: 1px solid #00FF99; }")
        else:
            self.mode_btn.setText("MANUAL")
            self.mode_toggled.emit("MANUAL")
            self.mode_btn.setStyleSheet("#ModeBtn { color: #FF9900; border: 1px solid #FF9900; }")

    def _setup_style(self):
        """The Onyx Design System: Black, Gold, Cyan."""
        self.setStyleSheet("""
            QWidget {
                font-family: 'Segoe UI', sans-serif;
            }
            #Container {
                background-color: #080808; /* Onyx Black */
                border: 1px solid #222222;
                border-radius: 24px;
            }
            #SearchBox {
                background-color: #111116;
                color: #00F0FF;
                border: 1px solid #333344;
                border-radius: 6px;
                padding: 2px;
                font-family: 'Consolas', monospace;
                font-weight: bold;
                font-size: 11px;
            }
            #SearchBox QAbstractItemView {
                background-color: #111116;
                color: #CCCCCC;
                selection-background-color: #333344;
            }
            #PulseLabel {
                font-size: 32px;
                color: #00FF99; /* Neon Green */
            }
            #TitleLabel {
                color: #EEEEEE;
                font-weight: 800;
                font-size: 18px;
                letter-spacing: 0.5px;
            }
            #SourceLabel {
                color: #888899; 
                font-size: 10px; 
                font-weight: 700;
                text-transform: uppercase;
                letter-spacing: 1px;
                padding-top: 4px;
            }
            #VerdictLabel {
                background-color: #111116;
                color: #FFFFFF;
                border: 1px solid #333344;
                border-radius: 6px;
                padding: 4px 8px;
                font-family: 'Consolas', monospace;
                font-weight: bold;
                font-size: 12px;
            }
            #DescLabel {
                color: #CCCCDD;
                font-size: 13px;
                line-height: 1.4;
            }
            #FundLabel {
                color: #00F0FF; 
                font-family: 'Consolas', monospace;
                font-size: 11px;
                padding: 4px;
                background: rgba(0, 240, 255, 0.1);
                border-radius: 4px;
            }
            #PrimaryBtn {
                background: qlineargradient(x1:0, y1:0, x2:1, y2:0, stop:0 #D4AF37, stop:1 #C5A028); /* Metallic Gold */
                color: #000000;
                border: none;
                padding: 12px;
                border-radius: 8px;
                font-weight: 900;
                letter-spacing: 1px;
                font-size: 14px;
            }
            #PrimaryBtn:hover {
                background: #E5C148;
            }
            #SecondaryBtn {
                background-color: #1A1A22;
                color: #D4AF37;
                border: 1px solid #333344;
                padding: 12px;
                border-radius: 8px;
                font-weight: bold;
            }
            #SecondaryBtn:hover {
                background-color: #252530;
                border: 1px solid #D4AF37;
            }
            #CloseBtn {
                background: transparent;
                color: #555566;
                font-size: 22px;
                font-weight: bold;
                border-radius: 15px;
            }
            #CloseBtn:hover {
                color: #AA4444;
            }
            #Divider {
                color: #222222;
                background-color: #222222;
            }
            #ModeBtn {
                background: transparent;
                color: #00FF99;
                border: 1px solid #00FF99;
                border-radius: 6px;
                padding: 6px 12px;
                font-size: 10px;
                font-weight: 800;
                letter-spacing: 1px;
            }
            #NextBtn {
                background: transparent;
                border: none;
                font-weight: 700;
                font-size: 12px;
            }
            #NextBtn:hover {
                 color: #FFFFFF;
            }
            /* High Impact Styles */
            .CriticalBorder {
                border: 2px solid #FF3333;
                background-color: #1a0505;
            }
            .HighImpactBorder {
                border: 2px solid #D4AF37;
                background-color: #1a1505;
            }
            #CriticalBadge {
                background-color: #FF3333;
                color: #FFFFFF;
                border-radius: 4px;
                padding: 2px 6px;
                font-weight: 900;
                font-size: 11px;
            }
            #HighBadge {
                background-color: #D4AF37;
                color: #000000;
                border-radius: 4px;
                padding: 2px 6px;
                font-weight: 900;
                font-size: 11px;
            }
        """)

    # --- Interaction Logic ---

    def set_universe(self, tickers: list):
        """Populate the search box with the universe."""
        self.search_box.blockSignals(True)
        self.search_box.clear()
        self.search_box.addItem("ALL")
        self.search_box.addItems(sorted(tickers))
        self.search_box.blockSignals(False)

    def _on_search_changed(self, text):
        """Emit filter signal with debounce."""
        self.pending_filter_text = text
        self.filter_timer.start() # Restart debounce timer
    
    def expand(self, title: str, description: str, verdict: str = None, history = None, 
               sources: list = None, fundamentals: str = None, url: str = None, impact: str = "NORMAL"):
        """Staggered Expansion: Resize -> Show Content -> Fade In"""
        
        # Stop any running animations
        if self._active_anim:
            self._active_anim.stop()
            self._active_anim = None
            
        # 1. Prepare Content (Hidden)
        self.pulse_label.setVisible(False)
        
        # Reset Container Style
        self.container.setStyleSheet("") # Clear previous dynamic styles
        self.container.setProperty("class", "") 
        
        # Apply Impact Styling
        if impact == "CRITICAL":
             self.container.setStyleSheet("#Container { border: 2px solid #FF3333; background-color: #150505; }")
             title = "🚨 " + title
        elif impact == "HIGH":
             self.container.setStyleSheet("#Container { border: 2px solid #D4AF37; background-color: #151005; }")
             title = "⚡ " + title
        else:
             self.container.setStyleSheet("#Container { border: 1px solid #222222; background-color: #080808; }")

        self.title_label.setText(title)
        self.desc_label.setText(description)
        
        # Update Verdict Colors
        if verdict:
            self.verdict_label.setText(verdict)
            if "BUY" in verdict:
                self.verdict_label.setStyleSheet("#VerdictLabel { color: #00FF99; border: 1px solid #00FF99; background: rgba(0,255,153,0.1); }")
            elif "SELL" in verdict:
                self.verdict_label.setStyleSheet("#VerdictLabel { color: #FF3333; border: 1px solid #FF3333; background: rgba(255,51,51,0.1); }")
            else:
                 self.verdict_label.setStyleSheet("#VerdictLabel { color: #D4AF37; border: 1px solid #D4AF37; background: rgba(212,175,55,0.1); }")
        
        if sources:
            source_text = f"VIA {' • '.join(sources)}"
            if impact == "CRITICAL":
                source_text = "⚠️ BREAKING • " + source_text
            self.source_label.setText(source_text)
            
            if impact == "CRITICAL":
                 self.source_label.setStyleSheet("color: #FF3333;")
            else:
                 self.source_label.setStyleSheet("color: #888899;")
        
        if fundamentals:
            self.fund_label.setText(fundamentals)
            self.fund_label.show()
        else:
            self.fund_label.hide()
            
        if url:
            self.current_url = url
            self.link_btn.show()
        else:
            self.current_url = None
            self.link_btn.hide()
        
        # CHART LOGIC (Adaptive)
        self.chart.clear()
        self.chart.hide()
        
        if history is not None and not history.empty:
            # CHART MODE
            self._pending_history = history
            
            # Reset Layout for Chart (Standard)
            self.desc_label.setSizePolicy(QSizePolicy.Policy.Preferred, QSizePolicy.Policy.Preferred)
            
            # Start Radar Animation
            self.loader.start()
            
            # Defer Rendering (Wait for Resize (350ms) + buffer)
            QTimer.singleShot(450, self._render_chart)
        else:
             # TEXT-ONLY MODE (Adaptive)
             self._pending_history = None
             self.loader.stop() # Ensure radar is off
             self.chart.hide()
             
             # Expand Description to fill the space
             self.desc_label.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)
             self.desc_label.adjustSize()

        # 2. Step A: Resize Window (Content is hidden but we can calculate text height)
        # Calculate dynamic height
        # Base height (Headers + Badges + Margins + Chart buffer) = ~250px
        # We need to approximate the text height.
        base_height = 250
        text_width = self.action_size.width() - 60 # Approx width minus margins
        
        # Use FontMetrics to calculate expected height of the description
        font = self.desc_label.font()
        fm = self.desc_label.fontMetrics()
        rect = fm.boundingRect(QRect(0, 0, text_width, 10000), Qt.TextFlag.TextWordWrap, description)
        text_height = rect.height()
        
        # Add buffer for Title, Verdict, Source (approx 100px)
        total_text_height = text_height + 120 
        
        # Chart height (if visible)
        chart_height = 160 if (history is not None and not history.empty) else 0
        
        target_height = base_height + total_text_height + chart_height
        
        # Clamp Logic
        # Clamp Logic
        target_height = max(400, min(target_height, 1800)) # Min 400, Max 1800
        
        target_size = QSize(self.action_size.width(), target_height)

        self._resize_anim = QPropertyAnimation(self, b"size")
        self._resize_anim.setDuration(350)
        self._resize_anim.setStartValue(self.size())
        self._resize_anim.setEndValue(target_size)
        self._resize_anim.setEasingCurve(QEasingCurve.Type.OutBack)
        self._resize_anim.finished.connect(self._start_expand_fade)
        self._resize_anim.start()
        
        # Track active animation
        self._active_anim = self._resize_anim

    def _start_expand_fade(self):
        """Called after resize is complete."""
        # Now we can safely show the content without forcing the window to grow
        self.action_frame.show()
        
        # Step B: Fade In
        self._fade_eff.setOpacity(0.0) 
        self._fade_anim = QPropertyAnimation(self._fade_eff, b"opacity")
        self._fade_anim.setDuration(250)
        self._fade_anim.setStartValue(0.0)
        self._fade_anim.setEndValue(1.0)
        self._fade_anim.setEasingCurve(QEasingCurve.Type.InQuad)
        self._fade_anim.start()
        
        # Track active animation
        self._active_anim = self._fade_anim

    def _render_chart(self):
        """Called by timer to render chart after animation."""
        if self._pending_history is not None and not self._pending_history.empty:
            history = self._pending_history
            timestamps = [x.timestamp() for x in history.index]
            prices = history['price'].values.flatten()
            
            # CLEAR first
            self.chart.clear()
            
            # PLOT LINE (Cyan, Width 2)
            self.chart.plot(timestamps, prices, 
                          pen=pg.mkPen(color='#00F0FF', width=2))
            
            # Enable Auto-Range to ensure data is visible
            self.chart.enableAutoRange()
            
            self.loader.stop()
            self.chart.show()
        
        self._pending_history = None

    def collapse(self):
        """Staggered Collapse: Fade Out -> Hide Content -> Resize"""
        # Stop any running animations
        if self._active_anim:
            self._active_anim.stop()
            self._active_anim = None
            
        # Step 1: Fade Out
        self._fade_anim = QPropertyAnimation(self._fade_eff, b"opacity")
        self._fade_anim.setDuration(150)
        self._fade_anim.setStartValue(1.0)
        self._fade_anim.setEndValue(0.0)
        self._fade_anim.finished.connect(self._start_collapse_resize)
        self._fade_anim.start()
        
        # Track active animation to prevent garbage collection
        self._active_anim = self._fade_anim

    def _start_collapse_resize(self):
        """Called after fade out is complete."""
        # Hide heavy content to remove layout constraints (Fixes QWindowsWindow warnings)
        self.action_frame.hide()
        
        # Step 2: Resize Window
        self._resize_anim = QPropertyAnimation(self, b"size")
        self._resize_anim.setDuration(350)
        self._resize_anim.setStartValue(self.size())
        self._resize_anim.setEndValue(self.pulse_size)
        self._resize_anim.setEasingCurve(QEasingCurve.Type.OutBack)
        self._resize_anim.finished.connect(self._on_collapse_finished)
        self._resize_anim.start()
        
        # Track active animation
        self._active_anim = self._resize_anim
    
    def _on_collapse_finished(self):
        self.pulse_label.setVisible(True)
        self._fade_eff.setOpacity(1.0) # Reset transparency for the pulse
        
    def _open_link(self):
        import webbrowser
        if self.current_url:
            webbrowser.open(self.current_url)

    def _on_action_clicked(self):
        self.action_triggered.emit("User clicked Execute")
        self.collapse()

    # --- Dragging Logic ---
    
    # --- Dragging & Click Logic ---
    
    def mousePressEvent(self, event):
        # Allow child widgets (Buttons, SearchBox) to handle their own clicks
        child = self.childAt(event.position().toPoint())
        if child and (isinstance(child, QPushButton) or isinstance(child, QComboBox) or "QLineEdit" in str(type(child))):
            super().mousePressEvent(event) # Propagate normally
            return

        if event.button() == Qt.MouseButton.LeftButton:
            self.old_pos = event.globalPosition().toPoint()
            self.click_start_pos = event.globalPosition().toPoint()

    def mouseMoveEvent(self, event):
        if self.old_pos:
            delta = event.globalPosition().toPoint() - self.old_pos
            self.move(self.x() + delta.x(), self.y() + delta.y())
            self.old_pos = event.globalPosition().toPoint()

    def mouseReleaseEvent(self, event):
        # Check for Click (Minimal movement)
        if hasattr(self, 'click_start_pos') and self.click_start_pos:
            delta = (event.globalPosition().toPoint() - self.click_start_pos).manhattanLength()
            if delta < 5: 
                # It's a click! Trigger manual next.
                self.next_clicked.emit()
        
        self.old_pos = None
        self.click_start_pos = None
