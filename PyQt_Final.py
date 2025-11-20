"""
Invoice Duplicate Detector - PyQt5 Version

Save as: invoice_duplicate_gui_pyqt.py

Before running:
 - Install dependencies: pip install PyQt5 pdfplumber pdf2image pytesseract pandas pillow
 - Ensure POPPLER_PATH and TESSERACT_PATH are set correctly below, or set them from the GUI.

This script provides a modern PyQt5 interface for invoice duplicate detection.
"""
import os
import shutil
import time
import hashlib
import pdfplumber
from pdf2image import convert_from_path
import pytesseract
import pandas as pd
from datetime import datetime
import threading
import queue
import sys

from PyQt5.QtWidgets import (QApplication, QMainWindow, QWidget, QVBoxLayout, 
                             QHBoxLayout, QPushButton, QLabel, QLineEdit, 
                             QFileDialog, QMessageBox, QTextEdit, QTableWidget,
                             QTableWidgetItem, QGroupBox, QGridLayout, QSplitter)
from PyQt5.QtCore import Qt, QTimer, pyqtSignal, QObject
from PyQt5.QtGui import QFont
from PyQt5.QtWidgets import QDesktopWidget

# ----------------- CONFIG (edit defaults if needed) -----------------
VERSION = "PyQt5_Version_Professional_GUI"

# Default base folder (will be used if user does not override)
BASE_FOLDER = r"D:\Invoice_Duplicate_MainFolder_test"

# Default external tool paths (edit if installed in different location)
POPPLER_PATH = r"C:\Program Files\poppler-25.07.0\Library\bin"
TESSERACT_PATH = r"C:\Program Files\Tesseract-OCR\tesseract.exe"
pytesseract.pytesseract.tesseract_cmd = TESSERACT_PATH

# -------------------------------------------------------------------

# ----------------- Helper: threaded-safe logger to GUI -----------------
class LogSignals(QObject):
    log_signal = pyqtSignal(str)

class GUI_Logger:
    def __init__(self, text_widget: QTextEdit):
        self.text_widget = text_widget
        self.queue = queue.Queue()
        self.signals = LogSignals()
        self.signals.log_signal.connect(self._append_to_widget)
        self._running = True
        self._start_timer()

    def log(self, msg):
        ts = time.strftime('%Y-%m-%d %H:%M:%S')
        line = f"[{ts}] {msg}"
        self.queue.put(line)

    def _start_timer(self):
        self.timer = QTimer()
        self.timer.timeout.connect(self._periodic_flush)
        self.timer.start(200)

    def _periodic_flush(self):
        try:
            while True:
                line = self.queue.get_nowait()
                self.signals.log_signal.emit(line)
        except queue.Empty:
            pass

    def _append_to_widget(self, msg):
        self.text_widget.append(msg)
        self.text_widget.verticalScrollBar().setValue(
            self.text_widget.verticalScrollBar().maximum()
        )

    def stop(self):
        self._running = False
        if hasattr(self, 'timer'):
            self.timer.stop()

# Redirect print() to console widget
class StdoutRedirector:
    def __init__(self, gui_logger: GUI_Logger):
        self.gui_logger = gui_logger
    def write(self, s):
        if s.strip():
            self.gui_logger.log(s.strip())
    def flush(self):
        pass

# ----------------- Core processing functions -----------------
def sha256_hash(text):
    normalized = ''.join(text.lower().split())
    return hashlib.sha256(normalized.encode('utf-8')).hexdigest()

class InvoicePipeline:
    def __init__(self, base_folder, poppler_path, tesseract_path, gui_logger, stop_event):
        self.BASE_FOLDER = base_folder
        self.MAIN_PDF_FOLDER = os.path.join(self.BASE_FOLDER, "Main_Invoices_Folder")
        self.EXTRACTED_TEXT_FOLDER = os.path.join(self.BASE_FOLDER, "Main_Extracted_Text")
        self.EXCEPTION_FOLDER = os.path.join(self.BASE_FOLDER, "Exception_Folder")
        self.UNIQUE_INVOICES_FOLDER = os.path.join(self.BASE_FOLDER, "Unique_Invoices_Folder")
        self.DUPLICATES_INVOICES_FOLDER = os.path.join(self.BASE_FOLDER, "Duplicates_Invoices_Folder")
        self.POPPLER_PATH = poppler_path
        self.TESSERACT_PATH = tesseract_path
        pytesseract.pytesseract.tesseract_cmd = self.TESSERACT_PATH
        self.log = gui_logger.log
        self.stop_event = stop_event

    def ensure_folders(self):
        os.makedirs(self.BASE_FOLDER, exist_ok=True)
        os.makedirs(self.MAIN_PDF_FOLDER, exist_ok=True)
        os.makedirs(self.EXTRACTED_TEXT_FOLDER, exist_ok=True)
        os.makedirs(self.EXCEPTION_FOLDER, exist_ok=True)
        os.makedirs(self.UNIQUE_INVOICES_FOLDER, exist_ok=True)
        os.makedirs(self.DUPLICATES_INVOICES_FOLDER, exist_ok=True)
        self.log("Ensured folder structure exists.")

    def copy_pdfs_to_main_folder(self, source_folder):
        if self.stop_event.is_set(): return
        self.log(f"Copying files from {source_folder} to {self.MAIN_PDF_FOLDER}")
        pdf_count = 0
        other_count = 0
        try:
            has_subfolders = any(os.path.isdir(os.path.join(source_folder, d)) for d in os.listdir(source_folder))
            def process_file(src_path, filename):
                nonlocal pdf_count, other_count
                dst_folder = self.MAIN_PDF_FOLDER if filename.lower().endswith('.pdf') else self.EXCEPTION_FOLDER
                shutil.copy2(src_path, os.path.join(dst_folder, filename))
                if dst_folder == self.MAIN_PDF_FOLDER:
                    pdf_count += 1
                else:
                    other_count += 1
                    self.log(f"⚠️ Non-PDF file '{filename}' moved to Exception folder")
            if has_subfolders:
                for root, _, files in os.walk(source_folder):
                    if self.stop_event.is_set(): break
                    for f in files:
                        if self.stop_event.is_set(): break
                        full_path = os.path.join(root, f)
                        process_file(full_path, f)
            else:
                for f in os.listdir(source_folder):
                    if self.stop_event.is_set(): break
                    full_path = os.path.join(source_folder, f)
                    if os.path.isfile(full_path):
                        process_file(full_path, f)
            self.log(f"✅ {pdf_count} PDFs copied to {self.MAIN_PDF_FOLDER}")
            self.log(f"📦 {other_count} non-PDF files moved to {self.EXCEPTION_FOLDER}")
        except Exception as e:
            self.log(f"❌ Error copying files: {e}")

    def extract_text_from_pdf(self, pdf_path):
        if self.stop_event.is_set(): return "", None
        try:
            with pdfplumber.open(pdf_path) as pdf:
                text = ''.join(page.extract_text() or '' for page in pdf.pages).strip()
                if len(text) >= 30:
                    return text, "pdfplumber"
        except Exception:
            pass
        try:
            pages = convert_from_path(pdf_path, dpi=300, poppler_path=self.POPPLER_PATH)
            text = ''.join(pytesseract.image_to_string(p).strip() for p in pages)
            if len(text) >= 30:
                return text, "ocr"
        except Exception:
            pass
        return "", None

    def extract_texts(self):
        if self.stop_event.is_set(): return
        self.log("Starting text extraction...")
        count_extracted = 0
        count_exception = 0
        start = time.time()
        for f in os.listdir(self.MAIN_PDF_FOLDER):
            if self.stop_event.is_set(): break
            if f.lower().endswith(".pdf"):
                pdf_path = os.path.join(self.MAIN_PDF_FOLDER, f)
                text, method = self.extract_text_from_pdf(pdf_path)
                if text:
                    try:
                        txt_path = os.path.join(self.EXTRACTED_TEXT_FOLDER, os.path.splitext(f)[0] + ".txt")
                        with open(txt_path, 'w', encoding='utf-8') as file:
                            file.write(text)
                        count_extracted += 1
                        self.log(f"✅ Extracted text from '{f}' using {method}")
                    except Exception as e:
                        self.log(f"❌ Failed to save text for '{f}': {e}")
                else:
                    try:
                        shutil.move(pdf_path, os.path.join(self.EXCEPTION_FOLDER, f))
                        count_exception += 1
                        self.log(f"🛑 Extraction failed. Moved '{f}' to Exception folder")
                    except Exception as e:
                        self.log(f"❌ Failed to move '{f}' to Exception folder: {e}")
        self.log(f"Extraction done. {count_extracted} succeeded, {count_exception} exceptions")
        self.log(f"⏱️ Took {time.time() - start:.2f} seconds")

    def deduplicate_text_files(self):
        if self.stop_event.is_set(): return
        self.log("Starting deduplication...")
        start = time.time()
        files = [f for f in os.listdir(self.EXTRACTED_TEXT_FOLDER) if f.lower().endswith('.txt')]
        seen_hashes = {}
        removed_count = 0
        for f in files:
            if self.stop_event.is_set(): break
            txt_path = os.path.join(self.EXTRACTED_TEXT_FOLDER, f)
            try:
                with open(txt_path, 'r', encoding='utf-8') as file:
                    content = file.read()
                h = sha256_hash(content)
                if h in seen_hashes:
                    os.remove(txt_path)
                    removed_count += 1
                    self.log(f"🗑️ Removed duplicate text file '{f}' (duplicate of '{seen_hashes[h]}')")
                    pdf_name = os.path.splitext(f)[0] + ".pdf"
                    pdf_path = os.path.join(self.MAIN_PDF_FOLDER, pdf_name)
                    dst_path = os.path.join(self.DUPLICATES_INVOICES_FOLDER, pdf_name)
                    if os.path.exists(pdf_path):
                        shutil.move(pdf_path, dst_path)
                        self.log(f"📁 Moved corresponding duplicate PDF '{pdf_name}' to Duplicates folder")
                else:
                    seen_hashes[h] = f
            except Exception as e:
                self.log(f"❌ Error during deduplication of '{f}': {e}")
        self.log(f"Deduplication done. Removed {removed_count} duplicates.")
        self.log(f"⏱️ Took {time.time() - start:.2f} seconds")

    def cross_compare_with_repository(self, repository_folder):
        if self.stop_event.is_set(): return
        self.log(f"Cross-comparing with repository: {repository_folder}")
        start = time.time()
        repo_hashes = set()
        for f in os.listdir(repository_folder):
            if self.stop_event.is_set(): break
            if f.lower().endswith(".txt"):
                try:
                    with open(os.path.join(repository_folder, f), 'r', encoding='utf-8') as file:
                        repo_hashes.add(sha256_hash(file.read()))
                except Exception as e:
                    self.log(f"❌ Could not read repo file '{f}': {e}")
        extracted_files = [f for f in os.listdir(self.EXTRACTED_TEXT_FOLDER) if f.lower().endswith('.txt')]
        count_unique = 0
        count_duplicates = 0
        for f in extracted_files:
            if self.stop_event.is_set(): break
            txt_path = os.path.join(self.EXTRACTED_TEXT_FOLDER, f)
            try:
                with open(txt_path, 'r', encoding='utf-8') as file:
                    content = file.read()
                h = sha256_hash(content)
                pdf_name = os.path.splitext(f)[0] + ".pdf"
                pdf_path = os.path.join(self.MAIN_PDF_FOLDER, pdf_name)
                if h in repo_hashes:
                    if os.path.exists(pdf_path):
                        shutil.move(pdf_path, os.path.join(self.DUPLICATES_INVOICES_FOLDER, pdf_name))
                    os.remove(txt_path)
                    count_duplicates += 1
                    self.log(f"🗂️ Moved duplicate PDF '{pdf_name}' to Duplicates and deleted text")
                else:
                    if os.path.exists(pdf_path):
                        shutil.move(pdf_path, os.path.join(self.UNIQUE_INVOICES_FOLDER, pdf_name))
                        count_unique += 1
                        self.log(f"✅ Moved unique PDF '{pdf_name}' to Unique folder")
            except Exception as e:
                self.log(f"❌ Error comparing file '{f}': {e}")
        for f in os.listdir(self.EXCEPTION_FOLDER):
            if self.stop_event.is_set(): break
            if f.lower().endswith('.pdf'):
                try:
                    shutil.copy2(os.path.join(self.EXCEPTION_FOLDER, f), os.path.join(self.UNIQUE_INVOICES_FOLDER, f))
                    self.log(f"📋 Copied exception PDF '{f}' to Unique folder for inspection")
                except Exception as e:
                    self.log(f"❌ Could not copy exception PDF '{f}': {e}")
        self.log(f"Cross-comparison complete. Unique: {count_unique}, Duplicates: {count_duplicates}")
        self.log(f"⏱️ Took {time.time() - start:.2f} seconds")

    def update_repository(self, repository_folder):
        if self.stop_event.is_set(): return
        self.log(f"Updating repository at '{repository_folder}' with renaming...")
        start = time.time()
        now = datetime.now()
        date_str = now.strftime("%d%m%Y")
        time_str = now.strftime("%H%M%S")
        month_year_str = now.strftime("%b_%y")
        moved_count = 0
        for f in os.listdir(self.EXTRACTED_TEXT_FOLDER):
            if self.stop_event.is_set(): break
            if f.lower().endswith(".txt"):
                src_txt_path = os.path.join(self.EXTRACTED_TEXT_FOLDER, f)
                base_name = f"INVC_{date_str}_{time_str}_{month_year_str}.txt"
                new_name = base_name
                count = 1
                while os.path.exists(os.path.join(repository_folder, new_name)):
                    new_name = base_name.replace(".txt", f"_{count}.txt")
                    count += 1
                dst_txt_path = os.path.join(repository_folder, new_name)
                try:
                    shutil.move(src_txt_path, dst_txt_path)
                    self.log(f"✅ Moved and renamed '{f}' to '{new_name}'")
                    moved_count += 1
                except Exception as e:
                    self.log(f"❌ Failed to move and rename '{f}': {e}")
        self.log(f"✅ Moved and renamed {moved_count} unique text files to repository")
        self.log(f"⏱️ Took {time.time() - start:.2f} seconds")

    def cleanup_main_pdf_folder(self):
        if self.stop_event.is_set(): return
        leftover_files = [f for f in os.listdir(self.MAIN_PDF_FOLDER) if f.lower().endswith(".pdf")]
        if not leftover_files:
            self.log("✅ No leftover PDFs in MAIN_PDF_FOLDER")
            return
        self.log("⚠️ Cleaning up leftover PDFs in MAIN_PDF_FOLDER...")
        for f in leftover_files:
            if self.stop_event.is_set(): break
            try:
                shutil.move(os.path.join(self.MAIN_PDF_FOLDER, f), os.path.join(self.EXCEPTION_FOLDER, f))
                self.log(f"📦 Moved leftover PDF '{f}' to Exception folder")
            except Exception as e:
                self.log(f"❌ Failed to move leftover PDF '{f}': {e}")

    def list_files_as_dataframe(self, folder_path):
        files = []
        if not os.path.exists(folder_path):
            return pd.DataFrame(files)
        for f in os.listdir(folder_path):
            full_path = os.path.join(folder_path, f)
            if os.path.isfile(full_path):
                info = os.stat(full_path)
                files.append({
                    "Filename": f,
                    "Size_Bytes": info.st_size,
                    "Modified_Time": time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(info.st_mtime))
                })
        df = pd.DataFrame(files)
        return df

    def get_duplicate_invoices_dataframe(self):
        return self.list_files_as_dataframe(self.DUPLICATES_INVOICES_FOLDER)

    def get_unique_invoices_dataframe(self):
        return self.list_files_as_dataframe(self.UNIQUE_INVOICES_FOLDER)

    def run_full_pipeline(self, source_folder, repository_folder):
        try:
            self.ensure_folders()
            self.copy_pdfs_to_main_folder(source_folder)
            if self.stop_event.is_set(): return
            self.extract_texts()
            if self.stop_event.is_set(): return
            self.deduplicate_text_files()
            if self.stop_event.is_set(): return
            self.cross_compare_with_repository(repository_folder)
            if self.stop_event.is_set(): return
            self.update_repository(repository_folder)
            if self.stop_event.is_set(): return
            self.cleanup_main_pdf_folder()
            self.log("🎉 Pipeline completed.")
        except Exception as e:
            self.log(f"❌ Pipeline error: {e}")

# ----------------- PyQt5 GUI Application -----------------
class InvoiceAppSignals(QObject):
    pipeline_finished = pyqtSignal()

class InvoiceApp(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle(f"Invoice Duplicate Detector - {VERSION}")
        
        # Remove minimize and maximize buttons, keep only close button
        self.setWindowFlags(Qt.Window | Qt.WindowCloseButtonHint | Qt.CustomizeWindowHint)
        
        # Make window fullscreen
        screen = QDesktopWidget().screenGeometry()
        self.setGeometry(0, 0, screen.width(), screen.height())
        self.showMaximized()

        self.base_folder = BASE_FOLDER
        self.poppler_path = POPPLER_PATH
        self.tesseract_path = TESSERACT_PATH
        self.stop_event = threading.Event()
        self.worker_thread = None
        self.pipeline = None
        self._orig_stdout = None

        self.signals = InvoiceAppSignals()
        self.signals.pipeline_finished.connect(self._on_pipeline_finished)

        self.init_ui()

    def init_ui(self):
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        main_layout = QVBoxLayout(central_widget)

        # Top section - Folder selectors
        folder_group = QGroupBox("Folder Configuration")
        folder_layout = QGridLayout()
        
        folder_layout.addWidget(QLabel("Source Folder:"), 0, 0)
        self.source_edit = QLineEdit()
        folder_layout.addWidget(self.source_edit, 0, 1)
        self.source_btn = QPushButton("Browse")
        self.source_btn.clicked.connect(self.browse_source)
        folder_layout.addWidget(self.source_btn, 0, 2)

        folder_layout.addWidget(QLabel("Repository Folder:"), 1, 0)
        self.repo_edit = QLineEdit()
        folder_layout.addWidget(self.repo_edit, 1, 1)
        self.repo_btn = QPushButton("Browse")
        self.repo_btn.clicked.connect(self.browse_repo)
        folder_layout.addWidget(self.repo_btn, 1, 2)

        folder_group.setLayout(folder_layout)
        main_layout.addWidget(folder_group)

        # Control section - Buttons and paths
        control_group = QGroupBox("Pipeline Controls")
        control_layout = QGridLayout()

        # Buttons
        button_layout = QHBoxLayout()
        self.start_btn = QPushButton("Start Pipeline")
        self.start_btn.clicked.connect(self.start_pipeline)
        self.start_btn.setStyleSheet("background-color: #4CAF50; color: white; font-weight: bold; padding: 8px;")
        button_layout.addWidget(self.start_btn)

        self.stop_btn = QPushButton("Stop")
        self.stop_btn.clicked.connect(self.stop_pipeline)
        self.stop_btn.setEnabled(False)
        self.stop_btn.setStyleSheet("background-color: #f44336; color: white; font-weight: bold; padding: 8px;")
        button_layout.addWidget(self.stop_btn)

        self.reset_btn = QPushButton("Reset")
        self.reset_btn.clicked.connect(self.reset_ui)
        self.reset_btn.setStyleSheet("background-color: #2196F3; color: white; font-weight: bold; padding: 8px;")
        button_layout.addWidget(self.reset_btn)

        button_layout.addStretch()
        control_layout.addLayout(button_layout, 0, 0, 1, 2)

        # Poppler path
        control_layout.addWidget(QLabel("Poppler Path:"), 1, 0)
        self.poppler_edit = QLineEdit(self.poppler_path)
        control_layout.addWidget(self.poppler_edit, 1, 1)

        # Tesseract path
        control_layout.addWidget(QLabel("Tesseract Path:"), 2, 0)
        self.tesseract_edit = QLineEdit(self.tesseract_path)
        control_layout.addWidget(self.tesseract_edit, 2, 1)

        control_group.setLayout(control_layout)
        main_layout.addWidget(control_group)

        # DataFrames section - Tables
        tables_splitter = QSplitter(Qt.Horizontal)
        
        # Unique invoices table
        unique_group = QGroupBox("New Invoices (Unique)")
        unique_layout = QVBoxLayout()
        self.unique_table = QTableWidget()
        self.unique_table.setColumnCount(3)
        self.unique_table.setHorizontalHeaderLabels(["Filename", "Size (Bytes)", "Modified Time"])
        self.unique_table.horizontalHeader().setStretchLastSection(True)
        unique_layout.addWidget(self.unique_table)
        
        self.export_unique_btn = QPushButton("Export Unique CSV")
        self.export_unique_btn.clicked.connect(self.export_unique_csv)
        unique_layout.addWidget(self.export_unique_btn)
        
        unique_group.setLayout(unique_layout)
        tables_splitter.addWidget(unique_group)

        # Duplicate invoices table
        dup_group = QGroupBox("Duplicate Invoices")
        dup_layout = QVBoxLayout()
        self.dup_table = QTableWidget()
        self.dup_table.setColumnCount(3)
        self.dup_table.setHorizontalHeaderLabels(["Filename", "Size (Bytes)", "Modified Time"])
        self.dup_table.horizontalHeader().setStretchLastSection(True)
        dup_layout.addWidget(self.dup_table)
        
        self.export_dup_btn = QPushButton("Export Duplicates CSV")
        self.export_dup_btn.clicked.connect(self.export_duplicates_csv)
        dup_layout.addWidget(self.export_dup_btn)
        
        dup_group.setLayout(dup_layout)
        tables_splitter.addWidget(dup_group)

        main_layout.addWidget(tables_splitter)

        # Console logs section
        console_group = QGroupBox("Console Logs")
        console_layout = QVBoxLayout()
        self.console_text = QTextEdit()
        self.console_text.setReadOnly(True)
        self.console_text.setFont(QFont("Courier", 9))
        console_layout.addWidget(self.console_text)
        console_group.setLayout(console_layout)
        main_layout.addWidget(console_group)

        # Initialize logger
        self.console_gui_logger = GUI_Logger(self.console_text)

    def browse_source(self):
        folder = QFileDialog.getExistingDirectory(self, "Select Source Folder")
        if folder:
            self.source_edit.setText(folder)

    def browse_repo(self):
        folder = QFileDialog.getExistingDirectory(self, "Select Repository Folder")
        if folder:
            self.repo_edit.setText(folder)

    def start_pipeline(self):
        src = self.source_edit.text().strip()
        repo = self.repo_edit.text().strip()

        if not src or not os.path.isdir(src):
            QMessageBox.critical(self, "Error", "Please select a valid Source folder.")
            return
        if not repo or not os.path.isdir(repo):
            QMessageBox.critical(self, "Error", "Please select a valid Repository folder.")
            return

        self.start_btn.setEnabled(False)
        self.stop_btn.setEnabled(True)
        self.reset_btn.setEnabled(False)

        self.poppler_path = self.poppler_edit.text().strip()
        self.tesseract_path = self.tesseract_edit.text().strip()
        self.stop_event.clear()

        class LoggerAdapter:
            def __init__(self, logger):
                self._logger = logger
            def log(self, msg):
                self._logger.log(msg)

        pipeline_logger = LoggerAdapter(self.console_gui_logger)
        self.pipeline = InvoicePipeline(self.base_folder, self.poppler_path, 
                                       self.tesseract_path, pipeline_logger, self.stop_event)

        self._orig_stdout = sys.stdout
        sys.stdout = StdoutRedirector(self.console_gui_logger)

        def worker():
            try:
                self.pipeline.run_full_pipeline(src, repo)
            finally:
                self.signals.pipeline_finished.emit()

        self.worker_thread = threading.Thread(target=worker, daemon=True)
        self.worker_thread.start()
        self.console_gui_logger.log("Worker started...")

    def stop_pipeline(self):
        reply = QMessageBox.question(self, "Stop", "Do you want to stop the running process?",
                                     QMessageBox.Yes | QMessageBox.No)
        if reply == QMessageBox.Yes:
            self.console_gui_logger.log("Stop requested by user...")
            self.stop_event.set()

    def reset_ui(self):
        if self.worker_thread and self.worker_thread.is_alive():
            QMessageBox.warning(self, "Running", "Cannot reset while pipeline is running. Please stop first.")
            return
        
        self.unique_table.setRowCount(0)
        self.dup_table.setRowCount(0)
        self.console_text.clear()
        self.source_edit.clear()
        self.repo_edit.clear()
        self.console_gui_logger.log("UI reset.")

    def _on_pipeline_finished(self):
        if self._orig_stdout is not None:
            sys.stdout = self._orig_stdout
            self._orig_stdout = None

        self.start_btn.setEnabled(True)
        self.stop_btn.setEnabled(False)
        self.reset_btn.setEnabled(True)
        self.console_gui_logger.log("Worker finished or stopped.")
        
        self.refresh_unique()
        self.refresh_duplicates()

    def refresh_unique(self):
        if not self.pipeline:
            temp = InvoicePipeline(self.base_folder, self.poppler_path, self.tesseract_path, 
                                  self.console_gui_logger, threading.Event())
            df = temp.get_unique_invoices_dataframe()
        else:
            df = self.pipeline.get_unique_invoices_dataframe()
        self._populate_table(self.unique_table, df)

    def refresh_duplicates(self):
        if not self.pipeline:
            temp = InvoicePipeline(self.base_folder, self.poppler_path, self.tesseract_path, 
                                  self.console_gui_logger, threading.Event())
            df = temp.get_duplicate_invoices_dataframe()
        else:
            df = self.pipeline.get_duplicate_invoices_dataframe()
        self._populate_table(self.dup_table, df)

    def _populate_table(self, table: QTableWidget, df: pd.DataFrame):
        table.setRowCount(0)
        if df is None or df.empty:
            return
        
        table.setRowCount(len(df))
        for i, row in df.iterrows():
            table.setItem(i, 0, QTableWidgetItem(str(row.get('Filename', ''))))
            table.setItem(i, 1, QTableWidgetItem(str(row.get('Size_Bytes', ''))))
            table.setItem(i, 2, QTableWidgetItem(str(row.get('Modified_Time', ''))))
        
        # Auto-resize columns to content
        table.resizeColumnsToContents()

    def export_unique_csv(self):
        if not self.pipeline:
            temp = InvoicePipeline(self.base_folder, self.poppler_path, self.tesseract_path, 
                                  self.console_gui_logger, threading.Event())
            df = temp.get_unique_invoices_dataframe()
        else:
            df = self.pipeline.get_unique_invoices_dataframe()
        
        if df is None or df.empty:
            QMessageBox.information(self, "Export", "No unique invoices to export.")
            return
        
        path, _ = QFileDialog.getSaveFileName(self, "Save Unique CSV", "", "CSV Files (*.csv)")
        if path:
            df.to_csv(path, index=False)
            QMessageBox.information(self, "Export", f"Saved Unique CSV to {path}")

    def export_duplicates_csv(self):
        if not self.pipeline:
            temp = InvoicePipeline(self.base_folder, self.poppler_path, self.tesseract_path, 
                                  self.console_gui_logger, threading.Event())
            df = temp.get_duplicate_invoices_dataframe()
        else:
            df = self.pipeline.get_duplicate_invoices_dataframe()
        
        if df is None or df.empty:
            QMessageBox.information(self, "Export", "No duplicate invoices to export.")
            return
        
        path, _ = QFileDialog.getSaveFileName(self, "Save Duplicates CSV", "", "CSV Files (*.csv)")
        if path:
            df.to_csv(path, index=False)
            QMessageBox.information(self, "Export", f"Saved Duplicates CSV to {path}")

    def closeEvent(self, event):
        if self.worker_thread and self.worker_thread.is_alive():
            reply = QMessageBox.question(self, "Exit", 
                                        "A process is running. Do you want to stop and exit?",
                                        QMessageBox.Yes | QMessageBox.No)
            if reply == QMessageBox.No:
                event.ignore()
                return
            self.stop_event.set()
            time.sleep(0.5)
        
        try:
            self.console_gui_logger.stop()
        except Exception:
            pass
        
        event.accept()

# ----------------- Run Application -----------------
def main():
    app = QApplication(sys.argv)
    
    # Optional: Set application style
    app.setStyle('Fusion')  # Modern look across platforms
    
    window = InvoiceApp()
    window.show()
    sys.exit(app.exec_())

if __name__ == "__main__":
    main()