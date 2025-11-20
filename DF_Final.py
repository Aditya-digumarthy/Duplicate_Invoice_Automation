import os
import shutil
import time
import hashlib
import pdfplumber
from pdf2image import convert_from_path
import pytesseract
import pandas as pd
from datetime import datetime

VERSION = "Updated_Version_DataFreame"

# === CONFIG ===
BASE_FOLDER = r"D:\Invoice_Duplicate_MainFolder"

MAIN_PDF_FOLDER = os.path.join(BASE_FOLDER, "Main_Invoices_Folder")
EXTRACTED_TEXT_FOLDER = os.path.join(BASE_FOLDER, "Main_Extracted_Text")
EXCEPTION_FOLDER = os.path.join(BASE_FOLDER, "Exception_Folder")
UNIQUE_INVOICES_FOLDER = os.path.join(BASE_FOLDER, "Unique_Invoices_Folder")
DUPLICATES_INVOICES_FOLDER = os.path.join(BASE_FOLDER, "Duplicates_Invoices_Folder")

POPPLER_PATH = r"C:\Program Files\poppler-25.07.0\Library\bin"
TESSERACT_PATH = r"C:\Program Files\Tesseract-OCR\tesseract.exe"
pytesseract.pytesseract.tesseract_cmd = TESSERACT_PATH


def log(msg):
    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {msg}")


def ensure_folders():
    os.makedirs(BASE_FOLDER, exist_ok=True)
    os.makedirs(MAIN_PDF_FOLDER, exist_ok=True)
    os.makedirs(EXTRACTED_TEXT_FOLDER, exist_ok=True)
    os.makedirs(EXCEPTION_FOLDER, exist_ok=True)
    os.makedirs(UNIQUE_INVOICES_FOLDER, exist_ok=True)
    os.makedirs(DUPLICATES_INVOICES_FOLDER, exist_ok=True)


def copy_pdfs_to_main_folder(source_folder):
    log(f"Copying files from {source_folder} to {MAIN_PDF_FOLDER}")
    pdf_count = 0
    other_count = 0

    try:
        has_subfolders = any(os.path.isdir(os.path.join(source_folder, d)) for d in os.listdir(source_folder))

        def process_file(src_path, filename):
            nonlocal pdf_count, other_count
            dst_folder = MAIN_PDF_FOLDER if filename.lower().endswith('.pdf') else EXCEPTION_FOLDER
            shutil.copy2(src_path, os.path.join(dst_folder, filename))
            if dst_folder == MAIN_PDF_FOLDER:
                pdf_count += 1
            else:
                other_count += 1
                log(f"⚠️ Non-PDF file '{filename}' moved to Exception folder")

        if has_subfolders:
            for root, _, files in os.walk(source_folder):
                for f in files:
                    full_path = os.path.join(root, f)
                    process_file(full_path, f)
        else:
            for f in os.listdir(source_folder):
                full_path = os.path.join(source_folder, f)
                if os.path.isfile(full_path):
                    process_file(full_path, f)

        log(f"✅ {pdf_count} PDFs copied to {MAIN_PDF_FOLDER}")
        log(f"📦 {other_count} non-PDF files moved to {EXCEPTION_FOLDER}")
    except Exception as e:
        log(f"❌ Error copying files: {e}")


def extract_text_from_pdf(pdf_path):
    try:
        with pdfplumber.open(pdf_path) as pdf:
            text = ''.join(page.extract_text() or '' for page in pdf.pages).strip()
            if len(text) >= 30:
                return text, "pdfplumber"
    except Exception:
        pass

    try:
        pages = convert_from_path(pdf_path, dpi=300, poppler_path=POPPLER_PATH)
        text = ''.join(pytesseract.image_to_string(p).strip() for p in pages)
        if len(text) >= 30:
            return text, "ocr"
    except Exception:
        pass

    return "", None


def extract_texts():
    log("Starting text extraction...")
    count_extracted = 0
    count_exception = 0
    start = time.time()

    for f in os.listdir(MAIN_PDF_FOLDER):
        if f.lower().endswith(".pdf"):
            pdf_path = os.path.join(MAIN_PDF_FOLDER, f)
            text, method = extract_text_from_pdf(pdf_path)

            if text:
                try:
                    txt_path = os.path.join(EXTRACTED_TEXT_FOLDER, os.path.splitext(f)[0] + ".txt")
                    with open(txt_path, 'w', encoding='utf-8') as file:
                        file.write(text)
                    count_extracted += 1
                    log(f"✅ Extracted text from '{f}' using {method}")
                except Exception as e:
                    log(f"❌ Failed to save text for '{f}': {e}")
            else:
                try:
                    shutil.move(pdf_path, os.path.join(EXCEPTION_FOLDER, f))
                    count_exception += 1
                    log(f"🛑 Extraction failed. Moved '{f}' to Exception folder")
                except Exception as e:
                    log(f"❌ Failed to move '{f}' to Exception folder: {e}")

    log(f"Extraction done. {count_extracted} succeeded, {count_exception} exceptions")
    log(f"⏱️ Took {time.time() - start:.2f} seconds")


def sha256_hash(text):
    normalized = ''.join(text.lower().split())
    return hashlib.sha256(normalized.encode('utf-8')).hexdigest()


def deduplicate_text_files():
    log("Starting deduplication...")
    start = time.time()
    files = [f for f in os.listdir(EXTRACTED_TEXT_FOLDER) if f.lower().endswith('.txt')]
    seen_hashes = {}
    removed_count = 0

    for f in files:
        txt_path = os.path.join(EXTRACTED_TEXT_FOLDER, f)
        try:
            with open(txt_path, 'r', encoding='utf-8') as file:
                content = file.read()
            h = sha256_hash(content)

            if h in seen_hashes:
                os.remove(txt_path)
                removed_count += 1
                log(f"🗑️ Removed duplicate text file '{f}' (duplicate of '{seen_hashes[h]}')")

                # Move corresponding PDF to duplicates folder
                pdf_name = os.path.splitext(f)[0] + ".pdf"
                pdf_path = os.path.join(MAIN_PDF_FOLDER, pdf_name)
                dst_path = os.path.join(DUPLICATES_INVOICES_FOLDER, pdf_name)

                if os.path.exists(pdf_path):
                    shutil.move(pdf_path, dst_path)
                    log(f"📁 Moved corresponding duplicate PDF '{pdf_name}' to Duplicates folder")
            else:
                seen_hashes[h] = f
        except Exception as e:
            log(f"❌ Error during deduplication of '{f}': {e}")

    log(f"Deduplication done. Removed {removed_count} duplicates.")
    log(f"⏱️ Took {time.time() - start:.2f} seconds")


def cross_compare_with_repository(repository_folder):
    log(f"Cross-comparing with repository: {repository_folder}")
    start = time.time()
    repo_hashes = set()

    for f in os.listdir(repository_folder):
        if f.lower().endswith(".txt"):
            try:
                with open(os.path.join(repository_folder, f), 'r', encoding='utf-8') as file:
                    repo_hashes.add(sha256_hash(file.read()))
            except Exception as e:
                log(f"❌ Could not read repo file '{f}': {e}")

    extracted_files = [f for f in os.listdir(EXTRACTED_TEXT_FOLDER) if f.lower().endswith('.txt')]
    count_unique = 0
    count_duplicates = 0

    for f in extracted_files:
        txt_path = os.path.join(EXTRACTED_TEXT_FOLDER, f)
        try:
            with open(txt_path, 'r', encoding='utf-8') as file:
                content = file.read()
            h = sha256_hash(content)

            pdf_name = os.path.splitext(f)[0] + ".pdf"
            pdf_path = os.path.join(MAIN_PDF_FOLDER, pdf_name)

            if h in repo_hashes:
                # Duplicate
                if os.path.exists(pdf_path):
                    shutil.move(pdf_path, os.path.join(DUPLICATES_INVOICES_FOLDER, pdf_name))
                os.remove(txt_path)
                count_duplicates += 1
                log(f"🗂️ Moved duplicate PDF '{pdf_name}' to Duplicates and deleted text")
            else:
                # Unique
                if os.path.exists(pdf_path):
                    shutil.move(pdf_path, os.path.join(UNIQUE_INVOICES_FOLDER, pdf_name))
                    count_unique += 1
                    log(f"✅ Moved unique PDF '{pdf_name}' to Unique folder")
        except Exception as e:
            log(f"❌ Error comparing file '{f}': {e}")

   
    for f in os.listdir(EXCEPTION_FOLDER):
        if f.lower().endswith('.pdf'):
            try:
                shutil.copy2(os.path.join(EXCEPTION_FOLDER, f), os.path.join(UNIQUE_INVOICES_FOLDER, f))
                log(f"📋 Copied exception PDF '{f}' to Unique folder for inspection")
            except Exception as e:
                log(f"❌ Could not copy exception PDF '{f}': {e}")

    log(f"Cross-comparison complete. Unique: {count_unique}, Duplicates: {count_duplicates}")
    log(f"⏱️ Took {time.time() - start:.2f} seconds")


def update_repository(repository_folder):
    log(f"Updating repository at '{repository_folder}' with renaming...")
    start = time.time()

    now = datetime.now()
    date_str = now.strftime("%d%m%Y")
    time_str = now.strftime("%H%M%S")
    month_year_str = now.strftime("%b_%y")  

    moved_count = 0

    for f in os.listdir(EXTRACTED_TEXT_FOLDER):
        if f.lower().endswith(".txt"):
            src_txt_path = os.path.join(EXTRACTED_TEXT_FOLDER, f)

            # Compose base new filename
            base_name = f"INVC_{date_str}_{time_str}_{month_year_str}.txt"
            new_name = base_name
            count = 1

            # Check for filename conflicts in repository and increment suffix if needed
            while os.path.exists(os.path.join(repository_folder, new_name)):
                new_name = base_name.replace(".txt", f"_{count}.txt")
                count += 1

            dst_txt_path = os.path.join(repository_folder, new_name)

            try:
                shutil.move(src_txt_path, dst_txt_path)
                log(f"✅ Moved and renamed '{f}' to '{new_name}'")
                moved_count += 1
            except Exception as e:
                log(f"❌ Failed to move and rename '{f}': {e}")

    log(f"✅ Moved and renamed {moved_count} unique text files to repository")
    log(f"⏱️ Took {time.time() - start:.2f} seconds")


def cleanup_main_pdf_folder():
    leftover_files = [f for f in os.listdir(MAIN_PDF_FOLDER) if f.lower().endswith(".pdf")]
    if not leftover_files:
        log("✅ No leftover PDFs in MAIN_PDF_FOLDER")
        return
    log("⚠️ Cleaning up leftover PDFs in MAIN_PDF_FOLDER...")
    for f in leftover_files:
        try:
            shutil.move(os.path.join(MAIN_PDF_FOLDER, f), os.path.join(EXCEPTION_FOLDER, f))
            log(f"📦 Moved leftover PDF '{f}' to Exception folder")
        except Exception as e:
            log(f"❌ Failed to move leftover PDF '{f}': {e}")


# ===Functions to get pandas DataFrames of duplicates and unique invoices ===

def list_files_as_dataframe(folder_path):
    files = []
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


def get_duplicate_invoices_dataframe():
    return list_files_as_dataframe(DUPLICATES_INVOICES_FOLDER)


def get_unique_invoices_dataframe():
    return list_files_as_dataframe(UNIQUE_INVOICES_FOLDER)


def main():
    log(f"Invoice Duplicate Detector - Version {VERSION}")
    ensure_folders()
    total_start = time.time()

    source_folder = input("Enter the source folder containing PDFs or subfolders: ").strip()
    repository_folder = input("Enter the repository text folder path: ").strip()

    copy_pdfs_to_main_folder(source_folder)
    extract_texts()
    deduplicate_text_files()
    cross_compare_with_repository(repository_folder)
    update_repository(repository_folder)
    cleanup_main_pdf_folder()

    log(f"🎉 All tasks completed in {time.time() - total_start:.2f} seconds.")

    # Generate and print DataFrames for frontend or review
    df_duplicates = get_duplicate_invoices_dataframe()
    df_unique = get_unique_invoices_dataframe()

    print("\n--- Duplicate Invoices DataFrame ---")
    print(df_duplicates)

    print("\n--- Unique Invoices DataFrame ---")
    print(df_unique)


if __name__ == "__main__":
    main()
