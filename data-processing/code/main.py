import os
import asyncio
import pdfplumber
import aiofiles
import logging, json
from concurrent.futures import ThreadPoolExecutor

# Configure logging
logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO
)

# Directory Paths
INPUT_DIR = "data-processing/pdfs"
OUTPUT_DIR = "extracted_pdfs_data"

# Create the output folder if it doesn't exist
os.makedirs(OUTPUT_DIR, exist_ok=True)

async def extract_text_from_pdf(pdf_file_path):
    """
    Asynchronously extract data from the PDF file.

    Args:
        pdf_path (str): Path to the PDF file.

    Returns:
        str: Extracted text from the PDF, or None if an error occurs.
    """
    current_loop = asyncio.get_event_loop()
    try:
        with ThreadPoolExecutor() as pool:
            text = await current_loop.run_in_executor(pool, extract_pdfs_data, pdf_file_path)
        return text
    except Exception as error:
        logging.error(f"Error processing {pdf_file_path}: {error}")
        return None

def extract_pdfs_data(pdf_file_path):
    """
    Synchronous function to extract text from a PDF file.

    Args:
        pdf_file_path (str): Path to the PDF file.

    Returns:
        str: Extracted text from the PDF.
    """
    extracted_text = ""
    try:
        with pdfplumber.open(pdf_file_path) as pdf:
            if not pdf.pages:
                logging.warning(f"PDF {pdf_file_path} is empty or malformed.")
                return ""
            for page in pdf.pages:
                extracted_text += page.extract_text() or ""
    except Exception as e:
        logging.error(f"Failed to open or process {pdf_file_path}: {e}")
        raise
    return extracted_text

async def save_to_json(file_path, extracted_data):
    """
    Asynchronously save extracted data to a JSON file.

    Args:
        file_path (str): Path to the output JSON file.
        extracted_data (dict): dictionary containing data.
    """
    try:
        async with aiofiles.open(file_path, mode="w", encoding="utf-8") as f:
            await f.write(json.dumps(extracted_data, indent=4))
        logging.info(f"Data saved successfully to JSON: {file_path}")
    except Exception as e:
        logging.error(f"Error saving JSON file {file_path}: {e}")

async def process_pdf(pdf_path):
    """
    Asynchronous task to process a single PDF file and save the data as JSON structure.
    """
    logging.info(f"Processing {pdf_path} ...")
    try:
        text = await extract_text_from_pdf(pdf_path)
        if text:
            extracted_data = {"content": text}
            base_name = os.path.basename(pdf_path).replace(".pdf", "")
            output_file = os.path.join(OUTPUT_DIR, f"{base_name}.json")
            await save_to_json(output_file, extracted_data)
            logging.info(f"Finished processing {pdf_path}.")
        else:
            logging.warning(f"No content extracted from {pdf_path}.")
    except Exception as e:
        logging.error(f"Error processing {pdf_path}: {e}")


async def process_all_pdfs():
    """
    Asynchronously process all PDFs in the input directory.
    """
    if not os.path.exists(INPUT_DIR):
        logging.error(f"Input directory does not exist: {INPUT_DIR}")
        return

    pdf_files = [os.path.join(INPUT_DIR, f) for f in os.listdir(INPUT_DIR) if f.endswith(".pdf")]
    if not pdf_files:
        logging.warning("No PDF files found in the input directory.")
        return

    logging.info(f"Found {len(pdf_files)} PDF(s) to process.")

    # Limit concurrent tasks with a semaphore Max 5 concurrent tasks
    sem = asyncio.Semaphore(5)  

    async def sem_task(pdf_file):
        async with sem:
            await process_pdf(pdf_file)

    tasks = [sem_task(pdf_file) for pdf_file in pdf_files]
    await asyncio.gather(*tasks)

if __name__ == "__main__":
    try:
        asyncio.run(process_all_pdfs())
    except Exception as error:
        logging.error(f"Unexpected error occurred: {error}")
