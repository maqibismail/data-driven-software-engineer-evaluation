# Data processing - Task Solution
This solution uses `asyncio` to process PDFs concurrently and efficiently by using asynchronous file I/O operations and controlled task execution. Each PDF is parsed with `pdfplumber` library in an asyncio-compatible way using `ThreadPoolExecutor`, ensuring non-blocking behavior. Extracted content is structured into JSON format and saved asynchronously, enabling efficient processing of multiple files. A semaphore limits the number of concurrent tasks to prevent resource exhaustion. This approach is scalable for moderate workloads, handling thousands of small-to-medium-sized PDFs on a single machine.

The output in the json format for each of the PDFs will be created in the separate folder named as 'extracted_pdfs_data' upon executing the code.

## Prerequisites

Python 3.9 or higher

## How to run the code

1. **Create a Virtual Environment** Run the following command to create a virtual environment:
`python3.9 -m venv venv`
2. **Activate the Virtual Environment** Activate the environment using the following command:
    - On macOS/Linux:
    `source venv/bin/activate`
    - On Windows:
    venv\Scripts\activate
3. **Install Dependencies** Install the project dependencies listed in requirements.txt:
`pip install -r data-processing/requirements.txt`
4. **Running the Code** After setting up the environment and installing dependencies, you can run the main script:
`python data-processing/code/main.py`

## Aditional Notes
1. **Error Logs:** Any issues encountered during PDF processing, such as malformed files, will be logged to the console.
2. **Customizable Settings:** You can modify the semaphore limit (default: 5 concurrent tasks) in the code to adjust concurrency based on your system’s capabilities.
3. **Other Possible Solutions:** Using Apache Spark can also be a possible solution for large-scale data processing across distributed clusters, especially when dealing with terabytes of data. Alos, by using Celery with a message broker like RabbitMQ or Redis, we can distribute the tasks across multiple worker machines to handle very high workloads. 