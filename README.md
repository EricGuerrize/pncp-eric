# Busca PNCP Pipeline

![Python Version](https://img.shields.io/badge/python-3.8%2B-blue.svg)
![Pandas](https://img.shields.io/badge/pandas-data%20analysis-150458.svg)
![httpx](https://img.shields.io/badge/httpx-async%20http-005571.svg)

**Busca PNCP** is an automated, highly-scalable asynchronous data pipeline designed to collect, normalize, and clean public procurement data from the Brazilian National Public Procurement Portal ([PNCP API](https://pncp.gov.br)). 

## 🚀 Key Features

*   **Asynchronous Architecture**: Leverages `asyncio` and `httpx` to perform parallel requests across multiple procurement modalities and pages, ensuring minimal execution time.
*   **Resilience & Error Handling**: Implements smart retry mechanisms with exponential backoff using `tenacity` to handle API rate limits and temporary network fluctuations gracefully.
*   **Data Normalization & Flattening**: Recursively flattens complex nested JSON structures from the PNCP API into straightforward, tabular formats.
*   **Built-in Data Cleaning**: Automatically filters down specific governmental spheres (Municipal and State (`M`, `E`)) and strictly curates the exported columns, preventing data bloating.
*   **Ready-to-Use Excel Export**: Delivers cleanly formatted datasets directly to `.xlsx` files using `pandas` and `openpyxl`.

## 📂 Project Structure

```text
busca_pncp/
├── pncp_pipeline/          # Core pipeline package
│   ├── main.py             # Main entry point and orchestrator
│   ├── config.py           # Centralized configuration (timeouts, endpoints, retries)
│   ├── pncp_api_client.py  # HTTP client built with httpx and tenacity
│   ├── collector.py        # Asynchronous data fetcher with concurrency limits
│   ├── normalizer.py       # JSON flattening and tracking logic
│   ├── dataset_builder.py  # Dataframe construction and pandas cleaning routines
│   └── excel_exporter.py   # Final output generation logic to openpyxl
├── requirements.txt        # Project dependencies
└── .gitignore              # Ignored generated files and environments
```

## 🛠️ Installation

1. **Clone the repository:**
   ```bash
   git clone https://github.com/BAZ85/busca_pncp.git
   cd busca_pncp
   ```

2. **Set up a virtual environment (optional but recommended):**
   ```bash
   python -m venv .venv
   source .venv/bin/activate  # On Windows use: .venv\Scripts\activate
   ```

3. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

## 🖥️ Usage

To execute the pipeline for yesterday's date (D-1 default behavior), simply run the main orchestrator:

```bash
python pncp_pipeline/main.py
```

### Execution Logs & Output
- **Logs**: Detailed execution logs tracking requests, pagination, and eventual errors are saved in the `pncp_pipeline/logs/` directory.
- **Data**: The cleaned output `.xlsx` files are automatically stored in the `pncp_pipeline/output/` directory, following the naming convention: `pncp_contratacoes_{UF}_{YYYYMMDD}.xlsx`.

## 📄 Source Details

This project is intended for data engineering, analytical purposes and tracking. The information consumed is entirely publicly provided through the official PNCP Open Data framework.
