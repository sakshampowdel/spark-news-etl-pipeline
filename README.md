# News ETL Pipeline
An ETL data pipeline that grabs news from various news organizations. 
Uses the Medallion Architecture to extract raw data, cleans it into a structured format, and prepares it for trend analysis.

# Technologies
- **Python**: Core logic and orchestration.
- **PySpark**: (Upcoming) Distributed processing for Gold-layer aggregations.
- **BeautifulSoup**: HTML parsing and cleaning.
- **Requests**: Resilient data extraction.
- **Dataclasses & Typing**: Strict schema enforcement.

# Architecture

1. **Bronze Layer (Raw)**: Captures the raw HTML response and metadata. This layer is append-only to preserve historical state.
2. **Silver Layer (Cleansed)**: Re-hydrates Bronze data, parses HTML into clean text, deduplicates records by URL, and enforces a strict schema.
3. **Gold Layer (Analytics)**: *(In Progress)* Aggregated views for trend analysis and keyword frequency.

# Features
- **Medallion Architecture**: Strict separation of data concerns.
- **Memory-Efficient Processing**: Uses **JSONL** to handle large datasets line-by-line, preventing "Out of Memory" errors.
- **Deduplication**: Built-in logic to ensure article uniqueness based on source URLs.

# Why a custom news pipeline?
I wanted hands-on experience building a production-style pipeline and cleaning data myself. I also wanted a way to get the most up-to-date stories from multiple news sources without the hassle of visiting each website. 

Since I am in charge of the scraper, I can choose exactly which sources to prioritize. This avoids being locked into a specific news ecosystem because **I own my own data**.

# Getting Started
Currently, the pipeline runs locally. 
1. Install dependencies: `pip install -r requirements.txt`
2. Configure targets in `src/app.py`.
3. Run the orchestrator: `python src/app.py`
