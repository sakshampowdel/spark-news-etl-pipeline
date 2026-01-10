# News ETL Pipeline
A distributed ETL data pipeline designed to aggregate and analyze global news trends. Using a **Medallion Architecture**, the system extracts raw data, transforms it into a structured format, and utilizes Spark-driven analysis to identify top trending keywords across multiple publishers.

# Technologies
* **Python**: Core logic and pipeline orchestration.
* **PySpark**: Distributed processing engine used for Gold-layer aggregations and multi-source pivoting.
* **Docker & Docker Compose**: Containerization for environment parity and simplified deployment.
* **BeautifulSoup**: Surgical HTML parsing and content extraction.
* **Requests**: Resilient data extraction from diverse news feeds.
* **Dataclasses & Typing**: Strict schema enforcement and code reliability.

# Architecture


1. **Bronze Layer (Raw)**: Captures raw HTML responses and metadata. This layer is append-only to preserve the historical state of the news cycle.
2. **Silver Layer (Cleansed)**: Re-hydrates Bronze data, parses HTML into clean text, deduplicates records by URL, and enforces a strict schema.
3. **Gold Layer (Analytics)**: High-performance Spark analysis that performs tokenization, stop-word removal, and dynamic pivot aggregations. Data is persisted in **Parquet** format for efficient downstream querying.

# Features
* **Medallion Architecture**: Industry-standard separation of data concerns for reliable pipelines.
* **Spark-Optimized Aggregations**: Uses PySpark to calculate keyword frequency across different publishers (NPR, Reuters, WaPo) in a single pass.
* **Containerized Execution**: Entire stack is containerized with Docker, ensuring the Spark session and dependencies run identically on any machine.
* **Deduplication & Schema Enforcement**: Built-in logic to ensure article uniqueness and data integrity across all layers.

# Why a custom news pipeline?
I wanted hands-on experience building a production-style pipeline using the same tools used by big data teams (Spark, Docker, Parquet). I also wanted a way to get the most up-to-date stories from multiple sources without visiting each site individually. 

By building my own scraper and analyzer, I can choose exactly which sources to prioritize. This avoids being locked into a specific news ecosystem or algorithm because **I own my own data**.

# Getting Started
The pipeline is fully containerized for ease of use.
1. **Clone the repo**: `git clone <repo-url>`
2. **Spin up the stack**: `docker-compose up --build`
3. **View Results**: The orchestrator will output the "Top Trending Keywords" directly to the console and save the Gold datasets to the `/data` directory.
