# Use the official Microsoft Playwright Python image (Ubuntu-based)
FROM mcr.microsoft.com/playwright/python:v1.57.0-noble

USER root

# 1. Update to Java 17 (Required for modern PySpark)
RUN apt-get update && apt-get install -y openjdk-17-jre-headless && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# 2. Update the Environment Path for Java 17
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$PATH:$JAVA_HOME/bin

# 3. Create a non-root user for safe scraping
RUN useradd -m newsuser
WORKDIR /home/newsuser/app

# 4. Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 5. Copy the application and fix permissions
COPY --chown=newsuser:newsuser . .

# 6. Switch to the non-root user
USER newsuser

# Ensure local imports work
ENV PYTHONPATH=/home/newsuser/app/src

# Start the application
CMD ["python", "src/app.py"]