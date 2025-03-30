# Use lightweight Python image
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the code
COPY . .

# Set environment variables (can be overridden at runtime)
ENV SNOWFLAKE_ACCOUNT=your_account
ENV SNOWFLAKE_USER=your_user
ENV SNOWFLAKE_PASSWORD=your_password
ENV SNOWFLAKE_DATABASE=COVID
ENV SNOWFLAKE_WAREHOUSE=COMPUTE_WH

# Run the script
ENTRYPOINT ["python", "ingestion.py"]
