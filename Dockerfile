FROM python:3.8-slim-buster

WORKDIR /app

# Set environment variables
ENV POSTGRES_USER=postgres \
    POSTGRES_PASSWORD=diansdomashna \
    POSTGRES_DB=primary_postgres \
    POSTGRES_HOST=postgres-primary \
    POSTGRES_PORT=5432 \
    KAFKA_BROKER=kafka:9092

# Install required Python packages
COPY requirements.txt . 
RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copy the rest of the code
COPY . .

# Expose port (if needed)
EXPOSE 8000

CMD ["python", "data_analysis_service.py"]
