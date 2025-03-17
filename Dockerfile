FROM python:3.8-slim-buster

WORKDIR /app

# Install required Python packages
COPY requirements.txt . 
RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copy the rest of the code
COPY . .

CMD ["python", "data_analysis_service.py"]
