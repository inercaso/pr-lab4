# dockerfile for the distributed key-value store
FROM python:3.11-slim

# set working directory
WORKDIR /app

# copy requirements first for caching
COPY requirements.txt .

# install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# copy application code
COPY app/ ./app/

# expose port
EXPOSE 8000

# run the application
CMD ["python", "-m", "uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
