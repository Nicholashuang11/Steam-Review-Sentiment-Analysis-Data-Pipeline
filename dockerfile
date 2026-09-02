FROM python:3.12-alpine
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY config.py .
COPY extract.py . 
COPY backfill.py . 
COPY main.py .
COPY storage.py .
CMD ["python","main.py"]
