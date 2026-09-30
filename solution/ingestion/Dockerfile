FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py load_reference.py ./
COPY src/ ./src/
COPY tests/ ./tests/

CMD ["python", "-u", "main.py"]
