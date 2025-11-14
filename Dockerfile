FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1

WORKDIR /app

# System deps (if you ever need more, add them here)
RUN apt-get update && apt-get install -y --no-install-recommends \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["python", "main.py"]
