FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Install dependencies first for better layer caching.
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy the worker code.
COPY bot.py ./

# Run as non-root user.
RUN useradd --create-home appuser
USER appuser

CMD ["python", "bot.py"]
