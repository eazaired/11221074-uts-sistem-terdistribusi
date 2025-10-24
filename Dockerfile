FROM python:3.11-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 DB_PATH=/app/data/data.db
WORKDIR /app

RUN adduser --disabled-password --gecos '' appuser && chown -R appuser:appuser /app
USER appuser

RUN mkdir -p /app/data

COPY --chown=appuser:appuser requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY --chown=appuser:appuser src/ ./src/
EXPOSE 8080
CMD ["python", "-m", "src.main"]
