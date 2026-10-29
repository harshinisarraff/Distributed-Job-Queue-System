FROM python:3.11-slim
WORKDIR /app
ENV PYTHONUNBUFFERED=1
RUN groupadd -r app && useradd -r -g app app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY app ./app
COPY benchmarks ./benchmarks
RUN mkdir /app/data && chown -R app:app /app
USER app
EXPOSE 8000
CMD ["python", "-m", "uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
