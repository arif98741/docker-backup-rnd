ARG PYTHON_IMAGE=python:3.12-slim
FROM ${PYTHON_IMAGE}

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    TZ=UTC

WORKDIR /srv

COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip \
 && pip install --no-cache-dir -r requirements.txt

COPY app ./app

# Staging + spill space for DuckDB. Mount fast local disk here in production.
RUN mkdir -p /var/lib/analytics/staging /var/lib/analytics/tmp /var/lib/analytics/exports

# Never run the pipeline as root.
RUN useradd --uid 10001 --create-home analytics \
 && chown -R analytics:analytics /srv /var/lib/analytics
USER analytics

# Bake the DuckDB extensions into the image so a worker restart never depends
# on reaching extensions.duckdb.org.
RUN python -c "import duckdb; c=duckdb.connect(); c.execute('INSTALL httpfs; INSTALL json;')"

CMD ["python", "-m", "app.archive"]
