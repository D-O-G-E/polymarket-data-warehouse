# One image containing the ingestion jobs and the dbt project, so the
# pipeline runs identically on a laptop, in CI, or anywhere else.
#
#   docker build -t polymarket-dw .
#   docker run --rm -e PDW_BQ_PROJECT=... -v ./data:/app/data polymarket-dw harvest-prices
#   docker run --rm --entrypoint dbt polymarket-dw build --project-dir /app/dbt --profiles-dir /app/dbt

FROM python:3.12-slim

WORKDIR /app

COPY pyproject.toml README.md ./
COPY ingestion ./ingestion
RUN pip install --no-cache-dir ".[warehouse]"

COPY dbt ./dbt

ENTRYPOINT ["python", "-m", "ingestion"]
