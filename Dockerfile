FROM python:3.12-slim
WORKDIR /app
ENV PYTHONPATH=/app

RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates && \
    rm -rf /var/lib/apt/lists/*

COPY certs/ /tmp/custom-certs/

RUN set -eu; \
    if find /tmp/custom-certs -maxdepth 1 -type f -name '*.crt' \
        -print -quit | grep -q .; then \
        cp /tmp/custom-certs/*.crt /usr/local/share/ca-certificates/; \
        update-ca-certificates; \
        echo "Custom CA certificates installed."; \
    else \
        echo "No custom CA certificates found; using system defaults."; \
    fi; \
    rm -rf /tmp/custom-certs

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD python data_extraction/Execute.py && \
    python data_extraction/CohortPatientsAdditionalFilters.py && \
    python data_analysis/Graphs.py