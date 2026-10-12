FROM python:3.12-slim
WORKDIR /app
ENV PYTHONPATH=/app

COPY certs/*.crt /usr/local/share/ca-certificates/
RUN update-ca-certificates
ENV REQUESTS_CA_BUNDLE=/etc/ssl/certs/ca-certificates.crt \
    SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD python data_extraction/Execute.py && \
    python data_extraction/CohortPatientsAdditionalFilters.py && \
    python data_analysis/Graphs.py