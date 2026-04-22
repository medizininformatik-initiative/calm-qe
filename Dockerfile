FROM python:latest
WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD python data_extraction/ExtractCohortwithResourcesExecute.py && \
    python data_extraction/CohortPatientsAdditionalFilters.py && \
    python data_analysis/Graphs.py