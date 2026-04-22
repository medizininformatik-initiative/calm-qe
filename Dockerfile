FROM python:latest

WORKDIR /app

COPY . .

RUN pip install --no-cache-dir -r requirements.txt

CMD python data_extraction/ExtractCohortwithResourcesExecute.py && \
    python data_extraction/CohortPatientsAdditionalFilters.py && \
    python data_analysis/Graphs.py