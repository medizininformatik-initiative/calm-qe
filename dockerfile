FROM python:latest

ENV USER_NAME="YOUR USER NAME"
ENV USER_PASSWORD="YOUR PASSWORD"
ENV SERVER_NAME="YOUR SERVER NAME"

WORKDIR /app

COPY . .

RUN pip install --no-cache-dir -r requirements.txt

CMD python data_extraction/CohortPatientsExecute.py && python data_extraction/ExtractResourcesForCohortExecute.py && python data_analysis/Graphs.py
