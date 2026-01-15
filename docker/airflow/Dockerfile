FROM apache/airflow:slim-3.1.6-python3.10

ARG PYTHON_VERSION=3.10
ARG CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

ADD --chown=airflow:airflow ${CONSTRAINT_URL} /constraints.txt
COPY --chown=airflow:airflow ./requirements.txt /requirements.txt
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt --constraint /constraints.txt