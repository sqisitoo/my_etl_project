FROM apache/airflow:slim-3.1.0rc2-python3.13
COPY ./requirements.txt /requirements.txt
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt