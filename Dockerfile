#FROM apache/airflow:2.7.0 didnt work
FROM ubuntu:22.04
ARG DEBIAN_FRONTEND=noninteractive
ARG AIRFLOW_VERSION=2.7.0

ENV TZ=Etc/UTC
ENV AIRFLOW__CORE__LOAD_EXAMPLES=False
#ENV AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow_user:airflow_pass@localhost/airflow_db

# install os dependencies
RUN apt-get update \
  && apt-get install -y \
  sudo build-essential \
  python3-all-dev python3-dev \
  python3-gdal python3-psycopg2 \
  python3-pip gdal-bin python3-gdal \
  libgdal-dev curl

COPY . /usr/src/airflow/
WORKDIR /usr/src/airflow

# install python dependencies
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r ./requirements.txt

RUN chmod +x ./entrypoint.sh
CMD ["./entrypoint.sh"]