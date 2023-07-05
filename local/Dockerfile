FROM python:3.9-slim

RUN apt-get update && apt-get install -y apt-utils default-jdk curl wget graphviz -y
RUN pip install --upgrade pip
COPY ./requirements.txt .
COPY ./local/requirements.txt requirements_dev.txt

RUN pip install -r requirements.txt
RUN pip install -r requirements_dev.txt

WORKDIR .
RUN wget https://archive.apache.org/dist/spark/spark-3.3.2/spark-3.3.2-bin-hadoop3-scala2.13.tgz
RUN tar xf spark-*
RUN mv spark-3.3.2-bin-hadoop3-scala2.13 /opt/spark

ENV SPARK_HOME "/opt/spark"
ENV HADOOP_HOME "${SPARK_HOME}"
ENV PATH "${PATH}:${SPARK_HOME}/bin:${SPARK_HOME}/sbin"
ENV PYSPARK_PYTHON "/usr/local/bin/python3"
ENV PYSPARK_DRIVER_PYTHON "${PYSPARK_PYTHON}"

COPY /flypipe /flypipe
ENV PYTHONPATH "${PYTHONPATH}:/local:/flypipe:/:"