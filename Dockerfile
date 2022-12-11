FROM python:3.9-slim

RUN apt-get update && apt-get install -y apt-utils default-jdk curl wget -y
RUN pip install --upgrade pip
COPY ./wait-for-it.sh .

COPY ./requirements.txt .
COPY ./requirements_dev.txt .


WORKDIR .
RUN pip install -r requirements.txt
RUN pip install -r requirements_dev.txt

RUN wget https://dlcdn.apache.org/spark/spark-3.3.1/spark-3.3.1-bin-hadoop3-scala2.13.tgz
RUN tar xf spark-*
run mv spark-3.3.1-bin-hadoop3-scala2.13 /opt/spark

ENV SPARK_HOME "/opt/spark"
ENV HADOOP_HOME "${SPARK_HOME}"
ENV PATH "${PATH}:${SPARK_HOME}/bin:${SPARK_HOME}/sbin"
ENV PYSPARK_PYTHON "/usr/local/bin/python3"
ENV PYSPARK_DRIVER_PYTHON "${PYSPARK_PYTHON}"

ENV PYTHONPATH "${PYTHONPATH}:/local:/flypipe:/:"