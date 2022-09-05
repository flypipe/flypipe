FROM python:3.8.10

RUN apt-get update && apt-get install -y apt-utils default-jdk curl -y
RUN pip install --upgrade pip
COPY ./notebooks ./notebooks
COPY ./src ./flypipe
COPY ./flypipe_dev ./flypipe_dev
COPY ./wait-for-it.sh .
COPY ./requirements.txt .

WORKDIR .
RUN pip install jupyterlab==3.4.5 pyspark[sql,pandas_on_spark]==3.2.2 pyarrow==8.0.0

RUN wget https://dlcdn.apache.org/spark/spark-3.2.2/spark-3.2.2-bin-hadoop3.2.tgz
RUN tar xf spark-*
run mv spark-3.2.2-bin-hadoop3.2 /opt/spark

ENV SPARK_HOME "/opt/spark"
ENV HADOOP_HOME "${SPARK_HOME}"
ENV PATH "${PATH}:${SPARK_HOME}/bin:${SPARK_HOME}/sbin"
ENV PYSPARK_PYTHON "/usr/local/bin/python3"
ENV PYSPARK_DRIVER_PYTHON "${PYSPARK_PYTHON}"

ENV PYTHONPATH "${PYTHONPATH}:/flypipe_dev:/flypipe:/:"
# WORKDIR /notebooks