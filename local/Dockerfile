FROM python:3.9-slim

RUN apt-get update && apt-get install -y apt-utils build-essential default-jdk curl wget graphviz man-db
RUN pip install --upgrade pip

WORKDIR .
RUN wget https://archive.apache.org/dist/spark/spark-3.5.4/spark-3.5.4-bin-hadoop3.tgz
RUN tar xf spark-* && \
    mv spark-3.5.4-bin-hadoop3 /opt/spark && \
    rm spark-*.tgz

ENV SPARK_HOME "/opt/spark"
ENV HADOOP_HOME "${SPARK_HOME}"
ENV PATH "${PATH}:${SPARK_HOME}/bin:${SPARK_HOME}/sbin"
ENV PYSPARK_PYTHON "/usr/local/bin/python3"
ENV PYSPARK_DRIVER_PYTHON "${PYSPARK_PYTHON}"
ENV USE_SPARK_CONNECT "0"

COPY ./requirements.txt .
COPY ./local/requirements.txt requirements_dev.txt
RUN pip install -r requirements.txt
RUN pip install -r requirements_dev.txt

## Set up Pandoc
RUN wget https://github.com/jgm/pandoc/releases/download/3.6.2/pandoc-3.6.2-1-`dpkg --print-architecture`.deb
RUN dpkg -i pandoc-3.6.2-1-`dpkg --print-architecture`.deb
## Check installation
RUN pandoc -v

### Set up NodeJS
RUN curl -fsSL https://deb.nodesource.com/setup_23.x -o nodesource_setup.sh
RUN bash nodesource_setup.sh
RUN apt-get install -y nodejs
RUN node -v

## Install dbml-renderer
RUN npm install -g @softwaretechnik/dbml-renderer
RUN npm install -g dbdocs

COPY /flypipe /flypipe
ENV PYTHONPATH "${PYTHONPATH}:/local:/flypipe:/:"