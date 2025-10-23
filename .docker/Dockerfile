FROM apache/spark:3.5.7-java17-python3@sha256:7897b9a69efa94caccc52579e310a2fe68fb75b88a3d55b97b31568e5e233176 AS base

USER root

# base
RUN apt-get update && apt-get install -y apt-utils curl wget graphviz gcc gnupg \
    software-properties-common && apt-get clean && rm -rf /var/lib/apt/lists/*

# use python3 as python
RUN ln -s $(which python3) /usr/local/bin/python
ENV USE_SPARK_CONNECT=0

### Set up NodeJS
RUN curl -fsSL https://deb.nodesource.com/setup_23.x -o nodesource_setup.sh
RUN bash nodesource_setup.sh
RUN apt-get install -y nodejs
RUN node -v

## Install dbml-renderer
RUN npm install -g @softwaretechnik/dbml-renderer
RUN npm install -g dbdocs

COPY ./requirements-dev.txt .
RUN pip install -r requirements-dev.txt

COPY ./flypipe /flypipe
ENV PYTHONPATH "${PYTHONPATH}:/local:/flypipe:/:"

WORKDIR /
