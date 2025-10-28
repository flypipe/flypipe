FROM spark:3.5.7-python3@sha256:5b75b64a42010ba072c2b0a987cc8d1c4b9ec70e0e99b1c72c7f1871855ff631

USER root

# Useful OS utilities
RUN apt-get update && apt-get install -y --no-install-recommends \
    apt-utils curl wget graphviz gcc gnupg -qq net-tools && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Link python3 to python command line
RUN ln -sf $(which python3) /usr/local/bin/python

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

COPY ./.docker/wait-for-it.sh /wait-for-it.sh
RUN chmod +x /wait-for-it.sh

COPY ./flypipe /flypipe

# Environment variables
ENV USE_SPARK_CONNECT=0
ENV PYTHONPATH=${PYTHONPATH}:/local:/flypipe:/:

WORKDIR /


