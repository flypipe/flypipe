FROM python:3.11-slim

USER root

# Useful OS utilities
RUN apt-get update && apt-get install -y --no-install-recommends \
    apt-utils curl wget graphviz gcc gnupg -qq git && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

### Set up NodeJS
RUN curl -fsSL https://deb.nodesource.com/setup_23.x -o nodesource_setup.sh
RUN bash nodesource_setup.sh
RUN apt-get install -y nodejs
RUN node -v

## Install dbml-renderer
RUN npm install -g @softwaretechnik/dbml-renderer
RUN npm install -g dbdocs

COPY ./requirements-dev-snowpark.txt .
RUN pip install -r requirements-dev-snowpark.txt

COPY ./flypipe /flypipe

# Environment variables
ENV PYTHONPATH=${PYTHONPATH}:/local:/flypipe:/:

WORKDIR /

