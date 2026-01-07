SHELL                       :=/bin/bash

DOCKER_BASE_DIR    =.docker
PYTEST_THREADS      ?=$(shell echo $$((`getconf _NPROCESSORS_ONLN` / 3)))
RUN_MODE            ?=CORE
version			    ?=

# Map RUN_MODE to Docker directory, test pattern, and coverage threshold
ifeq ($(RUN_MODE),CORE)
    DOCKER_DIR = $(DOCKER_BASE_DIR)/core
    CONTAINER_NAME = flypipe-core
    TEST_PATTERN = core_test.py
    COVERAGE_CONFIG = .coverage-core
    MIN_COVERAGE = 79
else ifeq ($(RUN_MODE),SPARK)
    DOCKER_DIR = $(DOCKER_BASE_DIR)/spark
    CONTAINER_NAME = flypipe-spark
    TEST_PATTERN = pyspark_test.py
    COVERAGE_CONFIG = .coverage-pyspark
    MIN_COVERAGE = 62
else ifeq ($(RUN_MODE),SPARK_CONNECT)
    DOCKER_DIR = $(DOCKER_BASE_DIR)/spark
    CONTAINER_NAME = flypipe-spark
    TEST_PATTERN = pyspark_test.py
    COVERAGE_CONFIG = .coverage-pyspark
    MIN_COVERAGE = 80
else ifeq ($(RUN_MODE),SNOWFLAKE)
    DOCKER_DIR = $(DOCKER_BASE_DIR)/snowflake
    CONTAINER_NAME = flypipe-snowflake
    TEST_PATTERN = snowpark_test.py
    COVERAGE_CONFIG = .coverage-snowpark
    MIN_COVERAGE = 61
else
    # Default to core for unknown modes
    DOCKER_DIR = $(DOCKER_BASE_DIR)/core
    CONTAINER_NAME = flypipe-core
    TEST_PATTERN = core_test.py
    COVERAGE_CONFIG = .coverage-core
    MIN_COVERAGE = 80
endif

min_branch_coverage =95

export PYTHONPATH := $(PYTHONPATH):./flypipe

# This block checks for .env and exports it for all recipes
ifneq (,$(wildcard .env))
  include .env
  export $(shell sed 's/=.*//' .env)
endif

notebooks-clean:
	python docs/notebooks/clean.py
.PHONY: clean

build:
	@echo "Building for RUN_MODE=$(RUN_MODE) using $(DOCKER_DIR)"
ifneq ($(filter $(RUN_MODE),SPARK SPARK_CONNECT),)
	@echo "Creating Spark log directories..."
	mkdir -p $(DOCKER_BASE_DIR)/spark/logs/spark-master $(DOCKER_BASE_DIR)/spark/logs/spark-worker $(DOCKER_BASE_DIR)/spark/logs/spark-connect
	chmod -R 777 $(DOCKER_BASE_DIR)/spark/logs || true
endif
	docker-compose -f $(DOCKER_DIR)/docker-compose.yaml build
.PHONY: build

up: build
	docker-compose -f $(DOCKER_DIR)/docker-compose.yaml up
.PHONY: up

down:
	rm -r $(DOCKER_DIR)/data || true
	docker-compose -f $(DOCKER_DIR)/docker-compose.yaml down -v --rmi all
.PHONY: down

black:
	docker-compose -f $(DOCKER_DIR)/docker-compose.yaml run --rm --entrypoint "" $(CONTAINER_NAME) sh -c "black flypipe"
.PHONY: black

black-check:
	docker-compose -f $(DOCKER_DIR)/docker-compose.yaml run --rm --entrypoint "" $(CONTAINER_NAME) sh -c "black flypipe --check"
.PHONY: black-check

lint:
	docker-compose -f $(DOCKER_DIR)/docker-compose.yaml run --rm --entrypoint "" $(CONTAINER_NAME) sh -c "python -m ruff check flypipe"
.PHONY: lint

coverage:
	docker-compose -f $(DOCKER_DIR)/docker-compose.yaml run --remove-orphans --entrypoint "" $(CONTAINER_NAME) bash -c "export RUN_MODE=$(RUN_MODE) && pytest --rootdir flypipe -n $(PYTEST_THREADS) --ignore=/flypipe/tests/activate/sparkleframe_pyspark_test.py --override-ini='python_files=*$(TEST_PATTERN)' --cov-config=flypipe/$(COVERAGE_CONFIG) --cov=flypipe --no-cov-on-fail --cov-fail-under=$(MIN_COVERAGE) flypipe"
.PHONY: coverage

test:
	docker-compose -f $(DOCKER_DIR)/docker-compose.yaml run --remove-orphans --entrypoint "" $(CONTAINER_NAME) bash -c "export RUN_MODE=$(RUN_MODE) && pytest -n $(PYTEST_THREADS) -vv --override-ini='python_files=*$(TEST_PATTERN)' \$$(if [ -n '$(f)' ]; then echo '$(f)'; else find flypipe -name '*$(TEST_PATTERN)' -type f; fi) --rootdir flypipe"
.PHONY: test

bash: build
	docker-compose -f $(DOCKER_DIR)/docker-compose.yaml run --entrypoint "" -it $(CONTAINER_NAME) bash
.PHONY: bash

run:
	docker compose -f $(DOCKER_DIR)/docker-compose.yaml run --remove-orphans $(CONTAINER_NAME) sh -c "python $(f)"
.PHONY: run

wheel:
	flit build --format wheel
.PHONY: wheel

pip-compile:
	pip install -r requirements-pkg.in
	pip-compile requirements-pkg.in --no-annotate --no-header
	pip-compile requirements-dev.in --no-annotate --no-header
	pip-compile requirements-dev-pyspark.in --no-annotate --no-header
	pip-compile requirements-dev-snowpark.in --no-annotate --no-header
.PHONY: pip-compile

pr-check: black lint
	make coverage RUN_MODE=CORE
	make coverage RUN_MODE=SPARK
	make coverage RUN_MODE=SPARK_CONNECT
	make coverage RUN_MODE=SNOWFLAKE
	make test f=flypipe/tests/activate/sparkleframe_test.py
	pytest scripts/*_test.py
.PHONY: pr-check

githooks:
	chmod +x .github/hooks/prepare-commit-msg
	git config --local core.hooksPath .github/hooks
	echo "Custom Git hooks enabled (core.hooksPath set to .githooks)"
.PHONY: githooks

setup: pip-compile githooks
	pip install -r requirements-dev.txt
	make build
.PHONY: setup

docs:
	@if [ ! -f changelog.md ]; then \
		echo "changelog.md does not exist, running command..."; \
		python scripts/generate_changelog.py; \
	fi
	cp changelog.md ./docs
	mkdocs serve
.PHONY: docs

docs-deploy:
	@[ -n "$(version)" ] || (echo "ERROR: version is required"; exit 1)
	cp changelog.md ./docs
	mike deploy --allow-empty --push --update-aliases $(shell echo $(version) | awk -F. '{print $$1"."$$2}') latest
	mike set-default --push latest
.PHONY: docs-deploy