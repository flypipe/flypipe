SHELL                       :=/bin/bash

DOCKER_DIR          =.docker
PYTEST_THREADS      ?=$(shell echo $$((`getconf _NPROCESSORS_ONLN` / 3)))
min_coverage        =80
min_branch_coverage =95
USE_SPARK_CONNECT   =0
version			    ?=

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
	mkdir -p .docker/logs/spark-master .docker/logs/spark-worker .docker/logs/spark-connect
	chmod -R 777 .docker/logs
	docker-compose -f $(DOCKER_DIR)/docker-compose.yaml build
.PHONY: build

up: build
	docker-compose -f $(DOCKER_DIR)/docker-compose.yaml up
.PHONY: up

down:
	rm -r $(DOCKER_DIR)/data || true
	docker-compose -f $(DOCKER_DIR)/docker-compose.yaml down -v --rmi all
.PHONY: down

ping:
	docker-compose -f $(DOCKER_DIR)/docker-compose.yaml run --entrypoint "" flypipe-jupyter sh -c "chmod +x ./wait-for-it.sh && ./wait-for-it.sh -h flypipe-mariadb -p 3306"
	docker-compose -f $(DOCKER_DIR)/docker-compose.yaml run --entrypoint "" flypipe-jupyter sh -c "chmod +x ./wait-for-it.sh && ./wait-for-it.sh -h flypipe-hive-metastore -p 9083"
.PHONY: ping

black:
	docker-compose -f $(DOCKER_DIR)/docker-compose.yaml run --rm --entrypoint "" flypipe-jupyter sh -c "black flypipe"
.PHONY: black

black-check:
	docker-compose -f $(DOCKER_DIR)/docker-compose.yaml run --rm --entrypoint "" flypipe-jupyter sh -c "black flypipe --check"
.PHONY: black-check

lint:
	docker-compose -f $(DOCKER_DIR)/docker-compose.yaml run --rm --entrypoint "" flypipe-jupyter sh -c "python -m ruff check flypipe"
.PHONY: lint

coverage:
	docker-compose -f $(DOCKER_DIR)/docker-compose.yaml run --remove-orphans --entrypoint "" flypipe-jupyter sh -c "export USE_SPARK_CONNECT=$(USE_SPARK_CONNECT) && pytest --rootdir flypipe -n $(PYTEST_THREADS) --ignore=/flypipe/tests/activate/sparkleframe_test.py -k '_test.py' --cov-config=flypipe/.coverage --cov=flypipe --no-cov-on-fail --cov-fail-under=$(min_coverage) flypipe"
.PHONY: coverage

test:
	docker-compose -f $(DOCKER_DIR)/docker-compose.yaml run --remove-orphans --entrypoint "" flypipe-jupyter sh -c "export USE_SPARK_CONNECT=$(USE_SPARK_CONNECT) && pytest -n $(PYTEST_THREADS) -k '_test.py' -vv $(f) --rootdir flypipe"
.PHONY: test

bash: build
	docker-compose -f $(DOCKER_DIR)/docker-compose.yaml run --entrypoint "" -it flypipe-jupyter bash
.PHONY: bash

run:
	docker compose -f $(DOCKER_DIR)/docker-compose.yaml run --remove-orphans flypipe-jupyter sh -c "python $(f)"
.PHONY: run

wheel:
	flit build --format wheel
.PHONY: wheel

pip-compile:
	pip install -r requirements-pkg.in
	pip-compile requirements-pkg.in --no-annotate --no-header
	pip-compile requirements-dev.in --no-annotate --no-header
.PHONY: pip-compile

pr-check: black lint
	make coverage USE_SPARK_CONNECT=0
	make coverage USE_SPARK_CONNECT=1
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