SHELL                       :=/bin/bash

LOCAL_DIR=./local
PYTEST_THREADS ?=$(shell echo $$((`getconf _NPROCESSORS_ONLN` / 3)))
min_coverage=85
min_branch_coverage=95
USE_SPARK_CONNECT=0

build-image:
	docker-compose -f $(LOCAL_DIR)/docker-compose.yaml build
.PHONY: build-image

up: build-image
	docker-compose -f $(LOCAL_DIR)/docker-compose.yaml up
.PHONY: up

down:
	rm -r $(LOCAL_DIR)/data || true
	docker-compose -f $(LOCAL_DIR)/docker-compose.yaml down -v --rmi all
.PHONY: down

ping:
	docker-compose -f $(LOCAL_DIR)/docker-compose.yaml run --entrypoint "" flypipe-jupyter sh -c "chmod +x ./wait-for-it.sh && ./wait-for-it.sh -h flypipe-mariadb -p 3306"
	docker-compose -f $(LOCAL_DIR)/docker-compose.yaml run --entrypoint "" flypipe-jupyter sh -c "chmod +x ./wait-for-it.sh && ./wait-for-it.sh -h flypipe-hive-metastore -p 9083"
.PHONY: ping

black:
	docker-compose -f $(LOCAL_DIR)/docker-compose.yaml run --rm --entrypoint "" flypipe-jupyter sh -c "black flypipe"
.PHONY: black

black-check:
	docker-compose -f $(LOCAL_DIR)/docker-compose.yaml run --rm --entrypoint "" flypipe-jupyter sh -c "black flypipe --check"
.PHONY: black-check

lint:
	docker-compose -f $(LOCAL_DIR)/docker-compose.yaml run --rm --entrypoint "" flypipe-jupyter sh -c "python -m ruff check flypipe"
.PHONY: lint

coverage:
	docker-compose -f $(LOCAL_DIR)/docker-compose.yaml run --remove-orphans --entrypoint "" flypipe-jupyter sh -c "export USE_SPARK_CONNECT=$(USE_SPARK_CONNECT) && pytest --rootdir flypipe -n $(PYTEST_THREADS) -k '_test.py' --cov=flypipe --no-cov-on-fail --cov-fail-under=$(min_coverage) flypipe"
.PHONY: coverage

test:
	docker-compose -f $(LOCAL_DIR)/docker-compose.yaml run --remove-orphans --entrypoint "" flypipe-jupyter sh -c "export USE_SPARK_CONNECT=$(USE_SPARK_CONNECT) && pytest -n $(PYTEST_THREADS) -k '_test.py' -vv $(f) --rootdir flypipe"
.PHONY: test

branch-coverage:
	coverage xml
	diff-cover coverage.xml --fail-under=$(min_branch_coverage)
.PHONY: branch-coverage

jupyter-bash: build-image
	docker-compose -f $(LOCAL_DIR)/docker-compose.yaml run --entrypoint "" -it flypipe-jupyter bash
.PHONY: jupyter-bash

docs:
	sh docs/build_docs.sh
.PHONY: docs

docs-dev: build-image
	docker-compose -f $(LOCAL_DIR)/docker-compose.yaml run --remove-orphans --entrypoint "" flypipe-jupyter sh -c "sh ./docs/build_docs_dev.sh"
.PHONY: docs-dev

build:
	flit build --format wheel
.PHONY: build

spark-bash:
	docker-compose -f $(LOCAL_DIR)/docker-compose.yaml build
	docker-compose -f $(LOCAL_DIR)/docker-compose.yaml run --entrypoint "" -it spark-master bash
.PHONY: spark-bash