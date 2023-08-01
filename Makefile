SHELL                       :=/bin/bash

LOCAL_DIR=./local
workers=1
file=flypipe
min_coverage=85
min_branch_coverage=95

up:
	docker compose -f $(LOCAL_DIR)/docker-compose.yaml up --build
.PHONY: up

down:
	rm -r $(LOCAL_DIR)/data || true
	docker compose -f $(LOCAL_DIR)/docker-compose.yaml down -v --rmi all
.PHONY: down

ping:
	docker compose -f $(LOCAL_DIR)/docker-compose.yaml run --entrypoint "" flypipe-jupyter ./wait-for-it.sh -h flypipe-mariadb -p 3306
	docker compose -f $(LOCAL_DIR)/docker-compose.yaml run --entrypoint "" flypipe-jupyter ./wait-for-it.sh -h flypipe-hive-metastore -p 9083
.PHONY: ping

black:
	black flypipe
.PHONY: black

black-check:
	black flypipe --check
.PHONY: black-check

pylint:
	python -m pylint flypipe
.PHONY: pylint

coverage:
	coverage run --source=flypipe -m pytest flypipe
	coverage report --fail-under=$(min_coverage)
.PHONY: coverage

branch-coverage:
	coverage xml
	diff-cover coverage.xml --fail-under=$(min_branch_coverage)
.PHONY: branch-coverage

jupyter-bash:
	docker compose -f $(LOCAL_DIR)/docker-compose.yaml build
	docker compose -f $(LOCAL_DIR)/docker-compose.yaml run --entrypoint "" -it flypipe-jupyter bash
.PHONY: jupyter-bash

docs:
	sh ./docs/build_docs_dev.sh
.PHONY: docs

build:
	flit build --format wheel
.PHONY: build