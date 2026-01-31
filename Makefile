.PHONY: lint test build check

lint:
	ruff check . --fix
	ruff format .
	mypy .

test:
	pytest

check: lint test

build:
	docker build -f docker/airflow/Dockerfile -t my-pet-project:latest docker/airflow