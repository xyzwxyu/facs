.PHONY: venv install dev lint format type test clean run docker-build docker-up docker-down docker-logs docker-dev-up docker-dev-down docker-dev-logs docker-shell docker-ps docker-clean bump-patch bump-minor bump-major

VENV := .venv
PY := $(VENV)/bin/python
PIP := $(VENV)/bin/pip

venv:
	python3 -m venv $(VENV)

install: venv
	$(PIP) install --upgrade pip
	$(PIP) install -e .

dev: venv
	$(PIP) install --upgrade pip
	$(PIP) install -e .[dev]
	$(VENV)/bin/pre-commit install

lint:
	$(VENV)/bin/ruff check .

format:
	$(VENV)/bin/ruff format .

type:
	$(VENV)/bin/mypy src/

test:
	$(VENV)/bin/pytest

run:
	$(PY) -m src

# Docker commands
docker-build:
	docker build -f docker/Dockerfile -t facs:latest .

docker-facs-up: docker-build
	cd docker/compose && docker-compose -f docker-compose.dev.yml up -d facs

docker-facs-stop:
	cd docker/compose && docker-compose -f docker-compose.dev.yml stop facs

docker-facs-logs:
	cd docker/compose && docker-compose -f docker-compose.dev.yml logs -f facs

docker-facs-shell:
	cd docker/compose && docker-compose -f docker-compose.dev.yml exec facs /bin/bash

docker-db-up:
	cd docker/compose && docker-compose -f docker-compose.dev.yml up -d postgres

docker-db-stop:
	cd docker/compose && docker-compose -f docker-compose.dev.yml stop postgres

docker-db-logs:
	cd docker/compose && docker-compose -f docker-compose.dev.yml logs -f postgres

docker-db-shell:
	cd docker/compose && docker-compose -f docker-compose.dev.yml exec postgres /bin/bash

docker-ps:
	cd docker/compose && docker-compose ps

docker-clean:
	cd docker/compose && docker-compose down -v
	docker system prune -f

clean: docker-clean
	rm -rf $(VENV)

# Version bumping
bump-patch:
	$(VENV)/bin/bump-my-version bump patch

bump-minor:
	$(VENV)/bin/bump-my-version bump minor

bump-major:
	$(VENV)/bin/bump-my-version bump major
