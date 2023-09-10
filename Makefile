.PHONY: clean clean-test clean-pyc clean-build help format lint test coverage validate
.DEFAULT_GOAL := help
PROJECT := downsat

define BROWSER_PYSCRIPT ## python script that opens file in a browser
import os, webbrowser, sys

from urllib.request import pathname2url

webbrowser.open("file://" + pathname2url(os.path.abspath(sys.argv[1])))
endef
export BROWSER_PYSCRIPT

BROWSER := python -c "$$BROWSER_PYSCRIPT"

define PRINT_HELP_PYSCRIPT ## python script that prints all goals the user can request
import re, sys

for line in sys.stdin:
	match = re.match(r'^([a-zA-Z_-]+):.*?## (.*)$$', line)  # ignore file targets
	if match:
		target, help = match.groups()
		print("%-20s %s" % (target, help))
endef
export PRINT_HELP_PYSCRIPT

help: ## print help message listing all goals
	@python -c "$$PRINT_HELP_PYSCRIPT" < $(MAKEFILE_LIST)

clean: clean-build clean-pyc clean-test ## remove all build, test, coverage and Python artifacts

clean-build: ## remove build artifacts
	rm -fr build/
	rm -fr dist/
	rm -fr .eggs/
	find . -name '*.egg-info' -exec rm -fr {} +
	find . -name '*.egg' -exec rm -f {} +

clean-pyc: ## remove Python file artifacts
	find . -name '*.pyc' -exec rm -f {} +
	find . -name '*.pyo' -exec rm -f {} +
	find . -name '*~' -exec rm -f {} +
	find . -name '__pycache__' -exec rm -fr {} +

clean-test: ## remove test and coverage artifacts
	rm -fr .tox/
	rm -f .coverage
	rm -fr htmlcov/
	rm -fr .pytest_cache

format:  ## format code using autoflake, black and isort
	poetry run autoflake --remove-all-unused-imports --recursive --remove-unused-variables --in-place $(PROJECT) --exclude=__init__.py
	poetry run autoflake --remove-all-unused-imports --recursive --remove-unused-variables --in-place tests --exclude=__init__.py
	poetry run black $(PROJECT)
	poetry run black tests
	poetry run isort $(PROJECT)
	poetry run isort tests

lint: ## check style with flake8 and mypy
	poetry run mypy --install-types --non-interactive --ignore-missing-imports --disallow-incomplete-defs --disallow-untyped-defs -p $(PROJECT)
	poetry run mypy --install-types --non-interactive --ignore-missing-imports --disallow-incomplete-defs --disallow-untyped-defs -p tests
	poetry run flake8 $(PROJECT)
	poetry run flake8 tests

test: ## run pytest
	poetry run pytest --cov=$(PROJECT) --cov-report=term-missing --cov-report=xml -vv -rxXs tests

coverage: ## check code coverage quickly with the default Python
	poetry run coverage run --source tests -m pytest
	poetry run coverage report -m
	poetry run coverage html
	poetry run $(BROWSER) htmlcov/index.html

validate: format lint test ## format, lint, test
