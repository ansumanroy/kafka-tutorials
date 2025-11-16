PYTHON_ENV ?= .venv
PYTHON ?= python3

.PHONY: python-env
python-env:
	$(PYTHON) -m venv $(PYTHON_ENV)
	$(PYTHON_ENV)/bin/pip install --upgrade pip
	$(PYTHON_ENV)/bin/pip install -r python/requirements.txt

.PHONY: python-check-connection
python-check-connection: python-env
	. infra/env.msk 2>/dev/null || . infra/env.local 2>/dev/null || true
	$(PYTHON_ENV)/bin/python python/ch01_environment/check_connection.py
