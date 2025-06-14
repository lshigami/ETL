.PHONY: install run-extract run-transform run-load clean

install:
	pip install -e .

run-extract:
	python -m extract.main

run-moving:
	python -m transform.22120334_moving

run-zscore:
	python -m transform.22120334_zscore

run-load:
	python -m load.main

clean:
	find . -type d -name "__pycache__" -exec rm -r {} +
	find . -type f -name "*.pyc" -delete
	find . -type f -name "*.pyo" -delete
	find . -type f -name "*.pyd" -delete
	find . -type f -name ".coverage" -delete
	find . -type d -name "*.egg-info" -exec rm -r {} +
	find . -type d -name "*.egg" -exec rm -r {} +
	find . -type d -name ".pytest_cache" -exec rm -r {} +
	find . -type d -name ".coverage" -exec rm -r {} +
	find . -type d -name "htmlcov" -exec rm -r {} +
	find . -type d -name ".tox" -exec rm -r {} +
	find . -type d -name ".venv" -exec rm -r {} +
	find . -type d -name "venv" -exec rm -r {} +
	find . -type d -name "env" -exec rm -r {} +
	find . -type d -name ".env" -exec rm -r {} +
	find . -type d -name ".idea" -exec rm -r {} +
	find . -type d -name ".vscode" -exec rm -r {} + 