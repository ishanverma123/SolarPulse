VENV ?= .venv-1
PYTHON := $(VENV)/bin/python
STREAMLIT := $(PYTHON) -m streamlit

.PHONY: install batch dashboard clean

install:
	$(PYTHON) -m pip install -r requirements.txt

batch:
	$(PYTHON) batch/batch_processing.py --input data/historical_space_weather.csv --output output

dashboard:
	mkdir -p .streamlit
	printf '[server]\nheadless = true\nport = 8501\naddress = "127.0.0.1"\n' > .streamlit/config.toml
	HOME="$(PWD)" $(STREAMLIT) run dashboard/app.py --server.port 8501 --server.address 127.0.0.1 --browser.gatherUsageStats false

clean:
	rm -rf output/*
