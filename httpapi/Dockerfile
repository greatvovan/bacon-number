FROM tiangolo/uvicorn-gunicorn-fastapi:python3.8
EXPOSE 8080
RUN pip install poetry==1.0.10
ADD pyproject.toml .
ADD poetry.lock .
RUN (poetry config virtualenvs.create false && poetry install)
ADD pytest.ini .
ADD benchmark.py .
ADD service/ service/
ADD tests/ tests/
