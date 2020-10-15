FROM python:3.8
RUN pip install asyncpg==0.21.0
WORKDIR /app
ADD dataset/ dataset/
ADD dataset_to_db.py .
