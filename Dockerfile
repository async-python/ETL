FROM python:3.9.7-buster
WORKDIR /etl
COPY requirements.txt .
RUN pip install -r requirements.txt --no-cache-dir
COPY ./etl .