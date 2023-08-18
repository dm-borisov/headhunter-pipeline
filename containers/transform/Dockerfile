FROM python:3.10-slim

WORKDIR ./app
COPY requirements.txt \
     mixins.py \
     processors.py \
     exceptions.py \
     tables.py \
     transform.py \
     .env .
RUN pip install -r requirements.txt

ENTRYPOINT ["python3", "transform.py"]
