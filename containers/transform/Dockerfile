FROM python:3.10-slim

WORKDIR ./app
COPY requirements.txt .
COPY data_fields.py .
COPY models.py .
COPY transform.py .
RUN pip install -r requirements.txt

ENTRYPOINT ["python3", "transform.py"]
