FROM python:3.10-slim

WORKDIR ./app
COPY requirements.txt .
COPY extract.py .
RUN pip install -r requirements.txt

ENTRYPOINT ["python3", "extract.py"]
