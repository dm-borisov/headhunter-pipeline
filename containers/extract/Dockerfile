FROM python:3.10-slim

WORKDIR ./app
COPY requirements.txt hh_requests.py extract.py .
RUN pip install --no-cache-dir -r requirements.txt

ENTRYPOINT ["python3", "extract.py"]
