FROM python:3.10-slim

WORKDIR ./app
COPY requirements.txt \
     extract_schedule_experience.py \
     extract_areas_industries.py \
     tables.py \
     run.sh \
     .
RUN pip install --no-cache-dir -r requirements.txt
RUN chmod a+x run.sh

ENTRYPOINT ["./run.sh"]
