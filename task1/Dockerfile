FROM python:3.11-bullseye

WORKDIR /task1

COPY ./app/ ./app
COPY ./gcs/ ./gcs
COPY ./requirements.txt ./

RUN pip install --no-cache-dir --upgrade pip \
  && pip install --no-cache-dir -r requirements.txt


ENTRYPOINT ["python","./app/main.py"]