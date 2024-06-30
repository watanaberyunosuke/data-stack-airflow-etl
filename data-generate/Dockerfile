FROM python:3.10-slim

ARG POSTGRES_USER
ARG POSTGRES_PASSWORD

ENV POSTGRES_USER=$POSTGRES_USER
ENV POSTGRES_PASSWORD=$POSTGRES_PASSWORD

WORKDIR /home/data-generate

RUN mkdir csv

RUN mkdir xml

COPY data-generate .

RUN pip install -r requirements.txt

CMD ["python", "initialise.py"]