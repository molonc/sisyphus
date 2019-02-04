# Sisyphus
# Version 1.0

FROM python:2.7.15

ENV PYTHONUNBUFFERED 1

ENV TANTALUS_API_USERNAME simong
ENV TANTALUS_API_PASSWORD pinchpinch

ENV COLOSSUS_API_USERNAME simong
ENV COLOSSUS_API_PASSWORD pinchpinch

ENV TANTALUS_API_URL http://127.0.0.1:8000/api/
ENV COLOSSUS_API_URL http://127.0.0.1:8001/api/

RUN mkdir /sisyphus

WORKDIR /sisyphus

ADD . /sisyphus/

RUN pip install --upgrade pip && pip install azure-storage azure-batch futures azure-mgmt coreapi openapi_codec pandas pysam pyyaml && pip install -r requirements.txt --ignore-installed

