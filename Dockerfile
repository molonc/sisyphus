# Sisyphus
# Version 1.0

FROM python:2.7.15

ENV PYTHONUNBUFFERED 1

ENV TANTALUS_API_USERNAME simong
ENV TANTALUS_API_PASSWORD pinchpinch

ENV COLOSSUS_API_USERNAME simong
ENV COLOSSUS_API_PASSWORD pinchpinch

ENV CLIENT_ID ebdd57b4-71cb-4e43-a21a-94010e11aeaa
ENV TENANT_ID 31126879-74b8-42b9-8ae6-68b3a277ebdc
ENV SECRET_KEY 1vVGoUndwFpZn7gfNzD+AoviZuwZWgfDfVVL7kvlJbs=
ENV AZURE_KEYVAULT_ACCOUNT production-storage-keys

ENV TANTALUS_API_URL http://127.0.0.1:8000/api/
ENV COLOSSUS_API_URL http://127.0.0.1:8001/api/

RUN mkdir /sisyphus

WORKDIR /sisyphus

ADD . /sisyphus/

RUN pip install --upgrade pip && pip install azure-storage azure-batch futures azure-mgmt coreapi openapi_codec pandas pysam pyyaml && pip install -r requirements.txt --ignore-installed

