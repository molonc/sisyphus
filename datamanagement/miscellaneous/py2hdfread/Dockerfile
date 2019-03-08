FROM python:2

ADD requirements.txt /
ADD hdf5_to_msgpack.py /

RUN pip install -r /requirements.txt --ignore-installed

ENTRYPOINT ["python", "hdf5_to_msgpack.py"]
