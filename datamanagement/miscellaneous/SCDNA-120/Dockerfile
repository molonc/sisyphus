FROM python:2

ADD requirements.txt /
ADD hdf5_to_msgpack.py /

ENV TABLE=default
ENV STORAGE=default

RUN pip install -r /requirements.txt --ignore-installed

CMD python hdf5_to_msgpack.py $TABLE $STORAGE
