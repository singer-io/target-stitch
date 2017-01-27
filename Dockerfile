FROM alpine:3.4

RUN mkdir /code
RUN mkdir /code/persist-stitch
WORKDIR /code/persist-stitch
ADD . /code/persist-stitch

RUN apk update
RUN apk upgrade
RUN apk add curl
RUN apk add python3
RUN pip3 install --upgrade pip setuptools && \
    rm -r /root/.cache
    
RUN python3 setup.py install
