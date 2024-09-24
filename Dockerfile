FROM python:3.12.6-alpine

RUN mkdir /solarlogging
COPY * /solarlogging/
RUN pip3 install -r /solarlogging/requirements.txt

WORKDIR /solarlogging
ENTRYPOINT [ "/usr/local/bin/python3 solarweb.py --database=/solarlogging/solarlogging.db" ]
