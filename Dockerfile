FROM elasticsearch:7.4.2

ENV discovery.type=single-node

USER root


FROM python:3.7

COPY requirements.txt requirements.txt
COPY . .

RUN pip3 install -r requirements.txt

EXPOSE 5000

ENTRYPOINT python3 app.py