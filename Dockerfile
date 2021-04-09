FROM elasticsearch:7.4.2

ENV discovery.type=single-node

USER root

COPY . .

RUN yum install -y https://repo.ius.io/ius-release-el7.rpm && yum -y update && yum install -y python36u python36u-libs python36u-devel python36u-pip && pip3 install -r requirements.txt

RUN echo "path.data: /index/elasticsearch/" >> /usr/share/elasticsearch/config/elasticsearch.yml

RUN sed -i -e '2inohup python3 app.py &\' /usr/local/bin/docker-entrypoint.sh