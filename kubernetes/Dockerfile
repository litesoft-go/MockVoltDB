FROM python:2.7.18-alpine3.11
#FROM python:2.7.9-slim

RUN apk add --no-cache \
        bind-tools \
        ca-certificates \
        curl \
        jq \
        python && \
        python -m ensurepip && \
        rm -r /usr/lib/python*/ensurepip && \
        pip install --upgrade --no-cache-dir pip setuptools && \
        pip install --no-cache-dir flask httplib2 && \
        rm -rf /var/cache/apk/*

WORKDIR /root/

# VoltDB ports
# See https://docs.voltdb.com/AdminGuide/adminserverports.php
#       boot int  http status
EXPOSE 11235 3021 8080 11780

# create and populate ~/bin with appropriate executable.
COPY ./instance/deployment/ ./

ENV PATH="/root/bin:$PATH"

# defaults to '/voltdbroot'
ENV VOLTDB_K8S_ADAPTER_PVVOLTDBROOT=/voltdbroot

# defaults to '/etc/voltdb'
ENV VOLTDB_INIT_VOLUME=/voltdbroot/init

# turn off peer searching
ENV VOLTDB_CONNECTION_HOST=Mock
