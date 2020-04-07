FROM ubuntu:18.04

# Assume creation of ~/bin
ENV PATH=$PATH:$HOME/bin

# Configure required software packages.
# dnsutils needed by voltdbk8s.py
# python-httplib2, curl, and jq needed by k8sreadycheck.py.
RUN apt-get clean && \
    apt-get update && \
    apt-get -yq --no-install-recommends --no-install-suggests install \
        openjdk-8-jre python dnsutils python-httplib2 curl jq \
        locales && \
    apt-get autoremove -yq && \
    rm -rf /var/lib/apt/lists/* && \
    locale-gen en_US.UTF-8

# VoltDB ports
# See https://docs.voltdb.com/AdminGuide/adminserverports.php
#      int  rep  zk   http jmx  admin client
EXPOSE 3021 5555 7181 8080 9090 21211 21212

# create and populate ~/bin with appropriate executable.
COPY ./instance/deployment/ ./
COPY ../deployable/linux_amd64/MockVoltDB ./bin/voltdb
