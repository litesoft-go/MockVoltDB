#!/usr/bin/env python

# This file is part of VoltDB.
# Copyright (C) 2008-2020 VoltDB Inc.

# VoltDB pristine container
# This container is intended for k8s stateful set deployments

FROM ubuntu:18.04
ARG VOLTDB_DIST_NAME

# Set up VoltDB environment variables, locale
ENV VOLTDB_DIST=/opt/$VOLTDB_DIST_NAME \
    PATH=$PATH:/opt/$VOLTDB_DIST_NAME/bin \
    LANG=en_US.UTF-8 \
    LANGUAGE=en_US:en \
    LC_ALL=en_US.UTF-8

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

# No longer included:
# apt-get update ...
#   sudo software-properties-common python-software-properties
#   python-pip python-setuptools python-dev build-essential
#   libxml2-utils procps python vim less
#   net-tools iputils-ping telnet iperf
# pip install --upgrade ...
#    pip httplib2
# TZ=America/New_York
# ln -snf /usr/share/zoneinfo/America/$TZ /etc/localtime
# echo $TZ > /etc/timezone

# VoltDB ports
# See https://docs.voltdb.com/AdminGuide/adminserverports.php
#      int  rep  zk   http jmx  admin client
EXPOSE 3021 5555 7181 8080 9090 21211 21212

# Create necessary directories, copy voltdb-kit.
# For voltdb bundles and extensions, copy your assets
# to the corresponding directories in the kit prior to
# building the image.
WORKDIR /opt
RUN mkdir -p $VOLTDB_DIST
COPY ./ $VOLTDB_DIST/
RUN rm -rf $VOLTDB_DIST/doc $VOLTDB_DIST/examples
COPY tools/kubernetes/bin/ $VOLTDB_DIST/bin
