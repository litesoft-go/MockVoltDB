FROM python:2.7.18-alpine3.11
#FROM python:2.7.9-slim

RUN apk --no-cache add ca-certificates curl

WORKDIR /root/

# VoltDB ports
# See https://docs.voltdb.com/AdminGuide/adminserverports.php
#      int  rep  zk   http jmx  admin client status
EXPOSE 3021 5555 7181 8080 9090 21211 21212  11780

# create and populate ~/bin with appropriate executable.
COPY ./instance/deployment/ ./

ENV PATH="./bin:$PATH"
